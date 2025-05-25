"""
Batch processing module for video pipeline.
"""

import os
import logging
import json
import time
import shutil
from typing import Dict, Any, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

from scrapers.base_scraper import BaseScraper
from validators.validation_pipeline import ValidationPipeline
from storage.cloud_storage import CloudStorageUploader
from processors.video_processor import trim_video

logger = logging.getLogger(__name__)

class BatchProcessor:
    """Module for batch processing of videos through the pipeline."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the batch processor with configuration.
        
        Args:
            config: Dictionary containing batch processor configuration
        """
        self.config = config
        self.logger = logging.getLogger("processor.batch")
        
        # Configure directories
        self.download_dir = config.get("download_dir", "downloads")
        self.processed_dir = config.get("processed_dir", "processed")
        self.failed_dir = config.get("failed_dir", "failed")
        
        # Ensure directories exist
        os.makedirs(self.download_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        os.makedirs(self.failed_dir, exist_ok=True)
        
        # Configure batch settings
        self.batch_size = config.get("batch_size", 10)
        self.max_workers = config.get("max_workers", 4)
        self.disk_space_threshold = config.get("disk_space_threshold", 1024 * 1024 * 1024)  # 1 GB
        
        # Configure state tracking
        self.state_file = config.get("state_file", "batch_state.json")
        self.state = self._load_state()
        
        # Initialize components
        self.validation_pipeline = None
        self.cloud_uploader = None
        self.scrapers = {}
    
    def _safe_cleanup(self, path: str):
        """Safely delete a file and its parent directory if empty."""
        try:
            if os.path.exists(path):
                os.remove(path)
            parent_dir = os.path.dirname(path)
            # Add check to prevent deleting the main download directory
            if os.path.isdir(parent_dir) and not os.listdir(parent_dir) and parent_dir != self.download_dir:
                os.rmdir(parent_dir)
        except Exception as e:
            self.logger.warning(f"Failed to clean up {path}: {e}")

    def set_validation_pipeline(self, validation_pipeline: ValidationPipeline):
        """Set the validation pipeline."""
        self.validation_pipeline = validation_pipeline
    
    def set_cloud_uploader(self, cloud_uploader: CloudStorageUploader):
        """Set the cloud storage uploader."""
        self.cloud_uploader = cloud_uploader
    
    def register_scraper(self, name: str, scraper: BaseScraper):
        """Register a scraper for a specific source."""
        self.scrapers[name] = scraper
    
    def process_batch(
        self,
        source: str,
        query: str = "nature",
        batch_size: int = 10,
        target_hours: float = 1.0,
        output_destination: str = "local",
        batch_id: Optional[str] = None,
        max_workers: Optional[int] = None,
        max_scrapers: Optional[int] = None,
        config_override: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Process a batch of videos from a specific source.
        
        Args:
            source: Source name (must match a registered scraper).
            query: Search query string for videos (default: "nature").
            batch_size: Number of videos to process per batch (default: 10).
            target_hours: Target number of hours of video to collect (default: 1.0).
            output_destination: Where to store output, either 'local' or 'cloud' (default: "local").
            batch_id: Optional batch identifier to resume or identify the batch.
            max_workers: Maximum number of parallel worker threads to use (default: from config or None).
            max_scrapers: Maximum number of parallel scrapers to use (default: None).
            config_override: Optional dictionary to override configuration for this batch run.
        
        Returns:
            Dictionary with batch processing results.
        """
        if source not in self.scrapers:
            return {"success": False, "error": f"No scraper registered for source: {source}"}
        
        if not self.validation_pipeline:
            return {"success": False, "error": "Validation pipeline not set"}
        
        if not self.cloud_uploader:
            return {"success": False, "error": "Cloud uploader not set"}
        
        try:
            self.logger.info(f"Starting batch processing for source: {source}, query: {query}")
            
            # Initialize batch state
            batch_id = f"{source}_{int(time.time())}"
            batch_state = {
                "id": batch_id,
                "source": source,
                "query": query,
                "max_videos": max_videos,
                "start_time": time.time(),
                "end_time": None,
                "videos_found": 0,
                "videos_downloaded": 0,
                "videos_validated": 0,
                "videos_uploaded": 0,
                "videos_failed": 0,
                "status": "running"
            }
            
            # Update and save state
            self.state["batches"][batch_id] = batch_state
            self._save_state()
            
            # Get scraper for this source
            scraper = self.scrapers[source]
            
            # Search for videos
            page = 1
            videos = []
            videos_found = 0
            while True:
                # Stop if we've already collected enough
                if max_videos and videos_found >= max_videos:
                    break

                self.logger.info(f"Searching {source} for '{query}', page {page}")
                page_results = scraper.search_videos(query, page)
                if not page_results:
                    self.logger.info(f"No more results from {source} for '{query}'")
                    break

                # Only take as many as needed
                if max_videos:
                    remaining = max_videos - videos_found
                    to_add = page_results[:remaining]
                else:
                    to_add = page_results

                videos.extend(to_add)
                videos_found += len(to_add)

                # Update state after adding new videos
                batch_state["videos_found"] = videos_found
                self._save_state()

                # Stop if we've reached the maximum
                if max_videos and videos_found >= max_videos:
                    break

                page += 1

            self.logger.info(f"Found {videos_found} videos from {source} for '{query}'")
            # Ensure videos_found reflects the truncated list length
            batch_state["videos_found"] = videos_found
            self._save_state()
            
            # Process videos in batches
            for start in range(0, len(videos), self.batch_size):
                self._ensure_disk_space()
                current_batch = videos[start:start + self.batch_size]

                # Parallelize downloads and schedule chunk processing
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    download_futures = {}
                    for video in current_batch:
                        video_id = video.get("id", "unknown")
                        source_name = video.get("source", "unknown")
                        extension = video.get("format", "mp4").lower()
                        if not extension.startswith("."):
                            extension = f".{extension}"
                        download_filename = f"{source_name}_{video_id}{extension}"
                        download_path = os.path.join(self.download_dir, download_filename)
                        self.logger.info(f"Downloading video {video_id} from {source_name}")
                        download_futures[executor.submit(self.scrapers[source_name].download_video, video.get("url", ""), download_path)] = (video, download_path)

                    # Once downloads finish, trim and process chunks
                    for future in as_completed(download_futures):
                        video, download_path = download_futures[future]
                        if not future.result():
                            batch_state["videos_failed"] += 1
                            self._save_state()
                            continue

                        # Trim video into chunks
                        source_name = video.get("source", "unknown")
                        video_id = video.get("id", "unknown")
                        chunk_dir = os.path.join(self.download_dir, f"{source_name}_{video_id}_chunks")
                        chunk_paths = trim_video(download_path, chunk_dir)

                        for idx, chunk_path in enumerate(chunk_paths):
                            chunk_meta = video.copy()
                            chunk_meta["id"] = f"{source_name}_{video_id}_chunk_{idx}"
                            chunk_meta["url"] = chunk_path
                            chunk_meta["is_local"] = True
                            executor.submit(self._process_video, chunk_meta)
            
            # Update final batch state
            batch_state["end_time"] = time.time()
            batch_state["status"] = "completed"
            self._save_state()
            
            self.logger.info(f"Batch processing completed for {source}, query: {query}")

            if True:
                try:
                    for root_dir in [self.download_dir, self.processed_dir, self.failed_dir]:
                        for root, _, files in os.walk(root_dir):
                            for file in files:
                                full_path = os.path.join(root, file)
                                self._safe_cleanup(full_path)
                except Exception as e:
                    self.logger.warning(f"Error during final batch cleanup: {e}")

                # Remove empty directories recursively
                try:
                    for root_dir in [self.download_dir, self.processed_dir, self.failed_dir]:
                        for root, dirs, files in os.walk(root_dir, topdown=False):
                            if not dirs and not files:
                                os.rmdir(root)
                except Exception as e:
                    self.logger.warning(f"Error removing empty directories: {e}")

            return {
                "success": True,
                "batch_id": batch_id,
                "videos_found": batch_state["videos_found"],
                "videos_downloaded": batch_state["videos_downloaded"],
                "videos_validated": batch_state["videos_validated"],
                "videos_uploaded": batch_state["videos_uploaded"],
                "videos_failed": batch_state["videos_failed"],
                "duration": batch_state["end_time"] - batch_state["start_time"]
            }
            
        except Exception as e:
            self.logger.error(f"Error in batch processing: {str(e)}")
            
            # Update batch state on error
            if batch_id in self.state["batches"]:
                self.state["batches"][batch_id]["status"] = "failed"
                self.state["batches"][batch_id]["error"] = str(e)
                self.state["batches"][batch_id]["end_time"] = time.time()
                self._save_state()
            
            return {"success": False, "error": str(e)}
    
    def _process_video(self, video_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single video through the pipeline.
        
        Args:
            video_metadata: Video metadata from scraper
            
        Returns:
            Dictionary with processing results
        """
        result = {
            "id": video_metadata.get("id", "unknown"),
            "source": video_metadata.get("source", "unknown"),
            "downloaded": False,
            "validated": False,
            "uploaded": False,
            "failed": False,
            "error": None
        }
        
        try:
            # Get scraper for this source
            source = video_metadata.get("source")
            if source not in self.scrapers:
                result["error"] = f"No scraper registered for source: {source}"
                result["failed"] = True
                download_path = None
                # Try to build download_path for cleanup, if possible
                extension = video_metadata.get("format", "mp4").lower()
                if not extension.startswith("."):
                    extension = f".{extension}"
                video_id = video_metadata.get("id", "unknown")
                if source:
                    download_filename = f"{source}_{video_id}{extension}"
                    download_path = os.path.join(self.download_dir, download_filename)
                # Cleanup if this was a chunk file
                if download_path and "chunks" in download_path:
                    # Only cleanup if the file actually exists before trying
                    if os.path.exists(download_path):
                        self._safe_cleanup(download_path)
                return result
            
            scraper = self.scrapers[source]
            
            # Generate download path
            video_id = video_metadata.get("id", "unknown")
            video_url = video_metadata.get("url", "")
            
            if not video_url:
                result["error"] = "No video URL in metadata"
                result["failed"] = True
                extension = video_metadata.get("format", "mp4").lower()
                if not extension.startswith("."):
                    extension = f".{extension}"
                download_filename = f"{source}_{video_id}{extension}"
                download_path = os.path.join(self.download_dir, download_filename)
                # Cleanup if this was a chunk file
                if "chunks" in download_path:
                    # Only cleanup if the file actually exists before trying
                    if os.path.exists(download_path):
                        self._safe_cleanup(download_path)
                return result
            
            # Determine file extension from URL or metadata
            extension = video_metadata.get("format", "mp4").lower()
            if not extension.startswith("."):
                extension = f".{extension}"
            
            # Create download filename
            download_filename = f"{source}_{video_id}{extension}"
            
            # Download the video
            self.logger.info(f"Downloading video {video_id} from {source}")
            if video_metadata.get("is_local"):
                download_path = video_url
                download_success = True
            else:
                download_path = os.path.join(self.download_dir, download_filename)
                download_success = scraper.download_video(video_url, download_path)
            
            if not download_success:
                result["error"] = "Failed to download video"
                result["failed"] = True
                # Cleanup if download failed and file exists
                if os.path.exists(download_path):
                    self._safe_cleanup(download_path)
                return result
            
            result["downloaded"] = True
            
            # Validate the video
            self.logger.info(f"Validating video {video_id} from {source}")
            is_valid, validation_results = self.validation_pipeline.validate_video(download_path, video_metadata)
            
            if not is_valid:
                # Move to failed directory
                failed_path = os.path.join(self.failed_dir, download_filename)
                shutil.move(download_path, failed_path)

                result["error"] = "Failed validation"
                result["failed"] = True
                # No cleanup needed here, file is moved to failed_dir
                return result
            
            result["validated"] = True
            
            # Upload to cloud storage
            self.logger.info(f"Uploading video {video_id} from {source} to cloud storage")
            from pathlib import Path
            parent_name = Path(download_path).parent.name
            upload_result = self.cloud_uploader.upload_video(
                download_path,
                {**video_metadata, "cloud_path_hint": f"{parent_name}_{os.path.basename(download_path)}"}
            )
            
            if not upload_result.get("success", False):
                result["error"] = f"Failed to upload: {upload_result.get('error', 'Unknown error')}"
                result["failed"] = True
                # Cleanup original downloaded file only if upload failed and file exists
                if os.path.exists(download_path):
                    self._safe_cleanup(download_path)
                return result
            
            result["uploaded"] = True
            
            # Move to processed directory
            processed_path = os.path.join(self.processed_dir, download_filename)
            shutil.move(download_path, processed_path)

            # If this was a chunk file, delete the file in processed_dir after moving
            if "chunks" in download_path:
                self._safe_cleanup(processed_path)

            # Add cloud URL to result
            result["cloud_url"] = upload_result.get("url", "")

            return result
            
        except Exception as e:
            self.logger.error(f"Error processing video {video_metadata.get('id', 'unknown')}: {str(e)}")
            result["error"] = str(e)
            result["failed"] = True
            # Cleanup downloaded file if an exception occurred and the file exists
            try:
                source = video_metadata.get("source")
                video_id = video_metadata.get("id", "unknown")
                extension = video_metadata.get("format", "mp4").lower()
                if not extension.startswith("."):
                    extension = f".{extension}"
                if source:
                    download_filename = f"{source}_{video_id}{extension}"
                    download_path = os.path.join(self.download_dir, download_filename)
                    if os.path.exists(download_path):
                         self._safe_cleanup(download_path)
            except Exception:
                pass # Ignore errors during cleanup attempt
            return result
    
    def _ensure_disk_space(self):
        """
        Ensure there is enough disk space by cleaning up old processed videos.
        """
        try:
            # Check available disk space
            disk_usage = shutil.disk_usage(self.download_dir)
            available_space = disk_usage.free
            
            if available_space > self.disk_space_threshold:
                # Enough space available
                return
            
            self.logger.info(f"Low disk space: {available_space} bytes. Cleaning up old processed videos.")
            
            # Get list of processed videos sorted by modification time (oldest first)
            processed_files = []
            for filename in os.listdir(self.processed_dir):
                file_path = os.path.join(self.processed_dir, filename)
                if os.path.isfile(file_path):
                    processed_files.append((file_path, os.path.getmtime(file_path)))
            
            processed_files.sort(key=lambda x: x[1])
            
            # Delete oldest files until we have enough space
            for file_path, _ in processed_files:
                if shutil.disk_usage(self.download_dir).free > self.disk_space_threshold:
                    break
                
                self.logger.info(f"Removing old processed file: {file_path}")
                os.remove(file_path)
            
        except Exception as e:
            self.logger.warning(f"Error ensuring disk space: {str(e)}")
    
    def _load_state(self) -> Dict[str, Any]:
        """
        Load batch processing state from file.
        
        Returns:
            Dictionary of batch processing state
        """
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, "r") as f:
                    return json.load(f)
            
            # Initialize new state
            return {
                "batches": {},
                "last_updated": time.time()
            }
            
        except Exception as e:
            self.logger.warning(f"Error loading batch state: {str(e)}")
            
            # Initialize new state
            return {
                "batches": {},
                "last_updated": time.time()
            }
    
    def _save_state(self):
        """Save batch processing state to file."""
        try:
            # Update last updated timestamp
            self.state["last_updated"] = time.time()
            
            with open(self.state_file, "w") as f:
                json.dump(self.state, f, indent=2)
                
        except Exception as e:
            self.logger.warning(f"Error saving batch state: {str(e)}")
    
    def get_batch_status(self, batch_id: str) -> Dict[str, Any]:
        """
        Get status of a specific batch.
        
        Args:
            batch_id: Batch ID
            
        Returns:
            Dictionary with batch status
        """
        if batch_id in self.state["batches"]:
            return self.state["batches"][batch_id]
        return {"error": f"Batch not found: {batch_id}"}
    
    def list_batches(self) -> List[Dict[str, Any]]:
        """
        List all batches.
        
        Returns:
            List of batch status dictionaries
        """
        return list(self.state["batches"].values())
    
    def resume_batch(self, batch_id: str) -> Dict[str, Any]:
        """
        Resume a failed or interrupted batch.
        
        Args:
            batch_id: Batch ID
            
        Returns:
            Dictionary with resume result
        """
        if batch_id not in self.state["batches"]:
            return {"success": False, "error": f"Batch not found: {batch_id}"}
        
        batch = self.state["batches"][batch_id]
        
        if batch["status"] not in ["failed", "interrupted"]:
            return {"success": False, "error": f"Batch cannot be resumed, status: {batch['status']}"}
        
        # Resume batch processing
        return self.process_batch(batch["source"], batch["query"], batch["max_videos"])
    
    def cleanup(self):
        """Clean up temporary files and resources."""
        try:
            # Save final state
            self._save_state()
            
        except Exception as e:
            self.logger.error(f"Error in cleanup: {str(e)}")
