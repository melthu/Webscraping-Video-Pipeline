"""
Enhanced batch processing module for video pipeline with improved parallelization and resource management.
"""

import os
import logging
import json
import time
import shutil
import threading
import queue
import psutil
from typing import Dict, Any, List, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

from scrapers.base_scraper import BaseScraper
from scrapers.parallel_scraper_manager import ParallelScraperManager
from validators.validation_pipeline import ValidationPipeline
from storage.cloud_storage import CloudStorageUploader
from processors.video_processor import trim_video

logger = logging.getLogger(__name__)

class EnhancedBatchProcessor:
    """Enhanced module for batch processing of videos through the pipeline with improved controls."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the enhanced batch processor with configuration.
        
        Args:
            config: Dictionary containing batch processor configuration
        """
        self.config = config
        self.logger = logging.getLogger("processor.enhanced_batch")
        
        # Configure directories
        self.download_dir = config.get("download_dir", "downloads")
        self.processed_dir = config.get("processed_dir", "processed")
        self.failed_dir = config.get("failed_dir", "failed")
        self.temp_dir = config.get("temp_dir", "temp")
        
        # Ensure directories exist
        os.makedirs(self.download_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        os.makedirs(self.failed_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Configure batch settings
        self.batch_size = config.get("batch_size", 10)
        self.max_workers = config.get("max_workers", 4)
        self.max_scrapers = config.get("max_scrapers", 3)
        self.target_hours = config.get("target_hours", 1000)
        self.disk_space_threshold = config.get("disk_space_threshold", 1024 * 1024 * 1024)  # 1 GB
        self.memory_threshold = config.get("memory_threshold", 80)  # percent
        self.cpu_threshold = config.get("cpu_threshold", 80)  # percent
        
        # Configure output destination
        self.output_destination = config.get("output_destination", "local")  # "local" or "cloud"
        self.cloud_bucket = config.get("cloud_bucket", "")
        
        # Configure state tracking
        self.state_file = config.get("state_file", "logs/batch_state.json")
        self.state = self._load_state()
        
        # Initialize components
        self.validation_pipeline = None
        self.cloud_uploader = None
        self.scrapers = {}
        
        # Initialize parallel scraper manager
        self.parallel_scraper_manager = ParallelScraperManager({
            "max_workers": self.max_workers,
            "max_scrapers": self.max_scrapers,
            "max_videos_per_scraper": self.batch_size * 2,  # Buffer for filtering
            "target_hours": self.target_hours,
            "memory_threshold": self.memory_threshold,
            "cpu_threshold": self.cpu_threshold
        })
        
        # Tracking variables
        self.total_video_seconds = 0
        self.total_videos_processed = 0
        self.total_videos_validated = 0
        self.total_videos_uploaded = 0
        self.total_videos_failed = 0
        
        # Shutdown flag
        self.shutdown_flag = threading.Event()
        
        # Video processing queue
        self.processing_queue = queue.Queue()
    
    def _load_state(self) -> Dict[str, Any]:
        """
        Load batch processing state from file.
        
        Returns:
            Dictionary with batch processing state
        """
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, "r") as f:
                    return json.load(f)
            else:
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        except Exception as e:
            self.logger.error(f"Error loading state: {str(e)}")
        
        # Return default state
        return {
            "batches": {},
            "last_batch_id": None,
            "total_videos_processed": 0,
            "total_videos_validated": 0,
            "total_videos_uploaded": 0,
            "total_videos_failed": 0,
            "total_video_seconds": 0
        }
    
    def _save_state(self):
        """Save batch processing state to file."""
        try:
            # Update global counters
            self.state["total_videos_processed"] = self.total_videos_processed
            self.state["total_videos_validated"] = self.total_videos_validated
            self.state["total_videos_uploaded"] = self.total_videos_uploaded
            self.state["total_videos_failed"] = self.total_videos_failed
            self.state["total_video_seconds"] = self.total_video_seconds
            
            with open(self.state_file, "w") as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving state: {str(e)}")
    
    def _ensure_disk_space(self) -> bool:
        """
        Ensure there is enough disk space available.
        
        Returns:
            True if enough space is available, False otherwise
        """
        try:
            # Ensure the download directory exists before querying disk usage
            if not os.path.exists(self.download_dir):
                self.logger.warning(f"Download directory {self.download_dir} does not exist. Creating it now.")
                os.makedirs(self.download_dir, exist_ok=True)
            # Get free space in download directory
            free_space = shutil.disk_usage(self.download_dir).free
            
            if free_space < self.disk_space_threshold:
                self.logger.warning(f"Low disk space: {free_space} bytes < {self.disk_space_threshold} bytes")
                
                # Clean up temporary files
                self._cleanup_temp_files()
                
                # Check again after cleanup
                free_space = shutil.disk_usage(self.download_dir).free
                if free_space < self.disk_space_threshold:
                    self.logger.error(f"Still low disk space after cleanup: {free_space} bytes")
                    return False
            
            return True
        except Exception as e:
            self.logger.error(f"Error checking disk space: {str(e)}")
            return False
    
    def _cleanup_temp_files(self):
        """Clean up temporary files to free disk space."""
        try:
            self.logger.info("Cleaning up temporary files")
            
            # Clean up temp directory
            for root, dirs, files in os.walk(self.temp_dir):
                for file in files:
                    try:
                        file_path = os.path.join(root, file)
                        os.remove(file_path)
                        self.logger.debug(f"Removed temporary file: {file_path}")
                    except Exception as e:
                        self.logger.warning(f"Error removing file {file}: {str(e)}")
                        
            # Clean up failed directory
            for root, dirs, files in os.walk(self.failed_dir):
                for file in files:
                    try:
                        file_path = os.path.join(root, file)
                        os.remove(file_path)
                        self.logger.debug(f"Removed temporary file: {file_path}")
                    except Exception as e:
                        self.logger.warning(f"Error removing file {file}: {str(e)}")
            
            # Remove empty directories but skip top-level download/temp directories
            for directory in [self.download_dir, self.temp_dir]:
                for root, dirs, files in os.walk(directory, topdown=False):
                    for dir_name in dirs:
                        try:
                            dir_path = os.path.join(root, dir_name)
                            if not os.listdir(dir_path) and dir_path != self.download_dir and dir_path != self.temp_dir:
                                os.rmdir(dir_path)
                                self.logger.debug(f"Removed empty directory: {dir_path}")
                        except Exception as e:
                            self.logger.warning(f"Error removing directory {dir_name}: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error cleaning up temporary files: {str(e)}")
    
    def _safe_cleanup(self, path: str):
        """Safely delete a file and its parent directory if empty."""
        try:
            if os.path.exists(path):
                os.remove(path)
                self.logger.debug(f"Removed file: {path}")
            
            # Check if parent directory is empty
            parent_dir = os.path.dirname(path)
            # Add check to prevent deleting the main download directory
            if os.path.isdir(parent_dir) and not os.listdir(parent_dir) and parent_dir != self.download_dir:
                os.rmdir(parent_dir)
                self.logger.debug(f"Removed empty directory: {parent_dir}")
        except Exception as e:
            self.logger.warning(f"Failed to clean up {path}: {e}")
    
    def set_validation_pipeline(self, validation_pipeline: ValidationPipeline):
        """Set the validation pipeline."""
        self.validation_pipeline = validation_pipeline
    
    def set_cloud_uploader(self, cloud_uploader: CloudStorageUploader):
        """Set the cloud storage uploader."""
        self.cloud_uploader = cloud_uploader
    
    def register_scraper(self, name: str, scraper: BaseScraper):
        """
        Register a scraper for a specific source.
        
        Args:
            name: Name of the scraper
            scraper: Scraper instance
        """
        self.scrapers[name] = scraper
        self.parallel_scraper_manager.register_scraper(name, scraper)
    
    def process_batch(
        self,
        sources: Dict[str, str],
        query: Optional[str] = None,
        batch_size: Optional[int] = None,
        target_hours: Optional[float] = None,
        output_destination: Optional[str] = None,
        config_override: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Process a batch of videos from multiple sources with enhanced controls.
        
        Args:
            sources: Dictionary mapping source names to search queries.
            query: Optional query string to apply globally (currently unused, reserved for future use).
            batch_size: Size of each processing batch (overrides config).
            target_hours: Target hours of video to collect (overrides config).
            output_destination: Output destination ("local" or "cloud", overrides config).
            config_override: Optional dictionary to override specific configuration settings (e.g., {"disk_space_threshold": ...}).
            
        Returns:
            Dictionary with batch processing results.
        """
        # Apply config_override if provided
        if config_override and "disk_space_threshold" in config_override:
            self.disk_space_threshold = config_override["disk_space_threshold"]
        # Apply overrides
        if batch_size is not None:
            self.batch_size = batch_size
        
        if target_hours is not None:
            self.target_hours = target_hours
        
        if output_destination is not None:
            self.output_destination = output_destination
        
        # Validate components
        if not self.validation_pipeline:
            return {"success": False, "error": "Validation pipeline not set"}
        
        if self.output_destination == "cloud" and not self.cloud_uploader:
            return {"success": False, "error": "Cloud uploader not set for cloud output destination"}
        
        # Check if any sources are valid
        valid_sources = {name: query for name, query in sources.items() if name in self.scrapers}
        if not valid_sources:
            return {"success": False, "error": "No valid sources provided"}
        
        try:
            self.logger.info(f"Starting enhanced batch processing for {len(valid_sources)} sources")
            self.logger.info(f"Batch size: {self.batch_size}, Target hours: {self.target_hours}")
            self.logger.info(f"Output destination: {self.output_destination}")
            
            # Initialize batch state
            batch_id = f"batch_{int(time.time())}"
            batch_state = {
                "id": batch_id,
                "sources": list(valid_sources.keys()),
                "queries": valid_sources,
                "batch_size": self.batch_size,
                "target_hours": self.target_hours,
                "output_destination": self.output_destination,
                "start_time": time.time(),
                "end_time": None,
                "videos_found": 0,
                "videos_downloaded": 0,
                "videos_validated": 0,
                "videos_uploaded": 0,
                "videos_failed": 0,
                "video_seconds": 0,
                "status": "running"
            }
            
            # Update and save state
            self.state["batches"][batch_id] = batch_state
            self.state["last_batch_id"] = batch_id
            self._save_state()
            
            # Reset tracking variables
            self.total_videos_processed = 0
            self.total_videos_validated = 0
            self.total_videos_uploaded = 0
            self.total_videos_failed = 0
            self.total_video_seconds = 0
            self.shutdown_flag.clear()
            
            # Ensure disk space
            if not self._ensure_disk_space():
                batch_state["status"] = "failed"
                batch_state["error"] = "Insufficient disk space"
                batch_state["end_time"] = time.time()
                self._save_state()
                return {"success": False, "error": "Insufficient disk space"}
            
            # Initialize accumulators for multi-round scraping/processing
            processed_count = 0
            validated_count = 0
            uploaded_count = 0
            failed_count = 0
            total_videos_found = 0
            # Loop until enough validated video duration has been collected
            expected_seconds = self.target_hours * 3600
            while self.total_video_seconds < expected_seconds and not self.shutdown_flag.is_set():
                self.logger.info("Running additional scraping round...")
                videos = self.parallel_scraper_manager.run_scrapers_until_target(valid_sources, self.target_hours)
                # Filter: allow all Coverr & Wikimedia videos; others must have URLs ending in .mp4
                filtered_videos = []
                for v in videos:
                    src = v.get("source", "").lower()
                    url = v.get("url", "").lower()
                    if src in ("coverr", "wikimedia"):
                        # accept all videos from Coverr and Wikimedia
                        filtered_videos.append(v)
                    else:
                        # for other sources, URL must end with .mp4
                        if url.endswith(".mp4"):
                            filtered_videos.append(v)
                videos = filtered_videos
                self.logger.info(f"Filtered to {len(videos)} videos (Coverr & Wikimedia + others ending in .mp4)")
                total_videos_found += len(videos)
                if len(videos) == 0:
                    self.logger.warning("No more videos available from scrapers.")
                    break
                # Process in batches to control memory usage
                for start_idx in range(0, len(videos), self.batch_size):
                    # Check if shutdown requested
                    if self.shutdown_flag.is_set():
                        self.logger.info("Shutdown requested, stopping batch processing")
                        break
                    # Ensure disk space before processing batch
                    if not self._ensure_disk_space():
                        self.logger.error("Insufficient disk space, pausing batch processing")
                        time.sleep(10)  # Wait before retrying
                        if not self._ensure_disk_space():
                            self.logger.error("Still insufficient disk space, stopping batch processing")
                            break
                    # Get current batch
                    end_idx = min(start_idx + self.batch_size, len(videos))
                    current_batch = videos[start_idx:end_idx]
                    self.logger.info(f"Processing batch {start_idx//self.batch_size + 1}/{(len(videos)-1)//self.batch_size + 1} ({len(current_batch)} videos)")

                    # Calculate the potential total duration if we process the current batch
                    current_batch_duration = sum(v.get('duration', 0) for v in current_batch)
                    potential_total_seconds = self.total_video_seconds + current_batch_duration

                    # Check if processing this batch would significantly exceed the target hours
                    # We allow a small buffer (e.g., the duration of one batch) to avoid stopping prematurely
                    expected_seconds = self.target_hours * 3600
                    allowed_overshoot_seconds = self.batch_size * 60 # Assuming average video is 1 minute, adjust if needed

                    if potential_total_seconds > expected_seconds + allowed_overshoot_seconds:
                        self.logger.warning(f"Processing this batch would exceed target hours. Trimming batch.")
                        # Determine how many videos to take from the current batch to get closer to the target
                        cumulative_seconds_in_batch = 0
                        trimmed_batch = []
                        for video in current_batch:
                            video_duration = video.get('duration', 0)
                            if self.total_video_seconds + cumulative_seconds_in_batch + video_duration <= expected_seconds + allowed_overshoot_seconds:
                                trimmed_batch.append(video)
                                cumulative_seconds_in_batch += video_duration
                            else:
                                break
                        current_batch = trimmed_batch
                        if not current_batch:
                            self.logger.info("Trimmed batch is empty, stopping processing.")
                            break # Stop processing further batches from this scraping round

                    # Process batch with thread pool
                    batch_results = self._process_video_batch(current_batch)
                    # Update counters
                    processed_count += batch_results["processed"]
                    validated_count += batch_results["validated"]
                    uploaded_count += batch_results["uploaded"]
                    failed_count += batch_results["failed"]
                    # Update batch state
                    batch_state["videos_downloaded"] = processed_count
                    batch_state["videos_validated"] = validated_count
                    batch_state["videos_uploaded"] = uploaded_count
                    batch_state["videos_failed"] = failed_count
                    batch_state["video_seconds"] = self.total_video_seconds
                    self._save_state()
                    # Clean up after batch
                    self._cleanup_temp_files()
                # End of batch round
            batch_state["videos_found"] = total_videos_found
            # Update final batch state
            batch_state["end_time"] = time.time()
            batch_state["status"] = "completed"
            batch_state["video_seconds"] = self.total_video_seconds
            self._save_state()
            self.logger.info(f"Batch processing completed: {validated_count} videos validated, {self.total_video_seconds/3600:.2f} hours")
            # Final cleanup
            self._cleanup_temp_files()
            return {
                "success": True,
                "batch_id": batch_id,
                "videos_found": batch_state["videos_found"],
                "videos_downloaded": batch_state["videos_downloaded"],
                "videos_validated": batch_state["videos_validated"],
                "videos_uploaded": batch_state["videos_uploaded"],
                "videos_failed": batch_state["videos_failed"],
                "video_seconds": self.total_video_seconds,
                "video_hours": self.total_video_seconds / 3600,
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
    
    def _process_video_batch(self, videos: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process a batch of videos.
        
        Args:
            videos: List of video metadata dictionaries
            
        Returns:
            Dictionary with batch processing results
        """
        results = {
            "processed": 0,
            "validated": 0,
            "uploaded": 0,
            "failed": 0,
            "seconds": 0
        }
        
        # Use thread pool for parallel processing to avoid pickling issues
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit download tasks
            download_futures = {}
            for video in videos:
                future = executor.submit(self._download_video, video)
                download_futures[future] = video
            
            # Process download results and submit validation tasks
            validation_futures = {}
            for future in as_completed(download_futures):
                video = download_futures[future]
                download_result = future.result()
                
                if download_result["success"]:
                    results["processed"] += 1
                    future = executor.submit(self._validate_video, download_result["path"], video)
                    validation_futures[future] = (video, download_result["path"])
                else:
                    results["failed"] += 1
            
            # Process validation results and submit upload tasks
            for future in as_completed(validation_futures):
                video, path = validation_futures[future]
                validation_result = future.result()
                
                if validation_result["success"]:
                    results["validated"] += 1
                    results["seconds"] += video.get("duration", 0)
                    self.total_video_seconds += video.get("duration", 0)
                    
                    if self.output_destination == "cloud":
                        # Upload synchronously to avoid pickle issues with ProcessPoolExecutor
                        upload_result = self._upload_video(path, video)
                        if upload_result["success"]:
                            results["uploaded"] += 1
                        else:
                            results["failed"] += 1
                        self._safe_cleanup(path)
                    else:
                        # For local destination, just move to processed directory
                        try:
                            filename = os.path.basename(path)
                            processed_path = os.path.join(self.processed_dir, filename)
                            shutil.move(path, processed_path)
                            results["uploaded"] += 1
                        except Exception as e:
                            self.logger.error(f"Error moving processed video: {str(e)}")
                            results["failed"] += 1
                else:
                    results["failed"] += 1
                    # Clean up failed video
                    self._safe_cleanup(path)
        
        return results
    
    def _download_video(self, video: Dict[str, Any]) -> Dict[str, Any]:
        """
        Download a video.
        
        Args:
            video: Video metadata dictionary
            
        Returns:
            Dictionary with download result
        """
        result = {
            "success": False,
            "path": "",
            "error": None
        }
        
        try:
            # Get source and video ID
            source = video.get("source", "unknown")
            video_id = video.get("id", "unknown")
            video_url = video.get("url", "")
            
            if not video_url:
                result["error"] = "No video URL in metadata"
                return result
            
            # Get scraper for this source
            if source not in self.scrapers:
                result["error"] = f"No scraper registered for source: {source}"
                return result
            
            scraper = self.scrapers[source]
            
            # Determine file extension
            extension = video.get("format", "mp4").lower()
            if not extension.startswith("."):
                extension = f".{extension}"
            
            # Create download filename and path
            download_filename = f"{source}_{video_id}{extension}"
            download_path = os.path.join(self.download_dir, download_filename)
            
            # Download the video
            self.logger.info(f"Downloading video {video_id} from {source}")
            
            # Check if it's a local file
            if os.path.exists(video_url):
                shutil.copy(video_url, download_path)
                download_success = True
            else:
                # Download from URL
                download_success = scraper.download_video(video_url, download_path)
            
            if not download_success:
                result["error"] = "Failed to download video"
                return result
            
            result["success"] = True
            result["path"] = download_path
            return result
            
        except Exception as e:
            self.logger.error(f"Error downloading video: {str(e)}")
            result["error"] = str(e)
            return result
    
    def _validate_video(self, video_path: str, video_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a video.
        
        Args:
            video_path: Path to the video file
            video_metadata: Video metadata dictionary
            
        Returns:
            Dictionary with validation result
        """
        result = {
            "success": False,
            "error": None
        }
        
        try:
            # Validate the video
            self.logger.info(f"Validating video: {os.path.basename(video_path)}")
            is_valid, validation_results = self.validation_pipeline.validate_video(video_path, video_metadata)
            
            if not is_valid:
                # Move to failed directory
                filename = os.path.basename(video_path)
                failed_path = os.path.join(self.failed_dir, filename)
                shutil.move(video_path, failed_path)
                
                result["error"] = "Failed validation"
                return result
            
            result["success"] = True
            return result
            
        except Exception as e:
            self.logger.error(f"Error validating video: {str(e)}")
            result["error"] = str(e)
            return result
    
    def _upload_video(self, video_path: str, video_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Upload a video to cloud storage.
        
        Args:
            video_path: Path to the video file
            video_metadata: Video metadata dictionary
            
        Returns:
            Dictionary with upload result
        """
        result = {
            "success": False,
            "error": None
        }
        
        try:
            if not self.cloud_uploader:
                result["error"] = "Cloud uploader not set"
                return result
            
            # Upload the video
            self.logger.info(f"Uploading video: {os.path.basename(video_path)}")
            
            # Upload to cloud storage using new API
            upload_result = self.cloud_uploader.upload_video(video_path, video_metadata)
            
            if not upload_result.get("success", False):
                result["error"] = f"Failed to upload: {upload_result.get('error', 'Unknown error')}"
                result["failed"] = True
                # Cleanup original downloaded file only if upload failed and file exists
                if os.path.exists(video_path):
                    self._safe_cleanup(video_path)
                return result
            
            result["uploaded"] = True
            
            # Move to processed directory
            processed_path = os.path.join(self.processed_dir, os.path.basename(video_path))
            shutil.move(video_path, processed_path)

            # If not a chunk file, cleanup original downloaded file after successful processing
            if "chunks" not in video_path and os.path.exists(video_path):
                 self._safe_cleanup(video_path)

            # Add cloud URL to result
            result["cloud_url"] = upload_result.get("url", "")

            # No need for unconditional cleanup here
            # Cleanup if this was a chunk file is handled after moving to processed_dir
            # Additional cleanup if enabled is handled above
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
    
    def resume_batch(self, batch_id: str) -> Dict[str, Any]:
        """
        Resume processing for a specific batch.
        
        Args:
            batch_id: Batch ID to resume
            
        Returns:
            Dictionary with batch processing results
        """
        if batch_id not in self.state["batches"]:
            return {"success": False, "error": f"Batch not found: {batch_id}"}
        
        batch_state = self.state["batches"][batch_id]
        
        if batch_state["status"] == "completed":
            return {"success": False, "error": f"Batch {batch_id} already completed"}
        
        # Resume batch processing with original parameters
        return self.process_batch(
            batch_state["queries"],
            batch_state["batch_size"],
            batch_state["target_hours"],
            batch_state["output_destination"]
        )
    
    def stop_batch(self):
        """Stop the current batch processing."""
        self.logger.info("Stopping batch processing")
        self.shutdown_flag.set()
        self.parallel_scraper_manager.stop_all_scrapers()
    
    def cleanup(self):
        """Clean up resources."""
        self.logger.info("Cleaning up resources")
        self.stop_batch()
        self._cleanup_temp_files()
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of batch processing.
        
        Returns:
            Dictionary with batch processing status
        """
        scraper_status = self.parallel_scraper_manager.get_status()
        
        return {
            "batch_id": self.state.get("last_batch_id"),
            "total_videos_processed": self.total_videos_processed,
            "total_videos_validated": self.total_videos_validated,
            "total_videos_uploaded": self.total_videos_uploaded,
            "total_videos_failed": self.total_videos_failed,
            "total_video_seconds": self.total_video_seconds,
            "total_video_hours": self.total_video_seconds / 3600,
            "scraper_status": scraper_status
        }
