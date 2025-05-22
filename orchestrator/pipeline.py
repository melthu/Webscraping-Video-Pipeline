"""
Main orchestrator for the video collection pipeline.
"""

import os
import time
import logging
import multiprocessing
from typing import Dict, Any, List
from pathlib import Path
import json
from tqdm import tqdm

from scrapers.pexels_scraper import PexelsScraper
from processors.video_processor import VideoProcessor
from config import SOURCES, VIDEO_SPECS, TEMP_DIR, OUTPUT_DIR, PARALLEL_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='pipeline.log'
)
logger = logging.getLogger(__name__)

class Orchestrator:
    """Coordinates the entire video collection pipeline."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the orchestrator with configuration.
        
        Args:
            config: Dictionary containing orchestrator configuration
        """
        self.config = config
        self.sources = {}
        self.processor = VideoProcessor(VIDEO_SPECS)
        self.target_hours = float(os.getenv("TARGET_HOURS", "1000"))  # Total hours to collect
        self.collected_seconds = 0
        self.processed_videos = 0
        self.failed_videos = 0
        self.metadata_db = {}
        
        # Create directories
        os.makedirs(TEMP_DIR, exist_ok=True)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Initialize scrapers
        self._init_scrapers()
        
        # Set up multiprocessing
        self.max_workers = PARALLEL_CONFIG.get("max_workers", 8)
        
        logger.info(f"Orchestrator initialized with {len(self.sources)} sources")
    
    def _init_scrapers(self):
        """Initialize scrapers for each enabled source."""
        if SOURCES.get("pexels", {}).get("enabled", False):
            self.sources["pexels"] = PexelsScraper(SOURCES["pexels"])
        
        # Add other scrapers as they are implemented
        # if SOURCES.get("pixabay", {}).get("enabled", False):
        #     self.sources["pixabay"] = PixabayScraper(SOURCES["pixabay"])
        
        logger.info(f"Initialized {len(self.sources)} scrapers")
    
    def run(self):
        """Run the complete pipeline."""
        logger.info("Starting video collection pipeline")
        
        try:
            # Collect videos from all sources
            for source_name, scraper in self.sources.items():
                logger.info(f"Collecting videos from {source_name}")
                
                # Get search terms for this source
                search_terms = SOURCES[source_name].get("search_terms", [])
                
                for term in search_terms:
                    self._collect_from_source(scraper, term)
                    
                    # Check if we've reached the target
                    if self._check_target_reached():
                        break
                
                if self._check_target_reached():
                    break
            
            logger.info(f"Pipeline completed. Collected {self.collected_seconds/3600:.2f} hours "
                       f"({self.processed_videos} videos, {self.failed_videos} failed)")
            
            # Save final metadata
            self._save_metadata()
            
        except Exception as e:
            logger.error(f"Pipeline error: {str(e)}")
    
    def _collect_from_source(self, scraper, query: str):
        """
        Collect videos from a specific source using the given query.
        
        Args:
            scraper: Initialized scraper instance
            query: Search term to use
        """
        logger.info(f"Searching for '{query}' videos")
        
        # Get max pages from source config
        source_name = scraper.name
        max_pages = SOURCES[source_name].get("max_pages", 10)
        
        # Create a pool of workers
        with multiprocessing.Pool(processes=self.max_workers) as pool:
            # Paginate through results
            for page_num, page_results in enumerate(scraper.paginate(query, max_pages), 1):
                if not page_results:
                    break
                
                logger.info(f"Processing page {page_num} with {len(page_results)} videos")
                
                # Process videos in parallel
                results = pool.map(self._process_video_task, 
                                  [(scraper, video_meta) for video_meta in page_results])
                
                # Update statistics
                for success, duration in results:
                    if success:
                        self.collected_seconds += duration
                        self.processed_videos += 1
                    else:
                        self.failed_videos += 1
                
                # Check if we've reached the target
                if self._check_target_reached():
                    logger.info(f"Target reached: {self.collected_seconds/3600:.2f} hours collected")
                    break
                
                # Avoid rate limiting
                time.sleep(1)
    
    def _process_video_task(self, args):
        """
        Process a single video (for multiprocessing).
        
        Args:
            args: Tuple of (scraper, video_metadata)
            
        Returns:
            Tuple of (success, duration)
        """
        scraper, video_meta = args
        video_id = video_meta.get("id", "unknown")
        source = video_meta.get("source", "unknown")
        
        try:
            # Skip if we already processed this video
            if video_id in self.metadata_db:
                return False, 0
            
            # Create temporary and output paths
            temp_path = os.path.join(TEMP_DIR, f"{source}_{video_id}_temp.mp4")
            output_path = os.path.join(OUTPUT_DIR, f"{source}_{video_id}.mp4")
            
            # Download the video
            if not scraper.download_video(video_meta["url"], temp_path):
                logger.warning(f"Failed to download video {video_id}")
                return False, 0
            
            # Get video info
            video_info = self.processor.get_video_info(temp_path)
            if not video_info:
                logger.warning(f"Failed to get info for video {video_id}")
                os.remove(temp_path)
                return False, 0
            
            # Check if video meets content requirements
            if self.processor.detect_cut_scenes(temp_path):
                logger.info(f"Video {video_id} contains cut scenes, skipping")
                os.remove(temp_path)
                return False, 0
            
            if self.processor.detect_text_overlay(temp_path):
                logger.info(f"Video {video_id} contains text overlay, skipping")
                os.remove(temp_path)
                return False, 0
            
            # Process the video
            if not self.processor.process_video(temp_path, output_path):
                logger.warning(f"Failed to process video {video_id}")
                os.remove(temp_path)
                return False, 0
            
            # Clean up temp file
            os.remove(temp_path)
            
            # Get processed video info
            processed_info = self.processor.get_video_info(output_path)
            duration = processed_info.get("duration", 0)
            
            # Update metadata database
            self.metadata_db[video_id] = {
                **video_meta,
                "processed_path": output_path,
                "processed_info": processed_info
            }
            
            logger.info(f"Successfully processed video {video_id} ({duration:.2f}s)")
            return True, duration
            
        except Exception as e:
            logger.error(f"Error processing video {video_id}: {str(e)}")
            return False, 0
    
    def _check_target_reached(self):
        """Check if we've reached the target hours."""
        hours_collected = self.collected_seconds / 3600
        return hours_collected >= self.target_hours
    
    def _save_metadata(self):
        """Save metadata database to file."""
        metadata_path = os.path.join(OUTPUT_DIR, "metadata.json")
        
        try:
            with open(metadata_path, 'w') as f:
                json.dump({
                    "total_hours": self.collected_seconds / 3600,
                    "total_videos": self.processed_videos,
                    "failed_videos": self.failed_videos,
                    "videos": self.metadata_db
                }, f, indent=2)
            
            logger.info(f"Metadata saved to {metadata_path}")
            
        except Exception as e:
            logger.error(f"Error saving metadata: {str(e)}")
