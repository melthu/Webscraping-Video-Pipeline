"""
Parallel scraper manager for video collection pipeline.

This module provides functionality to run multiple scrapers in parallel
and manage the scraping process efficiently.
"""

import os
import logging
import time
import json
from typing import Dict, Any, List, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
import threading
import queue
import psutil

from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class ParallelScraperManager:
    """Manager for running multiple scrapers in parallel."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the parallel scraper manager with configuration.
        
        Args:
            config: Dictionary containing scraper manager configuration
        """
        self.config = config
        self.logger = logging.getLogger("scraper.parallel_manager")
        
        # Configure parallelization settings
        self.max_workers = config.get("max_workers", 4)
        self.max_scrapers = config.get("max_scrapers", 3)
        self.max_videos_per_scraper = config.get("max_videos_per_scraper", 100)
        self.max_total_videos = config.get("max_total_videos", 1000)
        self.target_hours = config.get("target_hours", 1000)  # Target hours of video to collect
        self.estimated_video_length = config.get("estimated_video_length", 20)  # Estimated length in seconds
        
        # Rate limiting settings
        self.global_rate_limit = config.get("global_rate_limit", 1.0)  # seconds between global requests
        self.last_global_request_time = 0
        self.rate_limit_lock = threading.Lock()
        
        # Resource monitoring
        self.memory_threshold = config.get("memory_threshold", 80)  # percent
        self.cpu_threshold = config.get("cpu_threshold", 80)  # percent
        
        # Registered scrapers
        self.scrapers: Dict[str, BaseScraper] = {}
        
        # Results queue for collecting videos
        self.results_queue = queue.Queue()
        
        # Tracking variables
        self.total_videos_found = 0
        self.total_videos_processed = 0
        self.total_video_seconds = 0
        self.active_scrapers: Set[str] = set()
        self.scraper_status: Dict[str, Dict[str, Any]] = {}
        
        # Shutdown flag
        self.shutdown_flag = threading.Event()
    
    def register_scraper(self, name: str, scraper: BaseScraper):
        """
        Register a scraper for a specific source.
        
        Args:
            name: Name of the scraper
            scraper: Scraper instance
        """
        self.scrapers[name] = scraper
        self.scraper_status[name] = {
            "active": False,
            "videos_found": 0,
            "last_active": 0,
            "current_query": "",
            "current_page": 0,
            "error_count": 0
        }
        self.logger.info(f"Registered scraper: {name}")
    
    def _global_rate_limit(self):
        """Apply global rate limiting to avoid overloading APIs."""
        with self.rate_limit_lock:
            current_time = time.time()
            time_since_last_request = current_time - self.last_global_request_time
            
            if time_since_last_request < self.global_rate_limit:
                sleep_time = self.global_rate_limit - time_since_last_request
                time.sleep(sleep_time)
                
            self.last_global_request_time = time.time()
    
    def _check_resources(self) -> bool:
        """
        Check if system resources are within acceptable limits.
        
        Returns:
            True if resources are available, False otherwise
        """
        try:
            memory_percent = psutil.virtual_memory().percent
            cpu_percent = psutil.cpu_percent(interval=0.1)
            
            if memory_percent > self.memory_threshold:
                self.logger.warning(f"Memory usage too high: {memory_percent}% > {self.memory_threshold}%")
                return False
                
            if cpu_percent > self.cpu_threshold:
                self.logger.warning(f"CPU usage too high: {cpu_percent}% > {self.cpu_threshold}%")
                return False
                
            return True
        except Exception as e:
            self.logger.error(f"Error checking resources: {str(e)}")
            return True  # Default to allowing execution if check fails
    
    def _scraper_worker(self, scraper_name: str, query: str, max_videos: int) -> List[Dict[str, Any]]:
        """
        Worker function for running a scraper in a separate thread.
        
        Args:
            scraper_name: Name of the scraper to use
            query: Search query
            max_videos: Maximum number of videos to collect
            
        Returns:
            List of video metadata dictionaries
        """
        if scraper_name not in self.scrapers:
            self.logger.error(f"Scraper not found: {scraper_name}")
            return []
        
        scraper = self.scrapers[scraper_name]
        self.scraper_status[scraper_name]["active"] = True
        self.scraper_status[scraper_name]["current_query"] = query
        self.scraper_status[scraper_name]["last_active"] = time.time()
        self.active_scrapers.add(scraper_name)
        
        videos = []
        page = 1
        videos_found = 0
        
        try:
            while not self.shutdown_flag.is_set():
                # Check if we've reached the maximum
                if videos_found >= max_videos:
                    break

                # Check system resources
                if not self._check_resources():
                    self.logger.warning(f"Pausing scraper {scraper_name} due to resource constraints")
                    time.sleep(5)  # Wait before checking resources again
                    continue

                # Apply global rate limiting
                self._global_rate_limit()

                # Update status
                self.scraper_status[scraper_name]["current_page"] = page

                # Search for videos
                self.logger.info(f"Searching {scraper_name} for '{query}', page {page}")
                try:
                    page_results = scraper.search_videos(query, page)
                except Exception as e:
                    self.logger.error(f"Error searching {scraper_name}: {str(e)}")
                    self.scraper_status[scraper_name]["error_count"] += 1
                    if self.scraper_status[scraper_name]["error_count"] >= 3:
                        self.logger.error(f"Too many errors for {scraper_name}, stopping")
                        break
                    page += 1
                    continue

                # Ensure each video has a duration field
                for video in page_results:
                    if "duration" not in video:
                        video["duration"] = self.estimated_video_length

                if not page_results:
                    self.logger.info(f"No more results from {scraper_name} for '{query}'")
                    break

                # Only take as many as needed
                remaining = max_videos - videos_found
                to_add = page_results[:remaining]

                # Add videos to results
                for video in to_add:
                    self.results_queue.put((scraper_name, video))

                videos.extend(to_add)
                videos_found += len(to_add)

                # Update status
                self.scraper_status[scraper_name]["videos_found"] = videos_found

                # Stop if we've reached the maximum
                if videos_found >= max_videos:
                    break

                page += 1

        except Exception as e:
            self.logger.error(f"Error in scraper worker for {scraper_name}: {str(e)}")
        finally:
            # Update status
            self.scraper_status[scraper_name]["active"] = False
            self.scraper_status[scraper_name]["last_active"] = time.time()
            self.active_scrapers.discard(scraper_name)

            self.logger.info(f"Scraper {scraper_name} finished, found {videos_found} videos")

            return videos
    
    def run_parallel_scrapers(self, queries: Dict[str, str], max_videos_per_source: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Run multiple scrapers in parallel.
        
        Args:
            queries: Dictionary mapping scraper names to search queries
            max_videos_per_source: Maximum number of videos to collect per source
            
        Returns:
            List of video metadata dictionaries
        """
        if not queries:
            self.logger.error("No queries provided")
            return []
        
        if not self.scrapers:
            self.logger.error("No scrapers registered")
            return []
        
        # Reset tracking variables
        self.total_videos_found = 0
        self.total_videos_processed = 0
        self.total_video_seconds = 0
        self.active_scrapers.clear()
        self.shutdown_flag.clear()
        
        # Use provided max_videos_per_source or default
        if max_videos_per_source is None:
            max_videos_per_source = self.max_videos_per_scraper

        # Calculate target seconds and estimated videos needed
        target_seconds = self.target_hours * 3600
        estimated_videos_needed = target_seconds / self.estimated_video_length

        # Adjust max_videos_per_source if needed to reach target
        if len(queries) > 0:
            videos_per_source = min(max_videos_per_source, int(estimated_videos_needed / len(queries)) + 1)
        else:
            videos_per_source = max_videos_per_source

        self.logger.info(f"Starting parallel scraping with {len(queries)} scrapers")
        self.logger.info(f"Target: {self.target_hours} hours, estimated videos needed: {estimated_videos_needed}")
        self.logger.info(f"Videos per source: {videos_per_source}")
        
        # Start scraper workers
        with ThreadPoolExecutor(max_workers=min(self.max_scrapers, len(queries))) as executor:
            futures = {}
            
            # Submit scraper tasks
            for scraper_name, query in queries.items():
                if scraper_name not in self.scrapers:
                    self.logger.warning(f"Scraper not found: {scraper_name}")
                    continue
                
                self.logger.info(f"Starting scraper: {scraper_name}, query: {query}")
                future = executor.submit(self._scraper_worker, scraper_name, query, videos_per_source)
                futures[future] = scraper_name
            
            # Collect results
            all_videos = []
            for future in as_completed(futures):
                scraper_name = futures[future]
                try:
                    videos = future.result()
                    self.logger.info(f"Scraper {scraper_name} completed, found {len(videos)} videos")
                    all_videos.extend(videos)
                except Exception as e:
                    self.logger.error(f"Error in scraper {scraper_name}: {str(e)}")
        
        # Drain the results queue
        while not self.results_queue.empty():
            try:
                all_videos.append(self.results_queue.get_nowait()[1])
            except queue.Empty:
                break
        
        self.logger.info(f"Parallel scraping completed, found {len(all_videos)} videos total")

        # Trim videos if total duration exceeds target
        trimmed_videos = []
        accumulated_seconds = 0
        for video in all_videos:
            duration = video.get("duration", self.estimated_video_length)
            if accumulated_seconds + duration > target_seconds:
                break
            trimmed_videos.append(video)
            accumulated_seconds += duration
        all_videos = trimmed_videos

        self.logger.info(f"Total video duration after trimming: {accumulated_seconds / 3600:.2f} hours")

        # Check if total duration meets target
        total_seconds = sum(video.get("duration", self.estimated_video_length) for video in all_videos)
        if total_seconds < target_seconds:
            self.logger.warning(f"Target of {self.target_hours} hours not reached. Collected only {total_seconds / 3600:.2f} hours.")
        else:
            self.logger.info(f"Target of {self.target_hours} hours reached.")

        return all_videos
    
    def run_scrapers_until_target(self, queries: Dict[str, str], target_hours: Optional[float] = None) -> List[Dict[str, Any]]:
        """
        Run scrapers until a target number of hours of video is collected.
        
        Args:
            queries: Dictionary mapping scraper names to search queries
            target_hours: Target hours of video to collect (overrides config)
            
        Returns:
            List of video metadata dictionaries
        """
        if target_hours is not None:
            self.target_hours = target_hours
        
        self.logger.info(f"Running scrapers until target of {self.target_hours} hours is reached")
        
        # Start scraper workers
        with ThreadPoolExecutor(max_workers=min(self.max_scrapers, len(queries))) as executor:
            futures = {}
            
            # Submit scraper tasks
            for scraper_name, query in queries.items():
                if scraper_name not in self.scrapers:
                    self.logger.warning(f"Scraper not found: {scraper_name}")
                    continue
                
                self.logger.info(f"Starting scraper: {scraper_name}, query: {query}")
                future = executor.submit(self._scraper_worker, scraper_name, query, self.max_videos_per_scraper)
                futures[future] = scraper_name
            
            # Monitor progress and collect results
            all_videos = []
            total_seconds = 0
            
            # Process videos as they come in
            while futures and total_seconds < (self.target_hours * 3600):
                # Check if we need to stop
                if self.shutdown_flag.is_set():
                    self.logger.info("Shutdown flag set, stopping scrapers")
                    break
                
                # Process any completed scrapers
                done_futures = []
                for future in list(futures.keys()):
                    if future.done():
                        scraper_name = futures[future]
                        try:
                            videos = future.result()
                            self.logger.info(f"Scraper {scraper_name} completed, found {len(videos)} videos")
                            
                            # Calculate total seconds
                            for video in videos:
                                duration = video.get("duration", 0)
                                if duration > 0:
                                    total_seconds += duration
                            
                            all_videos.extend(videos)
                        except Exception as e:
                            self.logger.error(f"Error in scraper {scraper_name}: {str(e)}")
                        
                        done_futures.append(future)
                
                # Remove completed futures
                for future in done_futures:
                    futures.pop(future)
                
                # Check if we've reached the target
                if total_seconds >= (self.target_hours * 3600):
                    self.logger.info(f"Target of {self.target_hours} hours reached, stopping scrapers")
                    self.shutdown_flag.set()
                    break
                
                # Process videos from the queue
                while not self.results_queue.empty():
                    try:
                        _, video = self.results_queue.get_nowait()
                        duration = video.get("duration", 0)
                        if duration > 0:
                            total_seconds += duration
                        all_videos.append(video)
                        
                        # Check if we've reached the target
                        if total_seconds >= (self.target_hours * 3600):
                            self.logger.info(f"Target of {self.target_hours} hours reached, stopping scrapers")
                            self.shutdown_flag.set()
                            break
                    except queue.Empty:
                        break
                
                # Sleep briefly to avoid busy waiting
                time.sleep(0.1)
            
            # Cancel any remaining futures
            for future in futures:
                future.cancel()
        
        # Drain the results queue
        while not self.results_queue.empty():
            try:
                all_videos.append(self.results_queue.get_nowait()[1])
            except queue.Empty:
                break
        
        # Calculate total hours
        total_seconds = sum(video.get("duration", 0) for video in all_videos)

        # Trim videos to not exceed the target duration
        trimmed_videos = []
        accumulated_seconds = 0
        for video in all_videos:
            duration = video.get("duration", 0)
            if accumulated_seconds + duration > self.target_hours * 3600:
                break
            trimmed_videos.append(video)
            accumulated_seconds += duration

        total_hours = accumulated_seconds / 3600
        self.logger.info(f"Trimmed video list to target. Final duration: {total_hours:.2f} hours")

        return trimmed_videos
    
    def stop_all_scrapers(self):
        """Stop all running scrapers."""
        self.logger.info("Stopping all scrapers")
        self.shutdown_flag.set()
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of all scrapers.
        
        Returns:
            Dictionary with scraper status information
        """
        return {
            "active_scrapers": list(self.active_scrapers),
            "total_videos_found": self.total_videos_found,
            "total_videos_processed": self.total_videos_processed,
            "total_video_seconds": self.total_video_seconds,
            "total_video_hours": self.total_video_seconds / 3600,
            "scraper_status": self.scraper_status
        }
