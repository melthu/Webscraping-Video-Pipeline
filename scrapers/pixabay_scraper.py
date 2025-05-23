"""
Pixabay API scraper implementation for video pipeline.

This module provides functionality to search and download videos from Pixabay.
"""

import os
import time
import logging
import requests
import shutil
from typing import Dict, Any, List, Optional
import json

from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class PixabayScraper(BaseScraper):
    """Scraper for Pixabay API."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Pixabay scraper with configuration.
        
        Args:
            config: Dictionary containing scraper configuration
        """
        super().__init__(config)
        self.name = "pixabay"
        self.logger = logging.getLogger("scraper.pixabay")
        
        # API configuration
        self.api_key = os.getenv("PIXABAY_API_KEY", "")
        self.base_url = config.get("base_url", "https://pixabay.com/api/videos/")
        self.per_page = config.get("per_page", 20)
        self.request_delay = config.get("request_delay", 1.0)  # Delay between requests to respect rate limits
        
        # Validate API key
        if not self.api_key:
            self.logger.warning("Pixabay API key not found in environment variables")
    
    def search_videos(self, query: str, page: int = 1) -> List[Dict[str, Any]]:
        """
        Search for videos on Pixabay.
        
        Args:
            query: Search query
            page: Page number for pagination
            
        Returns:
            List of video metadata dictionaries
        """
        if not self.api_key:
            self.logger.warning("Pixabay API key is required but not provided")
            return []
        
        try:
            # Prepare request parameters
            params = {
                "key": self.api_key,
                "q": query,
                "page": page,
                "per_page": self.per_page,
                "safesearch": "true",  # Only return videos suitable for all ages
                "min_width": 512,      # Minimum width requirement
                "min_height": 512      # Minimum height requirement
            }
            
            # Make request to Pixabay API
            self.logger.info(f"Searching Pixabay for '{query}', page {page}")
            response = self._make_request(self.base_url, params=params)
            
            if not response:
                return []
            
            # Parse response
            data = response.json()
            
            # Check for API errors
            if "error" in data:
                self.logger.error(f"Pixabay API error: {data['error']}")
                return []
            
            # Extract video information
            videos = []
            for hit in data.get("hits", []):
                # Get the best quality video available
                video_files = hit.get("videos", {})
                best_video = self._get_best_quality_video(video_files)
                
                if not best_video:
                    continue
                
                # Create video metadata
                video_metadata = {
                    "id": str(hit.get("id", "")),
                    "source": self.name,
                    "title": hit.get("tags", "").replace(",", " ").strip(),
                    "description": f"Video by {hit.get('user', '')} on Pixabay",
                    "url": best_video.get("url", ""),
                    "thumbnail": hit.get("userImageURL", ""),
                    "width": best_video.get("width", 0),
                    "height": best_video.get("height", 0),
                    "duration": hit.get("duration", 0),
                    "format": "mp4",
                    "user": hit.get("user", ""),
                    "tags": hit.get("tags", "").split(","),
                    "page_url": hit.get("pageURL", ""),
                    "license": "Pixabay License",
                    "ai_generated": False  # Pixabay doesn't explicitly mark AI-generated content
                }
                
                videos.append(video_metadata)
            
            # Add delay to respect rate limits
            time.sleep(self.request_delay)
            
            return videos
            
        except Exception as e:
            self.logger.error(f"Error searching Pixabay: {str(e)}")
            return []
    
    def _get_best_quality_video(self, video_files: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get the best quality video from available formats.
        
        Args:
            video_files: Dictionary of available video formats
            
        Returns:
            Best quality video information
        """
        # Check for available formats in order of preference
        for format_name in ["large", "medium", "small", "tiny"]:
            if format_name in video_files and video_files[format_name]:
                return video_files[format_name]
        
        return {}
    
    def get_video_metadata(self, video_id: str) -> Dict[str, Any]:
        """
        Get metadata for a specific video.
        
        Args:
            video_id: Pixabay video ID
            
        Returns:
            Video metadata dictionary
        """
        if not self.api_key:
            self.logger.warning("Pixabay API key is required but not provided")
            return {}
        
        try:
            # Prepare request parameters
            params = {
                "key": self.api_key,
                "id": video_id
            }
            
            # Make request to Pixabay API
            self.logger.info(f"Getting metadata for Pixabay video {video_id}")
            response = self._make_request(self.base_url, params=params)
            
            if not response:
                return {}
            
            # Parse response
            data = response.json()
            
            # Check for API errors
            if "error" in data:
                self.logger.error(f"Pixabay API error: {data['error']}")
                return {}
            
            # Extract video information from the first hit (should be only one)
            hits = data.get("hits", [])
            if not hits:
                self.logger.warning(f"No video found with ID {video_id}")
                return {}
            
            hit = hits[0]
            video_files = hit.get("videos", {})
            best_video = self._get_best_quality_video(video_files)
            
            if not best_video:
                self.logger.warning(f"No video formats found for ID {video_id}")
                return {}
            
            # Build metadata
            video_metadata = {
                "id": str(hit.get("id", "")),
                "source": self.name,
                "title": hit.get("tags", "").replace(",", " ").strip(),
                "description": f"Video by {hit.get('user', '')} on Pixabay",
                "url": best_video.get("url", ""),
                "thumbnail": hit.get("userImageURL", ""),
                "width": best_video.get("width", 0),
                "height": best_video.get("height", 0),
                "duration": hit.get("duration", 0),
                "format": "mp4",
                "user": hit.get("user", ""),
                "tags": hit.get("tags", "").split(","),
                "page_url": hit.get("pageURL", ""),
                "license": "Pixabay License",
                "ai_generated": False
            }
            
            # Add delay to respect rate limits
            time.sleep(self.request_delay)
            
            return video_metadata
            
        except Exception as e:
            self.logger.error(f"Error getting Pixabay video metadata: {str(e)}")
            return {}
    
    def download_video(self, video_url: str, output_path: str) -> bool:
        """
        Download a video from Pixabay.
        
        Args:
            video_url: URL of the video to download
            output_path: Path to save the downloaded video
            
        Returns:
            True if download was successful, False otherwise
        """
        try:
            self.logger.info(f"Downloading Pixabay video from {video_url}")
            
            # If the "URL" is actually a local file path, just copy it
            if os.path.exists(video_url):
                shutil.copy(video_url, output_path)
                self.logger.info(f"Copied local video from {video_url} to {output_path}")
                return True
            
            # Otherwise, fetch over HTTP
            response = requests.get(video_url, stream=True, timeout=30)
            if response.status_code != 200:
                self.logger.error(f"Failed to download video: HTTP {response.status_code}")
                return False
            
            # Save video to output path
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Verify file was downloaded
            if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
                self.logger.error("Downloaded file is empty or does not exist")
                return False
            
            self.logger.info(f"Successfully downloaded video to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error downloading Pixabay video: {str(e)}")
            return False
