"""
Videvo API scraper implementation for video collection pipeline.
"""

import os
import logging
import time
from typing import List, Dict, Any, Optional
import requests
from urllib.parse import urlencode
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class VidevoScraper(BaseScraper):
    """Scraper for Videvo video platform."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Videvo scraper with configuration.
        
        Args:
            config: Dictionary containing scraper configuration
        """
        super().__init__(config)
        self.name = "videvo"  # Set the scraper name
        # API configuration
        self.api_key = os.getenv("VIDEVO_API_KEY", "")
        if not self.api_key:
            self.logger.warning("No API key provided for Videvo, using limited access")
            
        # Set up headers with or without API key
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/json"
        }
        
        if self.api_key:
            self.headers["Authorization"] = f"Bearer {self.api_key}"       
        self.base_url = config.get("base_url", "https://www.videvo.net/api/videos/")
        self.per_page = config.get("per_page", 20)
        
        # Set up headers for API access
        self.headers = {
            "User-Agent": "AfterQuery Video Collection Pipeline/1.0"
        }
        
        # Rate limiting settings
        self.request_delay = config.get("request_delay", 1.0)  # seconds between requests
        self.last_request_time = 0
    
    def _rate_limit(self):
        """Apply rate limiting to avoid overloading the API."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.request_delay:
            sleep_time = self.request_delay - time_since_last_request
            self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            
        self.last_request_time = time.time()
    
    def search_videos(self, query: str, page: int = 1) -> List[Dict[str, Any]]:
        """
        Search for videos based on query using Videvo API.
        
        Args:
            query: Search term
            page: Page number for pagination
            
        Returns:
            List of video metadata dictionaries
        """
        try:
            self._rate_limit()
            
            # Prepare API parameters
            params = {
                "key": self.api_key,
                "query": query,
                "clips_show": "free",  # Only free videos as per requirements
                "page": page,
                "per_page": self.per_page,
                "safesearch": "true"
            }
            
            response = self._make_request(self.base_url, headers=self.headers, params=params)
            if not response:
                self.logger.warning(f"No response from Videvo API for query '{query}' on page {page}")
                return []
            
            videos_dict = response.json()
            
            if not videos_dict or "results" not in videos_dict:
                self.logger.warning(f"No videos found for query '{query}' on page {page}")
                return []
            
            # Transform the API response to our standard format
            results = []
            for video in videos_dict["results"]:
                source = video.get("_source", {})
                
                # Skip premium videos
                if source.get("is_premium", 0) == 1:
                    continue
                
                # Create standardized metadata
                metadata = {
                    "id": str(source.get("id", "")),
                    "source": "videvo",
                    "title": source.get("title", ""),
                    "url": source.get("small_preview_mp4", ""),  # Preview URL, will need to get full URL later
                    "thumbnail": source.get("thumbnail", ""),
                    "duration": self._parse_duration(source.get("duration", "00:00")),
                    "width": self._parse_resolution(source.get("frame", "0x0"))[0],
                    "height": self._parse_resolution(source.get("frame", "0x0"))[1],
                    "format": source.get("codec", "mp4"),
                    "user": source.get("author", "Unknown"),
                    "license": source.get("license", "Unknown"),
                    "original_url": source.get("details_page", ""),
                    "description": source.get("description", ""),
                    "tags": [tag.strip() for tag in source.get("keywords", "").split(",") if tag.strip()],
                    "date_published": source.get("date_published", ""),
                    "is_editorial": source.get("is_editorial", 0) == 1,
                    "is_sensitive": source.get("is_sensitive", False)
                }
                
                results.append(metadata)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error searching Videvo: {str(e)}")
            return []
    
    def get_video_metadata(self, video_id: str) -> Dict[str, Any]:
        """
        Get detailed metadata for a specific video.
        
        Args:
            video_id: Unique identifier for the video
            
        Returns:
            Dictionary containing video metadata
        """
        try:
            self._rate_limit()
            
            # Prepare API parameters for individual video lookup
            params = {
                "key": self.api_key,
                "id": video_id
            }
            
            response = self._make_request(self.base_url, headers=self.headers, params=params)
            if not response:
                self.logger.error(f"Failed to get metadata for video {video_id}")
                return {}
            
            videos_dict = response.json()
            
            if not videos_dict or "results" not in videos_dict or not videos_dict["results"]:
                self.logger.warning(f"No video found with ID {video_id}")
                return {}
            
            # Get the first (and should be only) result
            video = videos_dict["results"][0]
            source = video.get("_source", {})
            
            # Skip premium videos
            if source.get("is_premium", 0) == 1:
                self.logger.warning(f"Video {video_id} is premium, skipping")
                return {}
            
            # Create standardized metadata
            metadata = {
                "id": str(source.get("id", "")),
                "source": "videvo",
                "title": source.get("title", ""),
                "url": source.get("small_preview_mp4", ""),  # Preview URL, will need to get full URL later
                "thumbnail": source.get("thumbnail", ""),
                "duration": self._parse_duration(source.get("duration", "00:00")),
                "width": self._parse_resolution(source.get("frame", "0x0"))[0],
                "height": self._parse_resolution(source.get("frame", "0x0"))[1],
                "format": source.get("codec", "mp4"),
                "user": source.get("author", "Unknown"),
                "license": source.get("license", "Unknown"),
                "original_url": source.get("details_page", ""),
                "description": source.get("description", ""),
                "tags": [tag.strip() for tag in source.get("keywords", "").split(",") if tag.strip()],
                "date_published": source.get("date_published", ""),
                "is_editorial": source.get("is_editorial", 0) == 1,
                "is_sensitive": source.get("is_sensitive", False)
            }
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error getting Videvo metadata: {str(e)}")
            return {}
    
    def download_video(self, video_url: str, output_path: str) -> bool:
        """
        Download video from URL to specified path.
        
        Args:
            video_url: URL of the video to download
            output_path: Path where the video should be saved
            
        Returns:
            True if download was successful, False otherwise
        """
        try:
            self._rate_limit()
            
            # For Videvo, we need to extract the actual download URL from the details page
            # This is because the API only provides preview URLs
            if "small_preview" in video_url:
                # Extract video ID from metadata
                video_id = None
                if "/video/" in video_url:
                    video_id = video_url.split("/video/")[1].split("/")[0]
                
                if not video_id:
                    self.logger.error("Could not extract video ID from URL")
                    return False
                
                # Get full metadata to find download URL
                metadata = self.get_video_metadata(video_id)
                if not metadata:
                    self.logger.error(f"Could not get metadata for video {video_id}")
                    return False
                
                # Use the original details page to scrape the download URL
                details_url = metadata.get("original_url", "")
                if not details_url:
                    self.logger.error("No details URL found in metadata")
                    return False
                
                # Use requests to get the details page
                response = self._make_request(details_url, headers=self.headers)
                if not response:
                    self.logger.error(f"Could not access details page: {details_url}")
                    return False
                
                # Parse the page to find the download URL
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')
                download_link = soup.select_one('a.download-button')
                
                if not download_link or not download_link.get('href'):
                    self.logger.error("Could not find download link on details page")
                    return False
                
                video_url = download_link['href']
            
            # Now download the actual video
            response = requests.get(video_url, stream=True, timeout=30)
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            self.logger.info(f"Successfully downloaded video to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error downloading video: {str(e)}")
            return False
    
    def _parse_duration(self, duration_str: str) -> int:
        """
        Parse duration string (MM:SS) to seconds.
        
        Args:
            duration_str: Duration string in MM:SS format
            
        Returns:
            Duration in seconds
        """
        try:
            parts = duration_str.split(':')
            if len(parts) == 2:
                minutes, seconds = parts
                return int(minutes) * 60 + int(seconds)
            elif len(parts) == 3:
                hours, minutes, seconds = parts
                return int(hours) * 3600 + int(minutes) * 60 + int(seconds)
            else:
                return 0
        except Exception:
            return 0
    
    def _parse_resolution(self, resolution_str: str) -> tuple:
        """
        Parse resolution string (WIDTHxHEIGHT) to width and height.
        
        Args:
            resolution_str: Resolution string in WIDTHxHEIGHT format
            
        Returns:
            Tuple of (width, height)
        """
        try:
            width, height = resolution_str.split('x')
            return int(width), int(height)
        except Exception:
            return 0, 0
