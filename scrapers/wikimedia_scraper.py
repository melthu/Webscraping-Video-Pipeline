"""
Wikimedia Commons API scraper implementation for video collection pipeline.
"""

import os
import logging
import time
import json
from typing import List, Dict, Any, Optional
import requests
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class WikimediaScraper(BaseScraper):
    """Scraper for Wikimedia Commons video content."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Wikimedia Commons scraper with configuration.
        
        Args:
            config: Dictionary containing scraper configuration
        """
        super().__init__(config)
        self.name = "wikimedia"
        
        # Wikimedia API endpoints
        self.search_url = config.get("search_url", "https://commons.wikimedia.org/w/api.php")
        self.file_url = config.get("file_url", "https://commons.wikimedia.org/w/api.php")
        self.core_rest_url = config.get("core_rest_url", "https://api.wikimedia.org/core/v1/commons/file")
        
        self.per_page = config.get("per_page", 50)
        
        # Set up headers for API access
        self.headers = {
            "User-Agent": "AfterQuery Video Collection Pipeline/1.0 (contact@example.com)"
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
        Search for videos based on query using Wikimedia Commons API.
        
        Args:
            query: Search term
            page: Page number for pagination
            
        Returns:
            List of video metadata dictionaries
        """
        try:
            self._rate_limit()
            
            # Calculate offset for pagination
            offset = (page - 1) * self.per_page
            
            # Prepare API parameters for search
            params = {
                "action": "query",
                "format": "json",
                "list": "search",
                "srsearch": f"{query} filetype:video",
                "srnamespace": "6",  # File namespace
                "srlimit": self.per_page,
                "sroffset": offset
            }
            
            response = self._make_request(self.search_url, headers=self.headers, params=params)
            if not response:
                self.logger.warning(f"No response from Wikimedia API for query '{query}' on page {page}")
                return []
            
            data = response.json()
            
            if not data or "query" not in data or "search" not in data["query"]:
                self.logger.warning(f"No videos found for query '{query}' on page {page}")
                return []
            
            # Transform the API response to our standard format
            results = []
            for item in data["query"]["search"]:
                title = item.get("title", "")
                if not title.startswith("File:"):
                    continue
                
                # Extract file name from title
                file_name = title.replace("File:", "")
                
                # Get detailed metadata for the file
                file_metadata = self._get_file_metadata(file_name)
                if not file_metadata:
                    continue
                
                # Skip if not a video or doesn't meet minimum resolution
                if not file_metadata.get("is_video", False) or file_metadata.get("width", 0) < 512 or file_metadata.get("height", 0) < 512:
                    continue
                
                # Create standardized metadata
                metadata = {
                    "id": file_name,
                    "source": "wikimedia",
                    "title": file_name,
                    "url": file_metadata.get("url", ""),
                    "thumbnail": file_metadata.get("thumbnail", ""),
                    "duration": file_metadata.get("duration", 0),
                    "width": file_metadata.get("width", 0),
                    "height": file_metadata.get("height", 0),
                    "format": file_metadata.get("format", ""),
                    "user": file_metadata.get("user", "Unknown"),
                    "license": file_metadata.get("license", "Unknown"),
                    "original_url": f"https://commons.wikimedia.org/wiki/File:{file_name}",
                    "description": item.get("snippet", ""),
                    "tags": file_metadata.get("categories", []),
                    "date_created": file_metadata.get("timestamp", "")
                }
                
                # Only add if we have a valid video URL
                if metadata.get("url"):
                    results.append(metadata)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error searching Wikimedia Commons: {str(e)}")
            return []
    
    def _get_file_metadata(self, file_name: str) -> Dict[str, Any]:
        """
        Get detailed metadata for a Wikimedia Commons file.
        
        Args:
            file_name: Name of the file (without 'File:' prefix)
            
        Returns:
            Dictionary with file metadata
        """
        try:
            self._rate_limit()
            
            # Prepare API parameters for file info
            params = {
                "action": "query",
                "format": "json",
                "prop": "imageinfo",
                "titles": f"File:{file_name}",
                "iiprop": "url|size|mime|metadata|extmetadata|timestamp|user",
                "iiurlwidth": 800  # For thumbnail
            }
            
            response = self._make_request(self.file_url, headers=self.headers, params=params)
            if not response:
                self.logger.warning(f"No response for file metadata: {file_name}")
                return {}
            
            data = response.json()
            
            if not data or "query" not in data or "pages" not in data["query"]:
                return {}
            
            # Extract page data (there should be only one)
            pages = data["query"]["pages"]
            if not pages:
                return {}
            
            page_id = next(iter(pages))
            page_data = pages[page_id]
            
            if "imageinfo" not in page_data or not page_data["imageinfo"]:
                return {}
            
            image_info = page_data["imageinfo"][0]
            
            # Check if it's a video
            mime_type = image_info.get("mime", "")
            is_video = mime_type.startswith("video/")
            
            if not is_video:
                return {}
            
            # Extract metadata
            metadata = image_info.get("metadata", [])
            ext_metadata = image_info.get("extmetadata", {})
            
            # Extract duration
            duration = 0
            for meta in metadata:
                if isinstance(meta, dict) and meta.get("name") == "length":
                    try:
                        duration = float(meta.get("value", 0))
                    except (ValueError, TypeError):
                        pass
                    break
            
            # Extract license
            license_info = "Unknown"
            if "License" in ext_metadata:
                license_data = ext_metadata["License"]
                license_info = license_data.get("value", "Unknown")
            
            # Extract categories
            categories = []
            if "Categories" in ext_metadata:
                categories_data = ext_metadata["Categories"]
                categories_str = categories_data.get("value", "")
                if isinstance(categories_str, str):
                    categories = [cat.strip() for cat in categories_str.split("|") if cat.strip()]
            
            # Get direct video URL
            video_url = image_info.get("url", "")
            
            # Get thumbnail URL
            thumbnail_url = image_info.get("thumburl", "")
            
            # Extract format from mime type
            format_type = mime_type.split("/")[-1] if mime_type else ""
            
            return {
                "url": video_url,
                "thumbnail": thumbnail_url,
                "duration": duration,
                "width": image_info.get("width", 0),
                "height": image_info.get("height", 0),
                "format": format_type,
                "user": image_info.get("user", "Unknown"),
                "license": license_info,
                "categories": categories,
                "timestamp": image_info.get("timestamp", ""),
                "is_video": is_video
            }
            
        except Exception as e:
            self.logger.error(f"Error getting file metadata for {file_name}: {str(e)}")
            return {}
    
    def get_video_metadata(self, video_id: str) -> Dict[str, Any]:
        """
        Get detailed metadata for a specific video.
        
        Args:
            video_id: Wikimedia Commons file name (without 'File:' prefix)
            
        Returns:
            Dictionary containing video metadata
        """
        try:
            # Get detailed file metadata
            file_metadata = self._get_file_metadata(video_id)
            if not file_metadata:
                self.logger.error(f"Failed to get metadata for video {video_id}")
                return {}
            
            # Skip if not a video
            if not file_metadata.get("is_video", False):
                self.logger.warning(f"File {video_id} is not a video")
                return {}
            
            # Create standardized metadata
            metadata = {
                "id": video_id,
                "source": "wikimedia",
                "title": video_id,
                "url": file_metadata.get("url", ""),
                "thumbnail": file_metadata.get("thumbnail", ""),
                "duration": file_metadata.get("duration", 0),
                "width": file_metadata.get("width", 0),
                "height": file_metadata.get("height", 0),
                "format": file_metadata.get("format", ""),
                "user": file_metadata.get("user", "Unknown"),
                "license": file_metadata.get("license", "Unknown"),
                "original_url": f"https://commons.wikimedia.org/wiki/File:{video_id}",
                "description": "",  # No description in this context
                "tags": file_metadata.get("categories", []),
                "date_created": file_metadata.get("timestamp", "")
            }
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error getting Wikimedia metadata: {str(e)}")
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
            
            # For Wikimedia Commons, we can directly download the video from the URL
            response = requests.get(video_url, stream=True, timeout=60)  # Longer timeout for potentially large videos
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            self.logger.info(f"Successfully downloaded video to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error downloading video: {str(e)}")
            return False
