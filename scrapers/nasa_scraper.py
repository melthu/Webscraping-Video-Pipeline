"""
NASA Image and Video Library API scraper implementation for video collection pipeline.
"""

import os
import logging
import time
from typing import List, Dict, Any, Optional
import requests
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class NASAScraper(BaseScraper):
    """Scraper for NASA Image and Video Library."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the NASA scraper with configuration.
        
        Args:
            config: Dictionary containing scraper configuration
        """
        super().__init__(config)
        self.name = "nasa"
        self.api_key = os.getenv("NASA_API_KEY", "DEMO_KEY")
        
        # NASA Image and Video Library API endpoints
        self.search_url = config.get("search_url", "https://images-api.nasa.gov/search")
        self.asset_url = config.get("asset_url", "https://images-api.nasa.gov/asset/")
        self.metadata_url = config.get("metadata_url", "https://images-api.nasa.gov/metadata/")
        
        self.per_page = config.get("per_page", 100)
        
        # Set up headers for API access
        self.headers = {
            "User-Agent": "AfterQuery Video Collection Pipeline/1.0"
        }
        
        # Rate limiting settings - NASA allows 1000 requests per hour with API key
        self.request_delay = config.get("request_delay", 3.6)  # seconds between requests (3.6s = 1000 req/hour)
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
        Search for videos based on query using NASA Image and Video Library API.
        
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
                "q": query,
                "media_type": "video",
                "page": page,
                "page_size": self.per_page
            }
            
            response = self._make_request(self.search_url, headers=self.headers, params=params)
            if not response:
                self.logger.warning(f"No response from NASA API for query '{query}' on page {page}")
                return []
            
            data = response.json()
            
            if not data or "collection" not in data or "items" not in data["collection"]:
                self.logger.warning(f"No videos found for query '{query}' on page {page}")
                return []
            
            # Transform the API response to our standard format
            results = []
            for item in data["collection"]["items"]:
                # Skip if not a video
                if "data" not in item or not item["data"]:
                    continue
                
                item_data = item["data"][0]
                if item_data.get("media_type", "").lower() != "video":
                    continue
                
                # Get NASA ID for further requests
                nasa_id = item_data.get("nasa_id", "")
                if not nasa_id:
                    continue
                
                # Get links for thumbnails
                thumbnail = ""
                if "links" in item:
                    for link in item["links"]:
                        if link.get("rel") == "preview" and "image" in link.get("render", ""):
                            thumbnail = link.get("href", "")
                            break
                
                # Create standardized metadata
                metadata = {
                    "id": nasa_id,
                    "source": "nasa",
                    "title": item_data.get("title", ""),
                    "url": "",  # Will be populated later with asset URL
                    "thumbnail": thumbnail,
                    "duration": 0,  # Will be populated later if available
                    "width": 0,  # Will be populated later if available
                    "height": 0,  # Will be populated later if available
                    "format": "mp4",  # Assuming mp4 format
                    "user": item_data.get("photographer", "NASA"),
                    "license": "Public Domain",  # NASA content is public domain
                    "original_url": item_data.get("href", ""),
                    "description": item_data.get("description", ""),
                    "tags": item_data.get("keywords", []),
                    "date_created": item_data.get("date_created", ""),
                    "center": item_data.get("center", ""),
                    "nasa_id": nasa_id
                }
                
                # Get asset information to find video URL
                asset_info = self._get_asset_info(nasa_id)
                if asset_info:
                    metadata.update(asset_info)
                
                # Only add if we have a valid video URL
                if metadata.get("url"):
                    results.append(metadata)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error searching NASA: {str(e)}")
            return []
    
    def _get_asset_info(self, nasa_id: str) -> Dict[str, Any]:
        """
        Get asset information for a NASA ID.
        
        Args:
            nasa_id: NASA ID for the asset
            
        Returns:
            Dictionary with asset information
        """
        try:
            self._rate_limit()
            
            asset_response = self._make_request(f"{self.asset_url}{nasa_id}", headers=self.headers)
            if not asset_response:
                self.logger.warning(f"No asset response for NASA ID {nasa_id}")
                return {}
            
            asset_data = asset_response.json()
            
            if not asset_data or "collection" not in asset_data or "items" not in asset_data["collection"]:
                return {}
            
            # Find the best quality MP4 video
            video_url = ""
            for item in asset_data["collection"]["items"]:
                href = item.get("href", "")
                if href.lower().endswith(".mp4"):
                    # Prefer original or high-quality videos
                    if "orig" in href.lower() or "high" in href.lower():
                        video_url = href
                        break
                    # Otherwise, take the first MP4 we find
                    if not video_url:
                        video_url = href
            
            if not video_url:
                return {}
            
            # Try to get additional metadata
            metadata_response = self._make_request(f"{self.metadata_url}{nasa_id}", headers=self.headers)
            metadata = {}
            
            if metadata_response:
                try:
                    metadata_data = metadata_response.json()
                    # Extract relevant metadata if available
                    # This would require parsing the metadata format, which varies
                    # For simplicity, we'll skip detailed metadata parsing
                except Exception as e:
                    self.logger.warning(f"Error parsing metadata for NASA ID {nasa_id}: {str(e)}")
            
            return {
                "url": video_url,
                **metadata
            }
            
        except Exception as e:
            self.logger.error(f"Error getting asset info for NASA ID {nasa_id}: {str(e)}")
            return {}
    
    def get_video_metadata(self, video_id: str) -> Dict[str, Any]:
        """
        Get detailed metadata for a specific video.
        
        Args:
            video_id: NASA ID for the video
            
        Returns:
            Dictionary containing video metadata
        """
        try:
            self._rate_limit()
            
            # Search for the specific video by ID
            params = {
                "nasa_id": video_id
            }
            
            response = self._make_request(self.search_url, headers=self.headers, params=params)
            if not response:
                self.logger.error(f"Failed to get metadata for video {video_id}")
                return {}
            
            data = response.json()
            
            if not data or "collection" not in data or "items" not in data["collection"] or not data["collection"]["items"]:
                self.logger.warning(f"No video found with ID {video_id}")
                return {}
            
            # Get the first (and should be only) result
            item = data["collection"]["items"][0]
            
            if "data" not in item or not item["data"]:
                return {}
            
            item_data = item["data"][0]
            
            # Skip if not a video
            if item_data.get("media_type", "").lower() != "video":
                self.logger.warning(f"Item {video_id} is not a video")
                return {}
            
            # Get links for thumbnails
            thumbnail = ""
            if "links" in item:
                for link in item["links"]:
                    if link.get("rel") == "preview" and "image" in link.get("render", ""):
                        thumbnail = link.get("href", "")
                        break
            
            # Create standardized metadata
            metadata = {
                "id": video_id,
                "source": "nasa",
                "title": item_data.get("title", ""),
                "url": "",  # Will be populated later with asset URL
                "thumbnail": thumbnail,
                "duration": 0,  # Will be populated later if available
                "width": 0,  # Will be populated later if available
                "height": 0,  # Will be populated later if available
                "format": "mp4",  # Assuming mp4 format
                "user": item_data.get("photographer", "NASA"),
                "license": "Public Domain",  # NASA content is public domain
                "original_url": item_data.get("href", ""),
                "description": item_data.get("description", ""),
                "tags": item_data.get("keywords", []),
                "date_created": item_data.get("date_created", ""),
                "center": item_data.get("center", ""),
                "nasa_id": video_id
            }
            
            # Get asset information to find video URL
            asset_info = self._get_asset_info(video_id)
            if asset_info:
                metadata.update(asset_info)
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error getting NASA metadata: {str(e)}")
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
            
            # For NASA, we can directly download the video from the URL
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
