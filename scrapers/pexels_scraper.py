"""
Pexels API scraper implementation for video collection pipeline.
"""

import os
import logging
from typing import List, Dict, Any, Optional
import requests
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class PexelsScraper(BaseScraper):
    """Scraper for Pexels video platform."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Pexels scraper with configuration.
        
        Args:
            config: Dictionary containing scraper configuration
        """
        super().__init__(config)
        self.name = "pexels"
        self.api_key = os.getenv("PEXELS_API_KEY")
        if not self.api_key:
            self.logger.warning("No API key provided for Pexels, using limited access")
        
        # Only strip a literal '/search' suffix (preserving any trailing slash otherwise)
        raw_base_url = config.get("base_url", "https://api.pexels.com/videos/")
        if raw_base_url.endswith('/search'):
            self.base_url = raw_base_url[:-len('/search')]
        else:
            self.base_url = raw_base_url
        self.per_page = config.get("per_page", 80)
        
        # Set up headers for direct API access
        self.headers = {
            "Authorization": self.api_key,
            "User-Agent": "AfterQuery Video Collection Pipeline/1.0"
        }
    
    def search_videos(self, query: str, page: int = 1) -> List[Dict[str, Any]]:
        """
        Search for videos based on query using Pexels API.
        
        Args:
            query: Search term
            page: Page number for pagination
            
        Returns:
            List of video metadata dictionaries
        """
        try:
            # Use direct API access instead of the wrapper
            url = f"{self.base_url}/search"
            params = {
                "query": query,
                "page": page,
                "per_page": self.per_page
            }
            
            response = self._make_request(url, headers=self.headers, params=params)
            if not response:
                self.logger.warning(f"No response from Pexels API for query '{query}' on page {page}")
                return []
            
            videos_dict = response.json()
            
            if not videos_dict or "videos" not in videos_dict:
                self.logger.warning(f"No videos found for query '{query}' on page {page}")
                return []
            
            # Transform the API response to our standard format
            results = []
            for video in videos_dict["videos"]:
                # Find the best quality video file
                video_files = sorted(video.get("video_files", []), 
                                    key=lambda x: x.get("width", 0) * x.get("height", 0), 
                                    reverse=True)
                
                if not video_files:
                    continue
                
                best_video = video_files[0]
                
                # Create standardized metadata
                metadata = {
                    "id": str(video.get("id", "")),
                    "source": "pexels",
                    "title": video.get("url", "").split("/")[-1].replace("-", " ").title(),
                    "url": best_video.get("link", ""),
                    "thumbnail": video.get("image", ""),
                    "duration": video.get("duration", 0),
                    "width": best_video.get("width", 0),
                    "height": best_video.get("height", 0),
                    "fps": best_video.get("fps", 0),
                    "format": best_video.get("file_type", "").split("/")[-1],
                    "user": video.get("user", {}).get("name", "Unknown"),
                    "license": "Pexels License (Free for commercial use, no attribution required)",
                    "original_url": video.get("url", ""),
                    "tags": [tag.strip() for tag in query.split(",")]
                }
                
                results.append(metadata)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error searching Pexels: {str(e)}")
            return []
    
    def get_video_metadata(self, video_id: str) -> Dict[str, Any]:
        """
        Get detailed metadata for a specific video.
        
        Args:
            video_id: Unique identifier for the video
            
        Returns:
            Dictionary containing video metadata
        """
        url = f"{self.base_url}/videos/{video_id}"
        response = self._make_request(url, headers=self.headers)
        
        if not response:
            self.logger.error(f"Failed to get metadata for video {video_id}")
            return {}
        
        try:
            data = response.json()
            
            # Find the best quality video file
            video_files = sorted(data.get("video_files", []), 
                                key=lambda x: x.get("width", 0) * x.get("height", 0), 
                                reverse=True)
            
            if not video_files:
                return {}
            
            best_video = video_files[0]
            
            # Create standardized metadata
            metadata = {
                "id": str(data.get("id", "")),
                "source": "pexels",
                "title": data.get("url", "").split("/")[-1].replace("-", " ").title(),
                "url": best_video.get("link", ""),
                "thumbnail": data.get("image", ""),
                "duration": data.get("duration", 0),
                "width": best_video.get("width", 0),
                "height": best_video.get("height", 0),
                "fps": best_video.get("fps", 0),
                "format": best_video.get("file_type", "").split("/")[-1],
                "user": data.get("user", {}).get("name", "Unknown"),
                "license": "Pexels License (Free for commercial use, no attribution required)",
                "original_url": data.get("url", ""),
                "tags": []
            }
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error parsing Pexels metadata: {str(e)}")
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
