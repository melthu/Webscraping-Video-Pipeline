"""
NOAA API scraper implementation for video collection pipeline.
"""

import os
import logging
import time
import json
from typing import List, Dict, Any, Optional
import requests
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class NOAAScraper(BaseScraper):
    """Scraper for NOAA video content."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the NOAA scraper with configuration.
        
        Args:
            config: Dictionary containing scraper configuration
        """
        super().__init__(config)
        self.name = "noaa"
        
        # NOAA API token
        self.api_token = os.getenv("NOAA_API_TOKEN", "")
        if not self.api_token:
            self.logger.warning("No API token provided for NOAA, some features may be limited")
        
        # NOAA website URLs for video content
        # Since NOAA doesn't have a dedicated video API, we'll use their multimedia library
        self.base_url = config.get("base_url", "https://www.noaa.gov")
        self.multimedia_url = config.get("multimedia_url", "https://www.noaa.gov/media/multimedia-library")
        self.search_url = config.get("search_url", "https://www.noaa.gov/media-search")
        
        self.per_page = config.get("per_page", 20)
        
        # Set up headers for web requests
        self.headers = {
            "User-Agent": "AfterQuery Video Collection Pipeline/1.0",
            "Accept": "application/json, text/plain, */*"
        }
        
        # Rate limiting settings
        self.request_delay = config.get("request_delay", 2.0)  # seconds between requests
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
        Search for videos based on query using NOAA multimedia library.
        
        Args:
            query: Search term
            page: Page number for pagination
            
        Returns:
            List of video metadata dictionaries
        """
        try:
            self._rate_limit()
            
            # Prepare search parameters
            params = {
                "search": query,
                "type": "video",
                "page": page - 1  # NOAA uses 0-based pagination
            }
            
            # Use the search API endpoint
            response = self._make_request(self.search_url, headers=self.headers, params=params)
            if not response:
                self.logger.warning(f"No response from NOAA search for query '{query}' on page {page}")
                return []
                
            # Handle 403 Forbidden errors by using a more browser-like User-Agent
            if response.status_code == 403:
                enhanced_headers = self.headers.copy()
                enhanced_headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                response = self._make_request(self.search_url, headers=enhanced_headers, params=params)
                if not response:
                    self.logger.warning(f"Still no response from NOAA search after retry with enhanced headers")
                    return []
            
            try:
                data = response.json()
            except json.JSONDecodeError:
                # If not JSON, try to parse HTML
                self.logger.warning("Response is not JSON, attempting to parse HTML")
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract video items from HTML
                video_items = soup.select(".media-item.video")
                
                results = []
                for item in video_items:
                    try:
                        # Extract video details
                        link = item.select_one("a")
                        if not link:
                            continue
                        
                        video_url = link.get("href", "")
                        if not video_url:
                            continue
                        
                        # Make absolute URL if relative
                        if video_url.startswith("/"):
                            video_url = f"{self.base_url}{video_url}"
                        
                        # Extract video ID from URL
                        video_id = video_url.split("/")[-1]
                        
                        # Extract title
                        title_elem = item.select_one(".media-title")
                        title = title_elem.text.strip() if title_elem else "NOAA Video"
                        
                        # Extract thumbnail
                        thumbnail = ""
                        img = item.select_one("img")
                        if img:
                            thumbnail = img.get("src", "")
                            if thumbnail.startswith("/"):
                                thumbnail = f"{self.base_url}{thumbnail}"
                        
                        # Get detailed metadata
                        metadata = self._get_video_details(video_url)
                        
                        # Create standardized metadata
                        video_metadata = {
                            "id": video_id,
                            "source": "noaa",
                            "title": title,
                            "url": metadata.get("download_url", ""),
                            "thumbnail": thumbnail,
                            "duration": metadata.get("duration", 0),
                            "width": metadata.get("width", 0),
                            "height": metadata.get("height", 0),
                            "format": metadata.get("format", "mp4"),
                            "user": "NOAA",
                            "license": "Public Domain",  # NOAA content is typically public domain
                            "original_url": video_url,
                            "description": metadata.get("description", ""),
                            "tags": [tag.strip() for tag in query.split(",") if tag.strip()]
                        }
                        
                        # Only add if we have a valid video URL
                        if video_metadata.get("url"):
                            results.append(video_metadata)
                            
                    except Exception as e:
                        self.logger.warning(f"Error processing video item: {str(e)}")
                        continue
                
                return results
            
            # Process JSON response
            if not data or "items" not in data:
                self.logger.warning(f"No videos found for query '{query}' on page {page}")
                return []
            
            # Transform the API response to our standard format
            results = []
            for item in data["items"]:
                try:
                    # Skip if not a video
                    if item.get("type") != "video":
                        continue
                    
                    # Extract video ID and URL
                    video_id = item.get("id", "")
                    video_url = item.get("url", "")
                    if not video_id or not video_url:
                        continue
                    
                    # Make absolute URL if relative
                    if video_url.startswith("/"):
                        video_url = f"{self.base_url}{video_url}"
                    
                    # Get detailed metadata
                    metadata = self._get_video_details(video_url)
                    
                    # Create standardized metadata
                    video_metadata = {
                        "id": video_id,
                        "source": "noaa",
                        "title": item.get("title", "NOAA Video"),
                        "url": metadata.get("download_url", ""),
                        "thumbnail": item.get("thumbnail", ""),
                        "duration": metadata.get("duration", 0),
                        "width": metadata.get("width", 0),
                        "height": metadata.get("height", 0),
                        "format": metadata.get("format", "mp4"),
                        "user": "NOAA",
                        "license": "Public Domain",  # NOAA content is typically public domain
                        "original_url": video_url,
                        "description": item.get("description", ""),
                        "tags": item.get("tags", []) or [tag.strip() for tag in query.split(",") if tag.strip()]
                    }
                    
                    # Only add if we have a valid video URL
                    if video_metadata.get("url"):
                        results.append(video_metadata)
                        
                except Exception as e:
                    self.logger.warning(f"Error processing video item: {str(e)}")
                    continue
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error searching NOAA: {str(e)}")
            return []
    
    def _get_video_details(self, video_url: str) -> Dict[str, Any]:
        """
        Get detailed metadata for a video by visiting its page.
        
        Args:
            video_url: URL of the video page
            
        Returns:
            Dictionary with video metadata
        """
        try:
            self._rate_limit()
            
            # Get the video page
            response = self._make_request(video_url, headers=self.headers)
            if not response:
                self.logger.warning(f"No response from video page: {video_url}")
                return {}
            
            # Parse the page content
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract download URL
            download_url = ""
            video_element = soup.select_one("video source")
            if video_element:
                download_url = video_element.get("src", "")
            
            # If no direct video source, look for download links
            if not download_url:
                download_links = soup.select("a.download-link")
                for link in download_links:
                    href = link.get("href", "")
                    if href and href.lower().endswith((".mp4", ".mov", ".avi")):
                        download_url = href
                        break
            
            # Make absolute URL if relative
            if download_url and download_url.startswith("/"):
                download_url = f"{self.base_url}{download_url}"
            
            # Extract video dimensions
            width = 0
            height = 0
            video_element = soup.select_one("video")
            if video_element:
                width_str = video_element.get("width", "0")
                height_str = video_element.get("height", "0")
                try:
                    width = int(width_str)
                    height = int(height_str)
                except (ValueError, TypeError):
                    pass
            
            # Extract duration
            duration = 0
            duration_elem = soup.select_one(".video-duration")
            if duration_elem:
                duration_str = duration_elem.text.strip()
                # Parse duration in format MM:SS
                try:
                    if ":" in duration_str:
                        minutes, seconds = duration_str.split(":")
                        duration = int(minutes) * 60 + int(seconds)
                except (ValueError, TypeError):
                    pass
            
            # Extract description
            description = ""
            desc_elem = soup.select_one(".media-description")
            if desc_elem:
                description = desc_elem.text.strip()
            
            # Determine format from URL
            format_type = "mp4"  # Default
            if download_url:
                if download_url.lower().endswith(".mov"):
                    format_type = "mov"
                elif download_url.lower().endswith(".avi"):
                    format_type = "avi"
            
            return {
                "download_url": download_url,
                "width": width,
                "height": height,
                "duration": duration,
                "description": description,
                "format": format_type
            }
            
        except Exception as e:
            self.logger.error(f"Error getting video details: {str(e)}")
            return {}
    
    def get_video_metadata(self, video_id: str) -> Dict[str, Any]:
        """
        Get detailed metadata for a specific video.
        
        Args:
            video_id: NOAA video ID or URL
            
        Returns:
            Dictionary containing video metadata
        """
        try:
            # Determine if video_id is a URL or an ID
            if video_id.startswith(("http://", "https://")):
                video_url = video_id
            else:
                # Construct video URL from ID
                video_url = f"{self.base_url}/media/{video_id}"
            
            # Get detailed metadata
            metadata = self._get_video_details(video_url)
            
            # Skip if we couldn't get detailed metadata
            if not metadata or not metadata.get("download_url"):
                self.logger.warning(f"Video metadata not available for {video_id}")
                return {}
            
            # Extract ID from URL if needed
            if video_id.startswith(("http://", "https://")):
                video_id = video_url.split("/")[-1]
            
            # Create standardized metadata
            video_metadata = {
                "id": video_id,
                "source": "noaa",
                "title": f"NOAA Video {video_id}",  # Default title if none available
                "url": metadata.get("download_url", ""),
                "thumbnail": "",  # No thumbnail in this context
                "duration": metadata.get("duration", 0),
                "width": metadata.get("width", 0),
                "height": metadata.get("height", 0),
                "format": metadata.get("format", "mp4"),
                "user": "NOAA",
                "license": "Public Domain",  # NOAA content is typically public domain
                "original_url": video_url,
                "description": metadata.get("description", ""),
                "tags": []
            }
            
            return video_metadata
            
        except Exception as e:
            self.logger.error(f"Error getting NOAA metadata: {str(e)}")
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
            
            # For NOAA, we can directly download the video from the URL
            response = requests.get(video_url, stream=True, timeout=60, headers=self.headers)
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            self.logger.info(f"Successfully downloaded video to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error downloading video: {str(e)}")
            return False
