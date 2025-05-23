"""
Coverr scraper implementation for video collection pipeline.
"""

import os
import logging
import time
import json
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup
import re
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class CoverrScraper(BaseScraper):
    """Scraper for Coverr video content."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Coverr scraper with configuration.
        
        Args:
            config: Dictionary containing scraper configuration
        """
        super().__init__(config)
        self.name = "coverr"
        
        # Coverr website URLs
        self.base_url = config.get("base_url", "https://coverr.co")
        self.search_url = config.get("search_url", "https://coverr.co/search")
        self.api_url = config.get("api_url", "https://coverr.co/api/videos")
        
        self.per_page = config.get("per_page", 20)
        
        # Set up headers for web requests
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://coverr.co/",
            "Origin": "https://coverr.co"
        }
        
        # Rate limiting settings
        self.request_delay = config.get("request_delay", 3.0)  # seconds between requests
        self.last_request_time = 0
    
    def _rate_limit(self):
        """Apply rate limiting to avoid overloading the website."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.request_delay:
            sleep_time = self.request_delay - time_since_last_request
            self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            
        self.last_request_time = time.time()
    
    def search_videos(self, query: str, page: int = 1) -> List[Dict[str, Any]]:
        """
        Search for videos based on query using Coverr website.
        
        Args:
            query: Search term
            page: Page number for pagination
            
        Returns:
            List of video metadata dictionaries
        """
        try:
            self._rate_limit()
            
            # Try API-based search first
            api_results = self._search_api(query, page)
            if api_results:
                return api_results
            
            # Fall back to web scraping if API fails
            self.logger.info("API search failed, falling back to web scraping")
            return self._search_web(query, page)
            
        except Exception as e:
            self.logger.error(f"Error searching Coverr: {str(e)}")
            return []
    
    def _search_api(self, query: str, page: int = 1) -> List[Dict[str, Any]]:
        """
        Search for videos using Coverr API.
        
        Args:
            query: Search term
            page: Page number for pagination
            
        Returns:
            List of video metadata dictionaries
        """
        try:
            # Prepare API parameters
            params = {
                "q": query,
                "page": page,
                "per_page": self.per_page
            }
            
            # Make API request
            response = self._make_request(self.api_url, headers=self.headers, params=params)
            if not response:
                return []
            
            try:
                data = response.json()
            except json.JSONDecodeError:
                self.logger.warning("Failed to parse API response as JSON")
                return []
            
            # Extract videos from response
            videos = data.get("videos", [])
            if not videos:
                return []
            
            # Transform the API response to our standard format
            results = []
            for video in videos:
                video_id = video.get("id", "")
                if not video_id:
                    continue
                
                # Get video URL
                video_files = video.get("video_files", [])
                if not video_files:
                    continue
                
                # Find the best quality video
                best_video = None
                best_width = 0
                for video_file in video_files:
                    width = video_file.get("width", 0)
                    if width >= 512 and width > best_width:
                        best_video = video_file
                        best_width = width
                
                if not best_video:
                    continue
                
                # Get video URL
                video_url = best_video.get("link", "")
                if not video_url:
                    continue
                
                # Get thumbnail
                thumbnail = ""
                video_pictures = video.get("video_pictures", [])
                if video_pictures:
                    thumbnail = video_pictures[0].get("picture", "")
                
                # Create standardized metadata
                metadata = {
                    "id": video_id,
                    "source": "coverr",
                    "title": video.get("name", "Coverr Video"),
                    "url": video_url,
                    "thumbnail": thumbnail,
                    "duration": video.get("duration", 0),
                    "width": best_video.get("width", 0),
                    "height": best_video.get("height", 0),
                    "format": "mp4",
                    "user": "Coverr",
                    "license": "Coverr License",  # Coverr has its own license
                    "original_url": f"https://coverr.co/videos/{video_id}",
                    "description": video.get("description", ""),
                    "tags": video.get("tags", [])
                }
                
                results.append(metadata)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in API search: {str(e)}")
            return []
    
    def _search_web(self, query: str, page: int = 1) -> List[Dict[str, Any]]:
        """
        Search for videos by scraping Coverr website.
        
        Args:
            query: Search term
            page: Page number for pagination
            
        Returns:
            List of video metadata dictionaries
        """
        try:
            # Prepare search URL
            search_url = f"{self.search_url}/{query}"
            if page > 1:
                search_url = f"{search_url}?page={page}"
            
            # Make request to search page
            response = self._make_request(search_url, headers=self.headers)
            if not response:
                return []
            
            # Parse HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find video elements
            video_elements = soup.select(".grid-item")
            
            results = []
            for element in video_elements:
                try:
                    # Extract video ID and URL
                    link = element.select_one("a")
                    if not link:
                        continue
                    
                    href = link.get("href", "")
                    if not href or "/videos/" not in href:
                        continue
                    
                    video_id = href.split("/videos/")[-1]
                    
                    # Get video details page
                    video_metadata = self._get_video_details(video_id)
                    if not video_metadata:
                        continue
                    
                    # Only add if we have a valid video URL
                    if video_metadata.get("url"):
                        results.append(video_metadata)
                        
                except Exception as e:
                    self.logger.warning(f"Error processing video element: {str(e)}")
                    continue
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in web search: {str(e)}")
            return []
    
    def _get_video_details(self, video_id: str) -> Dict[str, Any]:
        """
        Get detailed metadata for a video by visiting its page.
        
        Args:
            video_id: Coverr video ID
            
        Returns:
            Dictionary with video metadata
        """
        try:
            self._rate_limit()
            
            # Prepare video URL
            video_url = f"{self.base_url}/videos/{video_id}"
            
            # Make request to video page
            response = self._make_request(video_url, headers=self.headers)
            if not response:
                return {}
            
            # Parse HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract video data from JSON-LD
            json_ld = None
            for script in soup.select("script[type='application/ld+json']"):
                try:
                    data = json.loads(script.string)
                    if data.get("@type") == "VideoObject":
                        json_ld = data
                        break
                except (json.JSONDecodeError, AttributeError):
                    continue
            
            # If JSON-LD not found, try to extract data from the page
            if not json_ld:
                # Extract download link
                download_link = soup.select_one("a.download-button")
                if not download_link:
                    return {}
                
                download_url = download_link.get("href", "")
                if not download_url:
                    return {}
                
                # Extract title
                title_elem = soup.select_one("h1")
                title = title_elem.text.strip() if title_elem else "Coverr Video"
                
                # Extract thumbnail
                thumbnail = ""
                video_elem = soup.select_one("video")
                if video_elem:
                    poster = video_elem.get("poster", "")
                    if poster:
                        thumbnail = poster
                
                # Extract dimensions
                width = 0
                height = 0
                if video_elem:
                    width_str = video_elem.get("width", "0")
                    height_str = video_elem.get("height", "0")
                    try:
                        width = int(width_str)
                        height = int(height_str)
                    except (ValueError, TypeError):
                        pass
                
                # Extract duration
                duration = 0
                duration_elem = soup.select_one(".video-duration")
                if duration_elem:
                    duration_text = duration_elem.text.strip()
                    # Parse duration in format MM:SS
                    if ":" in duration_text:
                        try:
                            minutes, seconds = duration_text.split(":")
                            duration = int(minutes) * 60 + int(seconds)
                        except (ValueError, TypeError):
                            pass
                
                # Extract tags
                tags = []
                tag_elems = soup.select(".tag")
                for tag_elem in tag_elems:
                    tag = tag_elem.text.strip()
                    if tag:
                        tags.append(tag)
                
                return {
                    "id": video_id,
                    "source": "coverr",
                    "title": title,
                    "url": download_url,
                    "thumbnail": thumbnail,
                    "duration": duration,
                    "width": width,
                    "height": height,
                    "format": "mp4",
                    "user": "Coverr",
                    "license": "Coverr License",
                    "original_url": video_url,
                    "description": "",
                    "tags": tags
                }
            
            # Extract data from JSON-LD
            content_url = json_ld.get("contentUrl", "")
            if not content_url:
                return {}
            
            # Extract dimensions
            width = 0
            height = 0
            
            # Try to extract from JSON-LD
            width_str = json_ld.get("width", "")
            height_str = json_ld.get("height", "")
            
            if width_str and height_str:
                try:
                    # Remove "px" if present
                    width_str = width_str.replace("px", "")
                    height_str = height_str.replace("px", "")
                    width = int(width_str)
                    height = int(height_str)
                except (ValueError, TypeError):
                    pass
            
            # If dimensions not found in JSON-LD, try to extract from video element
            if width == 0 or height == 0:
                video_elem = soup.select_one("video")
                if video_elem:
                    width_str = video_elem.get("width", "0")
                    height_str = video_elem.get("height", "0")
                    try:
                        width = int(width_str)
                        height = int(height_str)
                    except (ValueError, TypeError):
                        pass
            
            # Extract duration
            duration = 0
            duration_str = json_ld.get("duration", "")
            if duration_str:
                # Parse ISO 8601 duration
                match = re.search(r'PT(\d+)M(\d+)S', duration_str)
                if match:
                    minutes, seconds = match.groups()
                    duration = int(minutes) * 60 + int(seconds)
            
            # If duration not found in JSON-LD, try to extract from page
            if duration == 0:
                duration_elem = soup.select_one(".video-duration")
                if duration_elem:
                    duration_text = duration_elem.text.strip()
                    # Parse duration in format MM:SS
                    if ":" in duration_text:
                        try:
                            minutes, seconds = duration_text.split(":")
                            duration = int(minutes) * 60 + int(seconds)
                        except (ValueError, TypeError):
                            pass
            
            # Extract tags
            tags = []
            keywords = json_ld.get("keywords", "")
            if keywords:
                if isinstance(keywords, str):
                    tags = [tag.strip() for tag in keywords.split(",") if tag.strip()]
                elif isinstance(keywords, list):
                    tags = keywords
            
            # If tags not found in JSON-LD, try to extract from page
            if not tags:
                tag_elems = soup.select(".tag")
                for tag_elem in tag_elems:
                    tag = tag_elem.text.strip()
                    if tag:
                        tags.append(tag)
            
            return {
                "id": video_id,
                "source": "coverr",
                "title": json_ld.get("name", "Coverr Video"),
                "url": content_url,
                "thumbnail": json_ld.get("thumbnailUrl", ""),
                "duration": duration,
                "width": width,
                "height": height,
                "format": "mp4",
                "user": "Coverr",
                "license": "Coverr License",
                "original_url": video_url,
                "description": json_ld.get("description", ""),
                "tags": tags
            }
            
        except Exception as e:
            self.logger.error(f"Error getting video details: {str(e)}")
            return {}
    
    def get_video_metadata(self, video_id: str) -> Dict[str, Any]:
        """
        Get detailed metadata for a specific video.
        
        Args:
            video_id: Coverr video ID
            
        Returns:
            Dictionary containing video metadata
        """
        return self._get_video_details(video_id)
    
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
            
            # For Coverr, we can directly download the video from the URL
            response = requests.get(video_url, stream=True, timeout=60, headers=self.headers)  # Longer timeout for potentially large videos
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            self.logger.info(f"Successfully downloaded video to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error downloading video: {str(e)}")
            return False
