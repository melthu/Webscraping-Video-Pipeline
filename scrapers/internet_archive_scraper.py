"""
Internet Archive API scraper implementation for video collection pipeline.
"""

import os
import logging
import time
import json
from typing import List, Dict, Any, Optional
import requests
from internetarchive import search_items, get_item
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class InternetArchiveScraper(BaseScraper):
    """Scraper for Internet Archive video content."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Internet Archive scraper with configuration.
        
        Args:
            config: Dictionary containing scraper configuration
        """
        super().__init__(config)
        self.name = "internet_archive"
        
        # Internet Archive credentials (optional)
        self.access_key = os.getenv("IA_ACCESS_KEY", "")
        self.secret_key = os.getenv("IA_SECRET_KEY", "")
        
        self.per_page = config.get("per_page", 50)
        
        # Set up headers for API access
        self.headers = {
            "User-Agent": "AfterQuery Video Collection Pipeline/1.0"
        }
        
        # Rate limiting settings
        self.request_delay = config.get("request_delay", 1.0)  # seconds between requests
        self.last_request_time = 0
        
        # Configure internetarchive library if credentials are provided
        if self.access_key and self.secret_key:
            try:
                import internetarchive
                internetarchive.configure(
                    username=self.access_key,
                    password=self.secret_key
                )
                self.logger.info("Configured Internet Archive with credentials")
            except Exception as e:
                self.logger.warning(f"Failed to configure Internet Archive with credentials: {str(e)}")
    
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
        Search for videos based on query using Internet Archive API.
        
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
            
            # Construct search query to find videos
            search_query = f"{query} AND mediatype:movies"
            
            # Use the internetarchive library to search
            search_results = search_items(search_query, fields=[
                'identifier', 'title', 'description', 'creator', 'date', 
                'subject', 'mediatype', 'collection', 'downloads', 'format'
            ], sorts=['downloads desc'], params={
                'rows': self.per_page,
                'page': page
            })
            
            results = []
            for item in search_results:
                # Skip if not a video
                if item.get('mediatype') != 'movies':
                    continue
                
                identifier = item.get('identifier')
                if not identifier:
                    continue
                
                # Get more detailed information about the item
                item_metadata = self._get_item_metadata(identifier)
                if not item_metadata:
                    continue
                
                # Skip if no video files found
                if not item_metadata.get('video_files'):
                    continue
                
                # Create standardized metadata
                metadata = {
                    "id": identifier,
                    "source": "internet_archive",
                    "title": item.get('title', ''),
                    "url": item_metadata.get('best_video_url', ''),
                    "thumbnail": item_metadata.get('thumbnail', ''),
                    "duration": item_metadata.get('duration', 0),
                    "width": item_metadata.get('width', 0),
                    "height": item_metadata.get('height', 0),
                    "format": item_metadata.get('format', 'mp4'),
                    "user": item.get('creator', 'Unknown'),
                    "license": item_metadata.get('license', 'Unknown'),
                    "original_url": f"https://archive.org/details/{identifier}",
                    "description": item.get('description', ''),
                    "tags": item.get('subject', []),
                    "date_created": item.get('date', ''),
                    "collection": item.get('collection', []),
                    "downloads": item.get('downloads', 0),
                    "video_files": item_metadata.get('video_files', [])
                }
                
                # Only add if we have a valid video URL
                if metadata.get('url'):
                    results.append(metadata)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error searching Internet Archive: {str(e)}")
            return []
    
    def _get_item_metadata(self, identifier: str) -> Dict[str, Any]:
        """
        Get detailed metadata for an Internet Archive item.
        
        Args:
            identifier: Internet Archive identifier
            
        Returns:
            Dictionary with item metadata
        """
        try:
            self._rate_limit()
            
            # Get item using the internetarchive library
            item = get_item(identifier)
            if not item:
                return {}
            
            # Get item metadata
            metadata = item.metadata
            
            # Find video files
            video_files = []
            best_video_url = ""
            best_resolution = 0
            thumbnail = ""
            
            # Check if files is a dictionary or list and handle accordingly
            if isinstance(item.files, dict):
                files_to_process = item.files.items()
            else:
                # If it's a list or other type, create an empty iterator
                self.logger.warning(f"Item {identifier} has unexpected files type: {type(item.files)}")
                files_to_process = []
            
            for file_name, file_info in files_to_process:
                # Check if it's a video file
                if file_info.get('format') in ['h.264', 'MPEG4', 'mp4', 'Matroska', 'QuickTime', 'Ogg Video']:
                    file_url = f"https://archive.org/download/{identifier}/{file_name}"
                    
                    # Get resolution if available
                    width = int(file_info.get('width', 0))
                    height = int(file_info.get('height', 0))
                    resolution = width * height
                    
                    # Check if this is the best quality video so far
                    if resolution > best_resolution and resolution >= 512*512:  # Ensure minimum resolution
                        best_resolution = resolution
                        best_video_url = file_url
                    
                    video_files.append({
                        'name': file_name,
                        'url': file_url,
                        'format': file_info.get('format', ''),
                        'width': width,
                        'height': height,
                        'size': file_info.get('size', 0)
                    })
                
                # Look for thumbnail
                if not thumbnail and file_name.endswith(('jpg', 'jpeg', 'png')) and 'thumb' in file_name.lower():
                    thumbnail = f"https://archive.org/download/{identifier}/{file_name}"
            
            # If no thumbnail found, use a default one
            if not thumbnail:
                thumbnail = f"https://archive.org/services/img/{identifier}"
            
            # Extract duration if available
            duration = 0
            if 'runtime' in metadata:
                try:
                    runtime = metadata['runtime']
                    if isinstance(runtime, list):
                        runtime = runtime[0]
                    # Convert various runtime formats to seconds
                    if 'min' in runtime:
                        minutes = float(runtime.replace('min', '').strip())
                        duration = int(minutes * 60)
                    elif ':' in runtime:
                        parts = runtime.split(':')
                        if len(parts) == 2:
                            minutes, seconds = parts
                            duration = int(minutes) * 60 + int(seconds)
                        elif len(parts) == 3:
                            hours, minutes, seconds = parts
                            duration = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
                except Exception:
                    pass
            
            # Extract license information
            license_info = metadata.get('licenseurl', metadata.get('license', 'Unknown'))
            
            # Extract width and height from the best video
            width = 0
            height = 0
            format_type = 'mp4'
            
            for file in video_files:
                if file['url'] == best_video_url:
                    width = file['width']
                    height = file['height']
                    format_type = file['format']
                    break
            
            return {
                'video_files': video_files,
                'best_video_url': best_video_url,
                'thumbnail': thumbnail,
                'duration': duration,
                'width': width,
                'height': height,
                'format': format_type,
                'license': license_info
            }
            
        except Exception as e:
            self.logger.error(f"Error getting item metadata for {identifier}: {str(e)}")
            return {}
    
    def get_video_metadata(self, video_id: str) -> Dict[str, Any]:
        """
        Get detailed metadata for a specific video.
        
        Args:
            video_id: Internet Archive identifier
            
        Returns:
            Dictionary containing video metadata
        """
        try:
            self._rate_limit()
            
            # Get item using the internetarchive library
            item = get_item(video_id)
            if not item:
                self.logger.error(f"Failed to get item for video {video_id}")
                return {}
            
            # Get item metadata
            metadata = item.metadata
            
            # Skip if not a video
            if metadata.get('mediatype') != 'movies':
                self.logger.warning(f"Item {video_id} is not a video")
                return {}
            
            # Get detailed item metadata
            item_metadata = self._get_item_metadata(video_id)
            if not item_metadata:
                return {}
            
            # Skip if no video files found
            if not item_metadata.get('video_files'):
                return {}
            
            # Create standardized metadata
            result = {
                "id": video_id,
                "source": "internet_archive",
                "title": metadata.get('title', ''),
                "url": item_metadata.get('best_video_url', ''),
                "thumbnail": item_metadata.get('thumbnail', ''),
                "duration": item_metadata.get('duration', 0),
                "width": item_metadata.get('width', 0),
                "height": item_metadata.get('height', 0),
                "format": item_metadata.get('format', 'mp4'),
                "user": metadata.get('creator', 'Unknown'),
                "license": item_metadata.get('license', 'Unknown'),
                "original_url": f"https://archive.org/details/{video_id}",
                "description": metadata.get('description', ''),
                "tags": metadata.get('subject', []),
                "date_created": metadata.get('date', ''),
                "collection": metadata.get('collection', []),
                "downloads": metadata.get('downloads', 0),
                "video_files": item_metadata.get('video_files', [])
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error getting Internet Archive metadata: {str(e)}")
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
            
            # For Internet Archive, we can directly download the video from the URL
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
