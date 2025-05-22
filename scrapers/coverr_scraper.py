"""
Coverr web scraper implementation for video collection pipeline.
"""

import os
import logging
import time
import json
import re
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class CoverrScraper(BaseScraper):
    """Scraper for Coverr video platform using web scraping (no official API)."""
    
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
        
        self.per_page = config.get("per_page", 20)  # Approximate number per page
        
        # Set up headers for web requests
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        }
        
        # Rate limiting settings - be respectful of the website
        self.request_delay = config.get("request_delay", 3.0)  # seconds between requests
        self.last_request_time = 0
        
        # Selenium driver for JavaScript-heavy pages
        self.driver = None
    
    def _rate_limit(self):
        """Apply rate limiting to avoid overloading the website."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.request_delay:
            sleep_time = self.request_delay - time_since_last_request
            self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            
        self.last_request_time = time.time()
    
    def _init_selenium(self):
        """Initialize Selenium WebDriver if not already initialized."""
        if self.driver is None:
            self.driver = self._setup_selenium(headless=True)
            self.logger.info("Initialized Selenium WebDriver for Coverr scraping")
    
    def _close_selenium(self):
        """Close Selenium WebDriver if initialized."""
        if self.driver is not None:
            try:
                self.driver.quit()
            except Exception as e:
                self.logger.warning(f"Error closing Selenium WebDriver: {str(e)}")
            finally:
                self.driver = None
    
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
            self._init_selenium()
            
            # Construct search URL with query
            search_url = f"{self.search_url}?q={query}"
            if page > 1:
                search_url += f"&page={page}"
            
            # Load the search page
            self.driver.get(search_url)
            
            # Wait for videos to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".coverr-grid-item"))
            )
            
            # Scroll down to load more videos if needed
            self._scroll_page()
            
            # Parse the page content
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Find all video items
            video_items = soup.select(".coverr-grid-item")
            
            results = []
            for item in video_items:
                try:
                    # Extract video ID and title
                    video_link = item.select_one("a")
                    if not video_link:
                        continue
                    
                    video_url = video_link.get("href", "")
                    if not video_url or not video_url.startswith("/videos/"):
                        continue
                    
                    video_id = video_url.split("/")[-1]
                    title = item.select_one(".coverr-grid-item-title")
                    title_text = title.text.strip() if title else video_id
                    
                    # Extract thumbnail
                    thumbnail = ""
                    img = item.select_one("img")
                    if img:
                        thumbnail = img.get("src", "")
                    
                    # Get detailed metadata by visiting the video page
                    metadata = self._get_video_details(f"{self.base_url}{video_url}")
                    
                    # Skip if we couldn't get detailed metadata or if it's not free
                    if not metadata or metadata.get("is_premium", False):
                        continue
                    
                    # Create standardized metadata
                    video_metadata = {
                        "id": video_id,
                        "source": "coverr",
                        "title": title_text,
                        "url": metadata.get("download_url", ""),
                        "thumbnail": thumbnail,
                        "duration": metadata.get("duration", 0),
                        "width": metadata.get("width", 0),
                        "height": metadata.get("height", 0),
                        "format": "mp4",  # Coverr videos are typically MP4
                        "user": metadata.get("user", "Coverr"),
                        "license": "Coverr License (Free for commercial use, no attribution required)",
                        "original_url": f"{self.base_url}{video_url}",
                        "tags": [tag.strip() for tag in query.split(",") if tag.strip()]
                    }
                    
                    # Only add if we have a valid video URL
                    if video_metadata.get("url"):
                        results.append(video_metadata)
                        
                except Exception as e:
                    self.logger.warning(f"Error processing video item: {str(e)}")
                    continue
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error searching Coverr: {str(e)}")
            return []
        finally:
            # Close Selenium WebDriver to free resources
            self._close_selenium()
    
    def _scroll_page(self, scroll_count: int = 3):
        """
        Scroll down the page to load more content.
        
        Args:
            scroll_count: Number of times to scroll
        """
        try:
            for _ in range(scroll_count):
                self.driver.execute_script("window.scrollBy(0, window.innerHeight);")
                time.sleep(1)  # Wait for content to load
        except Exception as e:
            self.logger.warning(f"Error scrolling page: {str(e)}")
    
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
            
            # Visit the video page
            self.driver.get(video_url)
            
            # Wait for video player to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "video"))
            )
            
            # Parse the page content
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Check if it's a premium video
            premium_badge = soup.select_one(".premium-badge")
            is_premium = premium_badge is not None
            
            # Extract download URL
            download_url = ""
            download_button = soup.select_one("a.download-button")
            if download_button:
                download_url = download_button.get("href", "")
            
            # If no direct download button, try to extract from video source
            if not download_url:
                video_element = soup.select_one("video source")
                if video_element:
                    download_url = video_element.get("src", "")
            
            # Extract video dimensions from video element
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
            
            # If dimensions not found in video element, try to extract from page metadata
            if width == 0 or height == 0:
                # Look for dimensions in page text
                dimensions_pattern = r"(\d+)\s*[xX]\s*(\d+)"
                dimensions_text = soup.select_one(".video-dimensions")
                if dimensions_text:
                    match = re.search(dimensions_pattern, dimensions_text.text)
                    if match:
                        width = int(match.group(1))
                        height = int(match.group(2))
            
            # Extract duration
            duration = 0
            duration_text = soup.select_one(".video-duration")
            if duration_text:
                duration_str = duration_text.text.strip()
                # Parse duration in format MM:SS
                try:
                    minutes, seconds = duration_str.split(":")
                    duration = int(minutes) * 60 + int(seconds)
                except (ValueError, TypeError):
                    pass
            
            # Extract uploader/creator
            user = "Coverr"
            creator_element = soup.select_one(".video-author")
            if creator_element:
                user = creator_element.text.strip()
            
            return {
                "download_url": download_url,
                "width": width,
                "height": height,
                "duration": duration,
                "user": user,
                "is_premium": is_premium
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
        try:
            self._rate_limit()
            self._init_selenium()
            
            # Construct video URL
            video_url = f"{self.base_url}/videos/{video_id}"
            
            # Get detailed metadata
            metadata = self._get_video_details(video_url)
            
            # Skip if we couldn't get detailed metadata or if it's premium
            if not metadata or metadata.get("is_premium", False):
                self.logger.warning(f"Video {video_id} is premium or metadata not available")
                return {}
            
            # Create standardized metadata
            video_metadata = {
                "id": video_id,
                "source": "coverr",
                "title": video_id.replace("-", " ").title(),  # Convert ID to title if no better title available
                "url": metadata.get("download_url", ""),
                "thumbnail": "",  # No thumbnail in this context
                "duration": metadata.get("duration", 0),
                "width": metadata.get("width", 0),
                "height": metadata.get("height", 0),
                "format": "mp4",  # Coverr videos are typically MP4
                "user": metadata.get("user", "Coverr"),
                "license": "Coverr License (Free for commercial use, no attribution required)",
                "original_url": video_url,
                "tags": []
            }
            
            return video_metadata
            
        except Exception as e:
            self.logger.error(f"Error getting Coverr metadata: {str(e)}")
            return {}
        finally:
            # Close Selenium WebDriver to free resources
            self._close_selenium()
    
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
            response = requests.get(video_url, stream=True, timeout=30, headers=self.headers)
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            self.logger.info(f"Successfully downloaded video to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error downloading video: {str(e)}")
            return False
