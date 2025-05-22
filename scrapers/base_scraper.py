"""
Base scraper module for video collection pipeline.
"""

import os
import time
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Generator, Optional
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    """Base class for all video scrapers."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the scraper with configuration.
        
        Args:
            config: Dictionary containing scraper configuration
        """
        self.config = config
        self.name = "base"
        self.session = requests.Session()
        self.logger = logging.getLogger(f"scraper.{self.name}")
        self.logger.info(f"Initializing {self.name} scraper")
        
    @abstractmethod
    def search_videos(self, query: str, page: int = 1) -> List[Dict[str, Any]]:
        """
        Search for videos based on query.
        
        Args:
            query: Search term
            page: Page number for pagination
            
        Returns:
            List of video metadata dictionaries
        """
        pass
    
    @abstractmethod
    def get_video_metadata(self, video_id: str) -> Dict[str, Any]:
        """
        Get detailed metadata for a specific video.
        
        Args:
            video_id: Unique identifier for the video
            
        Returns:
            Dictionary containing video metadata
        """
        pass
    
    @abstractmethod
    def download_video(self, video_url: str, output_path: str) -> bool:
        """
        Download video from URL to specified path.
        
        Args:
            video_url: URL of the video to download
            output_path: Path where the video should be saved
            
        Returns:
            True if download was successful, False otherwise
        """
        pass
    
    def paginate(self, query: str, max_pages: int = 10) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Paginate through search results.
        
        Args:
            query: Search term
            max_pages: Maximum number of pages to retrieve
            
        Yields:
            Lists of video metadata dictionaries
        """
        for page in range(1, max_pages + 1):
            self.logger.info(f"Searching {self.name} for '{query}' - page {page}")
            try:
                results = self.search_videos(query, page)
                if not results:
                    self.logger.info(f"No more results for '{query}' on {self.name}")
                    break
                yield results
                time.sleep(1)  # Rate limiting
            except Exception as e:
                self.logger.error(f"Error paginating {self.name}: {str(e)}")
                break
    
    def _make_request(self, url: str, headers: Optional[Dict[str, str]] = None, 
                     params: Optional[Dict[str, Any]] = None) -> Optional[requests.Response]:
        """
        Make HTTP request with error handling.
        
        Args:
            url: URL to request
            headers: Optional HTTP headers
            params: Optional query parameters
            
        Returns:
            Response object or None if request failed
        """
        try:
            response = self.session.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            self.logger.error(f"Request error: {str(e)}")
            return None
        except Exception as e:
            # Catch all other exceptions to ensure robustness
            self.logger.error(f"Unexpected error during request: {str(e)}")
            return None
            
    def _setup_selenium(self, headless: bool = True) -> webdriver.Chrome:
        """
        Set up Selenium WebDriver for JavaScript-heavy sites.
        
        Args:
            headless: Whether to run browser in headless mode
            
        Returns:
            Configured WebDriver instance
        """
        options = Options()
        if headless:
            options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
        
        driver = webdriver.Chrome(options=options)
        return driver
