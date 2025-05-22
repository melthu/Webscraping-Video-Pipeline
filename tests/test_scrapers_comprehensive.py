"""
Comprehensive test suite for all video pipeline scrapers.
"""

import os
import unittest
import tempfile
import json
from unittest.mock import patch, MagicMock, mock_open, PropertyMock
import logging
import requests

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import scrapers
from scrapers.base_scraper import BaseScraper
from scrapers.pexels_scraper import PexelsScraper
from scrapers.videvo_scraper import VidevoScraper
from scrapers.nasa_scraper import NASAScraper
from scrapers.internet_archive_scraper import InternetArchiveScraper
from scrapers.wikimedia_scraper import WikimediaScraper
from scrapers.coverr_scraper import CoverrScraper
from scrapers.noaa_scraper import NOAAScraper
from scrapers.pixabay_scraper import PixabayScraper

class TestBaseScraper(unittest.TestCase):
    """Test cases for the base scraper class."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {"test_config": True}
        # Use a concrete implementation since BaseScraper is abstract
        with patch.object(BaseScraper, '__abstractmethods__', set()):
            self.scraper = BaseScraper(self.config)
    
    def test_initialization(self):
        """Test scraper initialization."""
        self.assertEqual(self.scraper.config, self.config)
        self.assertEqual(self.scraper.name, "base")
        self.assertIsNotNone(self.scraper.logger)
    
    @patch('requests.Session.get')
    def test_make_request(self, mock_get):
        """Test the _make_request method."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        # Test successful request
        response = self.scraper._make_request("https://example.com")
        self.assertEqual(response, mock_response)
        mock_get.assert_called_once()
        
        # Test failed request
        mock_get.reset_mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Client Error")
        response = self.scraper._make_request("https://example.com")
        self.assertIsNone(response)
        mock_get.assert_called_once()
        
        # Test exception handling
        mock_get.reset_mock()
        mock_get.side_effect = Exception("Test exception")
        response = self.scraper._make_request("https://example.com")
        self.assertIsNone(response)
        mock_get.assert_called_once()


# Create a dummy concrete subclass that implements abstract methods with NotImplementedError
class DummyBaseScraper(BaseScraper):
    """Dummy implementation of BaseScraper for testing abstract methods."""
    
    def search_videos(self, query, page=1):
        """Stub implementation that raises NotImplementedError."""
        raise NotImplementedError("search_videos method must be implemented by subclasses")
    
    def get_video_metadata(self, video_id):
        """Stub implementation that raises NotImplementedError."""
        raise NotImplementedError("get_video_metadata method must be implemented by subclasses")
    
    def download_video(self, url, output_path):
        """Stub implementation that raises NotImplementedError."""
        raise NotImplementedError("download_video method must be implemented by subclasses")


class TestBaseScraperAbstractMethods(unittest.TestCase):
    """Test cases for the abstract methods of BaseScraper."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {"test_config": True}
        # Create a dummy subclass instance that implements abstract methods as stubs
        self.dummy_scraper = DummyBaseScraper(self.config)
    
    def test_abstract_methods(self):
        """Test that abstract methods raise NotImplementedError."""
        with self.assertRaises(NotImplementedError):
            self.dummy_scraper.search_videos("test")
        
        with self.assertRaises(NotImplementedError):
            self.dummy_scraper.get_video_metadata("test_id")
        
        with self.assertRaises(NotImplementedError):
            self.dummy_scraper.download_video("test_url", "test_path")


class TestPexelsScraper(unittest.TestCase):
    """Test cases for the Pexels scraper."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {
            "base_url": "https://api.pexels.com/videos/",
            "per_page": 10
        }
        
        # Mock environment variable
        self.env_patcher = patch.dict('os.environ', {'PEXELS_API_KEY': 'test_key'})
        self.env_patcher.start()
        
        self.scraper = PexelsScraper(self.config)
        
        # Mock the _make_request method
        self.make_request_patcher = patch.object(self.scraper, '_make_request')
        self.mock_make_request = self.make_request_patcher.start()
    
    def tearDown(self):
        """Clean up after tests."""
        self.make_request_patcher.stop()
        self.env_patcher.stop()
    
    def test_initialization(self):
        """Test scraper initialization."""
        self.assertEqual(self.scraper.name, "pexels")
        self.assertEqual(self.scraper.base_url, self.config["base_url"])
        self.assertEqual(self.scraper.per_page, self.config["per_page"])
        self.assertEqual(self.scraper.api_key, "test_key")
    
    def test_search_videos(self):
        """Test the search_videos method."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "videos": [
                {
                    "id": 1234,
                    "url": "https://www.pexels.com/video/test-video-1234/",
                    "image": "https://example.com/thumbnail.jpg",
                    "duration": 10,
                    "user": {"name": "Test User"},
                    "video_files": [
                        {
                            "link": "https://example.com/video.mp4",
                            "width": 1920,
                            "height": 1080,
                            "fps": 30,
                            "file_type": "video/mp4"
                        }
                    ]
                }
            ]
        }
        self.mock_make_request.return_value = mock_response
        
        # Test search
        results = self.scraper.search_videos("nature")
        
        # Verify results
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], "1234")
        self.assertEqual(results[0]["source"], "pexels")
        self.assertEqual(results[0]["url"], "https://example.com/video.mp4")
        self.assertEqual(results[0]["width"], 1920)
        self.assertEqual(results[0]["height"], 1080)
    
    def test_get_video_metadata(self):
        """Test the get_video_metadata method."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": 1234,
            "url": "https://www.pexels.com/video/test-video-1234/",
            "image": "https://example.com/thumbnail.jpg",
            "duration": 10,
            "user": {"name": "Test User"},
            "video_files": [
                {
                    "link": "https://example.com/video.mp4",
                    "width": 1920,
                    "height": 1080,
                    "fps": 30,
                    "file_type": "video/mp4"
                }
            ]
        }
        self.mock_make_request.return_value = mock_response
        
        # Test get metadata
        metadata = self.scraper.get_video_metadata("1234")
        
        # Verify metadata
        self.assertEqual(metadata["id"], "1234")
        self.assertEqual(metadata["source"], "pexels")
        self.assertEqual(metadata["url"], "https://example.com/video.mp4")
        self.assertEqual(metadata["width"], 1920)
        self.assertEqual(metadata["height"], 1080)
    
    @patch('requests.get')
    def test_download_video(self, mock_get):
        """Test the download_video method."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.iter_content.return_value = [b"test data"]
        mock_get.return_value = mock_response
        
        # Create temporary file for download
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Test download
            result = self.scraper.download_video("https://example.com/video.mp4", temp_path)
            
            # Verify result
            self.assertTrue(result)
            self.assertTrue(os.path.exists(temp_path))
            
        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_missing_api_key(self):
        """Test behavior when API key is missing."""
        # Remove API key from environment
        with patch.dict('os.environ', {}, clear=True):
            scraper = PexelsScraper(self.config)
            
            # Test search with missing API key
            results = scraper.search_videos("nature")
            self.assertEqual(len(results), 0)
            
            # Test get metadata with missing API key
            metadata = scraper.get_video_metadata("1234")
            self.assertEqual(metadata, {})


class TestPixabayScraper(unittest.TestCase):
    """Test cases for the Pixabay scraper."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {
            "base_url": "https://pixabay.com/api/videos/",
            "per_page": 10,
            "request_delay": 0.01  # Fast for testing
        }
        
        # Mock environment variable
        self.env_patcher = patch.dict('os.environ', {'PIXABAY_API_KEY': 'test_key'})
        self.env_patcher.start()
        
        self.scraper = PixabayScraper(self.config)
        
        # Mock the _make_request method
        self.make_request_patcher = patch.object(self.scraper, '_make_request')
        self.mock_make_request = self.make_request_patcher.start()
    
    def tearDown(self):
        """Clean up after tests."""
        self.make_request_patcher.stop()
        self.env_patcher.stop()
    
    def test_initialization(self):
        """Test scraper initialization."""
        self.assertEqual(self.scraper.name, "pixabay")
        self.assertEqual(self.scraper.base_url, self.config["base_url"])
        self.assertEqual(self.scraper.per_page, self.config["per_page"])
        self.assertEqual(self.scraper.api_key, "test_key")
    
    def test_search_videos(self):
        """Test the search_videos method."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "totalHits": 1,
            "hits": [
                {
                    "id": 1234,
                    "pageURL": "https://pixabay.com/videos/test-video-1234/",
                    "tags": "nature,landscape,mountains",
                    "duration": 10,
                    "user": "TestUser",
                    "userImageURL": "https://example.com/user.jpg",
                    "videos": {
                        "large": {
                            "url": "https://example.com/video_large.mp4",
                            "width": 1920,
                            "height": 1080,
                            "size": 12345
                        },
                        "medium": {
                            "url": "https://example.com/video_medium.mp4",
                            "width": 1280,
                            "height": 720,
                            "size": 6789
                        }
                    }
                }
            ]
        }
        self.mock_make_request.return_value = mock_response
        
        # Test search
        results = self.scraper.search_videos("nature")
        
        # Verify results
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], "1234")
        self.assertEqual(results[0]["source"], "pixabay")
        self.assertEqual(results[0]["url"], "https://example.com/video_large.mp4")
        self.assertEqual(results[0]["width"], 1920)
        self.assertEqual(results[0]["height"], 1080)
        self.assertEqual(results[0]["title"], "nature landscape mountains")
        self.assertEqual(results[0]["user"], "TestUser")
    
    def test_get_video_metadata(self):
        """Test the get_video_metadata method."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "hits": [
                {
                    "id": 1234,
                    "pageURL": "https://pixabay.com/videos/test-video-1234/",
                    "tags": "nature,landscape,mountains",
                    "duration": 10,
                    "user": "TestUser",
                    "userImageURL": "https://example.com/user.jpg",
                    "videos": {
                        "large": {
                            "url": "https://example.com/video_large.mp4",
                            "width": 1920,
                            "height": 1080,
                            "size": 12345
                        }
                    }
                }
            ]
        }
        self.mock_make_request.return_value = mock_response
        
        # Test get metadata
        metadata = self.scraper.get_video_metadata("1234")
        
        # Verify metadata
        self.assertEqual(metadata["id"], "1234")
        self.assertEqual(metadata["source"], "pixabay")
        self.assertEqual(metadata["url"], "https://example.com/video_large.mp4")
        self.assertEqual(metadata["width"], 1920)
        self.assertEqual(metadata["height"], 1080)
    
    def test_get_best_quality_video(self):
        """Test the _get_best_quality_video method."""
        # Test with all formats available
        video_files = {
            "large": {"url": "large.mp4", "width": 1920, "height": 1080},
            "medium": {"url": "medium.mp4", "width": 1280, "height": 720},
            "small": {"url": "small.mp4", "width": 640, "height": 360},
            "tiny": {"url": "tiny.mp4", "width": 320, "height": 180}
        }
        best_video = self.scraper._get_best_quality_video(video_files)
        self.assertEqual(best_video["url"], "large.mp4")
        
        # Test with only medium format
        video_files = {
            "medium": {"url": "medium.mp4", "width": 1280, "height": 720},
            "small": {"url": "small.mp4", "width": 640, "height": 360}
        }
        best_video = self.scraper._get_best_quality_video(video_files)
        self.assertEqual(best_video["url"], "medium.mp4")
        
        # Test with only small format
        video_files = {
            "small": {"url": "small.mp4", "width": 640, "height": 360}
        }
        best_video = self.scraper._get_best_quality_video(video_files)
        self.assertEqual(best_video["url"], "small.mp4")
        
        # Test with no formats
        video_files = {}
        best_video = self.scraper._get_best_quality_video(video_files)
        self.assertEqual(best_video, {})
    
    @patch('requests.get')
    def test_download_video(self, mock_get):
        """Test the download_video method."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.iter_content.return_value = [b"test video content"]
        mock_get.return_value = mock_response
        
        # Create temporary file for download
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Test download
            result = self.scraper.download_video("https://example.com/video.mp4", temp_path)
            
            # Verify result
            self.assertTrue(result)
            self.assertTrue(os.path.exists(temp_path))
            
        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_missing_api_key(self):
        """Test behavior when API key is missing."""
        # Remove API key from environment
        with patch.dict('os.environ', {}, clear=True):
            scraper = PixabayScraper(self.config)
            
            # Test search with missing API key
            results = scraper.search_videos("nature")
            self.assertEqual(len(results), 0)
            
            # Test get metadata with missing API key
            metadata = scraper.get_video_metadata("1234")
            self.assertEqual(metadata, {})


class TestVidevoScraper(unittest.TestCase):
    """Test cases for the Videvo scraper."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {
            "base_url": "https://www.videvo.net/api/videos/",
            "per_page": 10,
            "request_delay": 0.01  # Fast for testing
        }
        
        # Mock environment variable
        self.env_patcher = patch.dict('os.environ', {'VIDEVO_API_KEY': 'test_key'})
        self.env_patcher.start()
        
        self.scraper = VidevoScraper(self.config)
        
        # Mock the _make_request method
        self.make_request_patcher = patch.object(self.scraper, '_make_request')
        self.mock_make_request = self.make_request_patcher.start()
    
    def tearDown(self):
        """Clean up after tests."""
        self.make_request_patcher.stop()
        self.env_patcher.stop()
    
    def test_initialization(self):
        """Test scraper initialization."""
        self.assertEqual(self.scraper.name, "videvo")
        self.assertEqual(self.scraper.base_url, self.config["base_url"])
        self.assertEqual(self.scraper.per_page, self.config["per_page"])
        self.assertEqual(self.scraper.api_key, "test_key")
    
    def test_search_videos(self):
        """Test the search_videos method."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "_source": {
                        "id": "1234",
                        "title": "Test Video",
                        "description": "A test video description",
                        "details_page": "https://www.videvo.net/video/test-video/1234/",
                        "thumbnail": "https://example.com/thumbnail.jpg",
                        "small_preview_mp4": "https://example.com/video.mp4",
                        "frame": "1920x1080",
                        "duration": "00:10",
                        "codec": "mp4",
                        "author": "Test User",
                        "license": "Free",
                        "keywords": "nature,landscape",
                        "date_published": "2023-01-01",
                        "is_editorial": 0,
                        "is_premium": 0,
                        "is_sensitive": False
                    }
                }
            ]
        }
        self.mock_make_request.return_value = mock_response
        
        # Test search
        results = self.scraper.search_videos("nature")
        
        # Verify results
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], "1234")
        self.assertEqual(results[0]["source"], "videvo")
        self.assertEqual(results[0]["title"], "Test Video")
        self.assertEqual(results[0]["url"], "https://example.com/video.mp4")
        self.assertEqual(results[0]["width"], 1920)
        self.assertEqual(results[0]["height"], 1080)
    
    def test_missing_api_key(self):
        """Test behavior when API key is missing."""
        # Remove API key from environment
        with patch.dict('os.environ', {}, clear=True):
            scraper = VidevoScraper(self.config)
            
            # Mock the _make_request method to return None
            with patch.object(scraper, '_make_request', return_value=None):
                # Test search with missing API key
                results = scraper.search_videos("nature")
                self.assertEqual(len(results), 0)


class TestNASAScraper(unittest.TestCase):
    """Test cases for the NASA scraper."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {
            "search_url": "https://images-api.nasa.gov/search",
            "per_page": 10,
            "request_delay": 0.01  # Fast for testing
        }
        
        # Mock environment variable
        self.env_patcher = patch.dict('os.environ', {'NASA_API_KEY': 'test_key'})
        self.env_patcher.start()
        
        self.scraper = NASAScraper(self.config)
        
        # Mock the _make_request method
        self.make_request_patcher = patch.object(self.scraper, '_make_request')
        self.mock_make_request = self.make_request_patcher.start()
        
        # Mock the _get_asset_info method
        self.get_asset_info_patcher = patch.object(self.scraper, '_get_asset_info')
        self.mock_get_asset_info = self.get_asset_info_patcher.start()
        self.mock_get_asset_info.return_value = {"url": "https://example.com/video.mp4"}
    
    def tearDown(self):
        """Clean up after tests."""
        self.make_request_patcher.stop()
        self.get_asset_info_patcher.stop()
        self.env_patcher.stop()
    
    def test_initialization(self):
        """Test scraper initialization."""
        self.assertEqual(self.scraper.name, "nasa")
        self.assertEqual(self.scraper.search_url, self.config["search_url"])
        self.assertEqual(self.scraper.per_page, self.config["per_page"])
        self.assertEqual(self.scraper.api_key, "test_key")
    
    def test_search_videos(self):
        """Test the search_videos method."""
        # Setup mock response for search
        mock_search_response = MagicMock()
        mock_search_response.status_code = 200
        mock_search_response.json.return_value = {
            "collection": {
                "items": [
                    {
                        "href": "https://images-assets.nasa.gov/video/test-video/collection.json",
                        "data": [
                            {
                                "nasa_id": "test-video",
                                "title": "Test NASA Video",
                                "description": "A test NASA video",
                                "media_type": "video",
                                "date_created": "2023-01-01T00:00:00Z",
                                "keywords": ["space", "test"]
                            }
                        ],
                        "links": [
                            {
                                "href": "https://example.com/thumbnail.jpg",
                                "rel": "preview",
                                "render": "image"
                            }
                        ]
                    }
                ]
            }
        }
        
        self.mock_make_request.return_value = mock_search_response
        
        # Test search
        results = self.scraper.search_videos("space")
        
        # Verify results
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], "test-video")
        self.assertEqual(results[0]["source"], "nasa")
        self.assertEqual(results[0]["title"], "Test NASA Video")
        self.assertEqual(results[0]["url"], "https://example.com/video.mp4")


class TestInternetArchiveScraper(unittest.TestCase):
    """Test cases for the Internet Archive scraper."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {
            "per_page": 10,
            "request_delay": 0.01  # Fast for testing
        }
        
        # Mock environment variables
        self.env_patcher = patch.dict('os.environ', {
            'IA_ACCESS_KEY': 'test_access_key',
            'IA_SECRET_KEY': 'test_secret_key'
        })
        self.env_patcher.start()
        
        # Create the scraper with mocked methods
        self.scraper = InternetArchiveScraper(self.config)
    
    def tearDown(self):
        """Clean up after tests."""
        self.env_patcher.stop()
    
    def test_initialization(self):
        """Test scraper initialization."""
        self.assertEqual(self.scraper.name, "internet_archive")
        self.assertEqual(self.scraper.per_page, self.config["per_page"])
        self.assertEqual(self.scraper.access_key, "test_access_key")
        self.assertEqual(self.scraper.secret_key, "test_secret_key")
    
    @patch('scrapers.internet_archive_scraper.search_items')
    @patch('scrapers.internet_archive_scraper.get_item')
    def test_search_videos(self, mock_get_item, mock_search_items):
        """Test the search_videos method."""
        # Create a mock search result item
        mock_search_result = MagicMock()
        
        # Set up the metadata dictionary
        mock_metadata = {
            'identifier': 'test-video',
            'title': 'Test Internet Archive Video',
            'description': 'A test video from Internet Archive',
            'mediatype': 'movies',
            'format': ['h.264', 'mp4'],
            'creator': 'Test Creator',
            'subject': ['test', 'archive'],
            'downloads': 100,
            'item_size': 12345678,
            'publicdate': '2023-01-01T00:00:00Z'
        }
        
        # Configure the mock search result to return metadata values
        mock_search_result.get.side_effect = lambda key, default=None: mock_metadata.get(key, default)
        
        # Return the mock search result in the search results - only return one result
        mock_search_items.return_value = [mock_search_result]
        
        # Create a mock item details object
        mock_item_details = MagicMock()
        mock_item_details.identifier = 'test-video'
        mock_item_details.metadata = mock_metadata
        
        # Create a mock file object
        mock_file = MagicMock()
        mock_file.name = 'test_video.mp4'
        mock_file.format = 'h.264'
        mock_file.size = 12345678
        mock_file.source = 'original'
        mock_file.width = 1920
        mock_file.height = 1080
        
        # Create a mock files dictionary
        mock_files = {'test_video.mp4': mock_file}
        
        # Set the files property on the mock item details
        type(mock_item_details).files = PropertyMock(return_value=mock_files)
        
        # Configure get_item to return the mock item details
        mock_get_item.return_value = mock_item_details
        
        # Mock the _get_item_metadata method to return a valid result
        with patch.object(self.scraper, '_get_item_metadata') as mock_get_item_metadata:
            mock_get_item_metadata.return_value = {
                'video_files': [
                    {
                        'name': 'test_video.mp4',
                        'url': 'https://archive.org/download/test-video/test_video.mp4',
                        'format': 'h.264',
                        'width': 1920,
                        'height': 1080,
                        'size': 12345678
                    }
                ],
                'best_video_url': 'https://archive.org/download/test-video/test_video.mp4',
                'thumbnail': 'https://archive.org/services/img/test-video',
                'duration': 60,
                'width': 1920,
                'height': 1080,
                'format': 'mp4',
                'license': 'Public Domain'
            }
            
            # Test search
            results = self.scraper.search_videos("test")
            
            # Verify results - expect 1 result since we're only returning one mock result
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["id"], "test-video")
            self.assertEqual(results[0]["source"], "internet_archive")
            self.assertEqual(results[0]["title"], "Test Internet Archive Video")
            self.assertEqual(results[0]["url"], "https://archive.org/download/test-video/test_video.mp4")
            self.assertEqual(results[0]["width"], 1920)
            self.assertEqual(results[0]["height"], 1080)


class TestWikimediaScraper(unittest.TestCase):
    """Test cases for the Wikimedia Commons scraper."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {
            "search_url": "https://commons.wikimedia.org/w/api.php",
            "file_url": "https://commons.wikimedia.org/w/api.php",
            "per_page": 10,
            "request_delay": 0.01  # Fast for testing
        }
        
        # Create the scraper with mocked methods
        self.scraper = WikimediaScraper(self.config)
        
        # Mock the _make_request method
        self.make_request_patcher = patch.object(self.scraper, '_make_request')
        self.mock_make_request = self.make_request_patcher.start()
    
    def tearDown(self):
        """Clean up after tests."""
        self.make_request_patcher.stop()
    
    def test_initialization(self):
        """Test scraper initialization."""
        self.assertEqual(self.scraper.name, "wikimedia")
        self.assertEqual(self.scraper.search_url, self.config["search_url"])
        self.assertEqual(self.scraper.file_url, self.config["file_url"])
        self.assertEqual(self.scraper.per_page, self.config["per_page"])
    
    def test_search_videos(self):
        """Test the search_videos method."""
        # Setup mock response for search
        mock_search_response = MagicMock()
        mock_search_response.status_code = 200
        mock_search_response.json.return_value = {
            "query": {
                "search": [
                    {
                        "title": "File:Test_Video.webm",
                        "pageid": 12345,
                        "snippet": "A test video from Wikimedia Commons"
                    }
                ]
            }
        }
        
        # Setup mock response for file info
        mock_fileinfo_response = MagicMock()
        mock_fileinfo_response.status_code = 200
        mock_fileinfo_response.json.return_value = {
            "query": {
                "pages": {
                    "12345": {
                        "pageid": 12345,
                        "title": "File:Test_Video.webm",
                        "imageinfo": [
                            {
                                "url": "https://upload.wikimedia.org/wikipedia/commons/test/Test_Video.webm",
                                "descriptionurl": "https://commons.wikimedia.org/wiki/File:Test_Video.webm",
                                "width": 1920,
                                "height": 1080,
                                "size": 12345678,
                                "user": "Test User",
                                "timestamp": "2023-01-01T00:00:00Z",
                                "mime": "video/webm",
                                "extmetadata": {
                                    "ImageDescription": {
                                        "value": "A test video from Wikimedia Commons"
                                    },
                                    "Categories": {
                                        "value": "Test videos|Wikimedia Commons"
                                    },
                                    "License": {
                                        "value": "CC BY-SA 4.0"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
        
        # Configure mock to return different responses for different calls
        self.mock_make_request.side_effect = [mock_search_response, mock_fileinfo_response]
        
        # Test search
        results = self.scraper.search_videos("test")
        
        # Verify results
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], "Test_Video.webm")
        self.assertEqual(results[0]["source"], "wikimedia")
        self.assertEqual(results[0]["title"], "Test_Video.webm")
        self.assertEqual(results[0]["url"], "https://upload.wikimedia.org/wikipedia/commons/test/Test_Video.webm")
        self.assertEqual(results[0]["width"], 1920)
        self.assertEqual(results[0]["height"], 1080)


class TestCoverrScraper(unittest.TestCase):
    """Test cases for the Coverr scraper."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {
            "base_url": "https://coverr.co",
            "search_url": "https://coverr.co/search",
            "per_page": 10,
            "request_delay": 0.01  # Fast for testing
        }
        
        # Create the scraper with mocked methods
        self.scraper = CoverrScraper(self.config)
        
        # Mock the _make_request method
        self.make_request_patcher = patch.object(self.scraper, '_make_request')
        self.mock_make_request = self.make_request_patcher.start()
        
        # Mock Selenium WebDriver
        self.selenium_patcher = patch.object(self.scraper, '_init_selenium')
        self.mock_selenium = self.selenium_patcher.start()
        
        self.close_selenium_patcher = patch.object(self.scraper, '_close_selenium')
        self.mock_close_selenium = self.close_selenium_patcher.start()
        
        # Mock _get_video_details method
        self.get_video_details_patcher = patch.object(self.scraper, '_get_video_details')
        self.mock_get_video_details = self.get_video_details_patcher.start()
        
        # Mock driver
        self.driver_patcher = patch.object(self.scraper, 'driver', create=True)
        self.mock_driver = self.driver_patcher.start()
        
        # Mock BeautifulSoup
        self.bs_patcher = patch('scrapers.coverr_scraper.BeautifulSoup')
        self.mock_bs = self.bs_patcher.start()
    
    def tearDown(self):
        """Clean up after tests."""
        self.make_request_patcher.stop()
        self.selenium_patcher.stop()
        self.close_selenium_patcher.stop()
        self.get_video_details_patcher.stop()
        self.driver_patcher.stop()
        self.bs_patcher.stop()
    
    def test_initialization(self):
        """Test scraper initialization."""
        self.assertEqual(self.scraper.name, "coverr")
        self.assertEqual(self.scraper.base_url, self.config["base_url"])
        self.assertEqual(self.scraper.per_page, self.config["per_page"])
    
    def test_search_videos(self):
        """Test the search_videos method."""
        # Setup mock for driver page_source
        self.mock_driver.page_source = "<html><body>Test page</body></html>"
        
        # Setup mock for BeautifulSoup
        mock_soup = MagicMock()
        self.mock_bs.return_value = mock_soup
        
        # Setup mock for video items
        mock_item = MagicMock()
        mock_link = MagicMock()
        mock_link.get.return_value = "/videos/test-video"
        mock_item.select_one.side_effect = lambda selector: {
            "a": mock_link,
            ".coverr-grid-item-title": MagicMock(text="Test Coverr Video")
        }.get(selector)
        
        mock_soup.select.return_value = [mock_item]
        
        # Setup mock for _get_video_details
        self.mock_get_video_details.return_value = {
            "download_url": "https://example.com/video.mp4",
            "width": 1920,
            "height": 1080,
            "duration": 10,
            "user": "Coverr",
            "is_premium": False
        }
        
        # Test search
        results = self.scraper.search_videos("nature")
        
        # Verify results
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], "test-video")
        self.assertEqual(results[0]["source"], "coverr")
        self.assertEqual(results[0]["title"], "Test Coverr Video")
        self.assertEqual(results[0]["url"], "https://example.com/video.mp4")
        self.assertEqual(results[0]["width"], 1920)
        self.assertEqual(results[0]["height"], 1080)


class TestNOAAScraper(unittest.TestCase):
    """Test cases for the NOAA scraper."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {
            "base_url": "https://www.ncdc.noaa.gov/cdo-web/api/v2/",
            "per_page": 10,
            "request_delay": 0.01  # Fast for testing
        }
        
        # Mock environment variable
        self.env_patcher = patch.dict('os.environ', {'NOAA_API_TOKEN': 'test_token'})
        self.env_patcher.start()
        
        # Create the scraper with mocked methods
        self.scraper = NOAAScraper(self.config)
        
        # Mock the _make_request method
        self.make_request_patcher = patch.object(self.scraper, '_make_request')
        self.mock_make_request = self.make_request_patcher.start()
        
        # Mock the _get_video_details method
        self.get_video_details_patcher = patch.object(self.scraper, '_get_video_details')
        self.mock_get_video_details = self.get_video_details_patcher.start()
    
    def tearDown(self):
        """Clean up after tests."""
        self.make_request_patcher.stop()
        self.get_video_details_patcher.stop()
        self.env_patcher.stop()
    
    def test_initialization(self):
        """Test scraper initialization."""
        self.assertEqual(self.scraper.name, "noaa")
        self.assertEqual(self.scraper.base_url, self.config["base_url"])
        self.assertEqual(self.scraper.per_page, self.config["per_page"])
        self.assertEqual(self.scraper.api_token, "test_token")
    
    def test_search_videos(self):
        """Test the search_videos method."""
        # Setup mock response for search
        mock_search_response = MagicMock()
        mock_search_response.status_code = 200
        
        # Mock JSON response
        mock_search_response.json.return_value = {
            "items": [
                {
                    "id": "test-video",
                    "type": "video",
                    "title": "Test NOAA Video",
                    "description": "A test video from NOAA",
                    "url": "https://www.noaa.gov/media/test-video",
                    "thumbnail": "https://example.com/thumbnail.jpg"
                }
            ]
        }
        
        # Setup mock for _get_video_details
        self.mock_get_video_details.return_value = {
            "download_url": "https://example.com/video.mp4",
            "width": 1920,
            "height": 1080,
            "duration": 10,
            "description": "A test video from NOAA",
            "format": "mp4"
        }
        
        # Configure mock to return response for search
        self.mock_make_request.return_value = mock_search_response
        
        # Test search
        results = self.scraper.search_videos("weather")
        
        # Verify results
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], "test-video")
        self.assertEqual(results[0]["source"], "noaa")
        self.assertEqual(results[0]["title"], "Test NOAA Video")
        self.assertEqual(results[0]["url"], "https://example.com/video.mp4")
        self.assertEqual(results[0]["width"], 1920)
        self.assertEqual(results[0]["height"], 1080)
    
    def test_missing_api_token(self):
        """Test behavior when API token is missing."""
        # Remove API token from environment
        with patch.dict('os.environ', {}, clear=True):
            scraper = NOAAScraper(self.config)
            
            # Test search with missing API token
            results = scraper.search_videos("weather")
            self.assertEqual(len(results), 0)


if __name__ == '__main__':
    unittest.main()
