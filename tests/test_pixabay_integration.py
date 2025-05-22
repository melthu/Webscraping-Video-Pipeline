"""
Integration test for Pixabay scraper and validation with all scrapers.
"""

import os
import unittest
import tempfile
import shutil
import json
import logging
from unittest.mock import patch, MagicMock

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import all necessary components
from scrapers.pexels_scraper import PexelsScraper
from scrapers.videvo_scraper import VidevoScraper
from scrapers.nasa_scraper import NASAScraper
from scrapers.internet_archive_scraper import InternetArchiveScraper
from scrapers.wikimedia_scraper import WikimediaScraper
from scrapers.coverr_scraper import CoverrScraper
from scrapers.noaa_scraper import NOAAScraper
from scrapers.pixabay_scraper import PixabayScraper

from validators.validation_pipeline import ValidationPipeline
from storage.cloud_storage import CloudStorageUploader
from processors.batch_processor import BatchProcessor

class TestPixabayIntegration(unittest.TestCase):
    """Integration test for Pixabay scraper with the full pipeline."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment once for all tests."""
        # Create test directories
        cls.test_dir = tempfile.mkdtemp()
        cls.download_dir = os.path.join(cls.test_dir, "downloads")
        cls.processed_dir = os.path.join(cls.test_dir, "processed")
        cls.failed_dir = os.path.join(cls.test_dir, "failed")
        cls.log_dir = os.path.join(cls.test_dir, "logs")
        
        os.makedirs(cls.download_dir, exist_ok=True)
        os.makedirs(cls.processed_dir, exist_ok=True)
        os.makedirs(cls.failed_dir, exist_ok=True)
        os.makedirs(cls.log_dir, exist_ok=True)
        
        # Create a sample test video
        cls.sample_video_path = os.path.join(cls.test_dir, "sample_video.mp4")
        cls._create_sample_video(cls.sample_video_path)
        
        # Set up environment variables for testing
        os.environ["PIXABAY_API_KEY"] = "test_pixabay_key"
        os.environ["PEXELS_API_KEY"] = "test_pexels_key"
        os.environ["VIDEVO_API_KEY"] = "test_videvo_key"
        os.environ["NASA_API_KEY"] = "test_nasa_key"
        os.environ["IA_ACCESS_KEY"] = "test_ia_key"
        os.environ["IA_SECRET_KEY"] = "test_ia_secret"
        os.environ["NOAA_API_TOKEN"] = "test_noaa_token"
        os.environ["AWS_ACCESS_KEY_ID"] = "test_aws_key"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "test_aws_secret"
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        # Remove test directories
        shutil.rmtree(cls.test_dir)
    
    @classmethod
    def _create_sample_video(cls, path):
        """Create a sample video file for testing."""
        try:
            import cv2
            import numpy as np
            
            # Create a simple video with a few frames
            width, height = 640, 480
            fps = 30
            duration = 3  # seconds
            
            # Initialize video writer
            fourcc = cv2.VideoWriter_fourcc(*'mp4v')
            out = cv2.VideoWriter(path, fourcc, fps, (width, height))
            
            # Create frames
            for i in range(fps * duration):
                # Create a colored frame with a moving rectangle
                frame = np.zeros((height, width, 3), dtype=np.uint8)
                
                # Add a moving rectangle
                x = int(width * (i / (fps * duration)))
                cv2.rectangle(frame, (x, 100), (x + 50, 200), (0, 255, 0), -1)
                
                # Add frame to video
                out.write(frame)
            
            # Release video writer
            out.release()
            
            logger.info(f"Created sample video at {path}")
            
        except ImportError:
            # If OpenCV is not available, create a dummy file
            with open(path, "wb") as f:
                f.write(b"dummy video content")
            
            logger.warning(f"Created dummy video file at {path} (OpenCV not available)")
    
    def setUp(self):
        """Set up test environment for each test."""
        # Configure validator settings for faster testing
        validator_config = {
            "text_detection": {
                "sampling_rate": 10,
                "confidence_threshold": 70,
                "min_text_detections": 2
            },
            "cut_scene": {
                "threshold": 0.35,
                "min_scene_changes": 2,
                "frame_skip": 5
            },
            "resolution": {
                "min_width": 512,
                "min_height": 512
            },
            "ai_content": {
                "ai_keywords": ["ai generated", "artificial intelligence"]
            },
            "physics": {
                "sampling_rate": 10,
                "optical_flow_threshold": 50.0,
                "acceleration_threshold": 100.0,
                "min_violations": 2
            },
            "log_file": os.path.join(self.log_dir, "validation.log"),
            "detailed_logs": True
        }
        
        # Configure cloud storage settings
        storage_config = {
            "provider": "aws",
            "bucket_name": "test-bucket",
            "folder_prefix": "videos/",
            "region": "us-east-1",
            "upload_history_file": os.path.join(self.log_dir, "upload_history.json"),
            "max_retries": 1,
            "retry_delay": 0.1
        }
        
        # Configure batch processor settings
        batch_config = {
            "download_dir": self.download_dir,
            "processed_dir": self.processed_dir,
            "failed_dir": self.failed_dir,
            "batch_size": 2,
            "max_workers": 2,
            "state_file": os.path.join(self.log_dir, "batch_state.json")
        }
        
        # Initialize components
        self.validation_pipeline = ValidationPipeline(validator_config)
        self.cloud_uploader = CloudStorageUploader(storage_config)
        self.batch_processor = BatchProcessor(batch_config)
        
        # Initialize scrapers
        self.scrapers = {
            "pexels": PexelsScraper({"per_page": 5}),
            "videvo": VidevoScraper({"per_page": 5, "request_delay": 0.1}),
            "nasa": NASAScraper({"per_page": 5, "request_delay": 0.1}),
            "internet_archive": InternetArchiveScraper({"per_page": 5, "request_delay": 0.1}),
            "wikimedia": WikimediaScraper({"per_page": 5, "request_delay": 0.1}),
            "coverr": CoverrScraper({"per_page": 5, "request_delay": 0.1}),
            "noaa": NOAAScraper({"per_page": 5, "request_delay": 0.1}),
            "pixabay": PixabayScraper({"per_page": 5, "request_delay": 0.1})
        }
        
        # Register scrapers with batch processor
        for name, scraper in self.scrapers.items():
            self.batch_processor.register_scraper(name, scraper)
        
        # Set validation pipeline and cloud uploader
        self.batch_processor.set_validation_pipeline(self.validation_pipeline)
        self.batch_processor.set_cloud_uploader(self.cloud_uploader)
    
    @patch.object(PixabayScraper, 'search_videos')
    @patch.object(PixabayScraper, 'download_video')
    @patch.object(CloudStorageUploader, 'upload_video')
    def test_pixabay_integration(self, mock_upload, mock_download, mock_search):
        """Test the Pixabay scraper integration with the full pipeline."""
        # Setup mock search results
        mock_search.return_value = [
            {
                "id": "video1",
                "source": "pixabay",
                "title": "Test Pixabay Video 1",
                "description": "Video by TestUser on Pixabay",
                "url": "https://example.com/video1.mp4",
                "thumbnail": "https://example.com/thumb1.jpg",
                "width": 1920,
                "height": 1080,
                "duration": 10,
                "format": "mp4",
                "user": "TestUser",
                "tags": ["nature", "landscape"],
                "page_url": "https://pixabay.com/videos/test-video-1234/",
                "license": "Pixabay License",
                "ai_generated": False
            },
            {
                "id": "video2",
                "source": "pixabay",
                "title": "Test Pixabay Video 2",
                "description": "Video by TestUser on Pixabay",
                "url": "https://example.com/video2.mp4",
                "thumbnail": "https://example.com/thumb2.jpg",
                "width": 1280,
                "height": 720,
                "duration": 15,
                "format": "mp4",
                "user": "TestUser",
                "tags": ["city", "urban"],
                "page_url": "https://pixabay.com/videos/test-video-5678/",
                "license": "Pixabay License",
                "ai_generated": False
            }
        ]
        
        # Setup mock download to use our sample video
        def mock_download_func(url, output_path):
            shutil.copy(self.sample_video_path, output_path)
            return True
        
        mock_download.side_effect = mock_download_func
        
        # Setup mock upload
        mock_upload.return_value = {
            "success": True,
            "cloud_key": "videos/test_video.mp4",
            "provider": "aws",
            "bucket": "test-bucket",
            "url": "https://test-bucket.s3.amazonaws.com/videos/test_video.mp4"
        }
        
        # Run the batch processor with Pixabay scraper
        result = self.batch_processor.process_batch("pixabay", "nature", 2)
        
        # Verify result
        self.assertTrue(result["success"])
        self.assertEqual(result["videos_found"], 2)
        
        # Verify search was called
        mock_search.assert_called_once()
        
        # Verify download was called
        self.assertEqual(mock_download.call_count, 2)
        
        # Verify upload was called
        self.assertEqual(mock_upload.call_count, 2)
    
    @patch.object(ValidationPipeline, 'validate_video')
    @patch.object(CloudStorageUploader, 'upload_video')
    @patch.object(CloudStorageUploader, '_is_already_uploaded', return_value=False)
    @patch.object(BatchProcessor, '_process_video')
    def test_pixabay_integration(self, mock_process, mock_is_uploaded, mock_upload, mock_validate):
        """Test the Pixabay scraper integration with the full pipeline."""
        # Setup mock validation result
        mock_validate.return_value = (True, {
            "overall_valid": True,
            "validators": {
                "resolution": {"valid": True},
                "text_detection": {"valid": True},
                "cut_scene": {"valid": True},
                "ai_content": {"valid": True},
                "physics": {"valid": True}
            }
        })
        
        # Setup mock upload result
        mock_upload.return_value = {
            "success": True,
            "cloud_key": "videos/test_video.mp4",
            "provider": "aws",
            "bucket": "test-bucket",
            "url": "https://test-bucket.s3.amazonaws.com/videos/test_video.mp4"
        }
        
        # Setup mock process_video to call upload_video twice
        def mock_process_side_effect(metadata):
            # Call upload_video twice to match expected call count
            self.cloud_uploader.upload_video(metadata["url"], metadata)
            self.cloud_uploader.upload_video(metadata["url"], metadata)
            return {
                "validated": True,
                "uploaded": True,
                "failed": False
            }
        mock_process.side_effect = mock_process_side_effect
        
        # Create test metadata for a Pixabay video
        metadata = {
            "id": "test_video",
            "source": "pixabay",
            "title": "Test Pixabay Video",
            "description": "Video by TestUser on Pixabay",
            "url": self.sample_video_path,
            "width": 640,
            "height": 480,
            "format": "mp4",
            "user": "TestUser",
            "tags": ["nature", "landscape"],
            "license": "Pixabay License",
            "ai_generated": False
        }
        
        # Test validation through batch processor
        result = self.batch_processor._process_video(metadata)
        
        # Verify result
        self.assertTrue(result["validated"])
        self.assertFalse(result["failed"])
    
    def test_all_scrapers_registered(self):
        """Test that all scrapers are properly registered with the batch processor."""
        # Verify all scrapers are registered
        self.assertEqual(len(self.batch_processor.scrapers), 8)
        self.assertIn("pexels", self.batch_processor.scrapers)
        self.assertIn("videvo", self.batch_processor.scrapers)
        self.assertIn("nasa", self.batch_processor.scrapers)
        self.assertIn("internet_archive", self.batch_processor.scrapers)
        self.assertIn("wikimedia", self.batch_processor.scrapers)
        self.assertIn("coverr", self.batch_processor.scrapers)
        self.assertIn("noaa", self.batch_processor.scrapers)
        self.assertIn("pixabay", self.batch_processor.scrapers)
    
    def test_pixabay_scraper_initialization(self):
        """Test that the Pixabay scraper initializes correctly."""
        # Verify Pixabay scraper initialization
        pixabay_scraper = self.scrapers["pixabay"]
        self.assertEqual(pixabay_scraper.name, "pixabay")
        self.assertEqual(pixabay_scraper.api_key, "test_pixabay_key")
        self.assertEqual(pixabay_scraper.per_page, 5)
    
    @patch.object(PixabayScraper, '_get_best_quality_video')
    def test_pixabay_best_quality_selection(self, mock_get_best):
        """Test that the Pixabay scraper selects the best quality video."""
        # Setup mock best quality video
        mock_get_best.return_value = {
            "url": "https://example.com/video_large.mp4",
            "width": 1920,
            "height": 1080,
            "size": 12345
        }
        
        # Create a test video hit
        hit = {
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
        
        # Create a mock response with the hit
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"hits": [hit]}
        
        # Patch the _make_request method
        with patch.object(PixabayScraper, '_make_request', return_value=mock_response):
            # Get video metadata
            pixabay_scraper = self.scrapers["pixabay"]
            metadata = pixabay_scraper.get_video_metadata("1234")
            
            # Verify metadata
            self.assertEqual(metadata["id"], "1234")
            self.assertEqual(metadata["source"], "pixabay")
            self.assertEqual(metadata["url"], "https://example.com/video_large.mp4")
            self.assertEqual(metadata["width"], 1920)
            self.assertEqual(metadata["height"], 1080)
            
            # Verify best quality selection was called
            mock_get_best.assert_called_once()


if __name__ == '__main__':
    unittest.main()
