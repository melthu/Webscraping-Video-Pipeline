"""
Test suite for cloud storage and batch processing modules.
"""

import os
import unittest
import tempfile
import json
from unittest.mock import patch, MagicMock, mock_open
import logging

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import storage and processing modules
from storage.cloud_storage import CloudStorageUploader
from processors.batch_processor import BatchProcessor
from validators.validation_pipeline import ValidationPipeline
from scrapers.base_scraper import BaseScraper

class TestCloudStorageUploader(unittest.TestCase):
    """Test cases for the cloud storage uploader."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = {
            "provider": "aws",
            "bucket_name": "test-bucket",
            "folder_prefix": "videos/",
            "region": "us-east-1",
            "upload_history_file": "test_upload_history.json"
        }
        
        # Create a temporary file for testing
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
        self.temp_file.write(b"test video content")
        self.temp_file.close()
        
        # Patch environment variables
        self.env_patcher = patch.dict('os.environ', {
            'AWS_ACCESS_KEY_ID': 'test_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret'
        })
        self.env_patcher.start()
    
    def tearDown(self):
        """Clean up after tests."""
        # Remove temporary file
        if os.path.exists(self.temp_file.name):
            os.unlink(self.temp_file.name)
        
        # Remove test history file
        if os.path.exists(self.config["upload_history_file"]):
            os.unlink(self.config["upload_history_file"])
        
        # Stop environment patch
        self.env_patcher.stop()
    
    @patch('storage.cloud_storage.boto3.client')
    def test_initialization(self, mock_boto_client):
        """Test uploader initialization."""
        # Setup mock boto client
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        
        # Initialize uploader
        uploader = CloudStorageUploader(self.config)
        
        # Verify initialization
        self.assertEqual(uploader.provider, "aws")
        self.assertEqual(uploader.bucket_name, "test-bucket")
        self.assertEqual(uploader.folder_prefix, "videos/")
        mock_boto_client.assert_called_once_with(
            's3',
            aws_access_key_id='test_key',
            aws_secret_access_key='test_secret',
            region_name='us-east-1'
        )
    
    @patch('storage.cloud_storage.boto3.client')
    @patch('storage.cloud_storage.open', new_callable=mock_open, read_data='{}')
    def test_upload_video(self, mock_file, mock_boto_client):
        """Test uploading a video."""
        # Setup mock boto client
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        
        # Initialize uploader
        uploader = CloudStorageUploader(self.config)
        
        # Setup metadata
        metadata = {
            "title": "Test Video",
            "source": "test",
            "tags": ["test", "video"]
        }
        
        # Ensure _is_already_uploaded returns False to force actual upload
        with patch.object(CloudStorageUploader, '_is_already_uploaded', return_value=False):
            # Test upload
            result = uploader.upload_video(self.temp_file.name, metadata)
            
            # Verify upload
            self.assertTrue(result["success"])
            self.assertEqual(result["cloud_key"], f"videos/{os.path.basename(self.temp_file.name)}")
            self.assertEqual(result["provider"], "aws")
            self.assertEqual(result["bucket"], "test-bucket")
            
            # Verify S3 upload was called
            mock_client.upload_file.assert_called_once()
    
    @patch('storage.cloud_storage.boto3.client')
    @patch('storage.cloud_storage.open', new_callable=mock_open, read_data='{}')
    def test_upload_nonexistent_file(self, mock_file, mock_boto_client):
        """Test uploading a non-existent file."""
        # Setup mock boto client
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        
        # Initialize uploader
        uploader = CloudStorageUploader(self.config)
        
        # Test upload with non-existent file
        result = uploader.upload_video("nonexistent_file.mp4", {})
        
        # Verify upload failed
        self.assertFalse(result["success"])
        self.assertIn("error", result)
        
        # Verify S3 upload was not called
        mock_client.upload_file.assert_not_called()
    
    @patch('storage.cloud_storage.boto3.client')
    @patch('storage.cloud_storage.open', new_callable=mock_open, read_data='{}')
    def test_upload_with_error(self, mock_file, mock_boto_client):
        """Test uploading with an error."""
        # Setup mock boto client with error
        mock_client = MagicMock()
        mock_client.upload_file.side_effect = Exception("Test upload error")
        mock_boto_client.return_value = mock_client
        
        # Initialize uploader with fewer retries for faster test
        config = self.config.copy()
        config["max_retries"] = 1
        uploader = CloudStorageUploader(config)
        
        # Ensure _is_already_uploaded returns False to force actual upload
        with patch.object(CloudStorageUploader, '_is_already_uploaded', return_value=False):
            # Test upload
            result = uploader.upload_video(self.temp_file.name, {})
            
            # Verify upload failed
            self.assertFalse(result["success"])
            self.assertIn("error", result)
            self.assertIn("Test upload error", result["error"])
            self.assertEqual(result["provider"], "aws")
            self.assertEqual(result["bucket"], "test-bucket")
            
            # Verify S3 upload was called
            mock_client.upload_file.assert_called_once()
    
    @patch('storage.cloud_storage.boto3.client')
    @patch('storage.cloud_storage.open', new_callable=mock_open)
    def test_already_uploaded(self, mock_file, mock_boto_client):
        """Test uploading an already uploaded file."""
        # Setup mock boto client
        mock_client = MagicMock()
        mock_client.head_object.return_value = {}  # File exists
        mock_boto_client.return_value = mock_client
        
        # Setup mock file with existing upload history
        mock_file.return_value.read.return_value = json.dumps({
            self._get_file_hash(self.temp_file.name): {
                "local_path": self.temp_file.name,
                "cloud_key": f"videos/{os.path.basename(self.temp_file.name)}",
                "provider": "aws",
                "bucket": "test-bucket",
                "timestamp": 1621555555.0
            }
        })
        
        # Initialize uploader
        uploader = CloudStorageUploader(self.config)
        
        # Test upload
        result = uploader.upload_video(self.temp_file.name, {})
        
        # Verify upload was skipped
        self.assertTrue(result["success"])
        self.assertIn("message", result)
        self.assertEqual(result["message"], "Already uploaded")
        
        # Verify S3 upload was not called
        mock_client.upload_file.assert_not_called()
    
    def _get_file_hash(self, file_path):
        """Helper to generate file hash similar to the uploader."""
        import hashlib
        stat = os.stat(file_path)
        file_info = f"{file_path}:{stat.st_size}:{stat.st_mtime}"
        return hashlib.md5(file_info.encode()).hexdigest()


class MockScraper(BaseScraper):
    """Mock scraper for testing batch processor."""
    
    def __init__(self, config):
        super().__init__(config)
        self.name = "mock"
        self.search_results = []
        self.download_success = True
    
    def search_videos(self, query, page=1):
        """Mock search videos method."""
        if not self.search_results:
            return []
        
        # Return a subset of results for pagination
        start_idx = (page - 1) * 10
        end_idx = start_idx + 10
        
        if start_idx >= len(self.search_results):
            return []
        
        return self.search_results[start_idx:end_idx]
    
    def get_video_metadata(self, video_id):
        """Mock get video metadata method."""
        for video in self.search_results:
            if video.get("id") == video_id:
                return video
        return {}
    
    def download_video(self, video_url, output_path):
        """Mock download video method."""
        if not self.download_success:
            return False
        
        # Create a dummy video file
        with open(output_path, "wb") as f:
            f.write(b"mock video content")
        
        return True


class TestBatchProcessor(unittest.TestCase):
    """Test cases for the batch processor."""
    
    def setUp(self):
        """Set up test environment."""
        # Create test directories
        self.test_dir = tempfile.mkdtemp()
        self.download_dir = os.path.join(self.test_dir, "downloads")
        self.processed_dir = os.path.join(self.test_dir, "processed")
        self.failed_dir = os.path.join(self.test_dir, "failed")
        
        os.makedirs(self.download_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        os.makedirs(self.failed_dir, exist_ok=True)
        
        # Configure batch processor
        self.config = {
            "download_dir": self.download_dir,
            "processed_dir": self.processed_dir,
            "failed_dir": self.failed_dir,
            "batch_size": 5,
            "max_workers": 2,
            "state_file": os.path.join(self.test_dir, "batch_state.json")
        }
        
        # Create mock components
        self.mock_scraper = MockScraper({"test": True})
        self.mock_validation_pipeline = MagicMock()
        self.mock_cloud_uploader = MagicMock()
        
        # Setup mock search results
        self.mock_scraper.search_results = [
            {
                "id": f"video{i}",
                "source": "mock",
                "title": f"Test Video {i}",
                "url": f"https://example.com/video{i}.mp4",
                "thumbnail": f"https://example.com/thumb{i}.jpg",
                "width": 1920,
                "height": 1080,
                "format": "mp4"
            }
            for i in range(1, 21)  # 20 test videos
        ]
        
        # Setup mock validation results
        self.mock_validation_pipeline.validate_video.return_value = (True, {"overall_valid": True})
        
        # Setup mock upload results
        self.mock_cloud_uploader.upload_video.return_value = {
            "success": True,
            "cloud_key": "videos/test.mp4",
            "url": "https://test-bucket.s3.amazonaws.com/videos/test.mp4"
        }
        
        # Initialize batch processor
        self.processor = BatchProcessor(self.config)
        self.processor.register_scraper("mock", self.mock_scraper)
        self.processor.set_validation_pipeline(self.mock_validation_pipeline)
        self.processor.set_cloud_uploader(self.mock_cloud_uploader)
    
    def tearDown(self):
        """Clean up after tests."""
        # Remove test directories
        import shutil
        shutil.rmtree(self.test_dir)
    
    def test_initialization(self):
        """Test batch processor initialization."""
        self.assertEqual(self.processor.download_dir, self.download_dir)
        self.assertEqual(self.processor.processed_dir, self.processed_dir)
        self.assertEqual(self.processor.failed_dir, self.failed_dir)
        self.assertEqual(self.processor.batch_size, 5)
        self.assertEqual(self.processor.max_workers, 2)
        self.assertEqual(len(self.processor.scrapers), 1)
        self.assertIn("mock", self.processor.scrapers)
    
    @patch('processors.batch_processor.as_completed')
    @patch('processors.batch_processor.ThreadPoolExecutor')
    def test_process_batch(self, mock_executor, mock_as_completed):
        """Test processing a batch of videos."""
        # Setup mock executor
        mock_executor_instance = MagicMock()
        mock_executor.return_value.__enter__.return_value = mock_executor_instance
        
        # Setup mock future results
        mock_future = MagicMock()
        mock_future.result.return_value = {
            "id": "video1",
            "source": "mock",
            "downloaded": True,
            "validated": True,
            "uploaded": True,
            "failed": False
        }
        mock_executor_instance.submit.return_value = mock_future
        mock_executor_instance.as_completed.return_value = [mock_future]
        mock_as_completed.return_value = [mock_future]
        
        # Test processing a batch
        result = self.processor.process_batch("mock", "test query", 5)
        
        # Verify result
        self.assertTrue(result["success"])
        self.assertEqual(result["videos_found"], 5)
        
        # Verify executor was called
        mock_executor.assert_called_once()
        mock_executor_instance.submit.assert_called()
    
    def test_process_batch_invalid_source(self):
        """Test processing a batch with invalid source."""
        # Test with invalid source
        result = self.processor.process_batch("invalid", "test query", 5)
        
        # Verify result
        self.assertFalse(result["success"])
        self.assertIn("error", result)
        self.assertIn("No scraper registered", result["error"])
    
    def test_process_batch_no_validation_pipeline(self):
        """Test processing a batch without validation pipeline."""
        # Remove validation pipeline
        self.processor.validation_pipeline = None
        
        # Test processing a batch
        result = self.processor.process_batch("mock", "test query", 5)
        
        # Verify result
        self.assertFalse(result["success"])
        self.assertIn("error", result)
        self.assertIn("Validation pipeline not set", result["error"])
    
    def test_process_batch_no_cloud_uploader(self):
        """Test processing a batch without cloud uploader."""
        # Remove cloud uploader
        self.processor.cloud_uploader = None
        
        # Test processing a batch
        result = self.processor.process_batch("mock", "test query", 5)
        
        # Verify result
        self.assertFalse(result["success"])
        self.assertIn("error", result)
        self.assertIn("Cloud uploader not set", result["error"])
    
    @patch('processors.batch_processor.ThreadPoolExecutor')
    def test_process_video(self, mock_executor):
        """Test processing a single video."""
        # Setup test video metadata
        video_metadata = {
            "id": "testvideo",
            "source": "mock",
            "title": "Test Video",
            "url": "https://example.com/testvideo.mp4",
            "thumbnail": "https://example.com/thumb.jpg",
            "width": 1920,
            "height": 1080,
            "format": "mp4"
        }
        
        # Test processing a video
        result = self.processor._process_video(video_metadata)
        
        # Verify result
        self.assertEqual(result["id"], "testvideo")
        self.assertEqual(result["source"], "mock")
        self.assertTrue(result["downloaded"])
        self.assertTrue(result["validated"])
        self.assertTrue(result["uploaded"])
        self.assertFalse(result["failed"])
        
        # Verify validation was called
        self.mock_validation_pipeline.validate_video.assert_called_once()
        
        # Verify upload was called
        self.mock_cloud_uploader.upload_video.assert_called_once()
    
    @patch('processors.batch_processor.ThreadPoolExecutor')
    def test_process_video_download_failure(self, mock_executor):
        """Test processing a video with download failure."""
        # Setup download failure
        self.mock_scraper.download_success = False
        
        # Setup test video metadata
        video_metadata = {
            "id": "testvideo",
            "source": "mock",
            "title": "Test Video",
            "url": "https://example.com/testvideo.mp4",
            "thumbnail": "https://example.com/thumb.jpg",
            "width": 1920,
            "height": 1080,
            "format": "mp4"
        }
        
        # Test processing a video
        result = self.processor._process_video(video_metadata)
        
        # Verify result
        self.assertEqual(result["id"], "testvideo")
        self.assertEqual(result["source"], "mock")
        self.assertFalse(result["downloaded"])
        self.assertFalse(result["validated"])
        self.assertFalse(result["uploaded"])
        self.assertTrue(result["failed"])
        self.assertIn("error", result)
        
        # Verify validation was not called
        self.mock_validation_pipeline.validate_video.assert_not_called()
        
        # Verify upload was not called
        self.mock_cloud_uploader.upload_video.assert_not_called()
    
    @patch('processors.batch_processor.ThreadPoolExecutor')
    def test_process_video_validation_failure(self, mock_executor):
        """Test processing a video with validation failure."""
        # Setup validation failure
        self.mock_validation_pipeline.validate_video.return_value = (False, {"overall_valid": False})
        
        # Reset download success
        self.mock_scraper.download_success = True
        
        # Setup test video metadata
        video_metadata = {
            "id": "testvideo",
            "source": "mock",
            "title": "Test Video",
            "url": "https://example.com/testvideo.mp4",
            "thumbnail": "https://example.com/thumb.jpg",
            "width": 1920,
            "height": 1080,
            "format": "mp4"
        }
        
        # Test processing a video
        result = self.processor._process_video(video_metadata)
        
        # Verify result
        self.assertEqual(result["id"], "testvideo")
        self.assertEqual(result["source"], "mock")
        self.assertTrue(result["downloaded"])
        self.assertFalse(result["validated"])
        self.assertFalse(result["uploaded"])
        self.assertTrue(result["failed"])
        self.assertIn("error", result)
        
        # Verify validation was called
        self.mock_validation_pipeline.validate_video.assert_called_once()
        
        # Verify upload was not called
        self.mock_cloud_uploader.upload_video.assert_not_called()
    
    @patch('processors.batch_processor.ThreadPoolExecutor')
    def test_process_video_upload_failure(self, mock_executor):
        """Test processing a video with upload failure."""
        # Setup upload failure
        self.mock_cloud_uploader.upload_video.return_value = {
            "success": False,
            "error": "Upload failed"
        }
        
        # Reset validation success
        self.mock_validation_pipeline.validate_video.return_value = (True, {"overall_valid": True})
        
        # Setup test video metadata
        video_metadata = {
            "id": "testvideo",
            "source": "mock",
            "title": "Test Video",
            "url": "https://example.com/testvideo.mp4",
            "thumbnail": "https://example.com/thumb.jpg",
            "width": 1920,
            "height": 1080,
            "format": "mp4"
        }
        
        # Test processing a video
        result = self.processor._process_video(video_metadata)
        
        # Verify result
        self.assertEqual(result["id"], "testvideo")
        self.assertEqual(result["source"], "mock")
        self.assertTrue(result["downloaded"])
        self.assertTrue(result["validated"])
        self.assertFalse(result["uploaded"])
        self.assertTrue(result["failed"])
        self.assertIn("error", result)
        
        # Verify validation was called
        self.mock_validation_pipeline.validate_video.assert_called_once()
        
        # Verify upload was called
        self.mock_cloud_uploader.upload_video.assert_called_once()


if __name__ == '__main__':
    unittest.main()
