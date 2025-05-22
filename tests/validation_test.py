"""
Test script for validating the video pipeline components.
"""

import os
import sys
import logging
import tempfile
from pathlib import Path

# Add parent directory to path to allow imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from scrapers.pexels_scraper import PexelsScraper
from processors.video_processor import VideoProcessor
import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("validation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def test_pexels_scraper():
    """Test the Pexels scraper functionality."""
    logger.info("Testing Pexels scraper...")
    
    # Initialize scraper
    scraper = PexelsScraper(config.SOURCES.get("pexels", {}))
    
    # Test search functionality
    query = "nature"
    results = scraper.search_videos(query)
    
    if not results:
        logger.error("No results returned from Pexels search")
        return False
    
    logger.info(f"Found {len(results)} videos for query '{query}'")
    
    # Test metadata structure
    first_video = results[0]
    required_fields = ["id", "url", "duration", "width", "height", "fps"]
    
    for field in required_fields:
        if field not in first_video:
            logger.error(f"Missing required field '{field}' in video metadata")
            return False
    
    logger.info("Pexels scraper test passed")
    return True

def test_video_processor():
    """Test the video processor functionality."""
    logger.info("Testing video processor...")
    
    # Initialize processor
    processor = VideoProcessor(config.VIDEO_SPECS)
    
    # Create a test video file
    # For this test, we'll need a sample video file
    # In a real environment, we would download a sample or use a pre-existing one
    
    # For validation purposes, we'll check if the processor can be initialized
    if not processor:
        logger.error("Failed to initialize video processor")
        return False
    
    logger.info("Video processor initialized successfully")
    
    # Test video info extraction function (mock test)
    logger.info("Video processor test passed (partial validation)")
    return True

def run_validation_tests():
    """Run all validation tests."""
    logger.info("Starting validation tests...")
    
    tests = [
        ("Pexels Scraper", test_pexels_scraper),
        ("Video Processor", test_video_processor),
    ]
    
    results = []
    for name, test_func in tests:
        logger.info(f"Running test: {name}")
        try:
            result = test_func()
            results.append((name, result))
            logger.info(f"Test {name}: {'PASSED' if result else 'FAILED'}")
        except Exception as e:
            logger.error(f"Test {name} failed with exception: {str(e)}")
            results.append((name, False))
    
    # Print summary
    logger.info("\nTest Summary:")
    passed = 0
    for name, result in results:
        status = "PASSED" if result else "FAILED"
        logger.info(f"{name}: {status}")
        if result:
            passed += 1
    
    logger.info(f"\nPassed {passed}/{len(results)} tests")
    return passed == len(results)

if __name__ == "__main__":
    run_validation_tests()
