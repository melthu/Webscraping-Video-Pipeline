"""
Base test configuration and utilities for all test modules.
"""

import os
import unittest
import logging
from unittest.mock import patch

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ensure environment variables are set for tests
def setup_test_environment():
    """Set up environment variables for testing."""
    test_env_vars = {
        'PEXELS_API_KEY': 'test_pexels_key',
        'VIDEVO_API_KEY': 'test_videvo_key',
        'NASA_API_KEY': 'test_nasa_key',
        'IA_ACCESS_KEY': 'test_ia_key',
        'IA_SECRET_KEY': 'test_ia_secret',
        'NOAA_API_TOKEN': 'test_noaa_token',
        'PIXABAY_API_KEY': 'test_pixabay_key',
        'AWS_ACCESS_KEY_ID': 'test_aws_key',
        'AWS_SECRET_ACCESS_KEY': 'test_aws_secret'
    }
    
    for var_name, default_value in test_env_vars.items():
        if var_name not in os.environ:
            os.environ[var_name] = default_value

# Call setup at import time
setup_test_environment()

class BaseTestCase(unittest.TestCase):
    """Base test case with common utilities for all tests."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment once for all tests in the class."""
        # Create necessary directories
        os.makedirs(os.path.join(os.getcwd(), 'logs'), exist_ok=True)
        os.makedirs(os.path.join(os.getcwd(), 'temp'), exist_ok=True)
        os.makedirs(os.path.join(os.getcwd(), 'output'), exist_ok=True)
    
    def setUp(self):
        """Set up test environment for each test."""
        # Apply common patches
        self.patches = []
        
        # Add any common patches here
        # Example: self.patches.append(patch('module.function'))
        
        # Start all patches
        for p in self.patches:
            p.start()
    
    def tearDown(self):
        """Clean up after each test."""
        # Stop all patches
        for p in self.patches:
            p.stop()
