"""
Configuration module for the video pipeline.

This module loads configuration from environment variables and provides
default values for all required settings.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Base configuration for all scrapers
SCRAPER_CONFIG = {
    # Pexels configuration
    "pexels": {
        "base_url": "https://api.pexels.com/videos/search",
        "per_page": 10,
        "request_delay": 1.0  # seconds between requests to avoid rate limiting
    },
    
    # Pixabay configuration
    "pixabay": {
        "base_url": "https://pixabay.com/api/videos/",
        "per_page": 20,
        "request_delay": 0.5
    },
    
    # Videvo configuration
    "videvo": {
        "base_url": "https://www.videvo.net/api/videos/",
        "per_page": 20,
        "request_delay": 0.5
    },
    
    # NASA configuration
    "nasa": {
        "search_url": "https://images-api.nasa.gov/search",
        "per_page": 20,
        "request_delay": 0.5
    },
    
    # Internet Archive configuration
    "internet_archive": {
        "per_page": 20,
        "request_delay": 0.5
    },
    
    # Wikimedia Commons configuration
    "wikimedia": {
        "search_url": "https://commons.wikimedia.org/w/api.php",
        "file_url": "https://commons.wikimedia.org/w/api.php",
        "per_page": 20,
        "request_delay": 0.5
    },
    
    # Coverr configuration
    "coverr": {
        "base_url": "https://coverr.co",
        "search_url": "https://coverr.co/search",
        "per_page": 20,
        "request_delay": 0.5
    },
    
    # NOAA configuration
    "noaa": {
        "base_url": "https://www.ncdc.noaa.gov/cdo-web/api/v2/",
        "per_page": 20,
        "request_delay": 0.5
    }
}

# Validator configuration
VALIDATOR_CONFIG = {
    "text_detection": {
        "enabled": True,
        "min_confidence": 0.7,  # Minimum confidence for text detection
        "sample_frames": 10  # Number of frames to sample for text detection
    },
    "cut_scene": {
        "enabled": True,
        "threshold": 30.0,  # Threshold for scene change detection
        "min_scene_length": 1.0  # Minimum scene length in seconds
    },
    "resolution": {
        "enabled": True,
        "min_width": 512,  # Minimum width in pixels
        "min_height": 512  # Minimum height in pixels
    },
    "ai_content": {
        "enabled": True
    },
    "physics_realism": {
        "enabled": True,
        "flow_rate_threshold": 0.1  # Example threshold for physics realism checks
    }
}

# Cloud storage credentials
AWS_CREDENTIALS = {
    "access_key": os.getenv("AWS_ACCESS_KEY_ID", ""),
    "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    "region": os.getenv("AWS_REGION", "us-east-1")
}

GCS_CREDENTIALS = {
    "credentials_file": os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
}

AZURE_CREDENTIALS = {
    "connection_string": os.getenv("AZURE_STORAGE_CONNECTION_STRING", ""),
    "account_name": os.getenv("AZURE_STORAGE_ACCOUNT", ""),
    "account_key": os.getenv("AZURE_STORAGE_KEY", "")
}

# Alias for scrapers
SOURCES = SCRAPER_CONFIG

# Video processor specs
VIDEO_SPECS = {
    "resolution": (
        VALIDATOR_CONFIG["resolution"]["min_width"],
        VALIDATOR_CONFIG["resolution"]["min_height"]
    ),
    "min_fps": 20,
    "format": "mp4",
    "min_duration": 2  # seconds
}
