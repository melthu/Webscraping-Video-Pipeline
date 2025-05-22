"""
Test utilities for mocking HTTP responses in scraper tests.
"""

import os
import json
import requests
from unittest.mock import MagicMock

class MockResponse:
    """Mock HTTP response for testing."""
    
    def __init__(self, status_code=200, json_data=None, content=None, headers=None):
        """
        Initialize mock response.
        
        Args:
            status_code: HTTP status code
            json_data: JSON data to return from json() method
            content: Content to return from content property
            headers: Response headers
        """
        self.status_code = status_code
        self._json_data = json_data
        self._content = content if content is not None else b""
        self.headers = headers or {}
        
        # For streaming responses
        self._iter_content_data = [self._content]
    
    def json(self):
        """Return JSON data."""
        if isinstance(self._json_data, Exception):
            raise self._json_data
        return self._json_data
    
    @property
    def content(self):
        """Return content."""
        return self._content
    
    def iter_content(self, chunk_size=1):
        """Iterate over content in chunks."""
        for chunk in self._iter_content_data:
            yield chunk
    
    def raise_for_status(self):
        """Raise exception if status code indicates an error."""
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP Error: {self.status_code}")


def create_mock_scraper_responses():
    """
    Create a dictionary of mock responses for different scrapers.
    
    Returns:
        Dictionary mapping scraper names to mock response functions
    """
    mock_responses = {}
    
    # Pexels mock response
    def mock_pexels_response(*args, **kwargs):
        if "videos/search" in args[0]:
            return MockResponse(200, {
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
            })
        elif "videos/" in args[0]:
            return MockResponse(200, {
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
            })
        else:
            return MockResponse(404)
    
    mock_responses["pexels"] = mock_pexels_response
    
    # Pixabay mock response
    def mock_pixabay_response(*args, **kwargs):
        if "videos" in args[0]:
            return MockResponse(200, {
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
            })
        else:
            return MockResponse(404)
    
    mock_responses["pixabay"] = mock_pixabay_response
    
    # Videvo mock response
    def mock_videvo_response(*args, **kwargs):
        if "videos" in args[0]:
            return MockResponse(200, {
                "data": [
                    {
                        "id": "1234",
                        "title": "Test Video",
                        "description": "A test video description",
                        "url": "https://www.videvo.net/video/test-video/1234/",
                        "thumbnail": "https://example.com/thumbnail.jpg",
                        "clip": {
                            "url": "https://example.com/video.mp4",
                            "width": 1920,
                            "height": 1080,
                            "duration": 10
                        },
                        "contributor": {
                            "name": "Test User"
                        },
                        "tags": ["nature", "landscape"]
                    }
                ],
                "meta": {
                    "pagination": {
                        "total": 1,
                        "count": 1,
                        "per_page": 10,
                        "current_page": 1,
                        "total_pages": 1
                    }
                }
            })
        else:
            return MockResponse(404)
    
    mock_responses["videvo"] = mock_videvo_response
    
    # NASA mock response
    def mock_nasa_response(*args, **kwargs):
        if "search" in args[0]:
            return MockResponse(200, {
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
            })
        elif "collection.json" in args[0]:
            return MockResponse(200, {
                "collection": {
                    "items": [
                        {
                            "href": "https://example.com/video.mp4",
                            "size": 12345678
                        }
                    ]
                }
            })
        else:
            return MockResponse(404)
    
    mock_responses["nasa"] = mock_nasa_response
    
    # Internet Archive mock response
    def mock_ia_response(*args, **kwargs):
        if "advancedsearch.php" in args[0]:
            return MockResponse(200, {
                "response": {
                    "docs": [
                        {
                            "identifier": "test-video",
                            "title": "Test Internet Archive Video",
                            "description": "A test video from Internet Archive",
                            "mediatype": "movies",
                            "format": ["h.264", "mp4"],
                            "creator": "Test Creator",
                            "subject": ["test", "archive"],
                            "downloads": 100,
                            "item_size": 12345678,
                            "publicdate": "2023-01-01T00:00:00Z"
                        }
                    ],
                    "numFound": 1
                }
            })
        else:
            return MockResponse(404)
    
    mock_responses["internet_archive"] = mock_ia_response
    
    # Wikimedia mock response
    def mock_wikimedia_response(*args, **kwargs):
        if "api.php" in args[0]:
            params = kwargs.get("params", {})
            if params.get("action") == "query" and params.get("list") == "search":
                return MockResponse(200, {
                    "query": {
                        "search": [
                            {
                                "title": "File:Test_Video.webm",
                                "pageid": 12345,
                                "snippet": "A test video from Wikimedia Commons"
                            }
                        ]
                    }
                })
            elif params.get("action") == "query" and params.get("prop") == "imageinfo":
                return MockResponse(200, {
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
                })
        return MockResponse(404)
    
    mock_responses["wikimedia"] = mock_wikimedia_response
    
    # Coverr mock response
    def mock_coverr_response(*args, **kwargs):
        if "api/videos" in args[0]:
            return MockResponse(200, {
                "data": [
                    {
                        "id": "test-video",
                        "attributes": {
                            "name": "Test Coverr Video",
                            "description": "A test video from Coverr",
                            "width": 1920,
                            "height": 1080,
                            "duration": 10,
                            "preview": "https://example.com/thumbnail.jpg",
                            "mp4": "https://example.com/video.mp4",
                            "tags": ["nature", "test"]
                        }
                    }
                ],
                "meta": {
                    "pagination": {
                        "total": 1,
                        "count": 1,
                        "per_page": 10,
                        "current_page": 1,
                        "total_pages": 1
                    }
                }
            })
        else:
            return MockResponse(404)
    
    mock_responses["coverr"] = mock_coverr_response
    
    # NOAA mock response
    def mock_noaa_response(*args, **kwargs):
        if "datasets" in args[0]:
            return MockResponse(200, {
                "results": [
                    {
                        "id": "test-dataset",
                        "name": "Test NOAA Dataset",
                        "description": "A test dataset from NOAA"
                    }
                ],
                "metadata": {
                    "resultset": {
                        "count": 1,
                        "limit": 10,
                        "offset": 0
                    }
                }
            })
        elif "data" in args[0]:
            return MockResponse(200, {
                "results": [
                    {
                        "id": "test-video",
                        "name": "Test NOAA Video",
                        "description": "A test video from NOAA",
                        "url": "https://example.com/video.mp4",
                        "thumbnail": "https://example.com/thumbnail.jpg",
                        "width": 1920,
                        "height": 1080,
                        "duration": 10,
                        "date": "2023-01-01T00:00:00Z"
                    }
                ],
                "metadata": {
                    "resultset": {
                        "count": 1,
                        "limit": 10,
                        "offset": 0
                    }
                }
            })
        else:
            return MockResponse(404)
    
    mock_responses["noaa"] = mock_noaa_response
    
    # Generic download response
    def mock_download_response(*args, **kwargs):
        return MockResponse(200, content=b"test video content")
    
    mock_responses["download"] = mock_download_response
    
    return mock_responses


# Create mock responses at module level for easy import
MOCK_RESPONSES = create_mock_scraper_responses()
