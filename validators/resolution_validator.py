"""
Resolution validator module for video content validation.
"""

import os
import logging
import cv2
from typing import Dict, Any, Tuple

logger = logging.getLogger(__name__)

class ResolutionValidator:
    """Validator for checking video resolution requirements."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the resolution validator.
        
        Args:
            config: Dictionary containing validator configuration
        """
        self.config = config
        self.min_width = config.get("min_width", 512)
        self.min_height = config.get("min_height", 512)
        self.logger = logging.getLogger("validator.resolution")
    
    def validate(self, video_path: str) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Validate that a video meets minimum resolution requirements.
        
        Args:
            video_path: Path to the video file
            
        Returns:
            Tuple of (is_valid, reason, details)
        """
        try:
            self.logger.info(f"Checking resolution of video: {video_path}")
            
            # Open the video file
            cap = cv2.VideoCapture(video_path)
            if not cap.isOpened():
                self.logger.error(f"Could not open video: {video_path}")
                return False, "Could not open video file", {}
            
            # Get video properties
            width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            
            # Release the video capture
            cap.release()
            
            self.logger.debug(f"Video resolution: {width}x{height}")
            
            # Check if resolution meets minimum requirements
            is_valid = width >= self.min_width and height >= self.min_height
            
            if is_valid:
                reason = f"Resolution {width}x{height} meets minimum requirements"
            else:
                reason = f"Resolution {width}x{height} does not meet minimum requirements of {self.min_width}x{self.min_height}"
            
            details = {
                "width": width,
                "height": height,
                "min_width": self.min_width,
                "min_height": self.min_height,
                "aspect_ratio": round(width / height, 2) if height > 0 else 0
            }
            
            self.logger.info(f"Resolution validation result for {video_path}: {reason}")
            return is_valid, reason, details
            
        except Exception as e:
            self.logger.error(f"Error in resolution validation: {str(e)}")
            return False, f"Error in resolution validation: {str(e)}", {}
