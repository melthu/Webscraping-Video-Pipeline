"""
Unified validation pipeline for video content.
"""

import os
import logging
import json
from typing import Dict, Any, List, Tuple, Optional

from validators.text_detection_validator import TextDetectionValidator
from validators.cut_scene_validator import CutSceneDetectionValidator
from validators.resolution_validator import ResolutionValidator
from validators.ai_content_validator import AIGeneratedContentValidator
from validators.physics_realism_validator import PhysicsRealismValidator

logger = logging.getLogger(__name__)

class ValidationPipeline:
    """Unified pipeline for validating video content against all requirements."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the validation pipeline with configuration.
        
        Args:
            config: Dictionary containing validator configurations
        """
        self.config = config
        self.logger = logging.getLogger("validator.pipeline")
        
        # Initialize individual validators
        self.text_validator = TextDetectionValidator(config.get("text_detection", {}))
        self.cut_scene_validator = CutSceneDetectionValidator(config.get("cut_scene", {}))
        self.resolution_validator = ResolutionValidator(config.get("resolution", {}))
        self.ai_content_validator = AIGeneratedContentValidator(config.get("ai_content", {}))
        self.physics_validator = PhysicsRealismValidator(config.get("physics", {}))
        
        # Configure validation log
        self.log_file = config.get("log_file", "validation.log")
        self.detailed_logs = config.get("detailed_logs", True)
    
    def validate(self, video_path: str, metadata: Dict[str, Any] = None) -> Tuple[bool, Dict[str, Any]]:
        """
        Alias for validate_video for backward compatibility with tests.
        
        Args:
            video_path: Path to the video file
            metadata: Optional metadata for the video
            
        Returns:
            Tuple of (is_valid, validation_results)
        """
        return self.validate_video(video_path, metadata)
        
    def validate_video(self, video_path: str, metadata: Dict[str, Any] = None) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate a video against all requirements.
        
        Args:
            video_path: Path to the video file
            metadata: Optional metadata for the video
            
        Returns:
            Tuple of (is_valid, validation_results)
        """
        try:
            self.logger.info(f"Starting validation for video: {video_path}")
            # Initialize results dictionary
            results = {
                "video_path": video_path,
                "validated": False,
                "validators": {}
            }
            
            if not os.path.exists(video_path):
                results["error"] = f"Video file not found: {video_path}"
                results["failed_validators"] = []
            
            # If no metadata provided, create empty dict
            if metadata is None:
                metadata = {}
            
            # Run resolution validation first (fastest check)
            try:
                resolution_valid, resolution_reason, resolution_details = self.resolution_validator.validate(video_path)
            except Exception:
                resolution_valid, resolution_reason, resolution_details = False, "Could not open video file", {}
            results["validators"]["resolution"] = {
                "valid": resolution_valid,
                "reason": resolution_reason,
                "details": resolution_details if self.detailed_logs else {}
            }
            # Short-circuit further validations if resolution fails
            if not resolution_valid:
                results["validated"] = False
                results["overall_valid"] = False
                results["failed_validators"] = ["resolution"]
                self._log_validation_result(results)
                self.logger.info(f"Validation complete for {video_path}: FAIL")
                return False, results
            
            # Run text detection validation
            try:
                text_valid, text_reason, text_details = self.text_validator.validate(video_path)
            except Exception:
                text_valid, text_reason, text_details = False, "Could not open video file", {}
            results["validators"]["text_detection"] = {
                "valid": text_valid,
                "reason": text_reason,
                "details": text_details if self.detailed_logs else {}
            }

            # Run cut scene detection validation
            try:
                cut_scene_valid, cut_scene_reason, cut_scene_details = self.cut_scene_validator.validate(video_path)
            except Exception:
                cut_scene_valid, cut_scene_reason, cut_scene_details = False, "Could not open video file", {}
            results["validators"]["cut_scene"] = {
                "valid": cut_scene_valid,
                "reason": cut_scene_reason,
                "details": cut_scene_details if self.detailed_logs else {}
            }

            # Run AI-generated content validation
            try:
                ai_valid, ai_reason, ai_details = self.ai_content_validator.validate(video_path, metadata)
            except Exception:
                ai_valid, ai_reason, ai_details = False, "Could not open video file", {}
            results["validators"]["ai_content"] = {
                "valid": ai_valid,
                "reason": ai_reason,
                "details": ai_details if self.detailed_logs else {}
            }

            # Run physics realism validation
            try:
                physics_valid, physics_reason, physics_details = self.physics_validator.validate(video_path)
            except Exception:
                physics_valid, physics_reason, physics_details = False, "Could not open video file", {}
            results["validators"]["physics"] = {
                "valid": physics_valid,
                "reason": physics_reason,
                "details": physics_details if self.detailed_logs else {}
            }

            # Determine overall validity
            overall_valid = (
                resolution_valid and
                text_valid and
                cut_scene_valid and
                ai_valid and
                physics_valid
            )

            results["validated"] = True
            results["overall_valid"] = overall_valid
            
            # Add failed validators list for test compatibility
            failed_validators = []
            if not resolution_valid:
                failed_validators.append("resolution")
            if not text_valid:
                failed_validators.append("text_detection")
            if not cut_scene_valid:
                failed_validators.append("cut_scene")
            if not ai_valid:
                failed_validators.append("ai_content")
            if not physics_valid:
                failed_validators.append("physics")
                
            results["failed_validators"] = failed_validators

            # Log the validation result
            self._log_validation_result(results)

            self.logger.info(f"Validation complete for {video_path}: {'PASS' if overall_valid else 'FAIL'}")
            return overall_valid, results
            
        except Exception as e:
            self.logger.error(f"Error in validation pipeline: {str(e)}")
            return False, {"error": str(e)}
    
    def _log_validation_result(self, results: Dict[str, Any]):
        """
        Log validation results to file.
        
        Args:
            results: Validation results dictionary
        """
        try:
            # Create a simplified version for logging
            log_entry = {
                "video_path": results["video_path"],
                "timestamp": self._get_timestamp(),
                "validated": results["validated"],
                "validators": {}
            }
            
            # Add simplified validator results
            for validator_name, validator_result in results["validators"].items():
                log_entry["validators"][validator_name] = {
                    "valid": validator_result["valid"],
                    "reason": validator_result["reason"]
                }
            
            # Write to log file
            with open(self.log_file, "a") as f:
                f.write(json.dumps(log_entry) + "\n")
                
        except Exception as e:
            self.logger.error(f"Error logging validation result: {str(e)}")
    
    def _get_timestamp(self) -> str:
        """Get current timestamp string."""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
