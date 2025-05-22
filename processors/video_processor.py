"""
Video processor module for handling video transformations.
"""

import os
import logging
import subprocess
import tempfile
from typing import Dict, Any, Tuple, Optional
import cv2
import numpy as np
import ffmpeg
from validators.validation_pipeline import ValidationPipeline
from storage.cloud_storage import CloudStorageUploader

logger = logging.getLogger(__name__)


def trim_video(input_path: str, output_dir: str, chunk_duration: int = 20) -> list:
    """
    Trim a video into N chunks of `chunk_duration` seconds using ffmpeg.
    Returns a list of output chunk paths.
    """
    import math
    from pathlib import Path

    video_info = ffmpeg.probe(input_path)
    duration = float(video_info["format"]["duration"])
    if duration <= chunk_duration:
        logger.info(f"Video duration {duration:.2f}s is less than or equal to chunk duration. Skipping trimming.")
        return [input_path]
    num_chunks = math.ceil(duration / chunk_duration)

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    chunk_paths = []
    base_name = Path(input_path).stem
    for i in range(num_chunks):
        start = i * chunk_duration
        output_path = os.path.join(output_dir, f"{base_name}_chunk_{i}.mp4")
        try:
            (
                ffmpeg
                .input(input_path, ss=start, t=chunk_duration)
                .output(output_path, c='copy')
                .overwrite_output()
                .run(quiet=True)
            )
            chunk_paths.append(output_path)
        except ffmpeg.Error as e:
            logger.error(f"ffmpeg error during chunking: {e.stderr.decode() if e.stderr else str(e)}")

    # Clean up empty chunk directory if no chunks were generated
    if not chunk_paths:
        try:
            Path(output_dir).rmdir()
        except OSError:
            pass
    return chunk_paths

class VideoProcessor:
    """Handles video processing tasks like resizing, format conversion, and validation."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the video processor with configuration.
        
        Args:
            config: Dictionary containing processor configuration
        """
        self.config = config
        self.target_resolution = config.get("resolution", (512, 512))
        self.min_fps = config.get("min_fps", 20)
        self.target_format = config.get("format", "mp4")
        self.min_duration = config.get("min_duration", 2)  # seconds
        
        self.logger = logging.getLogger("processor.video")
        self.logger.info(f"Initializing video processor with target resolution {self.target_resolution}")
    
    def get_video_info(self, video_path: str) -> Dict[str, Any]:
        """
        Extract metadata from video file.
        
        Args:
            video_path: Path to the video file
            
        Returns:
            Dictionary containing video metadata
        """
        try:
            probe = ffmpeg.probe(video_path)
            video_info = next((stream for stream in probe['streams'] 
                              if stream['codec_type'] == 'video'), None)
            
            if not video_info:
                self.logger.error(f"No video stream found in {video_path}")
                return {}
            
            # Extract relevant information
            width = int(video_info.get('width', 0))
            height = int(video_info.get('height', 0))
            
            # Parse frame rate which can be in the format '24/1'
            fps_str = video_info.get('r_frame_rate', '0/1')
            if '/' in fps_str:
                num, den = map(int, fps_str.split('/'))
                fps = num / den if den else 0
            else:
                fps = float(fps_str)
            
            # Get duration in seconds
            duration = float(probe.get('format', {}).get('duration', 0))
            
            # Get format
            format_name = probe.get('format', {}).get('format_name', '').split(',')[0]
            
            return {
                'width': width,
                'height': height,
                'fps': fps,
                'duration': duration,
                'format': format_name,
                'codec': video_info.get('codec_name', ''),
                'bitrate': int(probe.get('format', {}).get('bit_rate', 0)) // 1000,  # kbps
                'size': os.path.getsize(video_path) // 1024  # KB
            }
            
        except Exception as e:
            self.logger.error(f"Error getting video info: {str(e)}")
            return {}
    
    def validate_video_specs(self, video_info: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate if video meets technical requirements.
        
        Args:
            video_info: Dictionary containing video metadata
            
        Returns:
            Tuple of (is_valid, reason)
        """
        if not video_info:
            return False, "Could not extract video information"
        
        if video_info.get('duration', 0) < self.min_duration:
            return False, f"Video too short: {video_info.get('duration', 0):.2f}s < {self.min_duration}s"
        
        if video_info.get('fps', 0) < self.min_fps:
            return False, f"Frame rate too low: {video_info.get('fps', 0):.2f} < {self.min_fps}"
        
        return True, "Video meets technical specifications"
    
    def process_video(self, input_path: str) -> Dict[str, Any]:
        # create a temp file for processed output
        try:
            tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{self.target_format}")
            output_path = tmp_file.name
            tmp_file.close()

            # original processing logic
            video_info = self.get_video_info(input_path)
            if not video_info:
                processed = False
            else:
                is_valid, reason = self.validate_video_specs(video_info)
                if not is_valid:
                    self.logger.info(f"Video needs processing: {reason}")
                target_fps = max(video_info.get('fps', 0), self.min_fps)
                try:
                    (
                        ffmpeg
                        .input(input_path)
                        .filter('scale', self.target_resolution[0], self.target_resolution[1])
                        .filter('fps', fps=target_fps)
                        .output(output_path, format=self.target_format,
                                video_bitrate='2000k',
                                **{'c:v': 'libx264', 'preset': 'medium'})
                        .overwrite_output()
                        .run(quiet=True, capture_stdout=True, capture_stderr=True)
                    )
                    processed_info = self.get_video_info(output_path)
                    processed, _ = self.validate_video_specs(processed_info)
                    if processed:
                        self.logger.info(f"Successfully processed video: {input_path} -> {output_path}")
                    else:
                        self.logger.error("Processed video still invalid")
                except ffmpeg.Error as e:
                    self.logger.error(f"FFmpeg error: {e.stderr.decode() if e.stderr else str(e)}")
                    processed = False
        except Exception as e:
            self.logger.error(f"Error processing video: {str(e)}")
            processed = False

        # validation step
        validation_pipeline = ValidationPipeline(self.config)
        is_valid, validation_result = validation_pipeline.validate_video(output_path)
        overall_valid = validation_result.get("overall_valid", False)

        # upload step
        if overall_valid:
            uploader = CloudStorageUploader(self.config)
            try:
                upload_info = uploader.upload_video(output_path, {})
                uploaded = upload_info.get("success", False)
            except Exception as e:
                self.logger.error(f"Error uploading video: {str(e)}")
                uploaded = False
                upload_info = {}
        else:
            uploaded = False
            upload_info = {}

        return {
            "processed": processed,
            "output_path": output_path,
            "validated": overall_valid,
            "validation": validation_result,
            "uploaded": uploaded,
            "upload_info": upload_info
        }
    
    def detect_cut_scenes(self, video_path: str, threshold: float = 0.4) -> bool:
        """
        Detect if video contains cut scenes.
        
        Args:
            video_path: Path to the video file
            threshold: Difference threshold for scene change detection
            
        Returns:
            True if cut scenes detected, False otherwise
        """
        try:
            cap = cv2.VideoCapture(video_path)
            if not cap.isOpened():
                self.logger.error(f"Could not open video: {video_path}")
                return True  # Assume invalid if can't open
            
            # Get video properties
            fps = cap.get(cv2.CAP_PROP_FPS)
            if fps <= 0:
                self.logger.error(f"Invalid FPS: {fps}")
                return True
            
            # Initialize variables
            prev_frame = None
            frame_count = 0
            cut_scenes = 0
            
            # Process frames
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                
                # Convert to grayscale and resize for faster processing
                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                gray = cv2.resize(gray, (160, 90))
                
                if prev_frame is not None:
                    # Calculate difference between frames
                    diff = cv2.absdiff(gray, prev_frame)
                    non_zero = np.count_nonzero(diff > 25) / diff.size
                    
                    # If difference is above threshold, it might be a cut scene
                    if non_zero > threshold:
                        cut_scenes += 1
                        self.logger.debug(f"Potential cut scene at frame {frame_count}, diff: {non_zero:.4f}")
                
                prev_frame = gray
                frame_count += 1
                
                # Process every 5th frame for efficiency
                for _ in range(4):
                    cap.read()
                    frame_count += 1
            
            cap.release()
            
            # Determine if video has cut scenes (more than 2 detected cuts)
            has_cuts = cut_scenes > 2
            self.logger.info(f"Cut scene detection: {cut_scenes} cuts found in {video_path}")
            return has_cuts
            
        except Exception as e:
            self.logger.error(f"Error detecting cut scenes: {str(e)}")
            return True  # Assume invalid if error occurs
    
    def detect_text_overlay(self, video_path: str, sampling_rate: int = 30) -> bool:
        """
        Detect if video contains text overlays.
        
        Args:
            video_path: Path to the video file
            sampling_rate: Sample every Nth frame
            
        Returns:
            True if text overlay detected, False otherwise
        """
        try:
            cap = cv2.VideoCapture(video_path)
            if not cap.isOpened():
                self.logger.error(f"Could not open video: {video_path}")
                return True  # Assume invalid if can't open
            
            frame_count = 0
            text_detected = False
            
            # Process frames
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                
                # Only process every Nth frame
                if frame_count % sampling_rate == 0:
                    # Convert to grayscale
                    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                    
                    # Apply threshold to highlight potential text
                    _, thresh = cv2.threshold(gray, 180, 255, cv2.THRESH_BINARY)
                    
                    # Find contours
                    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                    
                    # Filter contours that might be text
                    text_contours = []
                    for contour in contours:
                        x, y, w, h = cv2.boundingRect(contour)
                        aspect_ratio = w / h if h > 0 else 0
                        
                        # Text typically has specific aspect ratios and sizes
                        if (0.1 < aspect_ratio < 15) and (10 < w < gray.shape[1] // 2) and (5 < h < gray.shape[0] // 4):
                            text_contours.append(contour)
                    
                    # If we find enough potential text contours in structured arrangement
                    if len(text_contours) > 5:
                        # Check if contours are aligned (potential text)
                        y_coords = [cv2.boundingRect(c)[1] for c in text_contours]
                        y_coords.sort()
                        
                        # Check if y-coordinates are clustered (text lines)
                        y_diffs = [y_coords[i+1] - y_coords[i] for i in range(len(y_coords)-1)]
                        small_diffs = [d for d in y_diffs if d < 10]
                        
                        if len(small_diffs) > 3:
                            text_detected = True
                            self.logger.info(f"Text overlay detected in {video_path} at frame {frame_count}")
                            break
                
                frame_count += 1
            
            cap.release()
            return text_detected
            
        except Exception as e:
            self.logger.error(f"Error detecting text overlay: {str(e)}")
            return True  # Assume invalid if error occurs
