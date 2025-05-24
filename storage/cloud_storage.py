"""
Cloud storage module for uploading validated videos.
"""

import os
import logging
import json
import time
from typing import Dict, Any, List, Optional
import boto3
from botocore.exceptions import ClientError
from google.cloud import storage
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)

class CloudStorageUploader:
    """Module for uploading validated videos to cloud storage."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the cloud storage uploader with configuration.
        
        Args:
            config: Dictionary containing cloud storage configuration
        """
        self.config = config
        self.logger = logging.getLogger("storage.cloud")
        
        # Cloud provider configuration
        self.provider = config.get("provider", "aws").lower()
        self.bucket_name = config.get("bucket_name", "")
        self.folder_prefix = config.get("folder_prefix", "videos/")
        
        # Ensure folder prefix ends with a slash
        if self.folder_prefix and not self.folder_prefix.endswith("/"):
            self.folder_prefix += "/"
        
        # Initialize cloud client based on provider
        self.client = self._initialize_client()
        
        # Track upload history
        self.upload_history_file = config.get("upload_history_file", "upload_history.json")
        self.upload_history = self._load_upload_history()
        
        # Configure retry settings
        self.max_retries = config.get("max_retries", 3)
        self.retry_delay = config.get("retry_delay", 2)  # seconds
    
    def _initialize_client(self):
        """
        Initialize the appropriate cloud storage client based on provider.
        
        Returns:
            Cloud storage client
        """
        try:
            if self.provider == "aws":
                # AWS S3 client
                return boto3.client(
                    's3',
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                    region_name=self.config.get("region", "us-east-1")
                )
            
            elif self.provider == "gcp":
                # Google Cloud Storage client
                return storage.Client()
            
            elif self.provider == "azure":
                # Azure Blob Storage client
                connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
                return BlobServiceClient.from_connection_string(connection_string)
            
            elif self.provider == "local":
                # Local storage does not require a client
                return None
            
            else:
                self.logger.error(f"Unsupported cloud provider: {self.provider}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error initializing cloud client for {self.provider}: {str(e)}")
            return None
    
    def upload_video(self, video_path: str, metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Upload a video file to cloud storage.
        
        Args:
            video_path: Path to the video file
            metadata: Optional metadata to store with the video
            
        Returns:
            Dictionary with upload result information
        """
        if self.provider == "local":
            try:
                # Generate local storage path using same prefix logic as cloud
                filename = os.path.basename(video_path)
                # Strip any leading "downloads_" from filename
                cloud_filename = filename
                if cloud_filename.startswith("downloads_"):
                    cloud_filename = cloud_filename[len("downloads_"):]

                # Build destination key including folder_prefix
                cloud_key = f"{self.folder_prefix}{cloud_filename}"
                local_dir = self.bucket_name or self.config.get("local_path", "/tmp/videos")
                destination = os.path.join(local_dir, cloud_key)

                # Ensure target directory exists
                os.makedirs(os.path.dirname(destination), exist_ok=True)

                # Check for duplicate
                if os.path.exists(destination):
                    self.logger.info(f"File {video_path} already exists at {destination}")
                    # Format return as (success: bool, url_or_none: Optional[str])
                    return True, None

                # Copy video locally
                import shutil
                shutil.copy2(video_path, destination)

                self.logger.info(f"Successfully copied {video_path} to local storage at {destination}")
                # Format return as (success: bool, url_or_none: Optional[str])
                return True, None

            except Exception as e:
                self.logger.error(f"Error copying video to local storage: {str(e)}")
                # Format return as (success: bool, url_or_none: Optional[str])
                return False, str(e)

        if not self.client:
            # Format return as (success: bool, url_or_none: Optional[str])
            return False, f"Cloud client not initialized for {self.provider}"
        
        if not os.path.exists(video_path):
            # Format return as (success: bool, url_or_none: Optional[str])
            return False, f"Video file not found: {video_path}"
        
        try:
            # Generate cloud storage key/path
            filename = os.path.basename(video_path)
            # Strip any leading "downloads_" from the filename
            cloud_filename = filename
            if cloud_filename.startswith("downloads_"):
                cloud_filename = cloud_filename[len("downloads_"):]
            cloud_key = f"{self.folder_prefix}{cloud_filename}"
            
            # Check if already uploaded
            if self._is_already_uploaded(video_path, cloud_key):
                self.logger.info(f"File {video_path} already uploaded as {cloud_key}")
                # Format return as (success: bool, url_or_none: Optional[str])
                return True, self._get_public_url(cloud_key)
            
            # Upload with retries
            for attempt in range(self.max_retries):
                try:
                    if self.provider == "aws":
                        # Upload to AWS S3
                        extra_args = {}
                        if metadata:
                            extra_args["Metadata"] = {k: str(v) for k, v in metadata.items() if isinstance(v, (str, int, float, bool))}
                        
                        self.client.upload_file(
                            video_path, 
                            self.bucket_name, 
                            cloud_key,
                            ExtraArgs=extra_args
                        )
                        
                    elif self.provider == "gcp":
                        # Upload to Google Cloud Storage
                        bucket = self.client.bucket(self.bucket_name)
                        blob = bucket.blob(cloud_key)
                        
                        if metadata:
                            blob.metadata = {k: str(v) for k, v in metadata.items() if isinstance(v, (str, int, float, bool))}
                        
                        blob.upload_from_filename(video_path)
                        
                    elif self.provider == "azure":
                        # Upload to Azure Blob Storage
                        blob_client = self.client.get_blob_client(
                            container=self.bucket_name, 
                            blob=cloud_key
                        )
                        
                        with open(video_path, "rb") as data:
                            blob_client.upload_blob(
                                data, 
                                overwrite=True,
                                metadata={k: str(v) for k, v in (metadata or {}).items() if isinstance(v, (str, int, float, bool))}
                            )
                    
                    # Upload successful, break retry loop
                    break
                    
                except Exception as e:
                    self.logger.warning(f"Upload attempt {attempt+1} failed: {str(e)}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                    else:
                        # Make sure we return a properly formatted error response
                        # Format return as (success: bool, url_or_none: Optional[str])
                        return False, str(e)
            
            # Record successful upload
            self._record_upload(video_path, cloud_key, metadata)

            # Attempt to delete the chunk folder if it is now empty
            parent_dir = os.path.dirname(video_path)
            try:
                if os.path.exists(parent_dir) and not os.listdir(parent_dir):
                    os.rmdir(parent_dir)
                    self.logger.info(f"Removed empty directory: {parent_dir}")
            except Exception as e:
                self.logger.warning(f"Failed to remove directory {parent_dir}: {str(e)}")

            # Also remove the top-level downloads folder if it is empty
            grandparent_dir = os.path.dirname(parent_dir)
            try:
                if os.path.exists(grandparent_dir) and not os.listdir(grandparent_dir):
                    os.rmdir(grandparent_dir)
                    self.logger.info(f"Removed empty directory: {grandparent_dir}")
            except Exception as e:
                self.logger.warning(f"Failed to remove directory {grandparent_dir}: {str(e)}")

            self.logger.info(f"Successfully uploaded {video_path} to {self.provider}:{self.bucket_name}/{cloud_key}")

            # Format return as (success: bool, url_or_none: Optional[str])
            return True, self._get_public_url(cloud_key)
            
        except Exception as e:
            self.logger.error(f"Error uploading {video_path}: {str(e)}")
            # Format return as (success: bool, url_or_none: Optional[str])
            return False, str(e)
    
    def _is_already_uploaded(self, video_path: str, cloud_key: str) -> bool:
        """
        Check if a video has already been uploaded.
        
        Args:
            video_path: Local path to the video
            cloud_key: Cloud storage key/path
            
        Returns:
            True if already uploaded, False otherwise
        """
        # First check in upload history
        file_hash = self._get_file_hash(video_path)
        if file_hash in self.upload_history:
            return True
            
        # If not in history, try to check in cloud storage
        try:
            if self.provider == "aws" and self.client:
                self.client.head_object(Bucket=self.bucket_name, Key=cloud_key)
                # If no exception, file exists
                return True
        except Exception:
            # File doesn't exist in cloud
            pass
            
        return False
    
    def _get_file_hash(self, file_path: str) -> str:
        """
        Get a hash of the file for deduplication.
        """
        import hashlib, os
        try:
            stat = os.stat(file_path)
            file_info = f"{file_path}:{stat.st_size}:{stat.st_mtime}"
            return hashlib.md5(file_info.encode()).hexdigest()
        except Exception:
            return hashlib.md5(file_path.encode()).hexdigest()
    
    def _record_upload(self, video_path: str, cloud_key: str, metadata: Dict[str, Any] = None):
        """
        Record a successful upload in history.
        
        Args:
            video_path: Local path to the video
            cloud_key: Cloud storage key/path
            metadata: Optional metadata stored with the video
        """
        try:
            file_hash = self._get_file_hash(video_path)
            
            self.upload_history[file_hash] = {
                "local_path": video_path,
                "cloud_key": cloud_key,
                "provider": self.provider,
                "bucket": self.bucket_name,
                "timestamp": time.time(),
                "metadata": metadata
            }
            
            # Save updated history
            self._save_upload_history()
            
        except Exception as e:
            self.logger.warning(f"Error recording upload history: {str(e)}")
    
    def _load_upload_history(self) -> Dict[str, Any]:
        """
        Load upload history from file.
        
        Returns:
            Dictionary of upload history
        """
        try:
            if os.path.exists(self.upload_history_file):
                with open(self.upload_history_file, "r") as f:
                    return json.load(f)
            return {}
        except Exception as e:
            self.logger.warning(f"Error loading upload history: {str(e)}")
            return {}
    
    def _save_upload_history(self):
        """Save upload history to file."""
        try:
            with open(self.upload_history_file, "w") as f:
                json.dump(self.upload_history, f, indent=2)
        except Exception as e:
            self.logger.warning(f"Error saving upload history: {str(e)}")
    
    def _get_public_url(self, cloud_key: str) -> str:
        """
        Get public URL for an uploaded file.
        
        Args:
            cloud_key: Cloud storage key/path
            
        Returns:
            Public URL string
        """
        try:
            if self.provider == "aws":
                return f"https://{self.bucket_name}.s3.amazonaws.com/{cloud_key}"
                
            elif self.provider == "gcp":
                return f"https://storage.googleapis.com/{self.bucket_name}/{cloud_key}"
                
            elif self.provider == "azure":
                account_name = self.config.get("account_name", "")
                return f"https://{account_name}.blob.core.windows.net/{self.bucket_name}/{cloud_key}"
                
            return ""
            
        except Exception:
            return ""
