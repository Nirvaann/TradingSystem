import os
import time
import logging
from pathlib import Path
from datetime import datetime
from .s3_uploader import S3Uploader

logger = logging.getLogger(__name__)

class S3BatchUploader:
    """Periodically uploads raw crawler files to S3"""
    
    def __init__(self, raw_data_dir: str, upload_interval_minutes: int = 1):
        self.raw_data_dir = Path(raw_data_dir)
        self.upload_interval = upload_interval_minutes * 60  # Convert to seconds
        self.s3_uploader = S3Uploader()
        self.last_upload_time = 0
        self.uploaded_files = set()  # Track uploaded files to avoid re-uploading
        
    def should_upload(self) -> bool:
        """Check if enough time has passed since last upload"""
        current_time = time.time()
        return (current_time - self.last_upload_time) >= self.upload_interval
    
    def get_new_files(self) -> list:
        """Get list of unprocessed JSON files in raw directory"""
        if not self.raw_data_dir.exists():
            return []
        
        new_files = []
        for json_file in self.raw_data_dir.glob("*.json"):
            file_path = str(json_file)
            if file_path not in self.uploaded_files:
                new_files.append(file_path)
        
        return new_files
    
    def upload_batch(self) -> int:
        """Upload all new files to S3"""
        if not self.should_upload():
            return 0
        
        new_files = self.get_new_files()
        
        if not new_files:
            return 0
        
        logger.info(f"⏰ Starting S3 batch upload ({len(new_files)} files)...")
        uploaded_count = 0
        
        for file_path in new_files:
            try:
                file_name = Path(file_path).name
                s3_key = f"crawler/raw/{file_name}"
                
                if self.s3_uploader.upload_file(file_path, s3_key):
                    self.uploaded_files.add(file_path)
                    uploaded_count += 1
                    
            except Exception as e:
                logger.error(f"Failed to upload {file_path}: {e}")
        
        self.last_upload_time = time.time()
        
        if uploaded_count > 0:
            logger.info(f"✅ Batch upload complete: {uploaded_count}/{len(new_files)} files uploaded")
        
        return uploaded_count