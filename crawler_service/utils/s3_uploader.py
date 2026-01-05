import os
import boto3
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class S3Uploader:
    """Upload crawler raw files to AWS S3"""
    
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.s3_prefix = os.getenv('S3_PREFIX', 'crawler/raw')
        
        if not self.bucket_name:
            raise ValueError("S3_BUCKET_NAME environment variable not set")
    
    def upload_file(self, file_path: str, s3_key: str = None) -> bool:
        """
        Upload a single file to S3
        
        Args:
            file_path: Local file path
            s3_key: S3 key (if None, derives from filename)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return False
            
            if s3_key is None:
                s3_key = f"{self.s3_prefix}/{Path(file_path).name}"
            
            self.s3_client.upload_file(
                file_path,
                self.bucket_name,
                s3_key,
                ExtraArgs={'ContentType': 'application/json'}
            )
            
            logger.info(f"✅ Uploaded to S3: s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ S3 upload failed for {file_path}: {e}")
            return False
    
    def upload_directory(self, directory_path: str, s3_prefix: str = None) -> int:
        """
        Upload all JSON files from a directory to S3
        
        Args:
            directory_path: Local directory path
            s3_prefix: S3 prefix (if None, uses default)
            
        Returns:
            Number of files uploaded
        """
        uploaded_count = 0
        
        if not os.path.isdir(directory_path):
            logger.error(f"Directory not found: {directory_path}")
            return 0
        
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                if file.endswith('.json'):
                    file_path = os.path.join(root, file)
                    
                    # Construct S3 key preserving directory structure
                    rel_path = os.path.relpath(file_path, directory_path)
                    s3_key = f"{s3_prefix or self.s3_prefix}/{rel_path}"
                    
                    if self.upload_file(file_path, s3_key):
                        uploaded_count += 1
        
        return uploaded_count