#!/usr/bin/env python3

import asyncio
import aiohttp
import aiofiles
import os
import os.path
import logging
import tempfile
import shutil
from typing import List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
import threading
from dataclasses import dataclass

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError

_MIN_MULTIPART_SIZE_B = 100 * 1024 * 1024  # 100MB threshold for multipart
_DEFAULT_CHUNK_SIZE_B = 64 * 1024 * 1024   # 64MB chunks
_DEFAULT_MAX_CONCURRENT = 10
_DEFAULT_MULTIPART_THRESHOLD = 64 * 1024 * 1024
_DEFAULT_MAX_CONCURRENCY = 10

_logger = logging.getLogger(__name__)


@dataclass
class DownloadProgress:
    filename: str
    total_size: int
    downloaded: int = 0
    
    @property
    def percentage(self) -> float:
        if self.total_size == 0:
            return 100.0
        return (self.downloaded / self.total_size) * 100.0


class S3ModernDownloader:
    def __init__(self, 
                 aws_access_key_id: str,
                 aws_secret_access_key: str, 
                 bucket_name: str,
                 region_name: str = 'us-east-1',
                 chunk_size_b: int = _DEFAULT_CHUNK_SIZE_B,
                 max_concurrent: int = _DEFAULT_MAX_CONCURRENT,
                 multipart_threshold: int = _DEFAULT_MULTIPART_THRESHOLD):
        
        self.bucket_name = bucket_name
        self.chunk_size_b = chunk_size_b
        self.max_concurrent = max_concurrent
        self.multipart_threshold = multipart_threshold
        
        # Configure boto3 with optimized settings
        config = Config(
            region_name=region_name,
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            max_pool_connections=50,
            # Enable transfer acceleration if available
            s3={'addressing_style': 'auto'}
        )
        
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            config=config
        )
        
        # Create transfer config for optimized downloads
        self.transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=multipart_threshold,
            max_concurrency=max_concurrent,
            multipart_chunksize=chunk_size_b,
            use_threads=True,
            max_io_queue=1000
        )
        
        self.progress_lock = threading.Lock()
        self.download_progress = {}

    def _progress_callback(self, filename: str, bytes_transferred: int):
        """Callback for tracking download progress"""
        with self.progress_lock:
            if filename in self.download_progress:
                self.download_progress[filename].downloaded += bytes_transferred
                progress = self.download_progress[filename]
                _logger.debug(f"Progress {filename}: {progress.percentage:.1f}% "
                            f"({progress.downloaded}/{progress.total_size} bytes)")

    def get_object_info(self, key_name: str) -> dict:
        """Get object metadata including size"""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=key_name)
            return {
                'size': response['ContentLength'],
                'last_modified': response['LastModified'],
                'etag': response['ETag']
            }
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise FileNotFoundError(f"Object {key_name} not found in bucket {self.bucket_name}")
            raise

    def download_single_file(self, key_name: str, local_file_path: str) -> str:
        """Download a single file using boto3's optimized transfer manager"""
        try:
            # Get object info
            obj_info = self.get_object_info(key_name)
            file_size = obj_info['size']
            
            _logger.info(f"Starting download: {key_name} ({file_size:,} bytes)")
            
            # Create directory if it doesn't exist
            local_dir = os.path.dirname(local_file_path)
            if local_dir and not os.path.exists(local_dir):
                os.makedirs(local_dir, exist_ok=True)
            
            # Initialize progress tracking
            with self.progress_lock:
                self.download_progress[key_name] = DownloadProgress(key_name, file_size)
            
            # Create progress callback
            def progress_cb(bytes_amount):
                self._progress_callback(key_name, bytes_amount)
            
            # Use boto3's high-level transfer manager for optimized downloads
            self.s3_client.download_file(
                Bucket=self.bucket_name,
                Key=key_name,
                Filename=local_file_path,
                Config=self.transfer_config,
                Callback=progress_cb
            )
            
            _logger.info(f"Successfully downloaded: {key_name} -> {local_file_path}")
            
            # Clean up progress tracking
            with self.progress_lock:
                self.download_progress.pop(key_name, None)
                
            return local_file_path
            
        except Exception as e:
            _logger.error(f"Failed to download {key_name}: {str(e)}")
            # Clean up progress tracking
            with self.progress_lock:
                self.download_progress.pop(key_name, None)
            raise

    def list_objects(self, prefix: str = "", recursive: bool = True) -> List[dict]:
        """List objects in bucket with given prefix"""
        objects = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        page_iterator = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=prefix
        )
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Skip directories (keys ending with '/')
                    if not obj['Key'].endswith('/'):
                        objects.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                            'etag': obj['ETag']
                        })
        
        _logger.info(f"Found {len(objects)} objects with prefix '{prefix}'")
        return objects

    def download_folder(self, 
                       folder_prefix: str = "", 
                       local_download_path: str = "./downloads",
                       max_workers: int = None) -> List[str]:
        """Download entire folder recursively using ThreadPoolExecutor"""
        
        if max_workers is None:
            max_workers = min(self.max_concurrent, os.cpu_count() * 2)
        
        _logger.info(f"Starting folder download: bucket={self.bucket_name}, "
                    f"prefix='{folder_prefix}', local_path={local_download_path}")
        
        # Normalize folder prefix
        if folder_prefix and not folder_prefix.endswith('/'):
            folder_prefix += '/'
        
        # Create local download directory
        os.makedirs(local_download_path, exist_ok=True)
        
        # List all objects
        objects = self.list_objects(folder_prefix)
        
        if not objects:
            _logger.warning(f"No objects found with prefix: {folder_prefix}")
            return []
        
        # Calculate total download size
        total_size = sum(obj['size'] for obj in objects)
        _logger.info(f"Total download size: {total_size:,} bytes ({total_size/1024/1024:.1f} MB)")
        
        downloaded_files = []
        failed_downloads = []
        
        def download_worker(obj):
            try:
                key_name = obj['key']
                
                # Calculate relative path
                relative_path = key_name
                if folder_prefix:
                    relative_path = key_name[len(folder_prefix):]
                
                local_file_path = os.path.join(local_download_path, relative_path)
                
                # Download the file
                result_path = self.download_single_file(key_name, local_file_path)
                return result_path
                
            except Exception as e:
                _logger.error(f"Failed to download {obj['key']}: {str(e)}")
                failed_downloads.append((obj['key'], str(e)))
                return None
        
        # Download files using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            _logger.info(f"Starting download with {max_workers} concurrent workers")
            
            # Submit all download tasks
            future_to_obj = {executor.submit(download_worker, obj): obj for obj in objects}
            
            # Collect results
            for future in future_to_obj:
                result = future.result()
                if result:
                    downloaded_files.append(result)
        
        # Report results
        _logger.info(f"Download completed: {len(downloaded_files)} files successful, "
                    f"{len(failed_downloads)} files failed")
        
        if failed_downloads:
            _logger.warning("Failed downloads:")
            for key, error in failed_downloads:
                _logger.warning(f"  {key}: {error}")
        
        return downloaded_files

    def get_progress_summary(self) -> str:
        """Get current download progress summary"""
        with self.progress_lock:
            if not self.download_progress:
                return "No active downloads"
            
            summaries = []
            for filename, progress in self.download_progress.items():
                summaries.append(f"{os.path.basename(filename)}: {progress.percentage:.1f}%")
            
            return " | ".join(summaries)


# Convenience functions for backward compatibility
def download_file(aws_access_key_id: str, aws_secret_access_key: str, 
                  bucket_name: str, key_name: str, local_file_path: str = None,
                  **kwargs) -> str:
    """Download a single file"""
    if local_file_path is None:
        local_file_path = os.path.basename(key_name) or "downloaded_file"
    
    downloader = S3ModernDownloader(aws_access_key_id, aws_secret_access_key, bucket_name, **kwargs)
    return downloader.download_single_file(key_name, local_file_path)


def download_folder(aws_access_key_id: str, aws_secret_access_key: str,
                   bucket_name: str, folder_prefix: str = "", 
                   local_download_path: str = "./downloads", **kwargs) -> List[str]:
    """Download an entire folder recursively"""
    downloader = S3ModernDownloader(aws_access_key_id, aws_secret_access_key, bucket_name, **kwargs)
    return downloader.download_folder(folder_prefix, local_download_path)


if __name__ == '__main__':
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('s3_download.log')
        ]
    )
    
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='Modern S3 Downloader with boto3')
    parser.add_argument('access_key', help='AWS Access Key ID')
    parser.add_argument('secret_key', help='AWS Secret Access Key')
    parser.add_argument('bucket_name', help='S3 Bucket Name')
    parser.add_argument('--region', default='us-east-1', help='AWS Region')
    parser.add_argument('--file', help='Single file key to download')
    parser.add_argument('--folder', help='Folder prefix to download recursively')
    parser.add_argument('--local-path', default='./downloads', help='Local download path')
    parser.add_argument('--max-workers', type=int, default=10, help='Maximum concurrent downloads')
    parser.add_argument('--chunk-size', type=int, default=_DEFAULT_CHUNK_SIZE_B, 
                       help='Chunk size in bytes')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger('boto3').setLevel(logging.DEBUG)
        logging.getLogger('botocore').setLevel(logging.DEBUG)
    
    try:
        downloader = S3ModernDownloader(
            aws_access_key_id=args.access_key,
            aws_secret_access_key=args.secret_key,
            bucket_name=args.bucket_name,
            region_name=args.region,
            chunk_size_b=args.chunk_size,
            max_concurrent=args.max_workers
        )
        
        if args.file:
            # Single file download
            local_file = os.path.join(args.local_path, os.path.basename(args.file))
            os.makedirs(args.local_path, exist_ok=True)
            result = downloader.download_single_file(args.file, local_file)
            print(f"Downloaded: {result}")
            
        elif args.folder is not None:  # Allow empty string for root
            # Folder download
            results = downloader.download_folder(
                folder_prefix=args.folder,
                local_download_path=args.local_path,
                max_workers=args.max_workers
            )
            print(f"Downloaded {len(results)} files to: {args.local_path}")
            
        else:
            parser.error("Must specify either --file or --folder")
            
    except NoCredentialsError:
        print("Error: AWS credentials not found. Please check your access key and secret key.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)