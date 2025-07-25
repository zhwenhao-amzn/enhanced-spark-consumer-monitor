"""
S3 Checkpoint Reader Module

This module handles reading and parsing Spark streaming checkpoint data from S3.
It extracts offset information from checkpoint files for committing to Kafka topics.
"""

import json
import logging
from typing import Dict, List, Optional, Tuple
import boto3
from botocore.exceptions import ClientError, NoCredentialsError


class S3CheckpointReader:
    """
    Reads and parses Spark streaming checkpoint data from S3 storage.
    
    Supports multiple checkpoint folders and extracts offset information
    for committing to Kafka topics.
    """
    
    def __init__(self, aws_region: str = 'us-east-1'):
        """
        Initialize S3 checkpoint reader.
        
        Args:
            aws_region: AWS region for S3 client
        """
        self.logger = logging.getLogger(__name__)
        self.s3_client = boto3.client('s3', region_name=aws_region)
        
    def parse_s3_path(self, s3_path: str) -> Tuple[str, str]:
        """
        Parse S3 path into bucket and key components.
        
        Args:
            s3_path: Full S3 path (e.g., s3://bucket/path/to/file)
            
        Returns:
            Tuple of (bucket_name, key_path)
            
        Raises:
            ValueError: If S3 path format is invalid
        """
        if not s3_path.startswith('s3://'):
            raise ValueError(f"Invalid S3 path format: {s3_path}")
            
        path_parts = s3_path[5:].split('/', 1)
        if len(path_parts) != 2:
            raise ValueError(f"Invalid S3 path format: {s3_path}")
            
        return path_parts[0], path_parts[1]
    
    def list_checkpoint_files(self, s3_path: str) -> List[str]:
        """
        List all checkpoint files in the specified S3 path.
        
        Args:
            s3_path: S3 path to checkpoint folder
            
        Returns:
            List of checkpoint file keys
            
        Raises:
            ClientError: If S3 operation fails
        """
        try:
            bucket, prefix = self.parse_s3_path(s3_path)
            
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                self.logger.warning(f"No files found in S3 path: {s3_path}")
                return []
            
            # Filter for checkpoint files (typically numbered files) and keep metadata
            checkpoint_files = []
            for obj in response['Contents']:
                key = obj['Key']
                # Skip directories and metadata files
                if not key.endswith('/') and not key.endswith('.crc'):
                    checkpoint_files.append({
                        'key': key,
                        'last_modified': obj['LastModified']
                    })
            
            # Sort files by Last Modified time to get latest checkpoint
            checkpoint_files.sort(key=lambda x: x['last_modified'])
            self.logger.info(f"Found {len(checkpoint_files)} checkpoint files in {s3_path}")
            
            # Log details about the files and their timestamps
            if checkpoint_files:
                self.logger.debug("Checkpoint files sorted by Last Modified time:")
                for file_info in checkpoint_files:
                    self.logger.debug(f"  {file_info['key']} - {file_info['last_modified']}")
                
                latest_file_info = checkpoint_files[-1]
                self.logger.info(f"Latest checkpoint file: {latest_file_info['key']} (modified: {latest_file_info['last_modified']})")
            
            # Extract just the keys for backward compatibility
            return [file_info['key'] for file_info in checkpoint_files]
            
        except ClientError as e:
            self.logger.error(f"Failed to list checkpoint files from {s3_path}: {e}")
            raise
        except NoCredentialsError:
            self.logger.error("AWS credentials not found")
            raise
    
    def read_checkpoint_file(self, s3_path: str, file_key: str) -> Optional[str]:
        """
        Read content of a specific checkpoint file from S3.
        
        Args:
            s3_path: Base S3 path
            file_key: Specific file key to read
            
        Returns:
            File content as string, None if file doesn't exist
            
        Raises:
            ClientError: If S3 operation fails
        """
        try:
            bucket, _ = self.parse_s3_path(s3_path)
            
            response = self.s3_client.get_object(
                Bucket=bucket,
                Key=file_key
            )
            
            content = response['Body'].read().decode('utf-8')
            self.logger.debug(f"Read checkpoint file: {file_key}")
            
            return content
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                self.logger.warning(f"Checkpoint file not found: {file_key}")
                return None
            else:
                self.logger.error(f"Failed to read checkpoint file {file_key}: {e}")
                raise
    
    def parse_checkpoint_content(self, content: str) -> Dict:
        """
        Parse checkpoint file content to extract offset information.
        
        Expected format:
        Line 1: Version (e.g., "v1")
        Line 2: {"batchWatermarkMs":0,"batchTimestampMs":1752752310004,"conf":{...}}
        Line 3: {"topicname":{"partition":offset,...},...}
        
        Args:
            content: Raw checkpoint file content
            
        Returns:
            Dictionary containing parsed checkpoint data
            
        Raises:
            ValueError: If content format is invalid
            json.JSONDecodeError: If JSON parsing fails
        """
        try:
            lines = content.strip().split('\n')
            
            if len(lines) < 3:
                raise ValueError(f"Invalid checkpoint format: expected at least 3 lines (version, metadata, offsets), got {len(lines)}")
            
            # Line 1: Version (skip for now, just validate it exists)
            version = lines[0].strip()
            self.logger.debug(f"Checkpoint version: {version}")
            
            # Line 2: Parse metadata JSON
            metadata = json.loads(lines[1])
            
            # Line 3: Parse offset data JSON
            offset_data = json.loads(lines[2])
            
            parsed_data = {
                'version': version,
                'metadata': metadata,
                'offsets': offset_data,
                'batch_timestamp': metadata.get('batchTimestampMs', 0),
                'batch_watermark': metadata.get('batchWatermarkMs', 0)
            }
            
            self.logger.info(f"Parsed checkpoint version {version} with {len(offset_data)} topics")
            
            return parsed_data
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON in checkpoint content: {e}")
            self.logger.error(f"Content lines: {lines}")
            raise
        except (KeyError, ValueError) as e:
            self.logger.error(f"Invalid checkpoint content format: {e}")
            raise
    
    def get_latest_offsets(self, s3_paths: List[str]) -> Dict[str, Dict]:
        """
        Get latest offset information from multiple checkpoint folders.
        
        Args:
            s3_paths: List of S3 paths to checkpoint folders
            
        Returns:
            Dictionary mapping S3 path to latest checkpoint data
            
        Raises:
            Exception: If any checkpoint reading fails
        """
        results = {}
        
        for s3_path in s3_paths:
            try:
                self.logger.info(f"Processing checkpoint folder: {s3_path}")
                
                # List checkpoint files
                checkpoint_files = self.list_checkpoint_files(s3_path)
                
                if not checkpoint_files:
                    self.logger.warning(f"No checkpoint files found in {s3_path}")
                    continue
                
                # Get the latest checkpoint file (last in the sorted list)
                latest_file = checkpoint_files[-1]
                
                # Log some context about file selection
                if len(checkpoint_files) > 1:
                    self.logger.info(f"Found {len(checkpoint_files)} checkpoint files, selected latest: {latest_file}")
                    self.logger.debug(f"All checkpoint files (sorted by Last Modified): {checkpoint_files}")
                else:
                    self.logger.info(f"Found single checkpoint file: {latest_file}")
                
                self.logger.info(f"Reading latest checkpoint file: {latest_file}")
                
                # Read and parse the latest checkpoint
                content = self.read_checkpoint_file(s3_path, latest_file)
                
                if content:
                    parsed_data = self.parse_checkpoint_content(content)
                    results[s3_path] = parsed_data
                    
                    self.logger.info(f"Successfully processed checkpoint from {s3_path}")
                else:
                    self.logger.warning(f"Empty content in latest checkpoint file: {latest_file}")
                    
            except Exception as e:
                self.logger.error(f"Failed to process checkpoint folder {s3_path}: {e}")
                raise
        
        return results
    
    def extract_topic_offsets(self, checkpoint_data: Dict) -> Dict[str, Dict[int, int]]:
        """
        Extract topic-partition-offset mapping from checkpoint data.
        
        Args:
            checkpoint_data: Parsed checkpoint data
            
        Returns:
            Dictionary mapping topic -> partition -> offset
        """
        if 'offsets' not in checkpoint_data:
            raise ValueError("No offset data found in checkpoint")
        
        topic_offsets = {}
        
        for topic, partitions in checkpoint_data['offsets'].items():
            if isinstance(partitions, dict):
                # Convert string partition keys to integers
                topic_offsets[topic] = {
                    int(partition): int(offset) 
                    for partition, offset in partitions.items()
                }
            else:
                self.logger.warning(f"Unexpected partition data format for topic {topic}")
        
        return topic_offsets
