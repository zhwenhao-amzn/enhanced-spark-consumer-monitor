"""
Configuration Manager Module

This module handles configuration management using Airflow Variables
and provides centralized configuration for the Spark checkpoint monitor.
"""

import logging
from typing import Dict, List, Optional, Any
from airflow.models import Variable
from airflow.exceptions import AirflowException


class ConfigManager:
    """
    Manages configuration using Airflow Variables.
    
    Provides centralized access to configuration parameters
    with proper error handling and validation.
    """
    
    def __init__(self):
        """Initialize configuration manager."""
        self.logger = logging.getLogger(__name__)
        self._config_cache = {}
    
    def get_variable(self, key: str, default_value: Optional[Any] = None, 
                    deserialize_json: bool = False) -> Any:
        """
        Get Airflow Variable with caching and error handling.
        
        Args:
            key: Variable key name
            default_value: Default value if variable not found
            deserialize_json: Whether to deserialize JSON values
            
        Returns:
            Variable value or default value
            
        Raises:
            AirflowException: If required variable is missing and no default provided
        """
        try:
            # Check cache first
            if key in self._config_cache:
                return self._config_cache[key]
            
            # Get variable from Airflow
            value = Variable.get(
                key=key,
                default_var=default_value,
                deserialize_json=deserialize_json
            )
            
            # Cache the value
            self._config_cache[key] = value
            
            self.logger.debug(f"Retrieved variable {key}: {value}")
            return value
            
        except Exception as e:
            if default_value is not None:
                self.logger.warning(f"Failed to get variable {key}, using default: {default_value}")
                return default_value
            else:
                self.logger.error(f"Failed to get required variable {key}: {e}")
                raise AirflowException(f"Required Airflow Variable '{key}' not found")
    
    def get_msk_broker_string(self) -> str:
        """
        Get MSK broker string from Airflow Variables.
        
        Returns:
            Comma-separated MSK broker addresses
            
        Raises:
            AirflowException: If MSK broker string not configured
        """
        broker_string = self.get_variable(
            key='msk_broker_string',
            default_value=None
        )
        
        if not broker_string:
            raise AirflowException("MSK broker string not configured in Airflow Variables")
        
        # Validate broker string format
        brokers = broker_string.split(',')
        for broker in brokers:
            broker = broker.strip()
            if ':' not in broker:
                raise AirflowException(f"Invalid broker format: {broker}")
        
        self.logger.info(f"Using MSK brokers: {broker_string}")
        return broker_string
    
    def get_s3_checkpoint_paths(self) -> List[str]:
        """
        Get S3 checkpoint paths from Airflow Variables.
        
        Returns:
            List of S3 checkpoint folder paths
            
        Raises:
            AirflowException: If checkpoint paths not configured
        """
        # Try to get as JSON list first
        try:
            paths = self.get_variable(
                key='s3_checkpoint_paths',
                deserialize_json=True
            )
            
            if isinstance(paths, list):
                self.logger.info(f"Retrieved {len(paths)} checkpoint paths from JSON")
                return paths
                
        except Exception:
            pass
        
        # Fallback to comma-separated string
        paths_string = self.get_variable(
            key='s3_checkpoint_paths',
            default_value=None
        )
        
        if not paths_string:
            raise AirflowException("S3 checkpoint paths not configured in Airflow Variables")
        
        paths = [path.strip() for path in paths_string.split(',')]
        
        # Validate S3 paths
        for path in paths:
            if not path.startswith('s3://'):
                raise AirflowException(f"Invalid S3 path format: {path}")
        
        self.logger.info(f"Using checkpoint paths: {paths}")
        return paths
    
    def get_kafka_topics(self) -> List[str]:
        """
        Get Kafka topics for offset commits from Airflow Variables.
        
        Returns:
            List of Kafka topic names
            
        Raises:
            AirflowException: If topics not configured
        """
        # Try to get as JSON list first
        try:
            topics = self.get_variable(
                key='kafka_topics',
                deserialize_json=True
            )
            
            if isinstance(topics, list):
                self.logger.info(f"Retrieved {len(topics)} topics from JSON")
                return topics
                
        except Exception:
            pass
        
        # Fallback to comma-separated string
        topics_string = self.get_variable(
            key='kafka_topics',
            default_value=None
        )
        
        if not topics_string:
            raise AirflowException("Kafka topics not configured in Airflow Variables")
        
        topics = [topic.strip() for topic in topics_string.split(',')]
        
        self.logger.info(f"Using Kafka topics: {topics}")
        return topics
    
    def get_consumer_group(self) -> str:
        """
        Get default Kafka consumer group ID from Airflow Variables.
        
        Returns:
            Default consumer group ID (for backward compatibility)
        """
        return self.get_variable(
            key='kafka_consumer_group',
            default_value='spark-checkpoint-monitor'
        )
    
    def get_consumer_group_mapping(self) -> Dict[str, str]:
        """
        Get topic to consumer group mapping from Airflow Variables.
        
        Returns:
            Dictionary mapping topic names to consumer group IDs
            
        Raises:
            AirflowException: If consumer group mapping not configured properly
        """
        # Try to get as JSON mapping first
        try:
            mapping = self.get_variable(
                key='kafka_consumer_group_mapping',
                deserialize_json=True
            )
            
            if isinstance(mapping, dict):
                self.logger.info(f"Retrieved consumer group mapping for {len(mapping)} topics")
                return mapping
                
        except Exception:
            pass
        
        # Fallback to single consumer group for all topics
        default_group = self.get_consumer_group()
        topics = self.get_kafka_topics()
        
        mapping = {topic: default_group for topic in topics}
        
        self.logger.info(f"Using default consumer group '{default_group}' for all topics")
        return mapping
    
    def get_checkpoint_consumer_group_mapping(self) -> Dict[str, str]:
        """
        Get checkpoint path to consumer group mapping from Airflow Variables.
        
        Returns:
            Dictionary mapping S3 checkpoint paths to consumer group IDs
            
        Raises:
            AirflowException: If checkpoint consumer group mapping not configured properly
        """
        # Try to get checkpoint-based consumer group mapping
        try:
            mapping = self.get_variable(
                key='checkpoint_consumer_group_mapping',
                deserialize_json=True
            )
            
            if isinstance(mapping, dict):
                self.logger.info(f"Retrieved checkpoint consumer group mapping for {len(mapping)} paths")
                
                # Validate that all paths are valid S3 paths
                for path in mapping.keys():
                    if not path.startswith('s3://'):
                        raise ValueError(f"Invalid S3 path in checkpoint mapping: {path}")
                
                return mapping
                
        except Exception as e:
            self.logger.debug(f"No checkpoint consumer group mapping found: {e}")
        
        # Fallback to topic-based mapping
        topic_mapping = self.get_consumer_group_mapping()
        default_group = list(topic_mapping.values())[0] if topic_mapping else self.get_consumer_group()
        
        # Create mapping for all checkpoint paths using default group
        checkpoint_paths = self.get_s3_checkpoint_paths()
        mapping = {path: default_group for path in checkpoint_paths}
        
        self.logger.info(f"Using fallback consumer group mapping for {len(mapping)} checkpoint paths")
        return mapping
    
    def resolve_topic_consumer_groups(self, checkpoint_data: Dict[str, Dict]) -> Dict[str, str]:
        """
        Resolve topic to consumer group mapping based on checkpoint data and configuration.
        
        Args:
            checkpoint_data: Dictionary mapping S3 path -> checkpoint data
            
        Returns:
            Dictionary mapping topic -> consumer group
        """
        # Get checkpoint path to consumer group mapping
        checkpoint_mapping = self.get_checkpoint_consumer_group_mapping()
        
        # Extract topics from checkpoint data and map to consumer groups
        topic_consumer_groups = {}
        
        for s3_path, data in checkpoint_data.items():
            consumer_group = checkpoint_mapping.get(s3_path)
            if not consumer_group:
                self.logger.warning(f"No consumer group mapping found for checkpoint path: {s3_path}")
                continue
            
            # Extract topics from this checkpoint
            if 'offsets' in data:
                for topic in data['offsets'].keys():
                    if topic in topic_consumer_groups:
                        # Check for conflicts
                        existing_group = topic_consumer_groups[topic]
                        if existing_group != consumer_group:
                            self.logger.warning(
                                f"Topic '{topic}' found in multiple checkpoints with different consumer groups: "
                                f"'{existing_group}' vs '{consumer_group}'. Using latest: '{consumer_group}'"
                            )
                    
                    topic_consumer_groups[topic] = consumer_group
        
        self.logger.info(f"Resolved consumer groups for {len(topic_consumer_groups)} topics")
        for topic, group in topic_consumer_groups.items():
            self.logger.info(f"  Topic '{topic}' -> Consumer Group '{group}'")
        
        return topic_consumer_groups
    
    def get_consumer_groups(self) -> List[str]:
        """
        Get list of all unique consumer groups.
        
        Returns:
            List of unique consumer group IDs
        """
        mapping = self.get_consumer_group_mapping()
        unique_groups = list(set(mapping.values()))
        
        self.logger.info(f"Found {len(unique_groups)} unique consumer groups: {unique_groups}")
        return unique_groups
    
    def get_aws_region(self) -> str:
        """
        Get AWS region from Airflow Variables.
        
        Returns:
            AWS region name
        """
        return self.get_variable(
            key='aws_region',
            default_value='us-east-1'
        )
    
    def get_processing_config(self) -> Dict[str, Any]:
        """
        Get processing configuration parameters.
        
        Returns:
            Dictionary containing processing configuration
        """
        config = {
            'max_retries': self.get_variable('max_retries', default_value=3),
            'retry_delay_seconds': self.get_variable('retry_delay_seconds', default_value=60),
            'batch_size': self.get_variable('batch_size', default_value=100),
            'timeout_seconds': self.get_variable('timeout_seconds', default_value=300),
        }
        
        # Convert string values to appropriate types
        for key in ['max_retries', 'retry_delay_seconds', 'batch_size', 'timeout_seconds']:
            if isinstance(config[key], str) and config[key].isdigit():
                config[key] = int(config[key])
        
        return config
    
    def validate_configuration(self) -> Dict[str, bool]:
        """
        Validate all required configuration parameters.
        
        Returns:
            Dictionary mapping configuration item to validation status
        """
        validation_results = {}
        
        try:
            self.get_msk_broker_string()
            validation_results['msk_broker_string'] = True
        except Exception as e:
            self.logger.error(f"MSK broker string validation failed: {e}")
            validation_results['msk_broker_string'] = False
        
        try:
            paths = self.get_s3_checkpoint_paths()
            validation_results['s3_checkpoint_paths'] = len(paths) > 0
        except Exception as e:
            self.logger.error(f"S3 checkpoint paths validation failed: {e}")
            validation_results['s3_checkpoint_paths'] = False
        
        try:
            topics = self.get_kafka_topics()
            validation_results['kafka_topics'] = len(topics) > 0
        except Exception as e:
            self.logger.error(f"Kafka topics validation failed: {e}")
            validation_results['kafka_topics'] = False
        
        # Optional configurations
        validation_results['consumer_group'] = True  # Has default
        validation_results['aws_region'] = True      # Has default
        
        # Validate consumer group mapping
        try:
            mapping = self.get_consumer_group_mapping()
            validation_results['consumer_group_mapping'] = len(mapping) > 0
        except Exception as e:
            self.logger.error(f"Consumer group mapping validation failed: {e}")
            validation_results['consumer_group_mapping'] = False
        
        # Validate checkpoint consumer group mapping
        try:
            checkpoint_mapping = self.get_checkpoint_consumer_group_mapping()
            validation_results['checkpoint_consumer_group_mapping'] = len(checkpoint_mapping) > 0
        except Exception as e:
            self.logger.error(f"Checkpoint consumer group mapping validation failed: {e}")
            validation_results['checkpoint_consumer_group_mapping'] = False
        
        return validation_results
    
    def get_all_config(self) -> Dict[str, Any]:
        """
        Get all configuration parameters as a dictionary.
        
        Returns:
            Dictionary containing all configuration
        """
        try:
            config = {
                'msk_broker_string': self.get_msk_broker_string(),
                's3_checkpoint_paths': self.get_s3_checkpoint_paths(),
                'kafka_topics': self.get_kafka_topics(),
                'consumer_group': self.get_consumer_group(),
                'consumer_group_mapping': self.get_consumer_group_mapping(),
                'checkpoint_consumer_group_mapping': self.get_checkpoint_consumer_group_mapping(),
                'consumer_groups': self.get_consumer_groups(),
                'aws_region': self.get_aws_region(),
                'processing': self.get_processing_config()
            }
            
            return config
            
        except Exception as e:
            self.logger.error(f"Failed to get complete configuration: {e}")
            raise
    
    def clear_cache(self):
        """Clear configuration cache."""
        self._config_cache.clear()
        self.logger.debug("Configuration cache cleared")
