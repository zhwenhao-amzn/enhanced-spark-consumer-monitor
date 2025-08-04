"""
Kafka Offset Committer Module

This module handles committing offset information to MSK (Kafka) topics.
It supports multiple topics, IAM authentication, and provides robust error handling.
"""

import logging
import socket
from typing import Dict, List, Optional
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable

# IAM authentication imports
try:
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    IAM_AUTH_AVAILABLE = True
except ImportError:
    IAM_AUTH_AVAILABLE = False
    MSKAuthTokenProvider = None


class MSKTokenProvider:
    """
    Token provider for MSK IAM authentication using SASL_OAUTHBEARER mechanism.
    """
    
    def __init__(self, region: str):
        """
        Initialize MSK token provider.
        
        Args:
            region: AWS region where MSK cluster is located
        """
        if not IAM_AUTH_AVAILABLE:
            raise ImportError("aws-msk-iam-sasl-signer-python is required for IAM authentication")
        
        self.region = region
        self.logger = logging.getLogger(__name__)
    
    def token(self):
        """
        Generate MSK IAM authentication token.
        
        Returns:
            Authentication token for MSK IAM access
        """
        try:
            token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
            self.logger.debug("Successfully generated MSK IAM auth token")
            return token
        except Exception as e:
            self.logger.error(f"Failed to generate MSK IAM auth token: {e}")
            raise


class MultiConsumerGroupKafkaCommitter:
    """
    Manages multiple Kafka offset committers for different consumer groups.
    
    Supports topic-specific consumer groups and provides centralized
    management of multiple KafkaOffsetCommitter instances.
    """
    
    def __init__(self, bootstrap_servers: str, consumer_group_mapping: Dict[str, str],
                 use_iam_auth: bool = False, aws_region: str = None):
        """
        Initialize multi-consumer group committer.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses
            consumer_group_mapping: Dictionary mapping topic -> consumer_group
            use_iam_auth: Whether to use IAM authentication for MSK
            aws_region: AWS region for IAM authentication (required if use_iam_auth=True)
        """
        self.logger = logging.getLogger(__name__)
        self.bootstrap_servers = bootstrap_servers
        self.consumer_group_mapping = consumer_group_mapping
        self.use_iam_auth = use_iam_auth
        self.aws_region = aws_region
        self.committers = {}  # consumer_group -> KafkaOffsetCommitter
        
        # Validate IAM authentication requirements
        if self.use_iam_auth and not self.aws_region:
            raise ValueError("aws_region is required when use_iam_auth=True")
        
        # Group topics by consumer group
        self.group_topics = {}  # consumer_group -> [topics]
        for topic, group in consumer_group_mapping.items():
            if group not in self.group_topics:
                self.group_topics[group] = []
            self.group_topics[group].append(topic)
        
        auth_method = "IAM" if self.use_iam_auth else "Standard"
        self.logger.info(f"Initialized multi-consumer group committer for {len(self.group_topics)} groups using {auth_method} authentication")
        for group, topics in self.group_topics.items():
            self.logger.info(f"  Group '{group}': {topics}")
    
    def connect_all(self) -> Dict[str, bool]:
        """
        Connect all consumer group committers.
        
        Returns:
            Dictionary mapping consumer_group -> connection_success
        """
        connection_results = {}
        
        for consumer_group in self.group_topics.keys():
            try:
                committer = KafkaOffsetCommitter(
                    bootstrap_servers=self.bootstrap_servers,
                    consumer_group=consumer_group,
                    use_iam_auth=self.use_iam_auth,
                    aws_region=self.aws_region
                )
                
                if committer.connect():
                    self.committers[consumer_group] = committer
                    connection_results[consumer_group] = True
                    auth_method = "IAM" if self.use_iam_auth else "Standard"
                    self.logger.info(f"Connected consumer group: {consumer_group} using {auth_method} authentication")
                else:
                    connection_results[consumer_group] = False
                    self.logger.error(f"Failed to connect consumer group: {consumer_group}")
                    
            except Exception as e:
                connection_results[consumer_group] = False
                self.logger.error(f"Error connecting consumer group {consumer_group}: {e}")
        
        return connection_results
    
    def disconnect_all(self):
        """Disconnect all consumer group committers."""
        for consumer_group, committer in self.committers.items():
            try:
                committer.disconnect()
                self.logger.info(f"Disconnected consumer group: {consumer_group}")
            except Exception as e:
                self.logger.warning(f"Error disconnecting consumer group {consumer_group}: {e}")
        
        self.committers.clear()
    
    def commit_offsets(self, topic_offsets: Dict[str, Dict[int, int]]) -> Dict[str, Dict[str, bool]]:
        """
        Commit offsets for multiple topics using their respective consumer groups.
        
        Args:
            topic_offsets: Dictionary mapping topic -> partition -> offset
            
        Returns:
            Dictionary mapping consumer_group -> topic -> commit_success
        """
        results = {}
        
        # Group offsets by consumer group
        group_offsets = {}  # consumer_group -> {topic -> {partition -> offset}}
        
        for topic, partition_offsets in topic_offsets.items():
            consumer_group = self.consumer_group_mapping.get(topic)
            if not consumer_group:
                self.logger.warning(f"No consumer group mapping found for topic: {topic}")
                continue
            
            if consumer_group not in group_offsets:
                group_offsets[consumer_group] = {}
            
            group_offsets[consumer_group][topic] = partition_offsets
        
        # Commit offsets for each consumer group
        for consumer_group, offsets in group_offsets.items():
            if consumer_group not in self.committers:
                self.logger.error(f"No connected committer for consumer group: {consumer_group}")
                results[consumer_group] = {topic: False for topic in offsets.keys()}
                continue
            
            try:
                committer = self.committers[consumer_group]
                commit_results = committer.commit_offsets(offsets)
                results[consumer_group] = commit_results
                
                self.logger.info(f"Consumer group '{consumer_group}' commit results: {commit_results}")
                
            except Exception as e:
                self.logger.error(f"Error committing offsets for consumer group {consumer_group}: {e}")
                results[consumer_group] = {topic: False for topic in offsets.keys()}
        
        return results
    
    def get_current_offsets(self, topics: List[str]) -> Dict[str, Dict[str, Dict[int, int]]]:
        """
        Get current committed offsets for specified topics from their respective consumer groups.
        
        Args:
            topics: List of topic names
            
        Returns:
            Dictionary mapping consumer_group -> topic -> partition -> current_offset
        """
        results = {}
        
        # Group topics by consumer group
        group_topics = {}
        for topic in topics:
            consumer_group = self.consumer_group_mapping.get(topic)
            if consumer_group:
                if consumer_group not in group_topics:
                    group_topics[consumer_group] = []
                group_topics[consumer_group].append(topic)
        
        # Get current offsets for each consumer group
        for consumer_group, topic_list in group_topics.items():
            if consumer_group not in self.committers:
                self.logger.warning(f"No connected committer for consumer group: {consumer_group}")
                continue
            
            try:
                committer = self.committers[consumer_group]
                current_offsets = committer.get_current_offsets(topic_list)
                results[consumer_group] = current_offsets
                
            except Exception as e:
                self.logger.error(f"Error getting current offsets for consumer group {consumer_group}: {e}")
                results[consumer_group] = {}
        
        return results
    
    def validate_all_topics(self, topics: List[str]) -> Dict[str, Dict[str, bool]]:
        """
        Validate topics across all consumer groups.
        
        Args:
            topics: List of topic names to validate
            
        Returns:
            Dictionary mapping consumer_group -> topic -> exists
        """
        results = {}
        
        for consumer_group, committer in self.committers.items():
            try:
                group_topics = [t for t in topics if self.consumer_group_mapping.get(t) == consumer_group]
                if group_topics:
                    validation_results = committer.validate_topics(group_topics)
                    results[consumer_group] = validation_results
                    
            except Exception as e:
                self.logger.error(f"Error validating topics for consumer group {consumer_group}: {e}")
                results[consumer_group] = {}
        
        return results
    
    def __enter__(self):
        """Context manager entry."""
        connection_results = self.connect_all()
        failed_connections = [group for group, success in connection_results.items() if not success]
        
        if failed_connections:
            self.disconnect_all()
            raise RuntimeError(f"Failed to connect consumer groups: {failed_connections}")
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect_all()


class KafkaOffsetCommitter:
    """
    Commits offset information to Kafka topics using kafka-python library.
    
    Supports multiple topics and provides connection management with
    proper error handling for MSK environments.
    """
    
    def __init__(self, bootstrap_servers: str, consumer_group: str = 'spark-checkpoint-monitor',
                 use_iam_auth: bool = False, aws_region: str = None):
        """
        Initialize Kafka offset committer.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses
            consumer_group: Consumer group ID for offset commits
            use_iam_auth: Whether to use IAM authentication for MSK
            aws_region: AWS region for IAM authentication (required if use_iam_auth=True)
        """
        self.logger = logging.getLogger(__name__)
        self.bootstrap_servers = bootstrap_servers.split(',')
        self.consumer_group = consumer_group
        self.consumer = None
        self.use_iam_auth = use_iam_auth
        self.aws_region = aws_region
        
        # Validate IAM authentication requirements
        if self.use_iam_auth:
            if not IAM_AUTH_AVAILABLE:
                raise ImportError("aws-msk-iam-sasl-signer-python is required for IAM authentication")
            if not self.aws_region:
                raise ValueError("aws_region is required when use_iam_auth=True")
            
            self.logger.info(f"IAM authentication enabled for region: {self.aws_region}")
        
        # Kafka consumer configuration optimized for MSK
        self.consumer_config = {
            'bootstrap_servers': self.bootstrap_servers,
            'group_id': self.consumer_group,
            'auto_offset_reset': 'latest',
            'enable_auto_commit': False,  # We'll commit manually
            'session_timeout_ms': 30000,
            'heartbeat_interval_ms': 10000,
            'max_poll_interval_ms': 300000,
            'request_timeout_ms': 40000,
            'connections_max_idle_ms': 540000,
            'retry_backoff_ms': 100,
            'reconnect_backoff_ms': 50,
            'reconnect_backoff_max_ms': 1000,
        }
        
        # Add IAM authentication configuration if enabled
        if self.use_iam_auth:
            self.token_provider = MSKTokenProvider(self.aws_region)
            self.consumer_config.update({
                'security_protocol': 'SASL_SSL',
                'sasl_mechanism': 'OAUTHBEARER',
                'sasl_oauth_token_provider': self.token_provider,
                'client_id': socket.gethostname(),
            })
            self.logger.info("Configured Kafka consumer for IAM authentication")
        else:
            self.logger.info("Using standard Kafka authentication (no IAM)")
    
    def connect(self) -> bool:
        """
        Establish connection to Kafka cluster.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.logger.info(f"Connecting to Kafka brokers: {self.bootstrap_servers}")
            
            self.consumer = KafkaConsumer(**self.consumer_config)
            
            # Test connection by getting available topics
            available_topics = self.consumer.topics()
            self.logger.info(f"Successfully connected to Kafka cluster with {len(available_topics)} topics")
            
            return True
            
        except NoBrokersAvailable:
            self.logger.error(f"No Kafka brokers available at: {self.bootstrap_servers}")
            return False
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def disconnect(self):
        """Close Kafka consumer connection."""
        if self.consumer:
            try:
                self.consumer.close()
                self.logger.info("Kafka consumer connection closed")
            except Exception as e:
                self.logger.warning(f"Error closing Kafka consumer: {e}")
            finally:
                self.consumer = None
    
    def validate_topics(self, topics: List[str]) -> Dict[str, bool]:
        """
        Validate that topics exist in the Kafka cluster.
        
        Args:
            topics: List of topic names to validate
            
        Returns:
            Dictionary mapping topic name to existence status
        """
        if not self.consumer:
            raise RuntimeError("Kafka consumer not connected")
        
        topic_status = {}
        
        try:
            # Get available topics to check topic existence
            available_topics = self.consumer.topics()
            
            for topic in topics:
                topic_status[topic] = topic in available_topics
                
            self.logger.info(f"Topic validation results: {topic_status}")
            
        except Exception as e:
            self.logger.error(f"Failed to validate topics: {e}")
            # Assume topics exist if validation fails
            topic_status = {topic: True for topic in topics}
        
        return topic_status
    
    def get_topic_partitions(self, topic: str) -> List[int]:
        """
        Get list of partitions for a specific topic.
        
        Args:
            topic: Topic name
            
        Returns:
            List of partition numbers
        """
        if not self.consumer:
            raise RuntimeError("Kafka consumer not connected")
        
        try:
            partitions = self.consumer.partitions_for_topic(topic)
            if partitions is None:
                self.logger.warning(f"Topic {topic} not found or has no partitions")
                return []
            
            partition_list = sorted(list(partitions))
            self.logger.debug(f"Topic {topic} has partitions: {partition_list}")
            
            return partition_list
            
        except Exception as e:
            self.logger.error(f"Failed to get partitions for topic {topic}: {e}")
            return []
    
    def commit_offsets(self, topic_offsets: Dict[str, Dict[int, int]]) -> Dict[str, bool]:
        """
        Commit offsets for multiple topics and partitions.
        
        Args:
            topic_offsets: Dictionary mapping topic -> partition -> offset
            
        Returns:
            Dictionary mapping topic to commit success status
        """
        if not self.consumer:
            raise RuntimeError("Kafka consumer not connected")
        
        commit_results = {}
        
        for topic, partition_offsets in topic_offsets.items():
            try:
                self.logger.info(f"Committing offsets for topic: {topic}")
                
                # Validate topic exists
                topic_validation = self.validate_topics([topic])
                if not topic_validation.get(topic, False):
                    self.logger.error(f"Topic {topic} does not exist")
                    commit_results[topic] = False
                    continue
                
                # Create TopicPartition objects and offset mapping
                offsets_to_commit = {}
                
                for partition, offset in partition_offsets.items():
                    # Ensure partition is an integer (it might come as string from JSON)
                    partition_int = int(partition)
                    topic_partition = TopicPartition(topic, partition_int)
                    # Kafka expects the next offset to be consumed, so we add 1
                    # Wrap in OffsetAndMetadata as required by kafka-python
                    offsets_to_commit[topic_partition] = OffsetAndMetadata(offset + 1, None)
                    
                    self.logger.debug(f"Preparing to commit: {topic}:{partition_int} -> {offset + 1}")
                
                self.logger.info(f"About to assign partitions: {list(offsets_to_commit.keys())}")
                
                # Assign partitions to consumer (required for offset commits)
                self.consumer.assign(list(offsets_to_commit.keys()))
                
                self.logger.info(f"Assigned partitions: {self.consumer.assignment()}")
                self.logger.info(f"About to commit offsets: {offsets_to_commit}")
                
                # Commit the offsets
                self.consumer.commit(offsets=offsets_to_commit)
                
                commit_results[topic] = True
                self.logger.info(f"Successfully committed offsets for topic {topic}: {partition_offsets}")
                
            except KafkaTimeoutError:
                self.logger.error(f"Timeout while committing offsets for topic {topic}")
                commit_results[topic] = False
            except KafkaError as e:
                self.logger.error(f"Kafka error while committing offsets for topic {topic}: {e}")
                commit_results[topic] = False
            except Exception as e:
                import traceback
                self.logger.error(f"Unexpected error while committing offsets for topic {topic}: {e}")
                self.logger.error(f"Exception type: {type(e).__name__}")
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                commit_results[topic] = False
        
        return commit_results
    
    def commit_single_topic(self, topic: str, partition_offsets: Dict[int, int]) -> bool:
        """
        Commit offsets for a single topic.
        
        Args:
            topic: Topic name
            partition_offsets: Dictionary mapping partition -> offset
            
        Returns:
            True if commit successful, False otherwise
        """
        topic_offsets = {topic: partition_offsets}
        results = self.commit_offsets(topic_offsets)
        return results.get(topic, False)
    
    def get_current_offsets(self, topics: List[str]) -> Dict[str, Dict[int, int]]:
        """
        Get current committed offsets for specified topics.
        
        Args:
            topics: List of topic names
            
        Returns:
            Dictionary mapping topic -> partition -> current_offset
        """
        if not self.consumer:
            raise RuntimeError("Kafka consumer not connected")
        
        current_offsets = {}
        
        for topic in topics:
            try:
                partitions = self.get_topic_partitions(topic)
                if not partitions:
                    continue
                
                topic_partitions = [TopicPartition(topic, p) for p in partitions]
                
                # Get committed offsets
                committed = self.consumer.committed(*topic_partitions)
                
                topic_offsets = {}
                for tp in topic_partitions:
                    # Handle case where committed() returns None or OffsetAndMetadata
                    if committed is not None:
                        if isinstance(committed, dict):
                            # committed is a dictionary mapping TopicPartition -> OffsetAndMetadata
                            offset_metadata = committed.get(tp)
                            if offset_metadata is not None and hasattr(offset_metadata, 'offset'):
                                # Convert back to actual offset (subtract 1 from next offset)
                                topic_offsets[tp.partition] = max(0, offset_metadata.offset - 1)
                            else:
                                topic_offsets[tp.partition] = 0
                        else:
                            # committed might be a single OffsetAndMetadata object
                            if hasattr(committed, 'offset'):
                                topic_offsets[tp.partition] = max(0, committed.offset - 1)
                            else:
                                topic_offsets[tp.partition] = 0
                    else:
                        # No committed offsets exist for this consumer group yet
                        topic_offsets[tp.partition] = 0
                
                current_offsets[topic] = topic_offsets
                self.logger.debug(f"Current offsets for {topic}: {topic_offsets}")
                
            except Exception as e:
                self.logger.error(f"Failed to get current offsets for topic {topic}: {e}")
                current_offsets[topic] = {}
        
        return current_offsets
    
    def __enter__(self):
        """Context manager entry."""
        if not self.connect():
            raise RuntimeError("Failed to connect to Kafka")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
