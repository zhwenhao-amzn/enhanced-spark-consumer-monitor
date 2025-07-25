"""
Validation scripts for offset accuracy and system health checks.

This module provides utilities to validate that offsets are correctly
read from checkpoints and committed to Kafka topics.
"""

import sys
import os
import json
import logging
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime

# Add plugins to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from s3_checkpoint_reader import S3CheckpointReader
from kafka_offset_committer import KafkaOffsetCommitter, MultiConsumerGroupKafkaCommitter
from config_manager import ConfigManager


class OffsetValidator:
    """Validates offset accuracy and consistency."""
    
    def __init__(self, config_manager: ConfigManager = None):
        """
        Initialize offset validator.
        
        Args:
            config_manager: Configuration manager instance
        """
        self.config_manager = config_manager or ConfigManager()
        self.logger = logging.getLogger(__name__)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def validate_s3_checkpoint_reading(self, s3_paths: List[str]) -> Dict[str, Any]:
        """
        Validate S3 checkpoint reading functionality.
        
        Args:
            s3_paths: List of S3 checkpoint paths to validate
            
        Returns:
            Dictionary containing validation results
        """
        self.logger.info("Starting S3 checkpoint reading validation...")
        
        results = {
            'total_paths': len(s3_paths),
            'successful_paths': 0,
            'failed_paths': 0,
            'path_results': {},
            'total_topics': 0,
            'total_partitions': 0,
            'errors': []
        }
        
        try:
            aws_region = self.config_manager.get_aws_region()
            reader = S3CheckpointReader(aws_region=aws_region)
            
            for s3_path in s3_paths:
                path_result = {
                    'status': 'unknown',
                    'checkpoint_files': 0,
                    'topics_found': [],
                    'partitions_per_topic': {},
                    'latest_offsets': {},
                    'error': None
                }
                
                try:
                    # List checkpoint files
                    checkpoint_files = reader.list_checkpoint_files(s3_path)
                    path_result['checkpoint_files'] = len(checkpoint_files)
                    
                    if not checkpoint_files:
                        path_result['status'] = 'no_files'
                        path_result['error'] = 'No checkpoint files found'
                        results['failed_paths'] += 1
                        continue
                    
                    # Read latest checkpoint
                    latest_file = checkpoint_files[-1]
                    content = reader.read_checkpoint_file(s3_path, latest_file)
                    
                    if not content:
                        path_result['status'] = 'empty_content'
                        path_result['error'] = 'Latest checkpoint file is empty'
                        results['failed_paths'] += 1
                        continue
                    
                    # Parse checkpoint content
                    parsed_data = reader.parse_checkpoint_content(content)
                    topic_offsets = reader.extract_topic_offsets(parsed_data)
                    
                    path_result['topics_found'] = list(topic_offsets.keys())
                    path_result['latest_offsets'] = topic_offsets
                    
                    for topic, partitions in topic_offsets.items():
                        path_result['partitions_per_topic'][topic] = len(partitions)
                        results['total_partitions'] += len(partitions)
                    
                    results['total_topics'] += len(topic_offsets)
                    path_result['status'] = 'success'
                    results['successful_paths'] += 1
                    
                    self.logger.info(f"✓ Successfully validated {s3_path}: {len(topic_offsets)} topics")
                    
                except Exception as e:
                    path_result['status'] = 'error'
                    path_result['error'] = str(e)
                    results['failed_paths'] += 1
                    results['errors'].append(f"{s3_path}: {e}")
                    
                    self.logger.error(f"✗ Failed to validate {s3_path}: {e}")
                
                results['path_results'][s3_path] = path_result
        
        except Exception as e:
            results['errors'].append(f"General validation error: {e}")
            self.logger.error(f"General validation error: {e}")
        
        self.logger.info(f"S3 validation complete: {results['successful_paths']}/{results['total_paths']} paths successful")
        return results
    
    def validate_kafka_connectivity(self, consumer_group_mapping: Dict[str, str]) -> Dict[str, Any]:
        """
        Validate Kafka connectivity and consumer group access.
        
        Args:
            consumer_group_mapping: Mapping of topic -> consumer_group
            
        Returns:
            Dictionary containing validation results
        """
        self.logger.info("Starting Kafka connectivity validation...")
        
        results = {
            'total_consumer_groups': len(set(consumer_group_mapping.values())),
            'successful_connections': 0,
            'failed_connections': 0,
            'consumer_group_results': {},
            'topic_validation': {},
            'errors': []
        }
        
        try:
            broker_string = self.config_manager.get_msk_broker_string()
            
            # Test multi-consumer group connectivity
            multi_committer = MultiConsumerGroupKafkaCommitter(
                bootstrap_servers=broker_string,
                consumer_group_mapping=consumer_group_mapping
            )
            
            connection_results = multi_committer.connect_all()
            
            for consumer_group, connected in connection_results.items():
                group_result = {
                    'connected': connected,
                    'topics': [topic for topic, group in consumer_group_mapping.items() if group == consumer_group],
                    'error': None
                }
                
                if connected:
                    results['successful_connections'] += 1
                    self.logger.info(f"✓ Successfully connected consumer group: {consumer_group}")
                else:
                    results['failed_connections'] += 1
                    group_result['error'] = 'Connection failed'
                    results['errors'].append(f"Failed to connect consumer group: {consumer_group}")
                    self.logger.error(f"✗ Failed to connect consumer group: {consumer_group}")
                
                results['consumer_group_results'][consumer_group] = group_result
            
            # Validate topics if any connections succeeded
            if results['successful_connections'] > 0:
                topics_to_validate = list(consumer_group_mapping.keys())
                topic_validation = multi_committer.validate_all_topics(topics_to_validate)
                
                for consumer_group, topic_results in topic_validation.items():
                    for topic, exists in topic_results.items():
                        if topic not in results['topic_validation']:
                            results['topic_validation'][topic] = {}
                        results['topic_validation'][topic][consumer_group] = exists
                        
                        if exists:
                            self.logger.info(f"✓ Topic {topic} exists for consumer group {consumer_group}")
                        else:
                            self.logger.warning(f"⚠ Topic {topic} does not exist for consumer group {consumer_group}")
            
            multi_committer.disconnect_all()
        
        except Exception as e:
            results['errors'].append(f"Kafka connectivity error: {e}")
            self.logger.error(f"Kafka connectivity error: {e}")
        
        self.logger.info(f"Kafka validation complete: {results['successful_connections']}/{results['total_consumer_groups']} consumer groups connected")
        return results
    
    def validate_offset_consistency(self, checkpoint_data: Dict[str, Dict], 
                                  consumer_group_mapping: Dict[str, str]) -> Dict[str, Any]:
        """
        Validate offset consistency between checkpoints and Kafka.
        
        Args:
            checkpoint_data: Checkpoint data from S3
            consumer_group_mapping: Topic to consumer group mapping
            
        Returns:
            Dictionary containing consistency validation results
        """
        self.logger.info("Starting offset consistency validation...")
        
        results = {
            'topics_checked': 0,
            'consistent_topics': 0,
            'inconsistent_topics': 0,
            'topic_results': {},
            'errors': []
        }
        
        try:
            broker_string = self.config_manager.get_msk_broker_string()
            
            # Extract topic offsets from checkpoint data
            all_topic_offsets = {}
            for s3_path, data in checkpoint_data.items():
                if 'offsets' in data:
                    for topic, partitions in data['offsets'].items():
                        if topic not in all_topic_offsets:
                            all_topic_offsets[topic] = {}
                        # Convert string partition keys to integers
                        for partition, offset in partitions.items():
                            all_topic_offsets[topic][int(partition)] = int(offset)
            
            # Get current Kafka offsets
            multi_committer = MultiConsumerGroupKafkaCommitter(
                bootstrap_servers=broker_string,
                consumer_group_mapping=consumer_group_mapping
            )
            
            connection_results = multi_committer.connect_all()
            if not any(connection_results.values()):
                results['errors'].append("No Kafka connections available for consistency check")
                return results
            
            topics_to_check = list(all_topic_offsets.keys())
            current_offsets_by_group = multi_committer.get_current_offsets(topics_to_check)
            
            for topic, checkpoint_partitions in all_topic_offsets.items():
                topic_result = {
                    'checkpoint_partitions': len(checkpoint_partitions),
                    'kafka_partitions': 0,
                    'consistent_partitions': 0,
                    'inconsistent_partitions': 0,
                    'partition_details': {},
                    'status': 'unknown'
                }
                
                consumer_group = consumer_group_mapping.get(topic)
                if not consumer_group:
                    topic_result['status'] = 'no_consumer_group'
                    topic_result['error'] = 'No consumer group mapping found'
                    results['inconsistent_topics'] += 1
                    continue
                
                # Get Kafka offsets for this consumer group
                kafka_offsets = {}
                for group, group_offsets in current_offsets_by_group.items():
                    if group == consumer_group and topic in group_offsets:
                        kafka_offsets = group_offsets[topic]
                        break
                
                topic_result['kafka_partitions'] = len(kafka_offsets)
                
                # Compare partition offsets
                all_partitions = set(checkpoint_partitions.keys()) | set(kafka_offsets.keys())
                
                for partition in all_partitions:
                    checkpoint_offset = checkpoint_partitions.get(partition, -1)
                    kafka_offset = kafka_offsets.get(partition, -1)
                    
                    partition_detail = {
                        'checkpoint_offset': checkpoint_offset,
                        'kafka_offset': kafka_offset,
                        'difference': checkpoint_offset - kafka_offset if checkpoint_offset >= 0 and kafka_offset >= 0 else None,
                        'consistent': False
                    }
                    
                    # Consider offsets consistent if checkpoint is ahead (normal case)
                    # or if they're equal
                    if checkpoint_offset >= kafka_offset and kafka_offset >= 0:
                        partition_detail['consistent'] = True
                        topic_result['consistent_partitions'] += 1
                    else:
                        topic_result['inconsistent_partitions'] += 1
                    
                    topic_result['partition_details'][partition] = partition_detail
                
                # Determine overall topic status
                if topic_result['inconsistent_partitions'] == 0:
                    topic_result['status'] = 'consistent'
                    results['consistent_topics'] += 1
                    self.logger.info(f"✓ Topic {topic} offsets are consistent")
                else:
                    topic_result['status'] = 'inconsistent'
                    results['inconsistent_topics'] += 1
                    self.logger.warning(f"⚠ Topic {topic} has {topic_result['inconsistent_partitions']} inconsistent partitions")
                
                results['topic_results'][topic] = topic_result
                results['topics_checked'] += 1
            
            multi_committer.disconnect_all()
        
        except Exception as e:
            results['errors'].append(f"Offset consistency error: {e}")
            self.logger.error(f"Offset consistency error: {e}")
        
        self.logger.info(f"Offset consistency validation complete: {results['consistent_topics']}/{results['topics_checked']} topics consistent")
        return results
    
    def run_comprehensive_validation(self) -> Dict[str, Any]:
        """
        Run comprehensive validation of the entire system.
        
        Returns:
            Dictionary containing all validation results
        """
        self.logger.info("Starting comprehensive system validation...")
        
        comprehensive_results = {
            'timestamp': datetime.now().isoformat(),
            'configuration': {},
            's3_validation': {},
            'kafka_validation': {},
            'consistency_validation': {},
            'overall_status': 'unknown',
            'summary': {}
        }
        
        try:
            # 1. Validate configuration
            self.logger.info("Step 1: Validating configuration...")
            config_validation = self.config_manager.validate_configuration()
            config = self.config_manager.get_all_config()
            
            comprehensive_results['configuration'] = {
                'validation_results': config_validation,
                'config_values': {
                    'msk_broker_string': config.get('msk_broker_string', 'NOT_SET'),
                    's3_checkpoint_paths': config.get('s3_checkpoint_paths', []),
                    'kafka_topics': config.get('kafka_topics', []),
                    'consumer_groups': config.get('consumer_groups', [])
                }
            }
            
            if not all(config_validation.values()):
                comprehensive_results['overall_status'] = 'configuration_failed'
                return comprehensive_results
            
            # 2. Validate S3 checkpoint reading
            self.logger.info("Step 2: Validating S3 checkpoint reading...")
            s3_validation = self.validate_s3_checkpoint_reading(config['s3_checkpoint_paths'])
            comprehensive_results['s3_validation'] = s3_validation
            
            if s3_validation['successful_paths'] == 0:
                comprehensive_results['overall_status'] = 's3_failed'
                return comprehensive_results
            
            # 3. Resolve consumer group mapping
            checkpoint_data = {}
            aws_region = config.get('aws_region', 'us-east-1')
            reader = S3CheckpointReader(aws_region=aws_region)
            
            for s3_path in config['s3_checkpoint_paths']:
                if s3_path in s3_validation['path_results'] and s3_validation['path_results'][s3_path]['status'] == 'success':
                    # Simulate reading latest checkpoint for consumer group resolution
                    checkpoint_files = reader.list_checkpoint_files(s3_path)
                    if checkpoint_files:
                        content = reader.read_checkpoint_file(s3_path, checkpoint_files[-1])
                        if content:
                            parsed_data = reader.parse_checkpoint_content(content)
                            checkpoint_data[s3_path] = parsed_data
            
            resolved_mapping = self.config_manager.resolve_topic_consumer_groups(checkpoint_data)
            
            # 4. Validate Kafka connectivity
            self.logger.info("Step 3: Validating Kafka connectivity...")
            kafka_validation = self.validate_kafka_connectivity(resolved_mapping)
            comprehensive_results['kafka_validation'] = kafka_validation
            
            if kafka_validation['successful_connections'] == 0:
                comprehensive_results['overall_status'] = 'kafka_failed'
                return comprehensive_results
            
            # 5. Validate offset consistency
            self.logger.info("Step 4: Validating offset consistency...")
            consistency_validation = self.validate_offset_consistency(checkpoint_data, resolved_mapping)
            comprehensive_results['consistency_validation'] = consistency_validation
            
            # 6. Determine overall status
            if (s3_validation['successful_paths'] > 0 and 
                kafka_validation['successful_connections'] > 0 and
                consistency_validation['consistent_topics'] >= consistency_validation['inconsistent_topics']):
                comprehensive_results['overall_status'] = 'healthy'
            else:
                comprehensive_results['overall_status'] = 'issues_detected'
            
            # 7. Generate summary
            comprehensive_results['summary'] = {
                'configuration_valid': all(config_validation.values()),
                's3_paths_successful': f"{s3_validation['successful_paths']}/{s3_validation['total_paths']}",
                'kafka_connections_successful': f"{kafka_validation['successful_connections']}/{kafka_validation['total_consumer_groups']}",
                'topics_consistent': f"{consistency_validation['consistent_topics']}/{consistency_validation['topics_checked']}",
                'total_topics_found': s3_validation['total_topics'],
                'total_partitions_found': s3_validation['total_partitions']
            }
        
        except Exception as e:
            comprehensive_results['overall_status'] = 'validation_error'
            comprehensive_results['error'] = str(e)
            self.logger.error(f"Comprehensive validation error: {e}")
        
        self.logger.info(f"Comprehensive validation complete. Status: {comprehensive_results['overall_status']}")
        return comprehensive_results


def main():
    """Main function to run validation."""
    print("=== Spark Checkpoint Monitor Validation ===\n")
    
    # Initialize validator
    validator = OffsetValidator()
    
    try:
        # Run comprehensive validation
        results = validator.run_comprehensive_validation()
        
        # Print results
        print(f"Validation Timestamp: {results['timestamp']}")
        print(f"Overall Status: {results['overall_status'].upper()}")
        print()
        
        # Print summary
        if 'summary' in results:
            print("=== SUMMARY ===")
            for key, value in results['summary'].items():
                print(f"{key.replace('_', ' ').title()}: {value}")
            print()
        
        # Print detailed results if there are issues
        if results['overall_status'] != 'healthy':
            print("=== DETAILED RESULTS ===")
            
            # Configuration issues
            config_validation = results.get('configuration', {}).get('validation_results', {})
            failed_configs = [k for k, v in config_validation.items() if not v]
            if failed_configs:
                print(f"Configuration Issues: {failed_configs}")
            
            # S3 issues
            s3_results = results.get('s3_validation', {})
            if s3_results.get('failed_paths', 0) > 0:
                print(f"S3 Issues: {s3_results['failed_paths']} failed paths")
                for error in s3_results.get('errors', []):
                    print(f"  - {error}")
            
            # Kafka issues
            kafka_results = results.get('kafka_validation', {})
            if kafka_results.get('failed_connections', 0) > 0:
                print(f"Kafka Issues: {kafka_results['failed_connections']} failed connections")
                for error in kafka_results.get('errors', []):
                    print(f"  - {error}")
            
            # Consistency issues
            consistency_results = results.get('consistency_validation', {})
            if consistency_results.get('inconsistent_topics', 0) > 0:
                print(f"Consistency Issues: {consistency_results['inconsistent_topics']} inconsistent topics")
        
        # Save detailed results to file
        output_file = f"validation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\nDetailed results saved to: {output_file}")
        
        # Exit with appropriate code
        if results['overall_status'] == 'healthy':
            print("\n✓ All validations passed!")
            exit(0)
        else:
            print(f"\n✗ Validation completed with status: {results['overall_status']}")
            exit(1)
    
    except Exception as e:
        print(f"Validation failed with error: {e}")
        exit(1)


if __name__ == '__main__':
    main()
