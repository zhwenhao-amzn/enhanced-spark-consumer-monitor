"""
Spark Checkpoint Monitor DAG

This DAG reads Spark streaming checkpoint offsets from S3 and commits them to MSK topics.
Supports multiple checkpoint folders and topics with comprehensive error handling.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago

# Import custom modules
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from s3_checkpoint_reader import S3CheckpointReader
from kafka_offset_committer import KafkaOffsetCommitter, MultiConsumerGroupKafkaCommitter
from config_manager import ConfigManager


# DAG configuration
DAG_ID = 'spark_checkpoint_monitor'
DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Create DAG
dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description='Monitor Spark streaming checkpoints and commit offsets to MSK',
    schedule_interval=timedelta(minutes=1),  # Run every 1 minute
    max_active_runs=1,
    tags=['spark', 'kafka', 'msk', 'streaming', 'monitoring'],
)


def validate_configuration(**context) -> Dict[str, Any]:
    """
    Validate all required configuration parameters.
    
    Returns:
        Configuration dictionary
        
    Raises:
        AirflowException: If configuration validation fails
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting configuration validation")
    
    try:
        config_manager = ConfigManager()
        
        # Validate all configuration
        validation_results = config_manager.validate_configuration()
        
        failed_validations = [
            key for key, status in validation_results.items() 
            if not status
        ]
        
        if failed_validations:
            error_msg = f"Configuration validation failed for: {failed_validations}"
            logger.error(error_msg)
            raise AirflowException(error_msg)
        
        # Get complete configuration
        config = config_manager.get_all_config()
        
        logger.info("Configuration validation successful")
        logger.info(f"MSK Brokers: {config['msk_broker_string']}")
        logger.info(f"S3 Paths: {config['s3_checkpoint_paths']}")
        logger.info(f"Kafka Topics: {config['kafka_topics']}")
        logger.info(f"Consumer Group Mapping: {config['consumer_group_mapping']}")
        logger.info(f"Checkpoint Consumer Group Mapping: {config['checkpoint_consumer_group_mapping']}")
        logger.info(f"Unique Consumer Groups: {config['consumer_groups']}")
        logger.info(f"AWS Region: {config['aws_region']}")
        
        return config
        
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        raise AirflowException(f"Configuration validation failed: {e}")


def read_checkpoint_offsets(**context) -> Dict[str, Any]:
    """
    Read checkpoint offsets from S3 folders.
    
    Returns:
        Dictionary containing checkpoint data
        
    Raises:
        AirflowException: If checkpoint reading fails
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting checkpoint offset reading")
    
    try:
        # Get configuration from previous task
        config = context['task_instance'].xcom_pull(task_ids='validate_configuration')
        
        # Initialize S3 checkpoint reader
        s3_reader = S3CheckpointReader(aws_region=config['aws_region'])
        
        # Read latest offsets from all checkpoint folders
        checkpoint_data = s3_reader.get_latest_offsets(config['s3_checkpoint_paths'])
        
        if not checkpoint_data:
            raise AirflowException("No checkpoint data found in any S3 folder")
        
        # Extract topic offsets from checkpoint data
        all_topic_offsets = {}
        
        for s3_path, data in checkpoint_data.items():
            topic_offsets = s3_reader.extract_topic_offsets(data)
            
            logger.info(f"Extracted offsets from {s3_path}:")
            for topic, partitions in topic_offsets.items():
                logger.info(f"  Topic {topic}: {len(partitions)} partitions")
                
                # Merge topic offsets (later checkpoints override earlier ones)
                if topic not in all_topic_offsets:
                    all_topic_offsets[topic] = {}
                all_topic_offsets[topic].update(partitions)
        
        result = {
            'checkpoint_data': checkpoint_data,
            'topic_offsets': all_topic_offsets,
            'processed_paths': list(checkpoint_data.keys())
        }
        
        logger.info(f"Successfully read offsets for {len(all_topic_offsets)} topics")
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to read checkpoint offsets: {e}")
        raise AirflowException(f"Failed to read checkpoint offsets: {e}")


def commit_offsets_to_kafka(**context) -> Dict[str, Any]:
    """
    Commit offsets to Kafka topics.
    
    Returns:
        Dictionary containing commit results
        
    Raises:
        AirflowException: If offset commit fails
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting offset commit to Kafka")
    
    try:
        # Get configuration and checkpoint data from previous tasks
        config = context['task_instance'].xcom_pull(task_ids='validate_configuration')
        checkpoint_result = context['task_instance'].xcom_pull(task_ids='read_checkpoint_offsets')
        
        topic_offsets = checkpoint_result['topic_offsets']
        checkpoint_data = checkpoint_result['checkpoint_data']
        
        # Resolve topic to consumer group mapping based on checkpoint data
        config_manager = ConfigManager()
        resolved_consumer_group_mapping = config_manager.resolve_topic_consumer_groups(checkpoint_data)
        
        # Filter offsets for configured topics only
        configured_topics = set(config['kafka_topics'])
        filtered_offsets = {
            topic: offsets for topic, offsets in topic_offsets.items()
            if topic in configured_topics
        }
        
        if not filtered_offsets:
            logger.warning(f"No offsets found for configured topics: {configured_topics}")
            logger.warning(f"Available topics in checkpoint: {list(topic_offsets.keys())}")
            return {
                'commit_results_by_group': {},
                'successful_topics': [],
                'failed_topics': [],
                'skipped_topics': list(topic_offsets.keys()),
                'total_partitions_committed': 0,
                'consumer_groups_used': [],
                'resolved_consumer_group_mapping': resolved_consumer_group_mapping
            }
        
        # Initialize Multi-Consumer Group Kafka committer with resolved mapping
        with MultiConsumerGroupKafkaCommitter(
            bootstrap_servers=config['msk_broker_string'],
            consumer_group_mapping=resolved_consumer_group_mapping
        ) as multi_committer:
            
            # Get current offsets for comparison
            current_offsets_by_group = multi_committer.get_current_offsets(list(filtered_offsets.keys()))
            
            # Log offset comparison by consumer group
            for consumer_group, topics_offsets in current_offsets_by_group.items():
                logger.info(f"Consumer Group '{consumer_group}' offset comparison:")
                for topic, current_partitions in topics_offsets.items():
                    new_offsets = filtered_offsets.get(topic, {})
                    logger.info(f"  Topic {topic}:")
                    for partition, new_offset in new_offsets.items():
                        current_offset = current_partitions.get(partition, 0)
                        logger.info(f"    Partition {partition}: {current_offset} -> {new_offset}")
            
            # Commit offsets using multiple consumer groups
            commit_results_by_group = multi_committer.commit_offsets(filtered_offsets)
            
            # Analyze results across all consumer groups
            all_successful = True
            successful_topics = []
            failed_topics = []
            total_partitions = 0
            
            for consumer_group, topic_results in commit_results_by_group.items():
                for topic, success in topic_results.items():
                    if success:
                        successful_topics.append(f"{topic} (group: {consumer_group})")
                        total_partitions += len(filtered_offsets.get(topic, {}))
                    else:
                        failed_topics.append(f"{topic} (group: {consumer_group})")
                        all_successful = False
            
            if failed_topics:
                error_msg = f"Failed to commit offsets for: {failed_topics}"
                logger.error(error_msg)
                raise AirflowException(error_msg)
            
            result = {
                'commit_results_by_group': commit_results_by_group,
                'successful_topics': successful_topics,
                'failed_topics': failed_topics,
                'skipped_topics': [t for t in topic_offsets.keys() if t not in configured_topics],
                'total_partitions_committed': total_partitions,
                'consumer_groups_used': list(commit_results_by_group.keys()),
                'resolved_consumer_group_mapping': resolved_consumer_group_mapping
            }
            
            logger.info(f"Successfully committed offsets for {len(successful_topics)} topic-group combinations")
            logger.info(f"Consumer groups used: {result['consumer_groups_used']}")
            logger.info(f"Total partitions committed: {result['total_partitions_committed']}")
            
            return result
            
    except Exception as e:
        logger.error(f"Failed to commit offsets to Kafka: {e}")
        raise AirflowException(f"Failed to commit offsets to Kafka: {e}")


def generate_summary_report(**context) -> None:
    """
    Generate summary report of the checkpoint monitoring process.
    """
    logger = logging.getLogger(__name__)
    logger.info("Generating summary report")
    
    try:
        # Get results from all previous tasks
        config = context['task_instance'].xcom_pull(task_ids='validate_configuration')
        checkpoint_result = context['task_instance'].xcom_pull(task_ids='read_checkpoint_offsets')
        commit_result = context['task_instance'].xcom_pull(task_ids='commit_offsets_to_kafka')
        
        # Generate summary
        logger.info("=== SPARK CHECKPOINT MONITOR SUMMARY ===")
        logger.info(f"Execution Time: {datetime.now()}")
        logger.info(f"DAG Run ID: {context['dag_run'].run_id}")
        
        logger.info("\n--- Configuration ---")
        logger.info(f"S3 Checkpoint Paths: {len(config['s3_checkpoint_paths'])}")
        logger.info(f"Configured Kafka Topics: {len(config['kafka_topics'])}")
        logger.info(f"Consumer Groups: {len(config['consumer_groups'])}")
        logger.info(f"Checkpoint Consumer Group Mapping: {config['checkpoint_consumer_group_mapping']}")
        
        logger.info("\n--- Checkpoint Reading ---")
        logger.info(f"Processed S3 Paths: {len(checkpoint_result['processed_paths'])}")
        logger.info(f"Topics Found in Checkpoints: {len(checkpoint_result['topic_offsets'])}")
        
        logger.info("\n--- Consumer Group Resolution ---")
        logger.info(f"Resolved Topic -> Consumer Group Mapping: {commit_result['resolved_consumer_group_mapping']}")
        
        logger.info("\n--- Offset Commit ---")
        logger.info(f"Topics Successfully Committed: {len(commit_result['successful_topics'])}")
        logger.info(f"Consumer Groups Used: {len(commit_result['consumer_groups_used'])}")
        logger.info(f"Total Partitions Committed: {commit_result['total_partitions_committed']}")
        logger.info(f"Skipped Topics: {len(commit_result['skipped_topics'])}")
        
        if commit_result['successful_topics']:
            logger.info(f"Committed Topics: {commit_result['successful_topics']}")
        
        if commit_result['consumer_groups_used']:
            logger.info(f"Consumer Groups Used: {commit_result['consumer_groups_used']}")
        
        if commit_result['skipped_topics']:
            logger.info(f"Skipped Topics: {commit_result['skipped_topics']}")
        
        logger.info("=== END SUMMARY ===")
        
    except Exception as e:
        logger.error(f"Failed to generate summary report: {e}")
        # Don't fail the DAG for reporting issues
        pass


# Define tasks
validate_config_task = PythonOperator(
    task_id='validate_configuration',
    python_callable=validate_configuration,
    dag=dag,
)

read_checkpoints_task = PythonOperator(
    task_id='read_checkpoint_offsets',
    python_callable=read_checkpoint_offsets,
    dag=dag,
)

commit_offsets_task = PythonOperator(
    task_id='commit_offsets_to_kafka',
    python_callable=commit_offsets_to_kafka,
    dag=dag,
)

summary_report_task = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary_report,
    dag=dag,
)

# Define task dependencies
validate_config_task >> read_checkpoints_task >> commit_offsets_task >> summary_report_task
