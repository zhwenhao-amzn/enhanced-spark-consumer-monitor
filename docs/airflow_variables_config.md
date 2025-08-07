# Airflow Variables Configuration Guide

This document provides the complete configuration reference for Airflow Variables required by the Enhanced Spark Consumer Monitor system.

## Overview

The Enhanced Spark Consumer Monitor uses Airflow Variables for dynamic configuration management. All variables are read through the `ConfigManager` class in `plugins/config_manager.py` with built-in caching, error handling, and validation.

## Required Variables

### Core Configuration

#### `msk_broker_string` (Required)
**Type**: String (comma-separated)  
**Description**: MSK broker endpoints for Kafka connection  
**Format**: `host1:port,host2:port`  
**Example**: 
```
b-2.zhwenhaomskiam.v1x1ro.c7.kafka.us-east-1.amazonaws.com:9098,b-1.zhwenhaomskiam.v1x1ro.c7.kafka.us-east-1.amazonaws.com:9098
```
**Notes**: Use port 9098 for IAM authentication, 9092 for SASL/PLAINTEXT

#### `s3_checkpoint_paths` (Required)
**Type**: JSON Array or comma-separated string  
**Description**: S3 paths containing Spark checkpoint data  
**Format**: `["s3://bucket/path1/", "s3://bucket/path2/"]` or `s3://bucket/path1/,s3://bucket/path2/`  
**Example**:
```json
["s3://zhwenhaodatalake/checkpoints/stockprice/offsets/"]
```
**Validation**: Each path must start with `s3://`

#### `kafka_topics` (Required)
**Type**: JSON Array or comma-separated string  
**Description**: Kafka topics to monitor and commit offsets for  
**Format**: `["topic1", "topic2"]` or `topic1,topic2`  
**Example**: 
```
stockprice
```

### Consumer Group Configuration

#### `checkpoint_consumer_group_mapping` (Required)
**Type**: JSON Object  
**Description**: Maps S3 checkpoint paths to specific consumer groups  
**Format**: `{"s3://path/": "consumer-group"}`  
**Example**:
```json
{"s3://zhwenhaodatalake/checkpoints/stockprice/offsets/": "stockprice-monitor"}
```

#### `kafka_consumer_group_mapping` (Required)
**Type**: JSON Object  
**Description**: Maps Kafka topics to consumer groups  
**Format**: `{"topic": "consumer-group"}`  
**Example**:
```json
{"stockprice": "stockprice-monitor"}
```

#### `kafka_consumer_group` (Required)
**Type**: String  
**Description**: Default consumer group for all topics  
**Example**: 
```
stockprice-monitor
```

### IAM Authentication

#### `msk_use_iam_auth` (Required)
**Type**: Boolean (as string or JSON boolean)  
**Description**: Enable/disable IAM authentication for MSK  
**Example**: 
```
true
```

#### `aws_region` (Required)
**Type**: String  
**Description**: AWS region for IAM authentication and AWS services  
**Example**: 
```
us-east-1
```


### Processing Configuration

#### `max_retries` (Required)
**Type**: Integer  
**Description**: Maximum retry attempts for failed operations  
**Example**: `3`

#### `retry_delay_seconds` (Required)
**Type**: Integer  
**Description**: Delay between retry attempts in seconds  
**Example**: `60`

#### `batch_size` (Required)
**Type**: Integer  
**Description**: Batch size for processing operations  
**Example**: `100`

#### `timeout_seconds` (Required)
**Type**: Integer  
**Description**: Operation timeout in seconds  
**Example**: `300`

## Configuration Examples

### Example 1: Minimal Configuration
```
msk_broker_string = b-2.zhwenhaomskiam.v1x1ro.c7.kafka.us-east-1.amazonaws.com:9098,b-1.zhwenhaomskiam.v1x1ro.c7.kafka.us-east-1.amazonaws.com:9098
s3_checkpoint_paths = ["s3://zhwenhaodatalake/checkpoints/stockprice/offsets/"]
kafka_topics = stockprice
checkpoint_consumer_group_mapping = {"s3://zhwenhaodatalake/checkpoints/stockprice/offsets/": "stockprice-monitor"}
kafka_consumer_group_mapping = {"stockprice": "stockprice-monitor"}
kafka_consumer_group = stockprice-monitor
msk_use_iam_auth = true
aws_region = us-east-1
msk_iam_role_arn = arn:aws:iam::104172191111:role/service-role/AmazonMWAA-zhwenhao-mwaa-v4-ExecutionRole
max_retries = 3
retry_delay_seconds = 60
batch_size = 100
timeout_seconds = 300
```

### Example 2: Single Topic Configuration
```
msk_broker_string = b-2.zhwenhaomskiam.v1x1ro.c7.kafka.us-east-1.amazonaws.com:9098,b-1.zhwenhaomskiam.v1x1ro.c7.kafka.us-east-1.amazonaws.com:9098
s3_checkpoint_paths = ["s3://zhwenhaodatalake/checkpoints/stockprice/offsets/"]
kafka_topics = stockprice
checkpoint_consumer_group_mapping = {"s3://zhwenhaodatalake/checkpoints/stockprice/offsets/": "stockprice-monitor"}
kafka_consumer_group_mapping = {"stockprice": "stockprice-monitor"}
kafka_consumer_group = stockprice-monitor
msk_use_iam_auth = true
aws_region = us-east-1
msk_iam_role_arn = arn:aws:iam::104172191111:role/service-role/AmazonMWAA-zhwenhao-mwaa-v4-ExecutionRole
max_retries = 3
retry_delay_seconds = 60
batch_size = 100
timeout_seconds = 300
```

### Example 3: Multiple Topics Configuration
```
msk_broker_string = b-2.cluster.kafka.us-east-1.amazonaws.com:9098,b-1.cluster.kafka.us-east-1.amazonaws.com:9098
s3_checkpoint_paths = ["s3://data-lake/checkpoints/stockprice/", "s3://data-lake/checkpoints/orderdata/"]
kafka_topics = ["stockprice", "orderdata"]
checkpoint_consumer_group_mapping = {"s3://data-lake/checkpoints/stockprice/": "stock-monitor", "s3://data-lake/checkpoints/orderdata/": "order-monitor"}
kafka_consumer_group_mapping = {"stockprice": "stock-monitor", "orderdata": "order-monitor"}
kafka_consumer_group = analytics-group
msk_use_iam_auth = true
aws_region = us-east-1
msk_iam_role_arn = arn:aws:iam::104172191111:role/service-role/AmazonMWAA-ExecutionRole
max_retries = 3
retry_delay_seconds = 60
batch_size = 100
timeout_seconds = 300
```

### Example 4: Non-IAM Configuration
```
msk_broker_string = b-2.cluster.kafka.us-east-1.amazonaws.com:9092,b-1.cluster.kafka.us-east-1.amazonaws.com:9092
s3_checkpoint_paths = s3://legacy-data/checkpoints/events/
kafka_topics = events
checkpoint_consumer_group_mapping = {"s3://legacy-data/checkpoints/events/": "events-processor"}
kafka_consumer_group_mapping = {"events": "events-processor"}
kafka_consumer_group = events-processor
msk_use_iam_auth = false
aws_region = us-east-1
msk_iam_role_arn = arn:aws:iam::104172191111:role/service-role/AmazonMWAA-ExecutionRole
max_retries = 3
retry_delay_seconds = 60
batch_size = 100
timeout_seconds = 300
```

### Example 5: Comma-Separated Format
```
msk_broker_string = broker1:9098,broker2:9098
s3_checkpoint_paths = s3://bucket1/path1/,s3://bucket2/path2/
kafka_topics = topic1,topic2,topic3
checkpoint_consumer_group_mapping = {"s3://bucket1/path1/": "group1", "s3://bucket2/path2/": "group2"}
kafka_consumer_group_mapping = {"topic1": "group1", "topic2": "group2", "topic3": "group3"}
kafka_consumer_group = default-group
msk_use_iam_auth = true
aws_region = us-west-2
msk_iam_role_arn = arn:aws:iam::104172191111:role/service-role/AmazonMWAA-ExecutionRole
max_retries = 3
retry_delay_seconds = 60
batch_size = 100
timeout_seconds = 300
```

## Variable Format Guidelines

### String Variables
- Enter as plain text without quotes
- Comma-separated values supported for lists
- Case-sensitive
- No leading/trailing whitespace

### JSON Variables
- Must use valid JSON syntax
- Use double quotes `"` not single quotes `'`
- No trailing commas
- Supports nested objects and arrays

### Boolean Variables
- Can be string: `"true"` or `"false"`
- Can be JSON boolean: `true` or `false`
- Case-insensitive for string format

### Numeric Variables
- Can be string: `"300"`
- Can be number: `300`
- Automatically converted to appropriate type

## Variable Reading Logic

The `ConfigManager` implements intelligent variable reading with the following priority:

1. **JSON Deserialization**: Attempts to parse as JSON first
2. **String Fallback**: Falls back to string parsing if JSON fails
3. **Default Values**: Uses configured defaults if variable is missing
4. **Caching**: Caches values to improve performance
5. **Error Handling**: Provides detailed error messages for troubleshooting

## Configuration Validation

The system performs comprehensive validation through `validate_configuration()`:

### Validation Checks
- ✅ **MSK Broker String**: Format validation and connectivity test
- ✅ **S3 Checkpoint Paths**: Path format and accessibility verification
- ✅ **Kafka Topics**: Topic existence and format validation
- ✅ **Consumer Group Mappings**: Consistency and completeness checks
- ✅ **IAM Authentication**: Configuration and permissions validation

### Validation Results
```python
{
    'msk_broker_string': True,
    's3_checkpoint_paths': True,
    'kafka_topics': True,
    'consumer_group_mapping': True,
    'checkpoint_consumer_group_mapping': True
}
```

## Troubleshooting

### Common Configuration Errors

#### "Expecting value: line 1 column 1 (char 0)"
**Cause**: Variable is empty or contains only whitespace  
**Solution**: Ensure variable has a valid non-empty value  
**Check**: Verify variable exists in Airflow Variables list

#### "Extra data: line 1 column X (char Y)"
**Cause**: Invalid JSON format (trailing commas, syntax errors)  
**Solution**: Validate JSON syntax using online JSON validator  
**Check**: Ensure proper quote usage and no trailing commas

#### "Invalid S3 path format: path"
**Cause**: S3 path doesn't start with `s3://`  
**Solution**: Ensure all S3 paths use format `s3://bucket/path/`  
**Check**: Verify path accessibility and permissions

#### "MSK broker string not configured"
**Cause**: `msk_broker_string` variable is missing or empty  
**Solution**: Add valid broker string with `host:port` format  
**Check**: Verify broker endpoints and port numbers

#### "Required Airflow Variable 'variable_name' not found"
**Cause**: Required variable is not configured  
**Solution**: Add the missing variable with appropriate value  
**Check**: Review required variables list above

### Debug Information

The system provides detailed logging for troubleshooting:

```python
# Enable debug logging to see variable resolution
logger.setLevel(logging.DEBUG)

# Check configuration validation results
config_manager = ConfigManager()
validation_results = config_manager.validate_configuration()
print(validation_results)

# Get complete configuration
all_config = config_manager.get_all_config()
print(all_config)
```

## Best Practices

### Configuration Management
1. **Use JSON for Complex Data**: Prefer JSON arrays/objects over comma-separated strings
2. **Validate Before Deployment**: Test configuration in development environment
3. **Document Custom Values**: Maintain documentation for environment-specific settings
4. **Use Descriptive Names**: Choose clear consumer group and topic names

### Security Considerations
1. **Enable IAM Authentication**: Use `msk_use_iam_auth = true` for production
2. **Least Privilege**: Grant minimal required IAM permissions
3. **Network Security**: Ensure proper security group configurations
4. **Audit Access**: Monitor variable changes and access patterns

### Performance Optimization
1. **Configure Timeouts**: Set appropriate timeout values for your environment
2. **Batch Processing**: Adjust batch sizes based on data volume
3. **Retry Logic**: Configure retries based on expected failure patterns
4. **Caching**: Leverage built-in variable caching for performance

## Related Documentation

- [Enhanced Spark Consumer Monitor README](../README.md)
- [IAM Authentication Implementation Guide](../README-IAM.md)
- [System Deployment Guide](../DEPLOYMENT-REPORT-V4.md)
- [Apache Airflow Variables Documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html)
