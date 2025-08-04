# Airflow Variables Configuration Guide

This document provides the complete configuration guide for Airflow Variables required by the Enhanced Spark Consumer Monitor system.

## Overview

The Enhanced Spark Consumer Monitor uses Airflow Variables for dynamic configuration management. All variables are read through the `ConfigManager` class in `plugins/config_manager.py`.

## Required Variables

### Core Configuration Variables

#### `msk_broker_string` (Required)
- **Type**: String (comma-separated)
- **Description**: MSK broker endpoints for Kafka connection
- **Format**: `host1:port,host2:port`
- **Example**: 
  ```
  b-2.zhwenhaomskiam.v1x1ro.c7.kafka.us-east-1.amazonaws.com:9098,b-1.zhwenhaomskiam.v1x1ro.c7.kafka.us-east-1.amazonaws.com:9098
  ```
- **Notes**: Use port 9098 for IAM authentication, 9092 for SASL/PLAINTEXT

#### `s3_checkpoint_paths` (Required)
- **Type**: JSON Array
- **Description**: List of S3 paths containing Spark checkpoint data
- **Format**: `["s3://bucket/path1/", "s3://bucket/path2/"]`
- **Example**:
  ```json
  ["s3://zhwenhaodatalake/checkpoints/stockprice/offsets/"]
  ```
- **Notes**: Each path must start with `s3://` and end with `/`

#### `kafka_topics` (Required)
- **Type**: String (comma-separated) or JSON Array
- **Description**: Kafka topics to monitor and commit offsets for
- **Format**: `topic1,topic2` or `["topic1", "topic2"]`
- **Example**: 
  ```
  stockprice
  ```
- **Notes**: Can be a single topic or multiple topics separated by commas

### Consumer Group Mapping Variables

#### `checkpoint_consumer_group_mapping` (Required)
- **Type**: JSON Object
- **Description**: Maps S3 checkpoint paths to Kafka consumer groups
- **Format**: `{"s3://path/": "consumer-group"}`
- **Example**:
  ```json
  {"s3://zhwenhaodatalake/checkpoints/stockprice/offsets/": "stockprice-monitor"}
  ```
- **Notes**: Each S3 path must match exactly with `s3_checkpoint_paths`

#### `kafka_consumer_group_mapping` (Optional)
- **Type**: JSON Object
- **Description**: Maps Kafka topics to consumer groups (fallback mapping)
- **Format**: `{"topic": "consumer-group"}`
- **Example**:
  ```json
  {"stockprice": "stockprice-monitor"}
  ```
- **Notes**: Used when checkpoint-based mapping is not available

#### `kafka_consumer_group` (Optional)
- **Type**: String
- **Description**: Default consumer group for all topics (fallback)
- **Default**: `spark-checkpoint-monitor`
- **Example**: 
  ```
  stockprice-monitor
  ```

### IAM Authentication Variables

#### `msk_use_iam_auth` (Optional)
- **Type**: Boolean (as string)
- **Description**: Enable/disable IAM authentication for MSK
- **Default**: `false`
- **Example**: 
  ```
  true
  ```
- **Notes**: Set to `true` to enable IAM authentication

#### `aws_region` (Optional)
- **Type**: String
- **Description**: AWS region for IAM authentication and other AWS services
- **Default**: `us-east-1`
- **Example**: 
  ```
  us-east-1
  ```

#### `msk_iam_role_arn` (Optional)
- **Type**: String
- **Description**: IAM role ARN for MSK authentication (if different from execution role)
- **Example**: 
  ```
  arn:aws:iam::104172191111:role/service-role/AmazonMWAA-zhwenhao-mwaa-v4-ExecutionRole
  ```
- **Notes**: Usually not needed as MWAA uses execution role by default

### Processing Configuration Variables

#### `max_retries` (Optional)
- **Type**: Integer
- **Description**: Maximum number of retries for failed operations
- **Default**: `3`
- **Example**: 
  ```
  3
  ```

#### `retry_delay_seconds` (Optional)
- **Type**: Integer
- **Description**: Delay between retries in seconds
- **Default**: `60`
- **Example**: 
  ```
  60
  ```

#### `batch_size` (Optional)
- **Type**: Integer
- **Description**: Batch size for processing operations
- **Default**: `100`
- **Example**: 
  ```
  100
  ```

#### `timeout_seconds` (Optional)
- **Type**: Integer
- **Description**: Timeout for operations in seconds
- **Default**: `300`
- **Example**: 
  ```
  300
  ```

## Variable Configuration Methods

### Method 1: Airflow Web UI (Recommended)

1. **Access Airflow Web UI**: 
   ```
   https://your-mwaa-environment.airflow.amazonaws.com
   ```

2. **Navigate to Variables**:
   - Go to `Admin` → `Variables`

3. **Add Each Variable**:
   - Click `+` (Add a new record)
   - Enter `Key` and `Val` exactly as specified
   - Click `Save`

### Method 2: Airflow CLI

```bash
# Create CLI token
TOKEN=$(aws mwaa create-cli-token --name your-environment --region us-east-1 --query 'CliToken' --output text)

# Set a variable
curl -X POST "https://your-environment.airflow.amazonaws.com/aws_mwaa/cli" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: text/plain" \
  -d "variables set kafka_topics stockprice"
```

## Variable Format Guidelines

### String Variables
- Enter as plain text without quotes
- No leading/trailing spaces
- Case-sensitive

### JSON Variables
- Must use valid JSON syntax
- Use double quotes `"` not single quotes `'`
- No trailing commas
- Validate JSON before saving

### Boolean Variables
- Enter as string: `true` or `false`
- Do not use checkbox/boolean input

### Numeric Variables
- Enter as plain numbers without quotes
- Use integers for counts and timeouts

## Configuration Validation

The system validates configuration through the `validate_configuration()` method:

```python
validation_results = config_manager.validate_configuration()
```

### Validation Checks:
- ✅ MSK broker string format and connectivity
- ✅ S3 checkpoint paths accessibility and format
- ✅ Kafka topics configuration
- ✅ Consumer group mappings consistency
- ✅ IAM authentication settings

## Troubleshooting

### Common Issues

#### "Expecting value: line 1 column 1 (char 0)"
- **Cause**: Variable is empty or contains only whitespace
- **Solution**: Ensure variable has a valid value

#### "Extra data: line 1 column X (char Y)"
- **Cause**: Invalid JSON format
- **Solution**: Validate JSON syntax, check for trailing commas

#### "Invalid S3 path format"
- **Cause**: S3 path doesn't start with `s3://`
- **Solution**: Ensure all S3 paths use correct format

#### "MSK broker string not configured"
- **Cause**: `msk_broker_string` variable is missing or empty
- **Solution**: Add valid broker string with host:port format

### Debug Commands

```bash
# List all variables
aws mwaa create-cli-token --name your-environment --region us-east-1 | \
  jq -r '.CliToken' | xargs -I {} curl -X POST \
  "https://your-environment.airflow.amazonaws.com/aws_mwaa/cli" \
  -H "Authorization: Bearer {}" -H "Content-Type: text/plain" \
  -d "variables list"

# Get specific variable
aws mwaa create-cli-token --name your-environment --region us-east-1 | \
  jq -r '.CliToken' | xargs -I {} curl -X POST \
  "https://your-environment.airflow.amazonaws.com/aws_mwaa/cli" \
  -H "Authorization: Bearer {}" -H "Content-Type: text/plain" \
  -d "variables get kafka_topics"
```

## Example Complete Configuration

```
# Core Configuration
msk_broker_string = b-2.zhwenhaomskiam.v1x1ro.c7.kafka.us-east-1.amazonaws.com:9098,b-1.zhwenhaomskiam.v1x1ro.c7.kafka.us-east-1.amazonaws.com:9098
s3_checkpoint_paths = ["s3://zhwenhaodatalake/checkpoints/stockprice/offsets/"]
kafka_topics = stockprice

# Consumer Group Mapping
checkpoint_consumer_group_mapping = {"s3://zhwenhaodatalake/checkpoints/stockprice/offsets/": "stockprice-monitor"}
kafka_consumer_group_mapping = {"stockprice": "stockprice-monitor"}
kafka_consumer_group = stockprice-monitor

# IAM Authentication
msk_use_iam_auth = true
aws_region = us-east-1

# Processing Configuration (Optional)
max_retries = 3
retry_delay_seconds = 60
timeout_seconds = 300
```

## Security Considerations

- **IAM Permissions**: Ensure MWAA execution role has necessary permissions for MSK, S3, and CloudWatch
- **Network Access**: Verify security groups allow MWAA to access MSK brokers
- **Encryption**: Use IAM authentication for secure MSK access
- **Least Privilege**: Grant minimal required permissions

## Related Documentation

- [Enhanced Spark Consumer Monitor README](../README.md)
- [IAM Authentication Guide](../README-IAM.md)
- [Deployment Guide](../DEPLOYMENT-REPORT-V4.md)
- [Apache Airflow Variables Documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html)
