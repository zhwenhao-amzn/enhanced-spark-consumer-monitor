# Airflow Variables Configuration

This document provides the required Airflow Variables configuration for the Spark Checkpoint Monitor DAG.

## Required Variables

### 1. MSK Broker String
**Variable Key:** `msk_broker_string`  
**Type:** String  
**Description:** Comma-separated list of MSK broker addresses  
**Example:**
```
b-1.zhwenhaomsk.88je25.c5.kafka.us-east-1.amazonaws.com:9092,b-2.zhwenhaomsk.88je25.c5.kafka.us-east-1.amazonaws.com:9092
```

### 2. S3 Checkpoint Paths
**Variable Key:** `s3_checkpoint_paths`  
**Type:** JSON Array or Comma-separated String  
**Description:** List of S3 paths containing Spark streaming checkpoints  

**JSON Format (Recommended):**
```json
[
  "s3://zhwenhaodatalake/checkpoints/stockprice/offsets/",
  "s3://zhwenhaodatalake/checkpoints/userdata/offsets/",
  "s3://zhwenhaodatalake/checkpoints/transactions/offsets/"
]
```

**String Format (Alternative):**
```
s3://zhwenhaodatalake/checkpoints/stockprice/offsets/,s3://zhwenhaodatalake/checkpoints/userdata/offsets/
```

### 3. Kafka Topics
**Variable Key:** `kafka_topics`  
**Type:** JSON Array or Comma-separated String  
**Description:** List of Kafka topics to commit offsets to  

**JSON Format (Recommended):**
```json
[
  "stockprice",
  "userdata",
  "transactions"
]
```

**String Format (Alternative):**
```
stockprice,userdata,transactions
```

## Optional Variables (with defaults)

### 4. Checkpoint Consumer Group Mapping (NEW - Advanced Multiple Consumer Groups)
**Variable Key:** `checkpoint_consumer_group_mapping`  
**Type:** JSON Object  
**Description:** Maps each S3 checkpoint path to its specific consumer group (supports same topic with different consumer groups)

**JSON Format:**
```json
{
  "s3://zhwenhaodatalake/checkpoints/sp1/stockprice/offsets/": "sp1",
  "s3://zhwenhaodatalake/checkpoints/sp2/stockprice/offsets/": "sp2"
}
```

**Use Case:** When multiple Spark applications process the same Kafka topic but need separate consumer groups

### 5. Consumer Group Mapping (Standard Multiple Consumer Groups)
**Variable Key:** `kafka_consumer_group_mapping`  
**Type:** JSON Object  
**Description:** Maps each Kafka topic to its specific consumer group  

**JSON Format:**
```json
{
  "stockprice": "stock-consumer-group",
  "userdata": "user-consumer-group", 
  "transactions": "transaction-consumer-group"
}
```

### 6. Kafka Consumer Group (Fallback/Default)
**Variable Key:** `kafka_consumer_group`  
**Type:** String  
**Default:** `spark-checkpoint-monitor`  
**Description:** Default consumer group ID (used if mapping not provided)  
**Example:**
```
spark-checkpoint-monitor
```

### 6. AWS Region
**Variable Key:** `aws_region`  
**Type:** String  
**Default:** `us-east-1`  
**Description:** AWS region for S3 and other AWS services  
**Example:**
```
us-east-1
```

### 6. Processing Configuration
**Variable Key:** `max_retries`  
**Type:** String (converted to int)  
**Default:** `3`  
**Description:** Maximum number of retries for failed operations  

**Variable Key:** `retry_delay_seconds`  
**Type:** String (converted to int)  
**Default:** `60`  
**Description:** Delay between retries in seconds  

**Variable Key:** `batch_size`  
**Type:** String (converted to int)  
**Default:** `100`  
**Description:** Batch size for processing operations  

**Variable Key:** `timeout_seconds`  
**Type:** String (converted to int)  
**Default:** `300`  
**Description:** Timeout for operations in seconds  

## Setting Variables in Airflow

### Method 1: Airflow Web UI
1. Navigate to Admin → Variables
2. Click "+" to add a new variable
3. Enter the Key and Value
4. For JSON values, ensure proper JSON formatting

### Method 2: Airflow CLI
```bash
# Set string variables
airflow variables set msk_broker_string "b-1.zhwenhaomsk.88je25.c5.kafka.us-east-1.amazonaws.com:9092,b-2.zhwenhaomsk.88je25.c5.kafka.us-east-1.amazonaws.com:9092"

# Set JSON variables
airflow variables set s3_checkpoint_paths '["s3://zhwenhaodatalake/checkpoints/stockprice/offsets/"]'
airflow variables set kafka_topics '["stockprice"]'

# Set consumer group mapping (NEW - Multiple Consumer Groups)
airflow variables set kafka_consumer_group_mapping '{"stockprice": "spark-stock-monitor"}'

# Set optional variables
airflow variables set kafka_consumer_group "spark-checkpoint-monitor"
airflow variables set aws_region "us-east-1"
```

### Method 3: Environment Variables (MWAA)
For Amazon MWAA, you can also set these as environment variables with the prefix `AIRFLOW_VAR_`:

```bash
AIRFLOW_VAR_MSK_BROKER_STRING="b-1.zhwenhaomsk.88je25.c5.kafka.us-east-1.amazonaws.com:9092,b-2.zhwenhaomsk.88je25.c5.kafka.us-east-1.amazonaws.com:9092"
AIRFLOW_VAR_S3_CHECKPOINT_PATHS='["s3://zhwenhaodatalake/checkpoints/stockprice/offsets/"]'
AIRFLOW_VAR_KAFKA_TOPICS='["stockprice"]'
AIRFLOW_VAR_KAFKA_CONSUMER_GROUP_MAPPING='{"stockprice": "spark-stock-monitor"}'
```

## Configuration Examples

### Single Consumer Group (Simple Setup)
```bash
# Required variables
airflow variables set msk_broker_string "b-1.zhwenhaomsk.88je25.c5.kafka.us-east-1.amazonaws.com:9092,b-2.zhwenhaomsk.88je25.c5.kafka.us-east-1.amazonaws.com:9092"
airflow variables set s3_checkpoint_paths '["s3://zhwenhaodatalake/checkpoints/stockprice/offsets/"]'
airflow variables set kafka_topics '["stockprice"]'

# Optional: Set default consumer group (will be used for all topics)
airflow variables set kafka_consumer_group "spark-checkpoint-monitor"
```

### Multiple Spark Applications - Same Topic (Advanced Setup)
**Scenario:** Two Spark applications (sp1, sp2) processing the same topic (stockprice) with different consumer groups

```bash
# Required variables
airflow variables set msk_broker_string "b-1.zhwenhaomsk.88je25.c5.kafka.us-east-1.amazonaws.com:9092,b-2.zhwenhaomsk.88je25.c5.kafka.us-east-1.amazonaws.com:9092"

# Multiple checkpoint paths for different applications
airflow variables set s3_checkpoint_paths '[
  "s3://zhwenhaodatalake/checkpoints/sp1/stockprice/offsets/",
  "s3://zhwenhaodatalake/checkpoints/sp2/stockprice/offsets/"
]'

airflow variables set kafka_topics '["stockprice"]'

# Checkpoint-based consumer group mapping (NEW - Advanced)
airflow variables set checkpoint_consumer_group_mapping '{
  "s3://zhwenhaodatalake/checkpoints/sp1/stockprice/offsets/": "sp1",
  "s3://zhwenhaodatalake/checkpoints/sp2/stockprice/offsets/": "sp2"
}'
```

### Multiple Consumer Groups (Standard Setup)
```bash
# Required variables
airflow variables set msk_broker_string "b-1.zhwenhaomsk.88je25.c5.kafka.us-east-1.amazonaws.com:9092,b-2.zhwenhaomsk.88je25.c5.kafka.us-east-1.amazonaws.com:9092"
airflow variables set s3_checkpoint_paths '["s3://zhwenhaodatalake/checkpoints/stockprice/offsets/", "s3://zhwenhaodatalake/checkpoints/userdata/offsets/"]'
airflow variables set kafka_topics '["stockprice", "userdata", "transactions"]'

# Consumer group mapping (each topic gets its own consumer group)
airflow variables set kafka_consumer_group_mapping '{
  "stockprice": "spark-stock-monitor",
  "userdata": "spark-user-monitor", 
  "transactions": "spark-transaction-monitor"
}'
```

## Minimal Configuration Example

For your specific setup with multiple consumer group support:

```bash
# Required variables
airflow variables set msk_broker_string "b-1.zhwenhaomsk.88je25.c5.kafka.us-east-1.amazonaws.com:9092,b-2.zhwenhaomsk.88je25.c5.kafka.us-east-1.amazonaws.com:9092"
airflow variables set s3_checkpoint_paths '["s3://zhwenhaodatalake/checkpoints/stockprice/offsets/"]'
airflow variables set kafka_topics '["stockprice"]'
```

## Validation

The DAG includes a configuration validation task that will check all required variables and report any missing or invalid configurations. The validation will fail the DAG run if required variables are not properly configured.

## Troubleshooting

### Common Issues:
1. **JSON Format Errors**: Ensure JSON arrays are properly formatted with double quotes
2. **S3 Path Format**: All S3 paths must start with `s3://`
3. **Broker Format**: Each broker must include port number (e.g., `:9092`)
4. **Missing Variables**: Required variables must be set, optional ones will use defaults

### Testing Configuration:
You can test the configuration by running the `validate_configuration` task independently or by triggering the full DAG and checking the logs.
