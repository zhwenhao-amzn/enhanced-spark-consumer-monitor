# SP1 & SP2 Scenario Configuration Guide

## Scenario Description
- **Two Spark Streaming Applications**: sp1 和 sp2
- **Different Checkpoint Folders**: 各自有獨立的 S3 checkpoint 路徑
- **Same Kafka Topic**: stockprice
- **Different Consumer Groups**: sp1 和 sp2

## Solution: Checkpoint-Based Consumer Group Mapping

使用新的 `checkpoint_consumer_group_mapping` 配置，可以讓同一個 topic 的不同 checkpoint 來源使用不同的 consumer group。

## Configuration

### Required Airflow Variables

```bash
# MSK Broker String
airflow variables set msk_broker_string "b-1.zhwenhaomsk.88je25.c5.kafka.us-east-1.amazonaws.com:9092,b-2.zhwenhaomsk.88je25.c5.kafka.us-east-1.amazonaws.com:9092"

# S3 Checkpoint Paths - 包含兩個應用的路徑
airflow variables set s3_checkpoint_paths '[
  "s3://zhwenhaodatalake/checkpoints/sp1/stockprice/offsets/",
  "s3://zhwenhaodatalake/checkpoints/sp2/stockprice/offsets/"
]'

# Kafka Topics
airflow variables set kafka_topics '["stockprice"]'

# Checkpoint Consumer Group Mapping - 關鍵配置
airflow variables set checkpoint_consumer_group_mapping '{
  "s3://zhwenhaodatalake/checkpoints/sp1/stockprice/offsets/": "sp1",
  "s3://zhwenhaodatalake/checkpoints/sp2/stockprice/offsets/": "sp2"
}'
```

### Optional Variables

```bash
# AWS Region (optional, defaults to us-east-1)
airflow variables set aws_region "us-east-1"
```

## How It Works

1. **Checkpoint Reading**: DAG 讀取兩個 S3 checkpoint 路徑
2. **Consumer Group Resolution**: 根據 checkpoint path 決定使用哪個 consumer group
3. **Offset Commit**: 
   - sp1 checkpoint 的 stockprice offsets → 提交到 consumer group "sp1"
   - sp2 checkpoint 的 stockprice offsets → 提交到 consumer group "sp2"

## Expected Behavior

### DAG Execution Flow:
1. **Configuration Validation**: 驗證所有變數配置正確
2. **Checkpoint Reading**: 從兩個 S3 路徑讀取 checkpoint 數據
3. **Consumer Group Resolution**: 
   ```
   Topic 'stockprice' from s3://zhwenhaodatalake/checkpoints/sp1/stockprice/offsets/ → Consumer Group 'sp1'
   Topic 'stockprice' from s3://zhwenhaodatalake/checkpoints/sp2/stockprice/offsets/ → Consumer Group 'sp2'
   ```
4. **Offset Commit**: 使用不同的 consumer group 提交 offsets
5. **Summary Report**: 顯示使用的 consumer groups 和提交結果

### Log Output Example:
```
--- Consumer Group Resolution ---
Resolved Topic -> Consumer Group Mapping: {'stockprice': 'sp2'}

--- Offset Commit ---
Consumer Group 'sp1' commit results: {'stockprice': True}
Consumer Group 'sp2' commit results: {'stockprice': True}
Consumer Groups Used: ['sp1', 'sp2']
```

## Conflict Resolution

如果同一個 topic 出現在多個 checkpoint 中：
- **Latest Wins**: 最後處理的 checkpoint 決定最終的 consumer group
- **Warning Logged**: 系統會記錄衝突警告
- **Graceful Handling**: 不會導致 DAG 失敗

## Validation

DAG 包含自動驗證：
- 檢查所有 S3 路徑格式正確
- 驗證所有 consumer groups 可以連接到 Kafka
- 確認 checkpoint paths 和 consumer groups 的映射關係

## Troubleshooting

### Common Issues:
1. **S3 Path Format**: 確保所有 S3 路徑以 `s3://` 開頭
2. **JSON Format**: 確保 JSON 格式正確，使用雙引號
3. **Consumer Group Connection**: 確認所有 consumer groups 可以連接到 MSK
4. **Checkpoint Data**: 確認 checkpoint 文件包含有效的 offset 數據

### Testing:
```bash
# 測試配置
airflow dags test spark_checkpoint_monitor validate_configuration

# 測試完整流程
airflow dags test spark_checkpoint_monitor
```

## Migration from Previous Setup

如果之前使用單一 consumer group：
1. 保留現有的 `kafka_consumer_group` 作為 fallback
2. 添加 `checkpoint_consumer_group_mapping` 配置
3. 系統會自動使用新的 checkpoint-based mapping
4. 向後兼容，不會影響現有功能
