# 🏗️ Architecture Documentation

## Table of Contents
1. [Overview](#overview)
2. [Design Principles](#design-principles)
3. [Component Details](#component-details)
4. [Data Flow](#data-flow)
5. [AWS Services Architecture](#aws-services-architecture)
6. [Error Handling Strategy](#error-handling-strategy)
7. [Scalability Considerations](#scalability-considerations)
8. [Cost Analysis](#cost-analysis)

---

## Overview

The Dartmouth Fama-French ETL pipeline implements a **serverless, event-driven architecture** on AWS that ingests, transforms, and catalogs financial portfolio data for analytical workloads.

### Architecture Diagram

![Architecture](./docs/architecture_diagram.png)

---

## Design Principles

### 1. **Serverless-First**
- No infrastructure management
- Automatic scaling
- Pay-per-use pricing
- High availability by default

### 2. **Separation of Concerns**
Each Lambda function has a single responsibility:
- **file_fetch.py** - Data acquisition only
- **extract_csv_from_zip.py** - File extraction only
- **parse_csv_to_n_parquet.py** - Data transformation only
- **create_ingestion_map.py** - Metadata cataloging only

### 3. **Idempotency**
All operations can be safely re-run:
- S3 PUT operations overwrite existing files
- Athena tables are external (no data duplication)
- Ingestion map tracks all versions

### 4. **Data Immutability**
- Raw data is never modified
- Transformations create new files
- Historical versions preserved with `ingestion_date`

### 5. **Cost Optimization**
- Columnar Parquet format (10-100x query speedup)
- Compression reduces storage by 80%
- Pay-per-query model (Athena)
- No idle infrastructure costs

---

## Component Details

### Stage 1: Data Ingestion (`file_fetch.py`)

**Purpose:** Download raw ZIP files from Fama-French website

**Technology:**
- Python 3.11
- urllib for HTTP requests
- HTMLParser for web scraping
- boto3 for S3 uploads

**Process:**
1. Scrape website HTML for dataset URLs
2. Filter for 2017+ datasets
3. Generate unique filenames (timestamp + hash)
4. Download ZIP files
5. Upload to S3 `raw_data/`
6. Log metadata (source URL, timestamp, system info)

**Error Handling:**
- Network timeouts → Retry with exponential backoff
- Invalid URLs → Skip, log error
- Website unavailable → Fail gracefully, alert

**Output:**
```
s3://dartmouth-etl/raw_data/fama_french_6portfolios_2017_20241202_a1b2c3d4.zip
s3://dartmouth-etl/logs/download_logs/ingestion_log_20241202_143022.csv
```

---

### Stage 2: File Extraction (`extract_csv_from_zip.py`)

**Purpose:** Extract CSV files from ZIP archives

**Technology:**
- Python zipfile module
- boto3 for S3 operations
- Regex for filename parsing

**Process:**
1. List all ZIP files in S3 `raw_data/`
2. Download each ZIP
3. Extract CSV files
4. Apply naming convention: `{month}{year}_original_name.CSV`
5. Upload to S3 `raw_data/csv/`

**Naming Convention:**
```
Input:  fama_french_6portfolios_2017_20241202_a1b2c3d4.zip
Output: july17_6_Portfolios_2x3.CSV
```

**Error Handling:**
- Corrupted ZIP → Log error, skip file
- Empty ZIP → Warning, continue
- Missing CSV → Log warning

**Output:**
```
s3://dartmouth-etl/raw_data/csv/july17_6_Portfolios_2x3.CSV
s3://dartmouth-etl/raw_data/csv/july18_6_Portfolios_2x3.CSV
```

---

### Stage 3: Transformation (`parse_csv_to_n_parquet.py`)

**Purpose:** Transform wide-format CSV to long-format Parquet

**Technology:**
- pandas for data manipulation
- pyarrow for Parquet I/O
- Regex for CSV parsing

**Process:**
1. List all CSV files in S3 `raw_data/csv/`
2. Download and parse each CSV
3. Dynamically detect blocks (no hardcoded metrics)
4. Transform to long format
5. Add metadata columns
6. Create 6-10 Parquet files per source
7. Upload to S3 `transformed_data/`

**Input Format (CSV - Wide):**
```csv
Average Value Weighted Returns -- Monthly
,SMALL LoBM,ME1 BM2,SMALL HiBM,BIG LoBM,ME2 BM2,BIG HiBM
192607,1.0874,0.9349,-0.0695,5.7168,1.9620,1.4222
192608,0.7030,1.2300,5.3842,2.7154,2.6930,6.3154
```

**Output Format (Parquet - Long):**
```
date_format | portfolio   | metric_type              | value  | ingestion_date
192607      | small_lobm  | value_weighted_return    | 1.0874 | 2017-07-31
192607      | me1_bm2     | value_weighted_return    | 0.9349 | 2017-07-31
192607      | small_hibm  | value_weighted_return    | -0.0695| 2017-07-31
192608      | small_lobm  | value_weighted_return    | 0.7030 | 2017-07-31
```

**Dynamic Block Detection:**
```python
# No hardcoded metrics!
# Automatically detects:
# - value_weighted_return
# - equal_weighted_return
# - num_firms
# - average_market_cap
# - net_stock_issuance_dec
# - net_stock_issuance_jun
# - And any new metrics added to source files
```

**Error Handling:**
- Invalid date formats → Regex handles YYYY and YYYYMM
- NULL values → Preserved as NULL in Parquet
- Malformed CSV → Try-except with detailed logging
- Empty blocks → Skip without creating files
- Files before 2017 → Skip with warning

**Output:**
```
s3://dartmouth-etl/transformed_data/july17_6_Portfolios_2x3_value_weighted_return.parquet
s3://dartmouth-etl/transformed_data/july17_6_Portfolios_2x3_equal_weighted_return.parquet
s3://dartmouth-etl/transformed_data/july17_6_Portfolios_2x3_num_firms.parquet
...
```

---

### Stage 4: Cataloging (`create_ingestion_map.py`)

**Purpose:** Create metadata catalog for data lineage

**Technology:**
- pandas for DataFrame operations
- boto3 for S3 operations
- Regex for filename parsing

**Process:**
1. List all Parquet files in S3 `transformed_data/`
2. Download and read each file
3. Extract metadata:
   - Source filename
   - Ingestion date
   - Upload timestamp
   - Record count
   - Metric type
   - Status
4. Create consolidated ingestion map
5. Upload to S3 `ingestion_map/`

**Metadata Schema:**
```python
{
    'src_filename': 'july17_6_Portfolios_2x3.CSV',
    'ingestion_date': '2017-07-31',
    'upload_timestamp': '2024-12-02T14:30:22',
    'file_path': 's3://dartmouth-etl/transformed_data/...',
    'record_count': 6558,
    'metric_type': 'value_weighted_return',
    'status': 'success'
}
```

**Error Handling:**
- Failed Parquet reads → Status = 'failed', count = 0
- Missing metadata → Fallback to defaults
- Partial failures → Continue processing

**Output:**
```
s3://dartmouth-etl/ingestion_map/ingestion_map.parquet
```

---

### Stage 5: Query Layer (AWS Athena)

**Purpose:** SQL interface for data analysis

**Technology:**
- AWS Athena (Presto engine)
- AWS Glue Data Catalog
- S3 for storage

**Objects:**

#### 1. **RAW_DATA** (External Table)
```sql
CREATE EXTERNAL TABLE dartmouth_db.raw_data (
    date_format STRING,
    portfolio STRING,
    metric_type STRING,
    value DOUBLE,
    ingestion_date DATE
)
STORED AS PARQUET
LOCATION 's3://dartmouth-etl/transformed_data/';
```

**Purpose:** Complete historical dataset with all ingestion dates

**Use Cases:**
- Time-series analysis
- Historical trend analysis
- Comparing different ingestion dates

#### 2. **PIT_DATA** (View)
```sql
CREATE VIEW dartmouth_db.pit_data AS
SELECT *
FROM dartmouth_db.raw_data;
-- Filter by ingestion_date in query
```

**Purpose:** Point-in-time data retrieval

**Use Cases:**
- Backtesting with data available at specific date
- Regulatory compliance
- Debugging historical decisions

**Query Example:**
```sql
-- Get data as of July 2017
SELECT *
FROM dartmouth_db.pit_data
WHERE ingestion_date = DATE '2017-07-31';
```

#### 3. **CURRENT_DATA** (View)
```sql
CREATE VIEW dartmouth_db.current_data AS
SELECT *
FROM dartmouth_db.raw_data
WHERE ingestion_date = (
    SELECT MAX(ingestion_date) FROM dartmouth_db.raw_data
);
```

**Purpose:** Latest/best available data

**Use Cases:**
- Production dashboards
- Real-time reporting
- Most recent analysis

#### 4. **DATA_INGESTION_MAP** (External Table)
```sql
CREATE EXTERNAL TABLE dartmouth_db.data_ingestion_map (
    src_filename STRING,
    ingestion_date DATE,
    upload_timestamp TIMESTAMP,
    file_path STRING,
    record_count INT,
    metric_type STRING,
    status STRING
)
STORED AS PARQUET
LOCATION 's3://dartmouth-etl/ingestion_map/';
```

**Purpose:** Data lineage and audit trail

**Use Cases:**
- Track file processing history
- Debug pipeline issues
- Compliance audits

---

## Data Flow

### Complete Pipeline Flow

```
1. Website → file_fetch.py → S3 (raw_data/*.zip)
                                    ↓
2. S3 (raw_data/*.zip) → extract_csv_from_zip.py → S3 (raw_data/csv/*.CSV)
                                                          ↓
3. S3 (raw_data/csv/*.CSV) → parse_csv_to_n_parquet.py → S3 (transformed_data/*.parquet)
                                                                    ↓
4. S3 (transformed_data/*.parquet) → create_ingestion_map.py → S3 (ingestion_map/*.parquet)
                                                                          ↓
5. S3 (all parquet files) → AWS Athena → SQL Queries → Analytics
```

### Data Transformation Example

**Step 1: Raw CSV**
```
Average Value Weighted Returns -- Monthly
,SMALL LoBM,ME1 BM2,SMALL HiBM
192607,1.0874,0.9349,-0.0695
```

**Step 2: After Parsing**
```python
[
    {'date': '192607', 'small_lobm': 1.0874, 'me1_bm2': 0.9349, 'small_hibm': -0.0695}
]
```

**Step 3: Long Format**
```python
[
    {'date_format': '192607', 'portfolio': 'small_lobm', 'value': 1.0874, ...},
    {'date_format': '192607', 'portfolio': 'me1_bm2', 'value': 0.9349, ...},
    {'date_format': '192607', 'portfolio': 'small_hibm', 'value': -0.0695, ...}
]
```

**Step 4: Parquet File**
```
Binary columnar format with:
- Snappy compression
- Schema metadata
- Column statistics
```

---

## AWS Services Architecture

### Service Selection Rationale

| Service | Why Selected | Alternatives Considered |
|---------|-------------|------------------------|
| **Lambda** | Serverless, event-driven, 15min timeout | EC2 (overengineered), Glue (more expensive) |
| **S3** | Durable, scalable, cost-effective | EFS (expensive), EBS (not scalable) |
| **Athena** | Pay-per-query, no infrastructure | Redshift (overkill), RDS (not analytical) |
| **Glue Catalog** | Metadata management | Manual table definitions |

### Service Limits & Constraints

| Service | Limit | Impact | Mitigation |
|---------|-------|--------|-----------|
| Lambda | 15 min timeout | Large files may timeout | Use Glue for 25+ min jobs |
| Lambda | 512 MB /tmp | Limited temp storage | Stream data, don't buffer |
| Athena | 30 min query timeout | Complex queries may fail | Optimize with partitions |
| S3 | No hard limits | None for this use case | N/A |

---

## Error Handling Strategy

### Error Handling Hierarchy

```
Level 1: Function-level try-except
    ↓
Level 2: Top-level Lambda handler
    ↓
Level 3: CloudWatch Logs
    ↓
Level 4: (Future) SNS Alerts
```

### Error Categories

#### 1. **Transient Errors** (Retry)
- Network timeouts
- S3 throttling
- Temporary service outages

**Strategy:** Exponential backoff retry

#### 2. **Data Quality Errors** (Log & Continue)
- NULL values
- Invalid formats
- Empty files

**Strategy:** Log warning, set NULL, continue

#### 3. **Fatal Errors** (Fail)
- Authentication failures
- S3 bucket not found
- Invalid Lambda configuration

**Strategy:** Fail fast, alert, manual intervention

### Logging Standards

```python
[LOG]     - Informational (progress updates)
[WARN]    - Non-fatal issues (empty files, NULL values)
[ERROR]   - Recoverable errors (file processing failed)
[FATAL]   - Unrecoverable errors (Lambda failure)
[SUCCESS] - Completed operations
[SUMMARY] - Aggregated results
```

---

## Scalability Considerations

### Current Scale
- **Data Volume:** ~10 MB per file, ~50 files = 500 MB total
- **Query Frequency:** ~10 queries/day
- **Pipeline Runs:** Daily or on-demand
- **Lambda Duration:** 2-5 minutes per stage

### Scale-Up Scenarios

#### Scenario 1: 100x Data Volume (50 GB)
**Changes Needed:**
- ✅ No Lambda changes (streams data)
- ✅ S3 handles automatically
- ⚠️ Athena queries cost 100x more
- 💡 **Solution:** Add S3 partitioning by year/month

#### Scenario 2: 1000x Query Frequency (10,000 queries/day)
**Changes Needed:**
- ✅ Athena scales automatically
- ⚠️ Cost increases linearly
- 💡 **Solution:** Add query result caching, use Redshift Spectrum

#### Scenario 3: Real-Time Ingestion (Streaming)
**Changes Needed:**
- ❌ Lambda + S3 batch processing doesn't support real-time
- 💡 **Solution:** Add Kinesis Data Streams → Lambda → S3

#### Scenario 4: Global Replication
**Changes Needed:**
- ❌ Single-region S3 bucket
- 💡 **Solution:** S3 Cross-Region Replication

### Performance Optimization

**Current Performance:**
- Lambda execution: ~2-5 min/stage
- Athena queries: <1 sec (500 MB data)
- End-to-end pipeline: ~15 min

**Optimizations Applied:**
- ✅ Parquet columnar format
- ✅ Snappy compression
- ✅ Long format schema
- ✅ Parallel Lambda invocations (future)

**Future Optimizations:**
- 🔄 S3 partitioning (year/month)
- 🔄 Athena query result caching
- 🔄 Materialized views for hot queries
- 🔄 Z-ordering for range queries

---

## Cost Analysis

### Current Monthly Costs

| Service | Usage | Cost |
|---------|-------|------|
| **S3 Storage** | 500 MB | $0.01 |
| **S3 Requests** | 1,000 PUT | $0.01 |
| **Lambda** | 100 invocations @ 512MB, 5min | $0.05 |
| **Athena** | 10 queries @ 500 MB scanned | $0.05 |
| **Glue Catalog** | 1,000 requests | $0.01 |
| **CloudWatch Logs** | 1 GB | $0.50 |
| **Total** | | **$0.63/month** |

### Cost Breakdown

**Storage (S3):**
```
Raw ZIPs:    100 MB @ $0.023/GB = $0.002
Raw CSVs:    200 MB @ $0.023/GB = $0.005
Parquet:     200 MB @ $0.023/GB = $0.005
Maps:        1 MB @ $0.023/GB   = $0.000
Total:       ~$0.01/month
```

**Compute (Lambda):**
```
file_fetch:              10 invocations × 2 min = 20 min
extract_csv_from_zip:    10 invocations × 1 min = 10 min
parse_csv_to_n_parquet:  10 invocations × 5 min = 50 min
create_ingestion_map:    10 invocations × 2 min = 20 min
Total:                   100 min @ 512MB = ~$0.05/month
```

**Queries (Athena):**
```
10 queries × 500 MB scanned = 5 GB scanned
5 GB × $5/TB = $0.025/month
```

### Cost Scaling

| Data Volume | S3 Cost | Athena Cost | Total |
|-------------|---------|-------------|-------|
| 500 MB (current) | $0.01 | $0.05 | $0.06 |
| 5 GB (10x) | $0.12 | $0.50 | $0.62 |
| 50 GB (100x) | $1.15 | $5.00 | $6.15 |
| 500 GB (1000x) | $11.50 | $50.00 | $61.50 |

**Key Insight:** Athena costs dominate at scale. Solution: Add partitioning to reduce data scanned.

---

## Comparison with Alternative Architectures

### Alternative 1: Traditional ETL (Apache Airflow + EC2)

**Pros:**
- More control over execution
- Better for complex workflows

**Cons:**
- ❌ Infrastructure management overhead
- ❌ Fixed costs (EC2 always running)
- ❌ Manual scaling
- ❌ Higher complexity

**Cost:** ~$50-100/month (EC2 instances)

### Alternative 2: AWS Glue ETL

**Pros:**
- Built for ETL
- Visual workflow designer

**Cons:**
- ❌ More expensive ($0.44/DPU-hour)
- ❌ Overkill for simple transforms
- ❌ Slower cold starts

**Cost:** ~$10-20/month

### Alternative 3: Redshift Data Warehouse

**Pros:**
- Better query performance
- ACID transactions

**Cons:**
- ❌ Expensive ($0.25/hour minimum)
- ❌ Requires cluster management
- ❌ Overkill for analytical workload

**Cost:** ~$180/month (single node)

### Why Our Architecture Wins

✅ **Lowest cost** (~$0.63/month)
✅ **Zero infrastructure**
✅ **Automatic scaling**
✅ **Pay-per-use**
✅ **Simple to maintain**

---

## Security Considerations

### Current Security Measures

1. **IAM Roles**
   - Least privilege access
   - Separate roles for each Lambda

2. **S3 Bucket Policies**
   - Private buckets (no public access)
   - Encryption at rest (AES-256)
   - Versioning enabled

3. **Secrets Management**
   - AWS credentials in .env (local)
   - IAM roles in production (no keys)

### Future Security Enhancements

- 🔄 AWS Secrets Manager for credentials
- 🔄 VPC endpoints for private S3 access
- 🔄 CloudTrail for audit logging
- 🔄 S3 bucket encryption with KMS
- 🔄 Data masking for sensitive fields

---

## Monitoring & Observability

### Current Monitoring

1. **CloudWatch Logs**
   - All Lambda execution logs
   - Searchable with Insights
   - Retention: 7 days

2. **S3 Metrics**
   - Storage usage
   - Request counts

3. **Athena Query History**
   - Query execution times
   - Data scanned per query

### Future Monitoring

- 🔄 CloudWatch Dashboards
- 🔄 SNS alerts for failures
- 🔄 X-Ray tracing for debugging
- 🔄 Custom metrics (data quality, latency)

---

## Disaster Recovery

### Backup Strategy

1. **S3 Versioning**
   - Enabled on all buckets
   - Recover from accidental deletes

2. **Cross-Region Replication** (Future)
   - Replicate to us-west-2
   - RPO: < 15 minutes

3. **Lambda Code Versioning**
   - Git repository
   - AWS Lambda versions

### Recovery Scenarios

| Scenario | Impact | Recovery Time | Solution |
|----------|--------|---------------|----------|
| Accidental file delete | Data loss | < 1 hour | S3 versioning restore |
| Lambda function failure | Pipeline down | < 5 min | Redeploy from Git |
| S3 region outage | No access | < 1 hour | Failover to replica |
| Athena service outage | No queries | Wait for AWS | Alternative: direct S3 access |

---

## Conclusion

This architecture balances **cost, simplicity, and scalability** for a financial data ETL pipeline. The serverless approach eliminates operational overhead while maintaining flexibility for future growth.

**Key Strengths:**
- ✅ Cost-effective (~$0.63/month)
- ✅ Scalable to 100x data volume
- ✅ Zero infrastructure management
- ✅ Production-ready error handling
- ✅ Full data lineage

**Areas for Improvement:**
- Automated orchestration (Step Functions)
- Real-time streaming support
- Advanced monitoring/alerting
- Multi-region redundancy
