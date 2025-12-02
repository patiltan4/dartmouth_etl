# 📊 Dartmouth Fama-French ETL Pipeline

> **A scalable, serverless ETL pipeline for ingesting, transforming, and analyzing Fama-French 6 Portfolios financial data using AWS services.**

[![AWS](https://img.shields.io/badge/AWS-Lambda%20%7C%20S3%20%7C%20Athena-orange)](https://aws.amazon.com/)
[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Data Flow](#data-flow)
- [Setup & Installation](#setup--installation)
- [Usage](#usage)
- [AWS Services Used](#aws-services-used)
- [Data Schema](#data-schema)
- [Error Handling](#error-handling)
- [Cost Optimization](#cost-optimization)
- [Future Enhancements](#future-enhancements)

---

## 🎯 Overview

This project implements a **production-ready ETL pipeline** that:

1. **Ingests** Fama-French 6 Portfolios data from Ken French's Data Library (2017 onwards)
2. **Transforms** raw CSV data into optimized Parquet format with long-form schema
3. **Catalogs** metadata for data lineage and point-in-time analysis
4. **Queries** data using AWS Athena with SQL views for analytical workloads

### Business Use Case

Portfolio managers and financial analysts need historical performance data for:
- **Backtesting** investment strategies
- **Risk analysis** across market cap and book-to-market portfolios
- **Performance attribution** with point-in-time data accuracy
- **Regulatory compliance** with full data lineage

---

## 🏗️ Architecture

<img width="667" height="618" alt="image" src="https://github.com/user-attachments/assets/7299094e-dac8-423c-9065-a9ba2124edfd" />


### Pipeline Stages

```
┌─────────────────┐
│  Data Sources   │
│  (Fama-French)  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────┐
│         STAGE 1: Data Ingestion             │
│  ┌───────────────────────────────────────┐  │
│  │  file_fetch.py (Lambda)               │  │
│  │  • Scrapes website for ZIP files      │  │
│  │  • Downloads 2017+ datasets           │  │
│  │  • Uploads to S3 raw_data/            │  │
│  └───────────────┬───────────────────────┘  │
└──────────────────┼──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│         STAGE 2: File Extraction            │
│  ┌───────────────────────────────────────┐  │
│  │  extract_csv_from_zip.py (Lambda)     │  │
│  │  • Unzips files                       │  │
│  │  • Extracts CSV files                 │  │
│  │  • Applies naming conventions         │  │
│  └───────────────┬───────────────────────┘  │
└──────────────────┼──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│         STAGE 3: Transformation             │
│  ┌───────────────────────────────────────┐  │
│  │  parse_csv_to_n_parquet.py (Lambda)   │  │
│  │  • Parses CSV blocks dynamically      │  │
│  │  • Transforms to long format          │  │
│  │  • Creates 6-10 parquet files/source  │  │
│  │  • Adds ingestion metadata            │  │
│  └───────────────┬───────────────────────┘  │
└──────────────────┼──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│         STAGE 4: Cataloging                 │
│  ┌───────────────────────────────────────┐  │
│  │  create_ingestion_map.py (Lambda)     │  │
│  │  • Scans all parquet files            │  │
│  │  • Extracts metadata                  │  │
│  │  • Creates ingestion map              │  │
│  └───────────────┬───────────────────────┘  │
└──────────────────┼──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│         STAGE 5: Query Layer                │
│  ┌───────────────────────────────────────┐  │
│  │  AWS Athena           │  │
│  │  • raw_data (external table)          │  │
│  │  • pit_data (view)                    │  │
│  │  • current_data (view)                │  │
│  │  • data_ingestion_map (table)         │  │
│  └───────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
```

---

## ✨ Features

### 🚀 Scalability
- **Serverless architecture** - Scales automatically with data volume
- **Parallel processing** - Multiple Lambda invocations handle files concurrently
- **Partitioned storage** - Optimized for query performance

### 🔒 Data Quality
- **Schema validation** - Type checking and null handling
- **Error handling** - Graceful degradation with detailed logging
- **Idempotency** - Safe to re-run without duplicates

### 📊 Analytics Ready
- **Columnar format** - Parquet for 10-100x query speedup
- **Point-in-time views** - Historical data accuracy
- **SQL interface** - Standard Athena queries

### 💰 Cost Optimized
- **Pay-per-query** - No idle infrastructure costs
- **Compressed storage** - Parquet reduces S3 costs by 80%
- **Minimal data scanning** - Columnar format scans only needed columns

---

## 🔄 Data Flow

### Input Format (CSV)
```
Average Value Weighted Returns -- Monthly
,SMALL LoBM,ME1 BM2,SMALL HiBM,BIG LoBM,ME2 BM2,BIG HiBM
192607,1.0874,0.9349,-0.0695,5.7168,1.9620,1.4222
192608,0.7030,1.2300,5.3842,2.7154,2.6930,6.3154
...
```

### Output Format (Parquet - Long Format)
```
date_format | portfolio   | metric_type              | value  | ingestion_date
192607      | small_lobm  | value_weighted_return    | 1.0874 | 2017-07-31
192607      | me1_bm2     | value_weighted_return    | 0.9349 | 2017-07-31
192607      | small_hibm  | value_weighted_return    | -0.0695| 2017-07-31
...
```

### Why Long Format?
- ✅ Easier to query specific portfolios
- ✅ Better for time-series analysis
- ✅ Supports dynamic schema evolution
- ✅ Simplifies joins with other datasets
- ✅ Standard format for analytics tools

---

## 🛠️ Setup & Installation

### Prerequisites
- AWS Account with appropriate permissions
- Python 3.11+
- AWS CLI configured
- Git

### 1. Clone Repository
```bash
git clone https://github.com/yourusername/dartmouth-etl.git
cd dartmouth-etl
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment
```bash
cp .env.example .env
# Edit .env with your AWS credentials
```

**.env file:**
```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1
```

### 4. Create S3 Bucket
```bash
aws s3 mb s3://dartmouth-etl
aws s3 mb s3://dartmouth-etl/raw_data
aws s3 mb s3://dartmouth-etl/raw_data/csv
aws s3 mb s3://dartmouth-etl/transformed_data
aws s3 mb s3://dartmouth-etl/ingestion_map
aws s3 mb s3://dartmouth-etl/athena_results
```

### 5. Set Up Athena

**Create Database:**
```sql
CREATE DATABASE IF NOT EXISTS dartmouth_db
COMMENT 'Fama-French portfolio data warehouse'
LOCATION 's3://dartmouth-etl/data_catalog/';
```

**Create RAW_DATA Table:**
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS dartmouth_db.raw_data (
    date_format STRING,
    portfolio STRING,
    metric_type STRING,
    value DOUBLE,
    ingestion_date DATE
)
STORED AS PARQUET
LOCATION 's3://dartmouth-etl/transformed_data/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

**Create DATA_INGESTION_MAP Table:**
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS dartmouth_db.data_ingestion_map (
    src_filename STRING,
    ingestion_date DATE,
    upload_timestamp TIMESTAMP,
    file_path STRING,
    record_count INT,
    metric_type STRING,
    status STRING
)
STORED AS PARQUET
LOCATION 's3://dartmouth-etl/ingestion_map/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
```

**Create PIT_DATA View:**
```sql
CREATE OR REPLACE VIEW dartmouth_db.pit_data AS
SELECT 
    date_format,
    portfolio,
    metric_type,
    value,
    ingestion_date
FROM dartmouth_db.raw_data;
```

**Create CURRENT_DATA View:**
```sql
CREATE OR REPLACE VIEW dartmouth_db.current_data AS
SELECT 
    date_format,
    portfolio,
    metric_type,
    value,
    ingestion_date
FROM dartmouth_db.raw_data
WHERE ingestion_date = (
    SELECT MAX(ingestion_date) 
    FROM dartmouth_db.raw_data
);
```

---

## 🚀 Usage

### Local Execution

**Step 1: Fetch Data**
```bash
python src/lambdas/file_fetch.py
```

**Step 2: Extract CSVs**
```bash
python src/lambdas/extract_csv_from_zip.py
```

**Step 3: Transform to Parquet**
```bash
python src/lambdas/parse_csv_to_n_parquet.py
```

**Step 4: Create Ingestion Map**
```bash
python src/lambdas/create_ingestion_map.py
```

### Query Data (Athena)

**Get all value-weighted returns:**
```sql
SELECT 
    date_format,
    portfolio,
    value
FROM dartmouth_db.current_data
WHERE metric_type = 'value_weighted_return'
ORDER BY date_format DESC
LIMIT 100;
```

**Point-in-time analysis (July 2017 data):**
```sql
SELECT *
FROM dartmouth_db.raw_data
WHERE ingestion_date = DATE '2017-07-31'
AND metric_type = 'value_weighted_return';
```

**Check data lineage:**
```sql
SELECT 
    src_filename,
    ingestion_date,
    metric_type,
    record_count,
    status
FROM dartmouth_db.data_ingestion_map
ORDER BY upload_timestamp DESC;
```

---

## ☁️ AWS Services Used

| Service | Purpose | Cost Model |
|---------|---------|------------|
| **S3** | Data lake storage | $0.023/GB/month |
| **Lambda** | Serverless compute | $0.20 per 1M requests |
| **Athena** | SQL query engine | $5 per TB scanned |
| **Glue Catalog** | Metadata storage | $1 per 100K requests |

**Estimated Monthly Cost:** < $5 for typical usage

---

## 📊 Data Schema

### RAW_DATA Table
| Column | Type | Description |
|--------|------|-------------|
| `date_format` | STRING | Date in YYYYMM or YYYY format |
| `portfolio` | STRING | Portfolio name (small_lobm, me1_bm2, etc.) |
| `metric_type` | STRING | Metric type (value_weighted_return, etc.) |
| `value` | DOUBLE | Numeric value (can be NULL) |
| `ingestion_date` | DATE | Date file was ingested |

### DATA_INGESTION_MAP Table
| Column | Type | Description |
|--------|------|-------------|
| `src_filename` | STRING | Source CSV filename |
| `ingestion_date` | DATE | Ingestion date |
| `upload_timestamp` | TIMESTAMP | Upload time |
| `file_path` | STRING | S3 path to parquet file |
| `record_count` | INT | Number of records |
| `metric_type` | STRING | Type of metric |
| `status` | STRING | success/failed |

### Metric Types
- `value_weighted_return` - Value-weighted monthly/annual returns
- `equal_weighted_return` - Equal-weighted monthly/annual returns
- `num_firms` - Number of firms in portfolio
- `average_market_cap` - Average market capitalization
- `net_stock_issuance_dec` - Net stock issuance to December
- `net_stock_issuance_jun` - Net stock issuance to June
- And more (dynamically discovered)

---

## 🛡️ Error Handling

### Robust Error Handling at Every Stage

#### 1. File Fetch
- ✅ Website unavailability → Logs error, continues
- ✅ Invalid ZIP files → Skips, continues with next
- ✅ Network timeouts → Retry with exponential backoff

#### 2. CSV Extraction
- ✅ Corrupted ZIP files → Logged, skipped
- ✅ Empty ZIP files → Warning logged
- ✅ Missing CSV files → Graceful skip

#### 3. Parquet Transformation
- ✅ Invalid date formats → Handled with regex
- ✅ NULL values → Preserved as NULL in parquet
- ✅ Malformed CSV → Try-except with detailed logging
- ✅ Empty blocks → Skipped without creating files

#### 4. Ingestion Map
- ✅ Failed parquet reads → Logged with status='failed'
- ✅ Missing metadata → Fallback to defaults
- ✅ Partial failures → Continues processing

### Logging Strategy
```python
[LOG]     - Informational messages
[WARN]    - Non-fatal issues
[ERROR]   - Recoverable errors
[SUCCESS] - Completed operations
[SUMMARY] - Aggregated results
```

---

## 💡 Design Decisions

### Why Point-in-Time (PIT) Data?

**Problem:** Historical data gets revised. What you saw in July 2017 may differ from the "July 2017 data" downloaded today.

**Solution:** Store data with `ingestion_date` to preserve historical snapshots.

**Use Cases:**
- ✅ Backtesting strategies with data available at that time
- ✅ Regulatory compliance (prove what data you had when)
- ✅ Debugging historical decisions

### Why SQL Views Instead of Separate Tables?

**Pros:**
- ✅ Single source of truth (RAW_DATA)
- ✅ No data duplication
- ✅ Automatically updated when RAW_DATA changes
- ✅ Lower storage costs

**Cons:**
- ⚠️ Slower query performance (mitigated by Parquet)
- ⚠️ View complexity for advanced queries

**Alternative:** Materialized views for high-frequency queries

---


## 📚 Resources

- [Fama-French Data Library](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html)
- [AWS Lambda Documentation](https://docs.aws.amazon.com/lambda/)
- [AWS Athena Best Practices](https://docs.aws.amazon.com/athena/latest/ug/performance-tuning.html)
- [Parquet Format Specification](https://parquet.apache.org/docs/)

---

---

## 👥 Contributors

- Tanmay

---

## 🙏 Acknowledgments

- Ken French at Dartmouth for providing the data
- AWS for serverless infrastructure
- Apache Parquet community

--

