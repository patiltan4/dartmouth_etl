import boto3
import pandas as pd
import io
from datetime import datetime
import re
from dateutil.relativedelta import relativedelta
import os
import time

# Try to load dotenv for local development, but don't fail if not available
try:
    from dotenv import load_dotenv
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
    load_dotenv(dotenv_path)
    print("[LOG] Running locally with .env file")
except ImportError:
    print("[LOG] Running in Lambda with environment variables")

def get_s3_client():
    """Returns appropriate S3 client based on environment"""
    is_lambda = (
        os.getenv('AWS_LAMBDA_FUNCTION_NAME') or 
        os.getenv('AWS_EXECUTION_ENV') or 
        os.getenv('LAMBDA_TASK_ROOT')
    )
    
    if is_lambda:
        print("[LOG] Detected Lambda environment - Using IAM role for S3 access")
        return boto3.client('s3')
    else:
        print("[LOG] Running locally, checking for credentials")
        aws_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
        if aws_key and aws_secret:
            print("[LOG] Using credentials from environment variables")
            return boto3.client('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
        else:
            print("[LOG] Using default AWS configuration")
            return boto3.client('s3')

def get_athena_client():
    """Returns appropriate Athena client based on environment"""
    is_lambda = (
        os.getenv('AWS_LAMBDA_FUNCTION_NAME') or 
        os.getenv('AWS_EXECUTION_ENV') or 
        os.getenv('LAMBDA_TASK_ROOT')
    )
    
    if is_lambda:
        return boto3.client('athena')
    else:
        aws_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
        if aws_key and aws_secret:
            return boto3.client('athena', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
        else:
            return boto3.client('athena')

def clean_metric_name(text):
    """Convert metric description to clean snake_case name"""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '_', text)
    
    filler_words = ['the', 'a', 'an', 'and', 'or', 'of', 'to', 'in', 'for']
    words = text.split('_')
    words = [w for w in words if w not in filler_words and w]
    
    return '_'.join(words)

def parse_csv_blocks(csv_content, src_filename):
    """Parse Fama-French CSV into dynamic blocks"""
    lines = csv_content.decode('utf-8').split('\n')
    
    print(f"[LOG] Total lines in file: {len(lines)}")
    
    blocks = []
    i = 0
    block_count = 0
    
    while i < len(lines):
        metric_description = None
        
        while i < len(lines) and not lines[i].startswith(',SMALL'):
            line = lines[i].strip()
            if line and not re.match(r'^\s*\d{4,6},', lines[i]):
                metric_description = line
            i += 1
        
        if i >= len(lines):
            break
        
        header_row = lines[i].strip()
        cols = [col.strip() for col in header_row.split(',')]
        cols[0] = 'DATE'
        
        if metric_description:
            metric_type = clean_metric_name(metric_description)
        else:
            metric_type = f'metric_{block_count + 1}'
        
        print(f"[LOG] Block {block_count + 1}: {metric_type}")
        print(f"[LOG] Description: {metric_description}")
        print(f"[LOG] Header at line {i}: {cols}")
        i += 1
        
        data_rows = []
        start_line = i
        while i < len(lines):
            line = lines[i].strip()
            
            if line == '':
                break
            
            if re.match(r'^\s*\d{4,6},', lines[i]):
                data_rows.append(lines[i].strip())
            
            i += 1
        
        print(f"[LOG] Block {block_count + 1} data rows: {len(data_rows)} (lines {start_line} to {i-1})")
        
        if data_rows:
            blocks.append({
                'metric_type': metric_type,
                'metric_description': metric_description,
                'columns': cols,
                'data_rows': data_rows,
                'src_filename': src_filename,
                'block_num': block_count + 1
            })
            block_count += 1
            print(f"[LOG] Block {block_count} saved successfully")
        else:
            print(f"[WARN] Block at line {start_line} has no data rows, skipping")
    
    print(f"[LOG] Total blocks parsed: {len(blocks)}")
    return blocks

def parse_ingestion_date(filename):
    """Extract date from filename like july17 -> 2017-07-31"""
    months = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12
    }
    
    filename_lower = filename.lower()
    
    for month_name, month_num in months.items():
        if month_name in filename_lower:
            year_match = re.search(r'(\d{2})', filename_lower)
            if year_match:
                year = int(year_match.group(1))
                full_year = 1900 + year if year > 50 else 2000 + year
                
                if full_year < 2017:
                    print(f"[WARN] File {filename} is before 2017 (year={full_year}), skipping")
                    return None
                
                date = datetime(full_year, month_num, 1)
                date = date + relativedelta(months=1) - relativedelta(days=1)
                return date.date()
    
    print(f"[WARN] Could not extract date from filename: {filename}")
    return None

def blocks_to_parquets(blocks, src_filename):
    """Convert blocks to separate parquet dataframes"""
    ingestion_date = parse_ingestion_date(src_filename)
    
    if ingestion_date is None:
        print("[WARN] File before 2017 or invalid filename, skipping")
        return {}
    
    parquets = {}
    
    for block in blocks:
        metric_type = block['metric_type']
        columns = block['columns']
        
        portfolio_names = [col.lower().replace(' ', '_') for col in columns[1:]]
        
        data = []
        for row in block['data_rows']:
            parts = row.split(',')
            date_val = parts[0].strip()
            values = parts[1:]
            
            for portfolio_name, val in zip(portfolio_names, values):
                try:
                    val_float = float(val.strip()) if val and val.strip() else None
                except Exception as e:
                    print(f"[WARN] Could not parse value '{val}' for {date_val}/{portfolio_name}: {e}")
                    val_float = None
                
                data.append({
                    'date_format': date_val,
                    'portfolio': portfolio_name,
                    'metric_type': metric_type,
                    'value': val_float,
                    'ingestion_date': ingestion_date,
                    'src_filename': src_filename
                })
        
        if len(data) == 0:
            print(f"[WARN] No data for metric {metric_type}, skipping")
            continue
            
        df = pd.DataFrame(data)
        df = df[['date_format', 'portfolio', 'metric_type', 'value', 'ingestion_date', 'src_filename']]
        print(f"[LOG] {metric_type}: DataFrame has {len(df)} rows")
        parquets[metric_type] = df
    
    return parquets

def execute_athena_query(athena_client, query, database='dartmouth_db'):
    """Execute Athena query and return results"""
    output_location = 's3://dartmouth-etl/athena-results/'
    
    print(f"[LOG] Executing Athena query: {query}")
    
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    
    query_execution_id = response['QueryExecutionId']
    print(f"[LOG] Query execution ID: {query_execution_id}")
    
    # Wait for query to complete
    max_attempts = 30
    for attempt in range(max_attempts):
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        
        if status == 'SUCCEEDED':
            print("[LOG] Query succeeded")
            break
        elif status in ['FAILED', 'CANCELLED']:
            reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
            print(f"[ERROR] Query {status}: {reason}")
            return None
        
        time.sleep(2)
    
    # Get results
    result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    return result

def check_if_already_processed(athena_client, src_filename):
    """Check if file already processed using Athena table"""
    query = f"""
    SELECT COUNT(*) as count 
    FROM dartmouth_db.data_ingestion_map 
    WHERE src_filename = '{src_filename}' 
    AND status = 'success'
    """
    
    result = execute_athena_query(athena_client, query)
    
    if result and 'ResultSet' in result:
        rows = result['ResultSet']['Rows']
        if len(rows) > 1:  # Skip header row
            count = int(rows[1]['Data'][0]['VarCharValue'])
            if count > 0:
                print(f"[INFO] File {src_filename} already processed ({count} records found)")
                return True
    
    return False

def log_to_ingestion_map(s3_client, src_filename, ingestion_date, parquet_files):
    """Log processing metadata to ingestion map table"""
    bucket_name = "dartmouth-etl"
    map_folder = "ingestion_map"
    
    records = []
    for parquet_path in parquet_files:
        # Extract metric_type from parquet filename
        metric_type = parquet_path.split('_')[-1].replace('.parquet', '')
        
        records.append({
            'src_filename': src_filename,
            'ingestion_date': str(ingestion_date),
            'upload_timestamp': datetime.now().isoformat(),
            'file_path': parquet_path,
            'record_count': 0,  # Can be calculated if needed
            'metric_type': metric_type,
            'status': 'success'
        })
    
    if not records:
        return
    
    # Convert to DataFrame and save as parquet
    df = pd.DataFrame(records)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    map_key = f"{map_folder}/ingestion_{src_filename.replace('.CSV', '')}_{timestamp}.parquet"
    
    print(f"[LOG] Logging {len(records)} records to ingestion map: {map_key}")
    
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    s3_client.put_object(Bucket=bucket_name, Key=map_key, Body=parquet_buffer.getvalue())
    print(f"[SUCCESS] Logged to ingestion map")

def lambda_handler(event, context):
    s3_client = get_s3_client()
    athena_client = get_athena_client()
    
    bucket_name = "dartmouth-etl"
    csv_folder = "raw_data/csv"
    output_folder = "transformed_data"
    
    try:
        print("[LOG] Starting CSV parsing Lambda")
        
        # List CSV files
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=csv_folder)
        
        csv_files = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.CSV'):
                        csv_files.append(obj['Key'])
        
        if len(csv_files) == 0:
            print("[WARN] No CSV files found")
            return {
                'statusCode': 200,
                'message': 'No CSV files to process'
            }
        
        print(f"[LOG] Found {len(csv_files)} CSV files")
        
        all_parquets = []
        skipped_count = 0
        
        for csv_key in csv_files:
            print(f"\n[LOG] Processing {csv_key}")
            
            try:
                filename = csv_key.split('/')[-1]
                
                # Check if already processed using Athena
                if check_if_already_processed(athena_client, filename):
                    skipped_count += 1
                    continue
                
                # Download CSV
                response = s3_client.get_object(Bucket=bucket_name, Key=csv_key)
                csv_content = response['Body'].read()
                
                # Parse blocks
                blocks = parse_csv_blocks(csv_content, filename)
                print(f"[LOG] Parsed {len(blocks)} blocks")
                
                if len(blocks) == 0:
                    print("[WARN] No blocks found, skipping")
                    continue
                
                # Convert to parquets
                parquets = blocks_to_parquets(blocks, filename)
                
                if not parquets:
                    print("[WARN] No parquets generated, skipping")
                    continue
                
                # Get ingestion date for logging
                ingestion_date = parse_ingestion_date(filename)
                file_parquets = []
                
                # Upload parquets
                for metric_type, df in parquets.items():
                    s3_key = f"{output_folder}/{filename.replace('.CSV', '')}_{metric_type}.parquet"
                    
                    print(f"[LOG] Uploading {s3_key} ({len(df)} rows)")
                    parquet_buffer = io.BytesIO()
                    df.to_parquet(parquet_buffer, index=False)
                    parquet_buffer.seek(0)
                    
                    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=parquet_buffer.getvalue())
                    all_parquets.append(s3_key)
                    file_parquets.append(s3_key)
                    print(f"[SUCCESS] {metric_type}")
                
                # Log to ingestion map
                log_to_ingestion_map(s3_client, filename, ingestion_date, file_parquets)
                    
            except Exception as e:
                print(f"[ERROR] Failed to process {csv_key}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"\n[SUMMARY] Created {len(all_parquets)} parquet files, skipped {skipped_count} already processed")
        return {
            'statusCode': 200,
            'parquet_files': all_parquets,
            'count': len(all_parquets),
            'skipped': skipped_count
        }
    
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return {'statusCode': 500, 'body': str(e)}

# Run locally for testing
if __name__ == "__main__":
    result = lambda_handler({}, {})
    print(result)