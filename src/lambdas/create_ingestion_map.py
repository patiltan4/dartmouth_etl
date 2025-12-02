import boto3
import pandas as pd
import io
from datetime import datetime
import re
from dateutil.relativedelta import relativedelta
import os

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

def extract_metadata_from_filename(filename):
    """Extract ingestion date, source filename, and metric from parquet filename"""
    # Example: july17_6_Portfolios_2x3_value_weighted_return.parquet
    
    # Extract month and year for ingestion date
    months = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12
    }
    
    filename_lower = filename.lower()
    
    ingestion_date = None
    for month_name, month_num in months.items():
        if month_name in filename_lower:
            year_match = re.search(r'(\d{2})', filename_lower)
            if year_match:
                year = int(year_match.group(1))
                full_year = 1900 + year if year > 50 else 2000 + year
                
                date = datetime(full_year, month_num, 1)
                date = date + relativedelta(months=1) - relativedelta(days=1)
                ingestion_date = date.date()
                break
    
    # Extract metric type (everything after the last underscore before .parquet)
    # Split by common metric patterns
    metric_patterns = [
        '_value_weight', '_equal_weight', '_number_', '_average_',
        '_sum_', '_net_stock'
    ]
    
    metric_type = 'unknown'
    for pattern in metric_patterns:
        if pattern in filename_lower:
            idx = filename_lower.index(pattern) + 1  # +1 to skip the underscore
            metric_type = filename[idx:].replace('.parquet', '')
            break
    
    # Extract source filename (everything before the metric type)
    src_filename = filename
    for pattern in metric_patterns:
        if pattern in filename_lower:
            src_filename = filename[:filename_lower.index(pattern)]
            break
    
    # Add .CSV extension if not present
    if not src_filename.upper().endswith('.CSV'):
        src_filename = src_filename.rstrip('_') + '.CSV'
    
    return src_filename, ingestion_date, metric_type

def lambda_handler(event, context):
    s3_client = get_s3_client()
    bucket_name = "dartmouth-etl"
    transformed_folder = "transformed_data"
    output_folder = "ingestion_map"
    
    try:
        print("[LOG] Starting ingestion map creation")
        
        # List all parquet files
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=transformed_folder)
        
        parquet_files = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.parquet'):
                        parquet_files.append(obj)
        
        if len(parquet_files) == 0:
            print("[WARN] No parquet files found")
            return {
                'statusCode': 200,
                'message': 'No parquet files to process'
            }
        
        print(f"[LOG] Found {len(parquet_files)} parquet files")
        
        map_data = []
        
        for obj in parquet_files:
            file_key = obj['Key']
            filename = file_key.split('/')[-1]
            
            print(f"[LOG] Processing {filename}")
            
            try:
                # Download and read parquet to get record count
                response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
                parquet_content = response['Body'].read()
                
                df = pd.read_parquet(io.BytesIO(parquet_content))
                record_count = len(df)
                status = 'success'
                
            except Exception as e:
                print(f"[ERROR] Failed to read {filename}: {e}")
                record_count = 0
                status = 'failed'
            
            # Extract metadata from filename
            try:
                src_filename, ingestion_date, metric_type = extract_metadata_from_filename(filename)
            except Exception as e:
                print(f"[ERROR] Failed to extract metadata from {filename}: {e}")
                src_filename = filename
                ingestion_date = datetime.now().date()
                metric_type = 'unknown'
                status = 'metadata_extraction_failed'
            
            map_data.append({
                'src_filename': src_filename,
                'ingestion_date': ingestion_date,
                'upload_timestamp': obj['LastModified'],
                'file_path': f's3://{bucket_name}/{file_key}',
                'record_count': record_count,
                'metric_type': metric_type,
                'status': status
            })
            
            print(f"[LOG] {filename}: {record_count} records, ingestion_date={ingestion_date}, metric={metric_type}")
        
        if len(map_data) == 0:
            print("[WARN] No metadata extracted")
            return {
                'statusCode': 200,
                'message': 'No metadata to write'
            }
        
        # Create DataFrame and write to S3
        map_df = pd.DataFrame(map_data)
        
        # Ensure proper data types
        map_df['ingestion_date'] = pd.to_datetime(map_df['ingestion_date'])
        map_df['upload_timestamp'] = pd.to_datetime(map_df['upload_timestamp'])
        
        output_key = f"{output_folder}/ingestion_map.parquet"
        parquet_buffer = io.BytesIO()
        map_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        s3_client.put_object(Bucket=bucket_name, Key=output_key, Body=parquet_buffer.getvalue())
        
        print(f"[LOG] Ingestion map written to {output_key}")
        print(f"[LOG] Total entries: {len(map_data)}")
        
        # Print summary
        print("\n[SUMMARY]")
        print(f"Total parquet files: {len(parquet_files)}")
        print(f"Successfully processed: {sum(1 for x in map_data if x['status'] == 'success')}")
        print(f"Failed: {sum(1 for x in map_data if x['status'] != 'success')}")
        print(f"Total records: {sum(x['record_count'] for x in map_data)}")
        
        return {
            'statusCode': 200,
            'message': f'Created ingestion map with {len(map_data)} entries',
            'output_path': f's3://{bucket_name}/{output_key}',
            'total_records': sum(x['record_count'] for x in map_data)
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