import boto3
import pandas as pd
import io
from datetime import datetime
import re
from dotenv import load_dotenv
import os

dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(dotenv_path)

def extract_metadata_from_filename(filename):
    """Extract ingestion date and metric from filename"""
    # Example: july17_6_Portfolios_2x3_value_weighted_return.parquet
    
    # Extract month and year
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
                
                from dateutil.relativedelta import relativedelta
                date = datetime(full_year, month_num, 1)
                date = date + relativedelta(months=1) - relativedelta(days=1)
                ingestion_date = date.date()
                break
    
    # Extract metric type (last part before .parquet)
    metric_type = filename.replace('.parquet', '').split('_')
    # Find where metric starts (after the portfolio pattern)
    if '2x3' in filename:
        metric_start = filename.index('2x3') + 4
        metric_type = filename[metric_start:].replace('.parquet', '')
    else:
        metric_type = 'unknown'
    
    # Extract source filename
    src_filename = filename.split('_value_')[0].split('_equal_')[0].split('_number_')[0].split('_average_')[0].split('_sum_')[0]
    if not src_filename.endswith('.CSV'):
        src_filename = src_filename + '.CSV'
    
    return src_filename, ingestion_date, metric_type

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
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
        
        print(f"[LOG] Found {len(parquet_files)} parquet files")
        
        map_data = []
        
        for obj in parquet_files:
            file_key = obj['Key']
            filename = file_key.split('/')[-1]
            
            print(f"[LOG] Processing {filename}")
            
            # Download and read parquet to get record count
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            parquet_content = response['Body'].read()
            
            df = pd.read_parquet(io.BytesIO(parquet_content))
            record_count = len(df)
            
            # Extract metadata from filename
            src_filename, ingestion_date, metric_type = extract_metadata_from_filename(filename)
            
            map_data.append({
                'src_filename': src_filename,
                'ingestion_date': ingestion_date,
                'upload_timestamp': obj['LastModified'],
                'file_path': f's3://{bucket_name}/{file_key}',
                'record_count': record_count,
                'metric_type': metric_type,
                'status': 'success'
            })
            
            print(f"[LOG] {filename}: {record_count} records, ingestion_date={ingestion_date}")
        
        # Create DataFrame and write to S3
        map_df = pd.DataFrame(map_data)
        
        output_key = f"{output_folder}/ingestion_map.parquet"
        parquet_buffer = io.BytesIO()
        map_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        s3_client.put_object(Bucket=bucket_name, Key=output_key, Body=parquet_buffer.getvalue())
        
        print(f"[LOG] Ingestion map written to {output_key}")
        print(f"[LOG] Total entries: {len(map_data)}")
        
        return {
            'statusCode': 200,
            'message': f'Created ingestion map with {len(map_data)} entries',
            'output_path': f's3://{bucket_name}/{output_key}'
        }
    
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return {'statusCode': 500, 'body': str(e)}

if __name__ == "__main__":
    result = lambda_handler({}, {})
    print(result)