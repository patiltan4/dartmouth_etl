import boto3
import pandas as pd
import io
from datetime import datetime
import re
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import os

dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(dotenv_path)

def clean_metric_name(text):
    """Convert metric description to clean snake_case name"""
    # Remove common words and clean up
    text = text.lower().strip()
    
    # Remove special characters and replace spaces/dashes with underscore
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '_', text)
    
    # Remove common filler words
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
        # Look for header line (starts with ,SMALL)
        metric_description = None
        
        # Capture any non-empty, non-data lines before header as metric description
        while i < len(lines) and not lines[i].startswith(',SMALL'):
            line = lines[i].strip()
            # If non-empty and not a data line, could be metric description
            if line and not re.match(r'^\s*\d{4,6},', lines[i]):
                metric_description = line
            i += 1
        
        if i >= len(lines):
            break
        
        # Found header line
        header_row = lines[i].strip()
        cols = [col.strip() for col in header_row.split(',')]
        cols[0] = 'DATE'
        
        # Generate metric name from description
        if metric_description:
            metric_type = clean_metric_name(metric_description)
        else:
            metric_type = f'metric_{block_count + 1}'
        
        print(f"[LOG] Block {block_count + 1}: {metric_type}")
        print(f"[LOG] Description: {metric_description}")
        print(f"[LOG] Header at line {i}: {cols}")
        i += 1
        
        # Capture data rows until blank line OR end of file
        # Data rows start with optional spaces then YYYY (4 digits) or YYYYMM (6 digits)
        data_rows = []
        start_line = i
        while i < len(lines):
            line = lines[i].strip()
            
            # Blank line means block ended
            if line == '':
                break
            
            # Data row (optional spaces, then YYYY or YYYYMM, then comma)
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
                    return None
                
                date = datetime(full_year, month_num, 1)
                date = date + relativedelta(months=1) - relativedelta(days=1)
                return date.date()
    
    return None

def blocks_to_parquets(blocks, src_filename):
    """Convert blocks to separate parquet dataframes"""
    ingestion_date = parse_ingestion_date(src_filename)
    
    if ingestion_date is None:
        print("[WARN] File before 2017, skipping")
        return {}
    
    parquets = {}
    
    for block in blocks:
        metric_type = block['metric_type']
        columns = block['columns']
        
        # Get portfolio names from header (skip DATE column)
        portfolio_names = [col.lower().replace(' ', '_') for col in columns[1:]]
        
        data = []
        for row in block['data_rows']:
            parts = row.split(',')
            date_val = parts[0].strip()
            values = parts[1:]  # All value columns
            
            # Create one row per portfolio
            for portfolio_name, val in zip(portfolio_names, values):
                try:
                    val_float = float(val.strip()) if val and val.strip() else None
                except:
                    val_float = None
                
                data.append({
                    'date_format': date_val,
                    'portfolio': portfolio_name,
                    'metric_type': metric_type,
                    'value': val_float,
                    'ingestion_date': str(ingestion_date)
                })
        
        df = pd.DataFrame(data)
        # Ensure column order
        df = df[['date_format', 'portfolio', 'metric_type', 'value', 'ingestion_date']]
        print(f"[LOG] {metric_type}: DataFrame has {len(df)} rows")
        parquets[metric_type] = df
    
    return parquets

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
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
        
        print(f"[LOG] Found {len(csv_files)} CSV files")
        
        all_parquets = []
        
        for csv_key in csv_files:
            print(f"\n[LOG] Processing {csv_key}")
            
            # Download CSV
            response = s3_client.get_object(Bucket=bucket_name, Key=csv_key)
            csv_content = response['Body'].read()
            
            filename = csv_key.split('/')[-1]
            
            # Parse blocks
            blocks = parse_csv_blocks(csv_content, filename)
            print(f"[LOG] Parsed {len(blocks)} blocks")
            
            if len(blocks) == 0:
                print("[WARN] No blocks found, skipping")
                continue
            
            # Convert to parquets
            parquets = blocks_to_parquets(blocks, filename)
            
            if not parquets:
                print("[WARN] File before 2017, skipping")
                continue
            
            # Upload parquets
            for metric_type, df in parquets.items():
                s3_key = f"{output_folder}/{filename.replace('.CSV', '')}_{metric_type}.parquet"
                
                print(f"[LOG] Uploading {s3_key} ({len(df)} rows)")
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)
                
                s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=parquet_buffer.getvalue())
                all_parquets.append(s3_key)
                print(f"[SUCCESS] {metric_type}")
        
        print(f"\n[SUMMARY] Created {len(all_parquets)} parquet files")
        return {
            'statusCode': 200,
            'parquet_files': all_parquets,
            'count': len(all_parquets)
        }
    
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return {'statusCode': 500, 'body': str(e)}

if __name__ == "__main__":
    result = lambda_handler({}, {})
    print(result)