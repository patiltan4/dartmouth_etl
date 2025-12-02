import boto3
import zipfile
import io
from datetime import datetime
import re
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
    # Check multiple Lambda environment indicators
    is_lambda = (
        os.getenv('AWS_LAMBDA_FUNCTION_NAME') or 
        os.getenv('AWS_EXECUTION_ENV') or 
        os.getenv('LAMBDA_TASK_ROOT')
    )
    
    if is_lambda:
        # Lambda - use IAM role only
        print("[LOG] Detected Lambda environment - Using IAM role for S3 access")
        return boto3.client('s3')
    else:
        # Local - use credentials if available
        print("[LOG] Running locally, checking for credentials")
        aws_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
        if aws_key and aws_secret:
            print("[LOG] Using credentials from environment variables")
            return boto3.client('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
        else:
            print("[LOG] Using default AWS configuration")
            return boto3.client('s3')

def lambda_handler(event, context):
    s3_client = get_s3_client()
    
    bucket_name = "dartmouth-etl"
    raw_data_folder = "raw_data"
    csv_folder = "raw_data/csv"
    
    try:
        print("[LOG] Starting Lambda execution")
        print(f"[LOG] Bucket: {bucket_name}, Folder: {raw_data_folder}")
        
        # List all objects in raw_data
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=raw_data_folder)
        
        zip_files = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('.zip') and key != raw_data_folder + '/':
                        zip_files.append(key)
        
        print(f"[LOG] Found {len(zip_files)} zip files")
        for zf in zip_files:
            print(f"  - {zf}")
        
        if len(zip_files) == 0:
            print("[ERROR] No zip files found")
            return {'statusCode': 400, 'body': 'No zip files found'}
        
        extracted_files = []
        
        for zip_key in zip_files:
            print(f"\n[LOG] Processing: {zip_key}")
            
            # Extract year
            year_match = re.search(r'(\d{4})', zip_key)
            year_suffix = year_match.group(1)[-2:] if year_match else "00"
            
            month_prefix = "july"
            
            prefix = f"{month_prefix}{year_suffix}_"
            print(f"[LOG] Prefix: {prefix}")
            
            try:
                # Download zip
                print(f"[LOG] Downloading {zip_key}")
                response = s3_client.get_object(Bucket=bucket_name, Key=zip_key)
                zip_data = response['Body'].read()
                print(f"[LOG] Size: {len(zip_data)} bytes")
                
                # Extract
                with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
                    for file_info in zf.filelist:
                        fname = file_info.filename
                        print(f"[LOG] Checking: {fname}")
                        
                        if fname.upper().endswith('.CSV'):
                            content = zf.read(fname)
                            csv_name = fname.split('/')[-1]
                            new_name = f"{prefix}{csv_name}"
                            s3_key = f"{csv_folder}/{new_name}"
                            
                            print(f"[LOG] Uploading: {s3_key}")
                            s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=content)
                            extracted_files.append(s3_key)
                            print(f"[SUCCESS] {new_name}")
            
            except Exception as e:
                print(f"[ERROR] {zip_key}: {str(e)}")
                import traceback
                traceback.print_exc()
        
        print(f"\n[SUMMARY] Extracted {len(extracted_files)} files")
        return {
            'statusCode': 200,
            'extracted_files': extracted_files,
            'count': len(extracted_files)
        }
    
    except Exception as e:
        print(f"[FATAL ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        return {'statusCode': 500, 'body': str(e)}

# Run locally for testing
if __name__ == "__main__":
    result = lambda_handler({}, {})
    print("\n[RESULT]")
    print(result)