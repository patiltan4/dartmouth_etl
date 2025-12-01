import urllib.request
import urllib.parse
from html.parser import HTMLParser
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime
import getpass
import platform
import hashlib

load_dotenv()

class DatasetParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.datasets = []
        self.current_text = ""
        self.current_url = None
    
    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for attr, value in attrs:
                if attr == "href" and value.endswith(".zip"):
                    self.current_url = value
    
    def handle_data(self, data):
        self.current_text += data.strip()
    
    def handle_endtag(self, tag):
        if tag == "a" and self.current_url:
            text = self.current_text.strip()
            if text == "CSV":
                self.datasets.append({"name": self.current_text, "url": self.current_url})
            self.current_text = ""
            self.current_url = None

def fetch_datasets(url):
    print(f"[LOG] Fetching datasets from {url}")
    with urllib.request.urlopen(url) as response:
        html = response.read().decode("utf-8")
    
    parser = DatasetParser()
    parser.feed(html)
    print(f"[LOG] Total datasets found: {len(parser.datasets)}")
    
    filtered = [d for d in parser.datasets if any(str(year) in d["url"] for year in range(2017, 2030))]
    print(f"[LOG] Filtered datasets (2017+): {len(filtered)}")
    for d in filtered:
        print(f"  - {d['url']}")
    return filtered

def extract_year(url):
    for year in range(2017, 2030):
        if str(year) in url:
            return str(year)
    return "unknown"

def generate_unique_filename(source_url):
    hash_obj = hashlib.md5(source_url.encode())
    hash_hex = hash_obj.hexdigest()[:8]
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    year = extract_year(source_url)
    filename = f"fama_french_6portfolios_{year}_{timestamp}_{hash_hex}.zip"
    print(f"[LOG] Generated filename: {filename}")
    return filename

def download_to_s3(url, bucket_name, folder_name, unique_filename, source_url):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    
    try:
        print(f"[LOG] Downloading from {source_url}")
        url_encoded = urllib.parse.quote(url, safe=':/?#[]@!$&\'()*+,;=')
        with urllib.request.urlopen(url_encoded) as response:
            file_data = response.read()
        
        print(f"[LOG] Downloaded {len(file_data)} bytes")
        s3_key = f"{folder_name}/{unique_filename}"
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=file_data)
        print(f"[SUCCESS] Uploaded to s3://{bucket_name}/{s3_key}")
        
        return {
            "source_url": source_url,
            "unique_filename": unique_filename,
            "s3_path": s3_key,
            "ingestion_time": datetime.now().isoformat(),
            "ingested_by": getpass.getuser(),
            "system": platform.system(),
            "status": "success"
        }
        
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        return {
            "source_url": source_url,
            "unique_filename": unique_filename,
            "s3_path": "",
            "ingestion_time": datetime.now().isoformat(),
            "ingested_by": getpass.getuser(),
            "system": platform.system(),
            "status": f"failed: {str(e)}"
        }

def log_to_s3(metadata_list, bucket_name):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    
    log_filename = f"ingestion_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    log_key = f"logs/download_logs/{log_filename}"
    
    print(f"[LOG] Creating ingestion log: {log_filename}")
    csv_buffer = []
    if metadata_list:
        fieldnames = metadata_list[0].keys()
        csv_buffer.append(",".join(fieldnames))
        for row in metadata_list:
            csv_buffer.append(",".join(str(row[field]) for field in fieldnames))
    
    csv_content = "\n".join(csv_buffer)
    s3_client.put_object(Bucket=bucket_name, Key=log_key, Body=csv_content)
    print(f"[SUCCESS] Logged to s3://{bucket_name}/{log_key}")

# Main execution
print("[START] Dartmouth Fama-French Data Pipeline")
url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/Data_Library/six_portfolios_archive.html"
base_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/Data_Library/"

datasets = fetch_datasets(url)
metadata_list = []

for i, dataset in enumerate(datasets, 1):
    print(f"\n[PROCESSING] Dataset {i}/{len(datasets)}")
    source_url = base_url + dataset["url"]
    unique_filename = generate_unique_filename(source_url)
    metadata = download_to_s3(source_url, "dartmouth-etl", "raw_data", unique_filename, source_url)
    metadata_list.append(metadata)

print(f"\n[LOG] Total datasets processed: {len(metadata_list)}")
log_to_s3(metadata_list, "dartmouth-etl")
print("[END] Pipeline completed")