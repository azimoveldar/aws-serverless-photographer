import requests
from bs4 import BeautifulSoup
import boto3
import os
import sys

# --- CONFIGURATION ---
SOURCE_URL = "https://commons.wikimedia.org/wiki/Category:Candidates_Tournament_2024"
TARGET_BUCKET = "candidates-raw-eldarado"
MAX_BATCH_SIZE = 150 
# ---------------------

s3 = boto3.client('s3')

def fetch_and_upload():
    print(f"Starting ingestion from: {SOURCE_URL}")
    
    try:
        # standard user-agent to prevent 403 forbidden errors
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(SOURCE_URL, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching source page: {e}")
        sys.exit(1)

    soup = BeautifulSoup(response.content, 'html.parser')
    images = soup.find_all('img')
    
    print(f"Page scanned. Found {len(images)} elements. Processing batch...")

    upload_count = 0

    for img in images:
        if upload_count >= MAX_BATCH_SIZE:
            print(f"Batch limit of {MAX_BATCH_SIZE} reached. Ingestion complete.")
            break

        src = img.get('src')
        if not src or 'upload.wikimedia.org' not in src:
            continue
        
        try:
            # Normalize Wikimedia thumbnail URLs to retrieve higher resolution source
            clean_url = src.replace('/thumb/', '/')
            clean_url = "/".join(clean_url.split("/")[:-1])
            
            if clean_url.startswith("//"):
                full_url = "https:" + clean_url
            else:
                full_url = clean_url
            
            filename = full_url.split("/")[-1]
            
            # Stream download and upload to S3
            print(f"Processing: {filename}")
            img_data = requests.get(full_url, headers=headers).content
            
            s3.put_object(
                Bucket=TARGET_BUCKET, 
                Key=filename, 
                Body=img_data,
                ContentType='image/jpeg'
            )
            
            upload_count += 1
                
        except Exception as e:
            print(f"Failed to process {src}: {str(e)}")

    print(f"Job finished. Total files uploaded: {upload_count}")

if __name__ == "__main__":
    fetch_and_upload()
