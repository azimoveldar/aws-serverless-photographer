import requests
from bs4 import BeautifulSoup
import boto3
import sys

SOURCE_URL = "https://commons.wikimedia.org/wiki/Category:Candidates_Tournament_2024"
TARGET_BUCKET = "candidates-raw-eldarado"
MAX_BATCH = 150

s3 = boto3.client('s3')

def run_ingestion():
    print(f"Connecting to {SOURCE_URL}...")
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        response = requests.get(SOURCE_URL, headers=headers)
        response.raise_for_status()
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

    soup = BeautifulSoup(response.content, 'html.parser')
    images = soup.find_all('img')
    
    count = 0
    print(f"Scanning {len(images)} items...")

    for img in images:
        if count >= MAX_BATCH: break

        src = img.get('src')
        if not src or 'upload.wikimedia.org' not in src: continue

        # STRICT FILTER: No Logos, No Icons, No SVGs
        if any(x in src.lower() for x in ['.svg', 'logo', 'icon', 'blue_pencil']):
            continue

        try:
            # Clean URL for high-res
            clean_url = src.replace('/thumb/', '/')
            clean_url = "/".join(clean_url.split("/")[:-1])
            full_url = "https:" + clean_url if clean_url.startswith("//") else clean_url
            
            filename = full_url.split("/")[-1]
            
            # Verify it is an image file
            if not filename.lower().endswith(('.jpg', '.jpeg', '.png')):
                continue

            print(f"Uploading: {filename}")
            img_data = requests.get(full_url, headers=headers).content
            
            s3.put_object(
                Bucket=TARGET_BUCKET, 
                Key=filename, 
                Body=img_data
            )
            count += 1
            
        except Exception as e:
            print(f"Skipped {src}: {e}")

    print(f"Job Complete. Uploaded {count} photos.")

if __name__ == "__main__":
    run_ingestion()
