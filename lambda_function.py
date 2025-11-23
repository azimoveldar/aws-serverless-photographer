import boto3
import io
import uuid
from urllib.parse import unquote_plus # <--- NEW IMPORT
from datetime import datetime
from PIL import Image, ImageDraw

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ChessPhotos')

DEST_BUCKET = 'candidates-portfolio-eldarado'

def lambda_handler(event, context):
    try:
        record = event['Records'][0]
        source_bucket = record['s3']['bucket']['name']
        
        # FIX: Decode the filename (e.g. "Hikaru%20Nakamura.jpg" -> "Hikaru Nakamura.jpg")
        key = unquote_plus(record['s3']['object']['key']) 
        
        print(f"Processing: {key}")

        file_obj = s3.get_object(Bucket=source_bucket, Key=key)
        file_content = file_obj['Body'].read()
        
        with Image.open(io.BytesIO(file_content)) as image:
            # Resize
            image.thumbnail((1000, 1000))
            
            # Watermark
            draw = ImageDraw.Draw(image)
            text = "Candidates 2024 | Toronto"
            width, height = image.size
            
            # Draw text (Bottom Right)
            draw.text((width - 250, height - 50), text, fill=(255, 255, 255))
            
            out_buffer = io.BytesIO()
            image.save(out_buffer, format=image.format)
            out_buffer.seek(0)
            
            new_filename = f"processed-{key}"
            
            s3.put_object(
                Bucket=DEST_BUCKET,
                Key=new_filename,
                Body=out_buffer,
                ContentType='image/jpeg'
            )
            
            image_url = f"https://{DEST_BUCKET}.s3.amazonaws.com/{new_filename}"
            
            table.put_item(Item={
                'image_id': str(uuid.uuid4()),
                'original_name': key,
                'date': str(datetime.now()),
                'url': image_url
            })
            
        return {'statusCode': 200, 'body': f"Saved {new_filename}"}
        
    except Exception as e:
        print(f"Error: {e}")
        # We don't raise e here to prevent infinite retries in this specific lab setup
        return {'statusCode': 500, 'body': str(e)}
