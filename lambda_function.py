import boto3
import io
import uuid
import json
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ChessPhotos')

# Configuration
DEST_BUCKET = 'candidates-portfolio-eldarado'

def lambda_handler(event, context):
    try:
        # Get the incoming bucket and key
        record = event['Records'][0]
        source_bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        print(f"Processing: {key} from {source_bucket}")

        # Download image
        file_obj = s3.get_object(Bucket=source_bucket, Key=key)
        file_content = file_obj['Body'].read()
        
        # Open Image
        with Image.open(io.BytesIO(file_content)) as image:
            # Resize (Thumbnail)
            image.thumbnail((1000, 1000))
            
            # Add Watermark
            draw = ImageDraw.Draw(image)
            text = "Candidates 2024 | Toronto"
            
            # Simple positioning (Bottom Right)
            width, height = image.size
            x_pos = width - 250 
            y_pos = height - 50
            
            # Draw Text (White)
            draw.text((x_pos, y_pos), text, fill=(255, 255, 255))
            
            # Save to buffer
            out_buffer = io.BytesIO()
            image.save(out_buffer, format=image.format)
            out_buffer.seek(0)
            
            # Upload to Portfolio Bucket
            new_filename = f"processed-{key}"
            s3.put_object(
                Bucket=DEST_BUCKET,
                Key=new_filename,
                Body=out_buffer,
                ContentType=file_obj.get('ContentType', 'image/jpeg')
            )
            
            # Save Metadata
            image_url = f"https://{DEST_BUCKET}.s3.amazonaws.com/{new_filename}"
            table.put_item(Item={
                'image_id': str(uuid.uuid4()),
                'original_name': key,
                'upload_date': str(datetime.now()),
                'url': image_url
            })
            
        return {'statusCode': 200, 'body': f"Success: {new_filename}"}
        
    except Exception as e:
        print(e)
        return {'statusCode': 500, 'body': str(e)}
