import boto3
import io
import uuid
from urllib.parse import unquote_plus
from datetime import datetime
from PIL import Image, ImageDraw

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ChessPhotos')

DEST_BUCKET = 'candidates-portfolio-eldarado'

def lambda_handler(event, context):
    try:
        # Get incoming bucket and key
        record = event['Records'][0]
        source_bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        
        print(f"Watermarking: {key}")

        # Download image
        file_obj = s3.get_object(Bucket=source_bucket, Key=key)
        file_content = file_obj['Body'].read()
        
        with Image.open(io.BytesIO(file_content)) as image:
            # 3. Resize (Maintain aspect ratio, max 1000px)
            image.thumbnail((1000, 1000))
            
            # --- WATERMARK ---
            draw = ImageDraw.Draw(image)
            text = "© Eldar Azimov | Candidates 2024"
            
            width, height = image.size
            text_width = len(text) * 6 # Approx width per char for default font
            x = width - text_width - 20
            y = height - 20

            # Draw Shadow (Black) for visibility
            draw.text((x-1, y), text, fill="black")
            draw.text((x+1, y), text, fill="black")
            draw.text((x, y-1), text, fill="black")
            draw.text((x, y+1), text, fill="black")
            
            # Draw Main Text (White)
            draw.text((x, y), text, fill="white")
            # --- WATERMARK LOGIC END ---
            
            # Save to buffer
            out_buffer = io.BytesIO()
            # Convert to RGB to ensure JPEG compatibility (removes Alpha channel if png)
            if image.mode in ("RGBA", "P"): image = image.convert("RGB")
            image.save(out_buffer, format='JPEG', quality=90)
            out_buffer.seek(0)
            
            # Upload to Portfolio
            new_filename = f"processed-{key}"
            s3.put_object(
                Bucket=DEST_BUCKET,
                Key=new_filename,
                Body=out_buffer,
                ContentType='image/jpeg'
            )
            
            # Database Entry
            image_url = f"https://{DEST_BUCKET}.s3.amazonaws.com/{new_filename}"
            table.put_item(Item={
                'image_id': str(uuid.uuid4()),
                'original_name': key,
                'date': str(datetime.now()),
                'copyright': 'Eldar Azimov',
                'url': image_url
            })
            
        return {'statusCode': 200, 'body': f"Watermarked {new_filename}"}
        
    except Exception as e:
        print(f"Error: {e}")
        return {'statusCode': 500, 'body': str(e)}
