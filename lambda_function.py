import boto3
import os

# For image processing
def lambda_handler(event, context):
    print("Event received")
    # to do - S3 trigger
    # to do - Pillow resizing
    return {
        'statusCode': 200,
        'body': 'Hello from Lambda!'
    }
