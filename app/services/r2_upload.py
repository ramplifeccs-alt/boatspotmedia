
import boto3
import os

def get_r2_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("R2_SECRET_KEY"),
    )

def upload_to_r2(file_path, key):
    client = get_r2_client()
    bucket = "boatspotmedia-videos"

    client.upload_file(file_path, bucket, key)

    return f"{bucket}/{key}"
