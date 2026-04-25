import os, boto3
from flask import current_app

def client():
    endpoint = current_app.config.get("R2_ENDPOINT")
    access = current_app.config.get("R2_ACCESS_KEY")
    secret = current_app.config.get("R2_SECRET_KEY")
    if not endpoint or not access or not secret:
        return None
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
    )

def upload(local_path, bucket, key):
    c = client()
    if c:
        c.upload_file(local_path, bucket, key)
    public_base = current_app.config.get("R2_PUBLIC_BASE_URL", "").rstrip("/")
    if public_base:
        return f"{public_base}/{key}"
    return f"r2://{bucket}/{key}"
