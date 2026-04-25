import os
import boto3
from botocore.config import Config


def r2_configured():
    return all([
        os.getenv("R2_ACCOUNT_ID"),
        os.getenv("R2_ACCESS_KEY_ID"),
        os.getenv("R2_SECRET_ACCESS_KEY"),
        os.getenv("R2_BUCKET_NAME"),
    ])


def r2_client():
    account_id = os.getenv("R2_ACCOUNT_ID")
    endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
        config=Config(signature_version="s3v4"),
    )


def create_presigned_put_url(key, content_type="application/octet-stream", expires=3600):
    client = r2_client()
    return client.generate_presigned_url(
        "put_object",
        Params={
            "Bucket": os.getenv("R2_BUCKET_NAME"),
            "Key": key,
            "ContentType": content_type or "application/octet-stream",
        },
        ExpiresIn=expires,
    )


def public_url_for_key(key):
    base = os.getenv("R2_PUBLIC_URL", "").rstrip("/")
    if not base:
        return ""
    return f"{base}/{key}"
