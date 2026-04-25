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
    base = (os.getenv("R2_PUBLIC_URL") or os.getenv("R2_PUBLIC_BASE_URL") or "").strip().rstrip("/")
    # R2 API endpoint is not a public delivery URL. Return empty instead of storing an invalid public URL.
    if "cloudflarestorage.com" in base:
        return ""
    if not base:
        return ""
    return f"{base}/{key}"

# Backwards compatibility for older routes that import:
# from app.services.r2 import upload as r2_upload
# New v35 uploader uses presigned URLs, but this keeps app booting.
def upload(file_obj, key, content_type="application/octet-stream"):
    client = r2_client()
    client.upload_fileobj(
        file_obj,
        os.getenv("R2_BUCKET_NAME"),
        key,
        ExtraArgs={"ContentType": content_type or "application/octet-stream"},
    )
    return key


def upload_file(local_path, key, content_type="application/octet-stream"):
    client = _client()
    bucket = os.getenv("R2_BUCKET_NAME") or os.getenv("R2_BUCKET")
    with open(local_path, "rb") as f:
        client.put_object(Bucket=bucket, Key=key, Body=f, ContentType=content_type)
    return key



def create_multipart_upload(key, content_type="application/octet-stream"):
    client = _client()
    bucket = os.getenv("R2_BUCKET_NAME") or os.getenv("R2_BUCKET")
    return client.create_multipart_upload(Bucket=bucket, Key=key, ContentType=content_type)

def presign_upload_part(key, upload_id, part_number, expires_in=3600):
    client = _client()
    bucket = os.getenv("R2_BUCKET_NAME") or os.getenv("R2_BUCKET")
    return client.generate_presigned_url(
        "upload_part",
        Params={
            "Bucket": bucket,
            "Key": key,
            "UploadId": upload_id,
            "PartNumber": int(part_number)
        },
        ExpiresIn=expires_in
    )

def complete_multipart_upload(key, upload_id, parts):
    client = _client()
    bucket = os.getenv("R2_BUCKET_NAME") or os.getenv("R2_BUCKET")
    fixed_parts = []
    for p in parts:
        fixed_parts.append({
            "ETag": str(p.get("ETag") or p.get("etag") or "").replace('"', ''),
            "PartNumber": int(p.get("PartNumber") or p.get("partNumber") or p.get("part_number"))
        })
    fixed_parts = sorted(fixed_parts, key=lambda x: x["PartNumber"])
    return client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": fixed_parts}
    )

def abort_multipart_upload(key, upload_id):
    client = _client()
    bucket = os.getenv("R2_BUCKET_NAME") or os.getenv("R2_BUCKET")
    return client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
