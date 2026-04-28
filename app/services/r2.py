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


def abort_multipart_upload(key, upload_id):
    if not key or not upload_id:
        return False
    _client().abort_multipart_upload(Bucket=_bucket_name(), Key=key, UploadId=upload_id)
    return True


def _bucket_name():
    import os
    return (
        os.environ.get("R2_BUCKET_NAME")
        or os.environ.get("R2_BUCKET")
        or os.environ.get("CLOUDFLARE_R2_BUCKET")
        or "boatspotmedia-videos"
    )


def _client():
    import os, boto3
    return boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ.get('R2_ACCOUNT_ID')}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ.get("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    )



def delete_r2_object(key):
    if not key:
        return False
    _client().delete_object(Bucket=_bucket_name(), Key=str(key))
    return True


def delete_r2_prefix(prefix):
    if not prefix:
        return 0
    client = _client()
    bucket = _bucket_name()
    deleted = 0
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": str(prefix)}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        objects = resp.get("Contents") or []
        if objects:
            client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
            )
            deleted += len(objects)
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return deleted


def delete_r2_candidates(keys=None, prefixes=None):
    deleted = 0
    for key in (keys or []):
        if key and not str(key).startswith("http"):
            try:
                delete_r2_object(str(key))
                deleted += 1
            except Exception as e:
                try:
                    print("R2 key delete warning:", key, e)
                except Exception:
                    pass
    for prefix in (prefixes or []):
        if prefix:
            try:
                deleted += delete_r2_prefix(str(prefix))
            except Exception as e:
                try:
                    print("R2 prefix delete warning:", prefix, e)
                except Exception:
                    pass
    return deleted
