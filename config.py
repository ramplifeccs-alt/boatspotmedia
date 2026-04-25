import os

class Config:
    # Max single request/body. Browser/Railway may still timeout on huge files; direct R2 multipart is next phase.
    MAX_CONTENT_LENGTH = 128 * 1024 * 1024 * 1024
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-this")
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///boatspotmedia_local.db").replace("postgres://", "postgresql://")
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    MAX_BATCH_GB = 128
    DOWNLOAD_EXPIRATION_DAYS = 7

    R2_ENDPOINT = os.getenv("R2_ENDPOINT")
    R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
    R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
    R2_PUBLIC_BASE_URL = os.getenv("R2_PUBLIC_BASE_URL", "")

    R2_BUCKET_VIDEOS = os.getenv("R2_BUCKET_VIDEOS", "boatspotmedia-videos")
    R2_BUCKET_THUMBNAILS = os.getenv("R2_BUCKET_THUMBNAILS", "boatspotmedia-thumbnails")
    R2_BUCKET_PRODUCTS = os.getenv("R2_BUCKET_PRODUCTS", "boatspotmedia-products")
    R2_BUCKET_CHARTERS = os.getenv("R2_BUCKET_CHARTERS", "boatspotmedia-charters")
    R2_BUCKET_SERVICES = os.getenv("R2_BUCKET_SERVICES", "boatspotmedia-services")

    SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
    SENDGRID_SENDER = os.getenv("SENDGRID_SENDER", "noreply@boatspotmedia.com")

    STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
    STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
