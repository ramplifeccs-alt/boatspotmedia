
import os

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY","changeme")
    DATABASE_URL = os.getenv("DATABASE_URL")

    R2_ENDPOINT = os.getenv("R2_ENDPOINT")
    R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
    R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")

    R2_BUCKET_VIDEOS = "boatspotmedia-videos"
    R2_BUCKET_THUMBNAILS = "boatspotmedia-thumbnails"
