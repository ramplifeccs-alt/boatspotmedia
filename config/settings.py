
import os

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "devkey")
    DATABASE_URL = os.getenv("DATABASE_URL")
    R2_BUCKET = os.getenv("R2_BUCKET")
    SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
    STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
