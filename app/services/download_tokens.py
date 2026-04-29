
import os
import secrets
from datetime import datetime, timedelta
from app import db


def ensure_download_token_table():
    try:
        db.session.execute(db.text("""
            CREATE TABLE IF NOT EXISTS download_token (
                id SERIAL PRIMARY KEY,
                token VARCHAR(128) UNIQUE NOT NULL,
                video_id INTEGER NOT NULL,
                order_id VARCHAR(128),
                buyer_email VARCHAR(255),
                package VARCHAR(64),
                expires_at TIMESTAMP,
                used_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.commit()
    except Exception:
        db.session.rollback()


def create_download_token(video_id, buyer_email=None, order_id=None, package="original", days_valid=14):
    ensure_download_token_table()
    token = secrets.token_urlsafe(32)
    expires_at = datetime.utcnow() + timedelta(days=days_valid)
    db.session.execute(
        db.text("""
            INSERT INTO download_token (token, video_id, order_id, buyer_email, package, expires_at, used_count)
            VALUES (:token, :video_id, :order_id, :buyer_email, :package, :expires_at, 0)
        """),
        {
            "token": token,
            "video_id": video_id,
            "order_id": order_id,
            "buyer_email": buyer_email,
            "package": package,
            "expires_at": expires_at,
        },
    )
    db.session.commit()
    return token


def get_download_token_record(token):
    ensure_download_token_table()
    row = db.session.execute(
        db.text("SELECT * FROM download_token WHERE token = :token"),
        {"token": token},
    ).mappings().first()
    return row


def mark_download_token_used(token):
    try:
        db.session.execute(
            db.text("UPDATE download_token SET used_count = COALESCE(used_count,0) + 1 WHERE token = :token"),
            {"token": token},
        )
        db.session.commit()
    except Exception:
        db.session.rollback()
