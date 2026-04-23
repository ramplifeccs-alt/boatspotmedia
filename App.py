import os
import uuid
import json
import random
import tempfile
import subprocess
import urllib.request
from datetime import datetime
from functools import wraps
from pathlib import Path

from flask import (
    Flask, render_template, request, redirect,
    url_for, flash, session, send_from_directory, abort
)

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

try:
    import boto3
except:
    boto3 = None


# ==============================
# BASE PATHS
# ==============================

BASE_DIR = Path(__file__).resolve().parent

UPLOAD_DIR = BASE_DIR / "uploads"
VIDEO_DIR = UPLOAD_DIR / "videos"
THUMB_DIR = UPLOAD_DIR / "thumbs"
LOGO_DIR = UPLOAD_DIR / "logos"

for p in [VIDEO_DIR, THUMB_DIR, LOGO_DIR]:
    p.mkdir(parents=True, exist_ok=True)


# ==============================
# APP CONFIG
# ==============================

app = Flask(__name__)

app.config["SECRET_KEY"] = os.getenv(
    "SECRET_KEY",
    "boatspotmedia-dev-secret"
)

# 🔧 FIX IMPORTANTE
# antes estaba en 4KB
app.config["MAX_CONTENT_LENGTH"] = 4 * 1024 * 1024 * 1024


db_url = os.getenv(
    "DATABASE_URL",
    f"sqlite:///{BASE_DIR / 'boatspotmedia.db'}"
)

if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql://", 1)

app.config["SQLALCHEMY_DATABASE_URI"] = db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)


# ==============================
# R2 CONFIG
# ==============================

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_BUCKET = os.getenv("R2_BUCKET", "")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_PUBLIC_BASE_URL = os.getenv("R2_PUBLIC_BASE_URL", "").rstrip("/")

r2_client = None

if (
    boto3
    and R2_ACCOUNT_ID
    and R2_BUCKET
    and R2_ACCESS_KEY_ID
    and R2_SECRET_ACCESS_KEY
):

    endpoint = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

    r2_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto"
    )


# ==============================
# MODELS
# ==============================

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    role = db.Column(db.String(20), default="buyer")
    email = db.Column(db.String(255), unique=True)
    password_hash = db.Column(db.String(255))
    public_name = db.Column(db.String(120))
    approved = db.Column(db.Boolean, default=False)


class Batch(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    creator_id = db.Column(db.Integer)
    title = db.Column(db.String(150))
    location = db.Column(db.String(120))
    recorded_date = db.Column(db.Date)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class Video(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    batch_id = db.Column(db.Integer)
    creator_id = db.Column(db.Integer)
    filename = db.Column(db.String(255))
    file_path = db.Column(db.String(255))
    thumb_path = db.Column(db.String(255))
    location = db.Column(db.String(120))
    recorded_date = db.Column(db.Date)
    recorded_time = db.Column(db.Time)
    price = db.Column(db.Float, default=40.0)


# ==============================
# HELPERS
# ==============================

def get_current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    return db.session.get(User, uid)


def save_to_r2(src_path: Path, object_key: str):

    if not r2_client:
        return object_key

    r2_client.upload_file(
        str(src_path),
        R2_BUCKET,
        object_key
    )

    if R2_PUBLIC_BASE_URL:
        return f"{R2_PUBLIC_BASE_URL}/{object_key}"

    return object_key


# ==============================
# THUMBNAIL GENERATOR
# ==============================

def ffprobe_duration(path: Path):

    try:

        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "json",
                str(path)
            ],
            capture_output=True,
            text=True,
            check=True
        )

        data = json.loads(result.stdout)

        return float(data["format"]["duration"])

    except:

        return 8.0


def build_thumbnail(video_path: Path):

    stem = video_path.stem + "_" + uuid.uuid4().hex[:8]

    thumb_file = THUMB_DIR / f"{stem}.jpg"

    dur = ffprobe_duration(video_path)

    thumb_time = min(
        max(1.0, dur / 2),
        dur - 0.5
    )

    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i", str(video_path),
            "-ss", str(thumb_time),
            "-frames:v", "1",
            "-vf", "scale=640:-2",
            "-q:v", "3",
            str(thumb_file)
        ],
        capture_output=True
    )

    if thumb_file.exists():
        return thumb_file

    return None


# ==============================
# MEDIA ROUTE
# ==============================

@app.route("/uploads/<category>/<path:filename>")
def uploaded_file(category, filename):

    directory = {
        "videos": VIDEO_DIR,
        "thumbs": THUMB_DIR,
        "logos": LOGO_DIR
    }.get(category)

    if not directory:
        abort(404)

    return send_from_directory(directory, filename)


# ==============================
# HEALTH CHECK (Railway debug)
# ==============================

@app.route("/healthz")
def healthz():
    return "ok", 200


# ==============================
# HOME
# ==============================

@app.route("/")
def index():

    latest_videos = Video.query.order_by(
        Video.recorded_date.desc()
    ).limit(5).all()

    return render_template(
        "index.html",
        latest_videos=latest_videos
    )


# ==============================
# CREATE BATCH
# ==============================

@app.route("/creator/upload", methods=["POST"])
def creator_upload():

    user = get_current_user()

    if not user:
        return redirect("/login")

    title = request.form.get("title")

    existing = Batch.query.filter_by(
        creator_id=user.id,
        title=title
    ).first()

    if existing:

        flash("Batch already exists")

        return redirect("/dashboard")

    batch = Batch(
        creator_id=user.id,
        title=title,
        location=request.form.get("location"),
        recorded_date=datetime.strptime(
            request.form.get("recorded_date"),
            "%Y-%m-%d"
        )
    )

    db.session.add(batch)
    db.session.commit()

    files = request.files.getlist("videos")

    uploaded = 0

    for file in files:

        if not file.filename:
            continue

        filename = secure_filename(file.filename)

        temp_path = VIDEO_DIR / filename

        file.save(temp_path)

        thumb = build_thumbnail(temp_path)

        video = Video(
            batch_id=batch.id,
            creator_id=user.id,
            filename=filename,
            file_path=f"videos/{filename}",
            thumb_path=f"thumbs/{thumb.name}"
            if thumb else None,
            location=batch.location,
            recorded_date=batch.recorded_date,
            recorded_time=datetime.now().time()
        )

        db.session.add(video)

        uploaded += 1

    if uploaded == 0:

        db.session.delete(batch)

        db.session.commit()

        flash("No videos uploaded")

        return redirect("/dashboard")

    db.session.commit()

    flash("Batch uploaded")

    return redirect("/dashboard")
