import os
import uuid
import json
import subprocess
from datetime import datetime
from pathlib import Path
from functools import wraps

from flask import (
    Flask, render_template, request,
    redirect, url_for, flash,
    session, send_from_directory, abort
)

from flask_sqlalchemy import SQLAlchemy
from werkzeug.utils import secure_filename

try:
    import boto3
except:
    boto3 = None


########################################
# PATH CONFIG
########################################

BASE_DIR = Path(__file__).resolve().parent

UPLOAD_DIR = BASE_DIR / "uploads"
VIDEO_DIR = UPLOAD_DIR / "videos"
THUMB_DIR = UPLOAD_DIR / "thumbs"
LOGO_DIR = UPLOAD_DIR / "logos"

for folder in [VIDEO_DIR, THUMB_DIR, LOGO_DIR]:
    folder.mkdir(parents=True, exist_ok=True)


########################################
# APP CONFIG
########################################

app = Flask(__name__)

app.config["SECRET_KEY"] = os.getenv(
    "SECRET_KEY",
    "boatspotmedia-dev-secret"
)

# ✅ FIX CRÍTICO: antes estaba en 4KB
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


########################################
# R2 CONFIG
########################################

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


########################################
# MODELS
########################################

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    role = db.Column(db.String(20), default="creator")
    email = db.Column(db.String(255), unique=True)


class Batch(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    creator_id = db.Column(db.Integer)
    title = db.Column(db.String(150))
    location = db.Column(db.String(120))
    recorded_date = db.Column(db.Date)
    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )


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


########################################
# HELPERS
########################################

def get_current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    return db.session.get(User, uid)


########################################
# MEDIA URL FILTER
########################################

def media_url(path):

    if not path:
        return ""

    if path.startswith("http"):
        return path

    parts = path.split("/", 1)

    if len(parts) != 2:
        return ""

    return url_for(
        "uploaded_file",
        category=parts[0],
        filename=parts[1]
    )


app.jinja_env.filters["media_url"] = media_url


########################################
# R2 SAVE
########################################

def save_to_r2(local_path, object_key):

    if not r2_client:
        return object_key

    r2_client.upload_file(
        str(local_path),
        R2_BUCKET,
        object_key
    )

    if R2_PUBLIC_BASE_URL:
        return f"{R2_PUBLIC_BASE_URL}/{object_key}"

    return object_key


########################################
# THUMBNAIL GENERATOR
########################################

def get_video_duration(path):

    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-show_entries",
                "format=duration",
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
        return 6.0


def build_thumbnail(video_path):

    stem = video_path.stem + "_" + uuid.uuid4().hex[:6]

    thumb_file = THUMB_DIR / f"{stem}.jpg"

    duration = get_video_duration(video_path)

    capture_time = max(
        1,
        min(duration / 2, duration - 0.5)
    )

    subprocess.run([
        "ffmpeg",
        "-y",
        "-i", str(video_path),
        "-ss", str(capture_time),
        "-frames:v", "1",
        "-vf", "scale=640:-2",
        "-q:v", "3",
        str(thumb_file)
    ])

    if thumb_file.exists():
        return thumb_file

    return None


########################################
# STATIC MEDIA ROUTE
########################################

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


########################################
# HEALTH CHECK
########################################

@app.route("/healthz")
def healthz():
    return "ok", 200


########################################
# HOME
########################################

@app.route("/")
def index():

    latest_videos = Video.query.order_by(
        Video.recorded_date.desc()
    ).limit(5).all()

    return render_template(
        "index.html",
        latest_videos=latest_videos
    )


########################################
# CREATOR UPLOAD
########################################

@app.route("/creator/upload", methods=["POST"])
def creator_upload():

    user = get_current_user()

    if not user:
        return redirect("/login")

    title = request.form.get("title", "").strip()

    if not title:
        flash("Batch title required")
        return redirect("/dashboard")

    # evitar batch duplicado
    existing_batch = Batch.query.filter_by(
        creator_id=user.id,
        title=title
    ).first()

    if existing_batch:
        flash("Batch name already exists")
        return redirect("/dashboard")

    recorded_date_str = request.form.get("recorded_date")

    try:
        recorded_date = datetime.strptime(
            recorded_date_str,
            "%Y-%m-%d"
        ).date()
    except Exception:
        flash("Invalid date")
        return redirect("/dashboard")

    batch = Batch(
        creator_id=user.id,
        title=title,
        location=request.form.get("location"),
        recorded_date=recorded_date
    )

    db.session.add(batch)
    db.session.commit()

    files = request.files.getlist("videos")

    uploaded = 0

    for file in files:

        if not file.filename:
            continue

        filename = secure_filename(file.filename)

        unique_name = f"{uuid.uuid4().hex[:8]}_{filename}"

        local_video_path = VIDEO_DIR / unique_name

        # guardar temporalmente
        file.save(local_video_path)

        # generar thumbnail ANTES de borrar o subir
        thumb_local = build_thumbnail(local_video_path)

        # subir video a R2 o mantener local
        video_key = f"videos/{unique_name}"

        if r2_client:
            video_path = save_to_r2(
                local_video_path,
                video_key
            )
        else:
            video_path = video_key

        # subir thumbnail a R2 o mantener local
        thumb_path = None

        if thumb_local:

            thumb_key = f"thumbs/{thumb_local.name}"

            if r2_client:

                thumb_path = save_to_r2(
                    thumb_local,
                    thumb_key
                )

            else:

                thumb_path = thumb_key

        print("VIDEO PATH SAVED:", video_path, flush=True)
        print("THUMB PATH SAVED:", thumb_path, flush=True)

        video = Video(
            batch_id=batch.id,
            creator_id=user.id,
            filename=filename,
            file_path=video_path,
            thumb_path=thumb_path,
            location=batch.location,
            recorded_date=batch.recorded_date,
            recorded_time=datetime.now().time()
        )

        db.session.add(video)

        uploaded += 1

        # limpiar archivos temporales locales
        try:
            if local_video_path.exists():
                local_video_path.unlink()
        except Exception:
            pass

        try:
            if thumb_local and thumb_local.exists():
                thumb_local.unlink()
        except Exception:
            pass

    if uploaded == 0:

        db.session.delete(batch)
        db.session.commit()

        flash("No videos uploaded")

        return redirect("/dashboard")

    db.session.commit()

    flash("Batch uploaded successfully")

    return redirect("/dashboard")
