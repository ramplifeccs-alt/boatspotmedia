import os, tempfile, uuid
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app
from werkzeug.security import generate_password_hash, check_password_hash
from app.models import User, CreatorProfile, Batch, Video, Location, CreatorClickStats, Product
from app.services.db import db
from app.services.media import extract_creation_time, generate_center_thumbnail
from app.services.r2 import upload as r2_upload

creator_bp = Blueprint("creator", __name__)

@creator_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        return redirect(url_for("creator.dashboard"))
    return render_template("creator/login.html")

@creator_bp.route("/dashboard")
def dashboard():
    creator = CreatorProfile.query.first()
    stats = CreatorClickStats.query.filter_by(creator_id=creator.id).first() if creator else None
    videos_count = Video.query.filter_by(creator_id=creator.id, status="active").count() if creator else 0
    return render_template("creator/dashboard.html", creator=creator, stats=stats, videos_count=videos_count)

@creator_bp.route("/upload", methods=["GET", "POST"])
def upload():
    creator = CreatorProfile.query.first()
    if not creator:
        # dev seed creator for testing
        user = User(email="creator@test.com", role="creator", display_name="Test Creator", is_active=True)
        db.session.add(user); db.session.flush()
        creator = CreatorProfile(user_id=user.id, approved=True, storage_limit_gb=512, commission_rate=20)
        db.session.add(creator); db.session.flush()
        db.session.add(CreatorClickStats(creator_id=creator.id))
        db.session.commit()

    locations = Location.query.order_by(Location.name.asc()).all()

    if request.method == "POST":
        files = request.files.getlist("videos")
        location = request.form.get("location")
        try:
            original_price = float(request.form.get("original_price") or 40)
            edited_price = float(request.form.get("edited_price") or 60)
            bundle_price = float(request.form.get("bundle_price") or 80)
        except:
            original_price, edited_price, bundle_price = 40,60,80

        total_size = 0
        temp_paths = []
        for f in files:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(0)
            total_size += size
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(f.filename)[1] or ".mp4")
            f.save(tmp.name)
            temp_paths.append((tmp.name, f.filename, size))

        if total_size > current_app.config["MAX_BATCH_GB"] * 1024**3:
            return render_template("creator/upload.html", locations=locations, error="Maximum upload size per batch is 128GB.")

        if creator.storage_used_bytes + total_size > creator.storage_limit_gb * 1024**3:
            return render_template("creator/upload.html", locations=locations, error="Storage limit reached. Delete videos or upgrade your plan.")

        batch = Batch(creator_id=creator.id, location=location, total_size_bytes=total_size)
        db.session.add(batch); db.session.flush()

        for path, filename, size in temp_paths:
            recorded_at = extract_creation_time(path)
            video_key = f"creator_{creator.id}/batch_{batch.id}/{uuid.uuid4()}_{filename}"
            video_url = r2_upload(path, current_app.config["R2_BUCKET_VIDEOS"], video_key)

            thumb_path = path + ".jpg"
            try:
                generate_center_thumbnail(path, thumb_path)
                thumb_key = f"video_thumbs/{uuid.uuid4()}.jpg"
                thumb_url = r2_upload(thumb_path, current_app.config["R2_BUCKET_THUMBNAILS"], thumb_key)
            except Exception:
                thumb_key = None
                thumb_url = ""

            v = Video(
                creator_id=creator.id, batch_id=batch.id, location=location,
                recorded_at=recorded_at, r2_video_key=video_key, r2_thumbnail_key=thumb_key,
                public_thumbnail_url=thumb_url, file_size_bytes=size,
                original_price=original_price, edited_price=edited_price, bundle_price=bundle_price,
                internal_filename=filename
            )
            db.session.add(v)
            try:
                os.unlink(path)
                if os.path.exists(thumb_path): os.unlink(thumb_path)
            except Exception:
                pass

        creator.storage_used_bytes += total_size
        db.session.commit()
        return redirect(url_for("creator.batches"))

    return render_template("creator/upload.html", locations=locations)

@creator_bp.route("/batches")
def batches():
    batches = Batch.query.order_by(Batch.created_at.desc()).all()
    return render_template("creator/batches.html", batches=batches)

@creator_bp.route("/videos/delete/<int:video_id>", methods=["POST"])
def delete_video(video_id):
    v = Video.query.get_or_404(video_id)
    v.status = "deleted"
    creator = CreatorProfile.query.get(v.creator_id)
    creator.storage_used_bytes = max(0, creator.storage_used_bytes - (v.file_size_bytes or 0))
    db.session.commit()
    return redirect(url_for("creator.batches"))

@creator_bp.route("/settings", methods=["GET", "POST"])
def settings():
    creator = CreatorProfile.query.first()
    if request.method == "POST" and creator:
        creator.second_clip_discount_percent = int(request.form.get("second_clip_discount_percent") or 0)
        db.session.commit()
        return redirect(url_for("creator.settings"))
    return render_template("creator/settings.html", creator=creator)
