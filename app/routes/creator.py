from datetime import datetime, date, time, timezone
from werkzeug.security import check_password_hash
import os, tempfile, uuid
from flask import Blueprint, render_template, request, redirect, url_for, current_app, flash, jsonify, session
from app.models import User, CreatorProfile, Batch, Video, Location, CreatorClickStats, Product, VideoPricingPreset, OrderItem, StoragePlan, ProductVariant
from app.services.db import db
from app.services.media import extract_creation_time, generate_center_thumbnail
from app.services.r2 import upload as r2_upload

creator_bp = Blueprint("creator", __name__)

def creator_instagram(creator):
    try:
        value = getattr(creator, "instagram", None)
        if value:
            return str(value).replace("@", "")
    except Exception:
        pass

    try:
        if creator.user and creator.user.display_name:
            return str(creator.user.display_name).replace("@", "")
    except Exception:
        pass

    return ""

def creator_display_name(creator):
    try:
        if creator.user and creator.user.display_name and creator.user.display_name != "None":
            return creator.user.display_name
    except Exception:
        pass

    ig = creator_instagram(creator)
    return ig or "Creator"


def _ensure_creator_profile_deleted_column():
    """Create creator_profile.deleted before ORM queries reference it."""
    try:
        db.session.execute(db.text("ALTER TABLE creator_profile ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE"))
        db.session.execute(db.text("UPDATE creator_profile SET deleted = FALSE WHERE deleted IS NULL"))
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        print("creator_profile.deleted repair warning:", e)



ALLOWED_VIDEO_EXTENSIONS = {".mp4",".mov",".mxf",".avi",".mts",".m2ts",".3gp",".hevc",".h265",".h264",".m4v",".mpg",".mpeg",".wmv"}

def _is_allowed_video_filename(filename):
    return os.path.splitext((filename or "").lower())[1] in ALLOWED_VIDEO_EXTENSIONS

def _creator_default_prices(creator):
    snap = _creator_plan_snapshot(creator)
    original = snap.get("original_price") or 50
    edited = snap.get("edited_price")
    bundle = snap.get("bundle_price")
    try:
        edited = float(edited) if edited is not None and str(edited) != "" else 0
    except Exception:
        edited = 0
    try:
        bundle = float(bundle) if bundle is not None and str(bundle) != "" else 0
    except Exception:
        bundle = 0
    return float(original), edited, bundle


def _recalculate_creator_storage(creator_id):
    """Safely recalculate storage without aborting the SQLAlchemy transaction."""
    try:
        from app.models import CreatorProfile, Video
        total = int(db.session.query(db.func.coalesce(db.func.sum(Video.file_size_bytes), 0)).filter(
            Video.creator_id == creator_id,
            Video.status != "deleted"
        ).scalar() or 0)
        c = CreatorProfile.query.get(creator_id)
        if c and hasattr(c, "storage_used_bytes"):
            c.storage_used_bytes = int(total)
            db.session.commit()
        return int(total)
    except Exception as e:
        db.session.rollback()
        print("storage recalculation warning:", e)
        return 0


def current_creator():
    _ensure_creator_profile_deleted_column()
    user_id = session.get("user_id")
    user_email = session.get("user_email") or session.get("email")
    creator_id = session.get("creator_id")

    q = CreatorProfile.query.filter(
        CreatorProfile.approved == True,
        CreatorProfile.suspended == False,
        db.or_(CreatorProfile.deleted == False, CreatorProfile.deleted.is_(None))
    )

    if creator_id:
        creator = q.filter_by(id=creator_id).first()
        if creator:
            return creator

    if user_id:
        creator = q.filter_by(user_id=user_id).first()
        if creator:
            return creator

    if user_email:
        creator = q.join(User, CreatorProfile.user_id == User.id).filter(db.func.lower(User.email) == user_email.lower()).first()
        if creator:
            return creator

    return None


def render_creator_template(template_name, **kwargs):
    creator = kwargs.get("creator") or current_creator()
    kwargs["creator"] = creator
    kwargs["creator_name"] = creator_display_name(creator)
    kwargs["creator_instagram"] = creator_instagram(creator)
    kwargs.setdefault('plan_snapshot', _creator_plan_snapshot(kwargs.get('creator')))
    return render_template(template_name, **kwargs)


def _creator_location_suggestions():
    try:
        from app.models import Video
        rows = db.session.query(Video.location).filter(Video.location.isnot(None), Video.location != "", Video.status != "deleted").distinct().order_by(Video.location.asc()).all()
        return [" ".join(str(r[0]).strip().split()) for r in rows if r and r[0]]
    except Exception:
        db.session.rollback()
        return []



def _safe_secure_filename(name):
    try:
        return secure_filename(name or "video")
    except Exception:
        import re
        name = str(name or "video")
        name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("._")
        return name or "video"



def _normalize_completed_upload_files(data):
    """Accept all frontend payload names for completed uploads."""
    files = (
        data.get("files")
        or data.get("uploaded_files")
        or data.get("videos")
        or data.get("uploads")
        or data.get("uploadedVideos")
        or []
    )
    if isinstance(files, dict):
        files = list(files.values())
    normalized = []
    for item in files:
        if not isinstance(item, dict):
            continue
        upload = item.get("upload") or item.get("uploaded") or item
        filename = (
            item.get("filename")
            or item.get("name")
            or upload.get("filename")
            or upload.get("name")
            or upload.get("original_filename")
        )
        key = (
            item.get("key")
            or item.get("r2_video_key")
            or item.get("r2_key")
            or upload.get("key")
            or upload.get("r2_video_key")
            or upload.get("r2_key")
        )
        size = (
            item.get("file_size")
            or item.get("size")
            or item.get("file_size_bytes")
            or upload.get("file_size")
            or upload.get("size")
            or upload.get("file_size_bytes")
            or 0
        )
        if filename:
            normalized.append({
                "filename": filename,
                "key": key,
                "file_size": int(size or 0),
                "upload": upload,
                "last_modified": item.get("last_modified") or upload.get("last_modified"),
                "last_modified_iso": item.get("last_modified_iso") or upload.get("last_modified_iso"),
                "last_modified_local": item.get("last_modified_local") or upload.get("last_modified_local"),
                "thumbnail_key": item.get("thumbnail_key") or item.get("r2_thumbnail_key") or upload.get("thumbnail_key") or upload.get("r2_thumbnail_key"),
                "public_thumbnail_url": item.get("public_thumbnail_url") or upload.get("public_thumbnail_url") or upload.get("thumbnail_url"),
            })
    return normalized



def _safe_recorded_date_time_from_file_info(file_info):
    """
    Prefer the local file timestamp sent by the browser.
    This avoids UTC shifting and is closer to the time shown on the creator's computer.
    """
    upload = file_info.get("upload") or {}

    for source in (file_info, upload):
        local = source.get("last_modified_local") or source.get("file_last_modified_local")
        if local:
            try:
                dt = datetime.fromisoformat(str(local))
                return dt.date(), dt.time().replace(microsecond=0)
            except Exception:
                pass

        iso = source.get("last_modified_iso") or source.get("file_last_modified_iso")
        if iso:
            try:
                dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00")).astimezone()
                return dt.date(), dt.time().replace(microsecond=0)
            except Exception:
                pass

        ms = source.get("last_modified") or source.get("file_last_modified")
        if ms:
            try:
                dt = datetime.fromtimestamp(float(ms) / 1000.0)
                return dt.date(), dt.time().replace(microsecond=0)
            except Exception:
                pass

        recorded_at = source.get("recorded_at") or source.get("created_at") or source.get("media_created_at")
        if isinstance(recorded_at, datetime):
            return recorded_at.date(), recorded_at.time().replace(microsecond=0)
        if isinstance(recorded_at, str) and recorded_at:
            try:
                dt = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
                return dt.date(), dt.time().replace(microsecond=0)
            except Exception:
                pass

    return date.today(), time(0, 0, 0)


def _cleanup_uploaded_r2_files(files):
    try:
        from app.services.r2 import delete_r2_object
        for f in files or []:
            upload = f.get("upload") or {}
            key = (
                f.get("key")
                or f.get("r2_video_key")
                or f.get("r2_key")
                or upload.get("key")
                or upload.get("r2_video_key")
                or upload.get("r2_key")
            )
            if key:
                try:
                    delete_r2_object(key)
                except Exception:
                    pass
    except Exception:
        pass


def _safe_video_create_from_upload(creator_id, batch, file_info, location=None):
    from app.models import Video

    upload = file_info.get("upload") or {}
    filename = file_info.get("filename") or file_info.get("name") or upload.get("filename") or upload.get("name") or "video"
    key = (
        file_info.get("key")
        or file_info.get("r2_video_key")
        or file_info.get("r2_key")
        or upload.get("key")
        or upload.get("r2_video_key")
        or upload.get("r2_key")
        or filename
    )
    size = int(file_info.get("file_size") or file_info.get("size") or upload.get("file_size") or upload.get("size") or 0)

    recorded_date, recorded_time = _safe_recorded_date_time_from_file_info(file_info)
    original_price = getattr(batch, "original_price", None) or 0
    edited_price = getattr(batch, "edited_price", None) or 0
    bundle_price = getattr(batch, "bundle_price", None) or 0

    cols = set(Video.__table__.columns.keys())
    kwargs = {}

    if "creator_id" in cols:
        kwargs["creator_id"] = creator_id
    if "batch_id" in cols:
        kwargs["batch_id"] = getattr(batch, "id", None)
    if "location" in cols:
        kwargs["location"] = location or getattr(batch, "location", None)
    if "filename" in cols:
        kwargs["filename"] = filename
    if "internal_filename" in cols:
        kwargs["internal_filename"] = filename
    if "file_path" in cols:
        kwargs["file_path"] = key
    if "r2_video_key" in cols:
        kwargs["r2_video_key"] = key
    thumb_key = file_info.get("thumbnail_key") or file_info.get("r2_thumbnail_key") or upload.get("thumbnail_key") or upload.get("r2_thumbnail_key")
    thumb_url = file_info.get("public_thumbnail_url") or upload.get("public_thumbnail_url") or upload.get("thumbnail_url")
    if thumb_key and "r2_thumbnail_key" in cols:
        kwargs["r2_thumbnail_key"] = thumb_key
    if thumb_url and "public_thumbnail_url" in cols:
        kwargs["public_thumbnail_url"] = thumb_url
    if thumb_url and "thumbnail_url" in cols:
        kwargs["thumbnail_url"] = thumb_url
    if thumb_key and "thumbnail_path" in cols:
        kwargs["thumbnail_path"] = thumb_key
    if "file_size_bytes" in cols:
        kwargs["file_size_bytes"] = size
    if "recorded_date" in cols:
        kwargs["recorded_date"] = recorded_date
    if "recorded_time" in cols:
        kwargs["recorded_time"] = recorded_time
    if "recorded_at" in cols:
        kwargs["recorded_at"] = datetime.combine(recorded_date, recorded_time)
    if "price" in cols:
        kwargs["price"] = original_price or 0
    if "original_price" in cols:
        kwargs["original_price"] = original_price or 0
    if "edited_price" in cols:
        kwargs["edited_price"] = edited_price or 0
    if "bundle_price" in cols:
        kwargs["bundle_price"] = bundle_price or 0
    if "status" in cols:
        kwargs["status"] = "active"
    if "created_at" in cols:
        kwargs["created_at"] = datetime.utcnow()

    return Video(**kwargs)


def _r2_public_url_for_key(key):
    import os
    if not key:
        return None
    if str(key).startswith("http://") or str(key).startswith("https://"):
        return key
    base = (os.getenv("R2_PUBLIC_BASE_URL") or "").rstrip("/")
    if base:
        return f"{base}/{str(key).lstrip('/')}"
    return None




def _fill_public_thumbnail_url(video):
    try:
        import os
        base = (os.getenv("R2_PUBLIC_BASE_URL") or "").rstrip("/")
        key = getattr(video, "r2_thumbnail_key", None) or getattr(video, "thumbnail_path", None)
        if base and key and not str(key).startswith("http"):
            url = f"{base}/{str(key).lstrip('/')}"
            cols = set(video.__table__.columns.keys())
            if "public_thumbnail_url" in cols:
                video.public_thumbnail_url = url
            if "thumbnail_url" in cols:
                video.thumbnail_url = url
            db.session.add(video)
            return True
    except Exception:
        pass
    return False



def _parse_ffprobe_creation_time(local_video):
    try:
        import subprocess, json
        probe = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format_tags=creation_time:stream_tags=creation_time",
                "-of", "json",
                local_video
            ],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30
        )
        data = json.loads(probe.stdout or "{}")
        candidates = []
        fmt_tags = ((data.get("format") or {}).get("tags") or {})
        if fmt_tags.get("creation_time"):
            candidates.append(fmt_tags.get("creation_time"))
        for stream in data.get("streams") or []:
            tags = stream.get("tags") or {}
            if tags.get("creation_time"):
                candidates.append(tags.get("creation_time"))
        for raw in candidates:
            try:
                dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone()
                return dt.date(), dt.time().replace(microsecond=0)
            except Exception:
                pass
    except Exception as e:
        try: print("optional ffprobe creation_time warning:", e)
        except Exception: pass
    return None, None


def _probe_video_duration(local_video):
    try:
        import subprocess
        probe = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=nw=1:nk=1", local_video],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=25
        )
        return float((probe.stdout or "0").strip() or 0)
    except Exception:
        return 0


def _looks_dark_thumbnail(path):
    try:
        # Use ffmpeg signalstats if Pillow is unavailable.
        import os
        if not os.path.exists(path) or os.path.getsize(path) < 1000:
            return True
        try:
            from PIL import Image, ImageStat
            img = Image.open(path).convert("L").resize((80, 45))
            stat = ImageStat.Stat(img)
            mean = stat.mean[0] if stat.mean else 0
            extrema = img.getextrema()
            spread = (extrema[1] - extrema[0]) if extrema else 0
            return mean < 18 and spread < 45
        except Exception:
            return False
    except Exception:
        return False


def _generate_frame_at(local_video, local_thumb, seek):
    import subprocess, os
    if os.path.exists(local_thumb):
        try: os.remove(local_thumb)
        except Exception: pass
    vf = "scale=1664:-1,crop=1280:720"
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(max(0.1, float(seek))),
        "-i", local_video,
        "-frames:v", "1",
        "-vf", vf,
        "-q:v", "3",
        local_thumb
    ]
    subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=80)
    return os.path.exists(local_thumb) and os.path.getsize(local_thumb) > 1000


def _generate_and_attach_thumbnail_for_video(video):
    """
    Download video from R2, read creation_time with ffprobe, generate non-black thumbnail,
    upload thumbnail to R2, and save thumbnail fields.
    """
    try:
        import os, tempfile, uuid, shutil
        from app.services.r2 import get_r2_client, _bucket_name

        video_key = getattr(video, "r2_video_key", None) or getattr(video, "file_path", None)
        if not video_key:
            return False

        client = get_r2_client()
        bucket = _bucket_name()
        tmp_dir = tempfile.mkdtemp(prefix="bsm_thumb_")
        local_video = os.path.join(tmp_dir, "video_input")
        local_thumb = os.path.join(tmp_dir, "thumb.jpg")

        client.download_file(bucket, video_key, local_video)

        cols = set(video.__table__.columns.keys())

        rd, rt = _parse_ffprobe_creation_time(local_video)
        if rd and "recorded_date" in cols:
            video.recorded_date = rd
        if rt and "recorded_time" in cols:
            video.recorded_time = rt
        if rd and rt and "recorded_at" in cols:
            video.recorded_at = datetime.combine(rd, rt)

        duration = _probe_video_duration(local_video)
        if duration and duration > 10:
            seeks = [duration*0.50, duration*0.33, duration*0.66, duration*0.20, duration*0.80, 3, 1]
        elif duration and duration > 3:
            seeks = [duration*0.50, duration*0.30, duration*0.70, 2, 1]
        else:
            seeks = [1, 0.5, 2]

        good = False
        for seek in seeks:
            try:
                if _generate_frame_at(local_video, local_thumb, seek) and not _looks_dark_thumbnail(local_thumb):
                    good = True
                    break
            except Exception:
                pass

        if not good:
            for seek in seeks:
                try:
                    if _generate_frame_at(local_video, local_thumb, seek):
                        good = True
                        break
                except Exception:
                    pass

        if good:
            filename = getattr(video, "filename", "video") or "video"
            thumb_key = f"creators/{getattr(video, 'creator_id', 'unknown')}/batches/{getattr(video, 'batch_id', 'unknown')}/thumbs/{uuid.uuid4().hex}_{filename}.jpg"
            client.upload_file(local_thumb, bucket, thumb_key, ExtraArgs={"ContentType": "image/jpeg"})

            public_url = _r2_public_url_for_key(thumb_key) if "_r2_public_url_for_key" in globals() else None
            if "r2_thumbnail_key" in cols:
                video.r2_thumbnail_key = thumb_key
            if "thumbnail_path" in cols:
                video.thumbnail_path = thumb_key
            if public_url and "public_thumbnail_url" in cols:
                video.public_thumbnail_url = public_url
            if public_url and "thumbnail_url" in cols:
                video.thumbnail_url = public_url

        db.session.add(video)
        try: shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception: pass
        return True
    except Exception as e:
        try: print("optional thumbnail/metadata generation warning:", e)
        except Exception: pass
        return False



@creator_bp.route("/video/<int:video_id>/generate-thumbnail", methods=["POST"])
def generate_video_thumbnail(video_id):
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401
    from app.models import Video
    v = Video.query.get_or_404(video_id)
    if getattr(v, "creator_id", None) != getattr(creator, "id", None):
        return jsonify({"ok": False, "error": "Not allowed."}), 403
    ok = _generate_and_attach_thumbnail_for_video(v)
    if ok:
        db.session.commit()
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "Could not generate thumbnail. Make sure ffmpeg is installed on the server."}), 500


@creator_bp.route("/health")
def health():
    c = current_creator()
    return {
        "ok": True,
        "creator_id": c.id,
        "display_name": creator_display_name(c),
        "instagram_safe": creator_instagram(c),
        "storage_limit_gb": c.storage_limit_gb
    }

@creator_bp.route("/logout")
def logout():
    return redirect("/")

@creator_bp.route("/login", methods=["GET", "POST"])
def login():
    _ensure_creator_profile_deleted_column()
    if request.method == "POST":
        email = (request.form.get("email") or request.form.get("username") or "").strip().lower()
        password = request.form.get("password") or ""

        user = User.query.filter(db.func.lower(User.email) == email).first()
        if not user:
            flash("Email or password is incorrect. Please check and try again.", "error")
            return render_template("creator/login.html")

        stored_hash = getattr(user, "password_hash", None) or getattr(user, "password", None)
        valid_password = False
        try:
            valid_password = bool(stored_hash and check_password_hash(stored_hash, password))
        except Exception:
            valid_password = False
        if not valid_password and stored_hash and stored_hash == password:
            valid_password = True

        if not valid_password:
            flash("Email or password is incorrect. Please check and try again.", "error")
            return render_template("creator/login.html")

        creator = CreatorProfile.query.filter(
            CreatorProfile.user_id == user.id,
            CreatorProfile.approved == True,
            CreatorProfile.suspended == False,
            db.or_(CreatorProfile.deleted == False, CreatorProfile.deleted.is_(None))
        ).first()

        if not creator:
            flash("Creator account is not approved or is no longer active.", "error")
            return render_template("creator/login.html")

        session.clear()
        session["user_id"] = user.id
        session["user_email"] = user.email
        session["creator_id"] = creator.id
        session["role"] = "creator"
        return redirect(url_for("creator.dashboard"))

    return render_template("creator/login.html")




@creator_bp.route("/login/apple")
def apple_login_under_construction():
    flash("Apple login is under construction. You'll be able to log in with Apple soon.", "info")
    return redirect(url_for("creator.login"))


@creator_bp.route("/dashboard")
def dashboard():
    _ensure_creator_profile_deleted_column()
    creator = current_creator()
    if creator:
        safe_recalc = _recalculate_creator_storage(creator.id)
    storage_limit_gb, max_batch_gb = _creator_plan_limits(creator)
    storage_used_gb = _creator_storage_used_gb(creator.id)
    if not creator:
        flash('Please log in with an approved creator account.', 'error')
        return redirect(url_for('creator.login'))
    stats = CreatorClickStats.query.filter_by(creator_id=creator.id).first()
    if not stats:
        stats = CreatorClickStats(creator_id=creator.id)
        db.session.add(stats)
        db.session.commit()

    try:
        videos_count = Video.query.filter_by(creator_id=creator.id, status="active").count()
    except Exception:
        db.session.rollback()
        videos_count = 0

    return render_creator_template(
        "creator/dashboard.html",
        creator=creator,
        stats=stats,
        videos_count=videos_count
    )





@creator_bp.route("/videos/delete-selected", methods=["POST"])
def delete_selected_videos():
    creator = current_creator()
    for video_id in request.form.getlist("video_ids"):
        v = Video.query.filter_by(id=int(video_id), creator_id=creator.id).first()
        if v and v.status != "deleted":
            v.status = "deleted"
            creator.storage_used_bytes = max(0, creator.storage_used_bytes - (v.file_size_bytes or 0))
    db.session.commit()
    return redirect(request.referrer or url_for("creator.batches"))

@creator_bp.route("/orders")
def orders():
    creator = current_creator()
    status = request.args.get("status", "pending")

    try:
        q = OrderItem.query.filter_by(creator_id=creator.id)
        if status == "pending":
            q = q.filter(OrderItem.edited_status == "pending")
        elif status == "completed":
            q = q.filter(OrderItem.edited_status.in_(["ready", "not_required"]))
        items = q.order_by(OrderItem.id.desc()).all()
    except Exception:
        db.session.rollback()
        items = []

    return render_creator_template("creator/orders.html", creator=creator, items=items, status=status)

@creator_bp.route("/products", methods=["GET", "POST"])
def products():
    creator = current_creator()
    edit_id = request.args.get("edit")
    edit_product = None

    if edit_id and edit_id.isdigit():
        edit_product = Product.query.filter_by(id=int(edit_id), creator_id=creator.id).first()

    if request.method == "POST":
        product_id = request.form.get("product_id")

        if product_id:
            p = Product.query.filter_by(id=int(product_id), creator_id=creator.id).first_or_404()
        else:
            p = Product(creator_id=creator.id)
            db.session.add(p)

        p.title = request.form.get("title")
        p.description = request.form.get("description")
        p.price = float(request.form.get("price") or 0)
        p.shipping_cost = float(request.form.get("shipping_cost") or 0)
        p.processing_time = request.form.get("processing_time")
        p.shipping_method = request.form.get("shipping_method")
        p.active = True
        db.session.commit()

        return redirect(url_for("creator.products"))

    products = Product.query.filter_by(creator_id=creator.id).all()
    return render_creator_template("creator/products.html", creator=creator, products=products, edit_product=edit_product)

@creator_bp.route("/products/<int:product_id>/delete", methods=["POST"])
def delete_product(product_id):
    creator = current_creator()
    p = Product.query.filter_by(id=product_id, creator_id=creator.id).first_or_404()
    p.active = False
    db.session.commit()
    return redirect(url_for("creator.products"))

@creator_bp.route("/pricing", methods=["GET", "POST"])
def pricing():
    creator = current_creator()

    if request.method == "POST":
        preset_id = request.form.get("preset_id")

        if request.form.get("is_default"):
            VideoPricingPreset.query.filter_by(creator_id=creator.id).update({"is_default": False})

        if preset_id:
            p = VideoPricingPreset.query.filter_by(id=int(preset_id), creator_id=creator.id).first_or_404()
        else:
            p = VideoPricingPreset(creator_id=creator.id)
            db.session.add(p)

        p.title = request.form.get("title") or "Default Video Price"
        p.description = request.form.get("description")
        p.price = float(request.form.get("price") or 40)
        p.delivery_type = request.form.get("delivery_type") or "instant"
        p.is_default = bool(request.form.get("is_default"))
        p.active = True

        creator.second_clip_discount_percent = int(request.form.get("second_clip_discount_percent") or creator.second_clip_discount_percent or 0)

        db.session.commit()
        return redirect(url_for("creator.pricing"))

    presets = VideoPricingPreset.query.filter_by(creator_id=creator.id, active=True).order_by(VideoPricingPreset.id.desc()).all()
    edit_id = request.args.get("edit")
    edit_preset = VideoPricingPreset.query.filter_by(id=int(edit_id), creator_id=creator.id).first() if edit_id and edit_id.isdigit() else None

    return render_creator_template("creator/pricing.html", creator=creator, presets=presets, edit_preset=edit_preset)

@creator_bp.route("/pricing/<int:preset_id>/delete", methods=["POST"])
def delete_pricing(preset_id):
    creator = current_creator()
    p = VideoPricingPreset.query.filter_by(id=preset_id, creator_id=creator.id).first_or_404()
    p.active = False
    db.session.commit()
    return redirect(url_for("creator.pricing"))

@creator_bp.route("/settings", methods=["GET", "POST"])
def settings():
    creator = current_creator()

    if request.method == "POST":
        action = request.form.get("action")

        if action == "change_plan":
            plan_id = request.form.get("plan_id")
            plan = StoragePlan.query.get(plan_id) if plan_id else None
            if plan and plan.active:
                creator.plan_id = plan.id
                creator.storage_limit_gb = plan.storage_limit_gb
                creator.commission_rate = plan.commission_rate
                db.session.commit()
            return redirect(url_for("creator.settings"))

        if creator.user:
            creator.user.display_name = request.form.get("display_name") or creator.user.display_name
            creator.user.email = request.form.get("email") or creator.user.email

        db.session.commit()
        return redirect(url_for("creator.settings"))

    plans = StoragePlan.query.filter_by(active=True).order_by(StoragePlan.storage_limit_gb.asc()).all()
    return render_creator_template("creator/settings.html", creator=creator, plans=plans)


@creator_bp.route("/products/<int:product_id>/variants", methods=["GET", "POST"])
def product_variants(product_id):
    creator = current_creator()
    product = Product.query.filter_by(id=product_id, creator_id=creator.id).first_or_404()
    if request.method == "POST":
        variant = ProductVariant(product_id=product.id)
        variant.color_name = request.form.get("color_name")
        variant.color_hex = request.form.get("color_hex")
        variant.price_adjustment = float(request.form.get("price_adjustment") or 0)
        variant.active = True
        db.session.add(variant)
        db.session.commit()
        return redirect(url_for("creator.product_variants", product_id=product.id))
    variants = ProductVariant.query.filter_by(product_id=product.id, active=True).all()
    return render_creator_template("creator/product_variants.html", creator=creator, product=product, variants=variants)

@creator_bp.route("/products/variants/<int:variant_id>/delete", methods=["POST"])
def delete_product_variant(variant_id):
    creator = current_creator()
    variant = ProductVariant.query.get_or_404(variant_id)
    product = Product.query.filter_by(id=variant.product_id, creator_id=creator.id).first_or_404()
    variant.active = False
    db.session.commit()
    return redirect(url_for("creator.product_variants", product_id=product.id))

# ===== Creator video upload v35 Cloudflare R2 Direct Upload =====

BATCH_LIMIT_BYTES = 128 * 1024 * 1024 * 1024  # 128 GB per batch


def _creator_storage_limit_bytes(creator):
    try:
        plan = getattr(creator, "plan", None) or getattr(creator, "storage_plan", None)
        if plan and getattr(plan, "storage_limit_gb", None):
            return int(plan.storage_limit_gb) * 1024 * 1024 * 1024
    except Exception:
        pass
    try:
        if getattr(creator, "storage_limit_gb", None):
            return int(creator.storage_limit_gb) * 1024 * 1024 * 1024
    except Exception:
        pass
    return 128 * 1024 * 1024 * 1024


def _creator_used_storage_bytes(creator_id):
    from app.models import Video
    total = db.session.query(db.func.coalesce(db.func.sum(Video.file_size_bytes), 0)).filter_by(creator_id=creator_id).scalar()
    return int(total or 0)


@creator_bp.route("/upload", methods=["GET"])
def upload():
    _ensure_creator_profile_deleted_column()
    creator = current_creator()
    if not creator:
        return redirect("/creator/login")

    used = _creator_used_storage_bytes(creator.id)
    limit = _creator_storage_limit_bytes(creator)
    storage_limit_gb, max_batch_gb = _creator_plan_limits(creator)
    storage_used_gb = _creator_storage_used_gb(creator.id)
    return render_template("creator/upload.html",
        used_bytes=used,
        limit_bytes=limit,
        used_gb=round(used / 1024 / 1024 / 1024, 2),
        video_locations=_creator_location_suggestions(),
        plan_snapshot=_creator_plan_snapshot(creator),
        storage_limit_gb=storage_limit_gb,
        max_batch_gb=max_batch_gb,
        storage_used_gb=storage_used_gb,
        limit_gb=round(limit / 1024 / 1024 / 1024, 2),
        batch_limit_gb=128,
    )






def _creator_active_plan(creator):
    """Return the real plan assigned by Owner, avoiding stale/default plan values."""
    if not creator:
        return None
    try:
        plan_id = getattr(creator, "plan_id", None)
        if plan_id:
            try:
                from app.models import Plan
                plan = Plan.query.get(plan_id)
                if plan:
                    return plan
            except Exception:
                db.session.rollback()
        plan = getattr(creator, "plan", None)
        if plan:
            return plan
    except Exception:
        db.session.rollback()
    return None


def _creator_plan_snapshot(creator):
    plan = _creator_active_plan(creator)
    def val(obj, names, default=None):
        for name in names:
            try:
                v = getattr(obj, name, None) if obj else None
                if v is not None and str(v) != "":
                    return v
            except Exception:
                pass
        return default

    return {
        "plan": plan,
        "plan_name": val(plan, ["name", "title", "plan_name"], "No plan"),
        "storage_limit_gb": float(val(plan, ["storage_limit_gb", "storage_gb", "max_storage_gb"], getattr(creator, "storage_limit_gb", 500) or 500)),
        "max_batch_gb": float(val(plan, ["max_batch_gb", "batch_limit_gb", "upload_limit_gb"], 128)),
        "original_price": float(val(plan, ["original_price", "default_original_price", "instant_download_price"], getattr(creator, "original_price", getattr(creator, "default_original_price", 50)) or 50)),
        "edited_price": val(plan, ["edited_price", "default_edited_price"], getattr(creator, "edited_price", getattr(creator, "default_edited_price", None))),
        "bundle_price": val(plan, ["bundle_price", "default_bundle_price"], getattr(creator, "bundle_price", getattr(creator, "default_bundle_price", None))),
    }


def _creator_plan_limits(creator):
    snap = _creator_plan_snapshot(creator)
    return snap["storage_limit_gb"], snap["max_batch_gb"]


def _creator_storage_used_gb(creator_id):
    """Safe dashboard storage calculation.
    Uses video.file_size_bytes and profile fallback only. Avoids broken SQL against unknown batch columns.
    """
    total = 0
    try:
        from app.models import Video
        total = int(db.session.query(db.func.coalesce(db.func.sum(Video.file_size_bytes), 0)).filter(
            Video.creator_id == creator_id,
            Video.status != "deleted"
        ).scalar() or 0)
    except Exception as e:
        db.session.rollback()
        print("storage video sum warning:", e)

    try:
        from app.models import CreatorProfile
        c = CreatorProfile.query.get(creator_id)
        if c and getattr(c, "storage_used_bytes", None):
            total = max(total, int(c.storage_used_bytes or 0))
    except Exception as e:
        db.session.rollback()
        print("storage profile warning:", e)

    return round(float(total) / (1024 ** 3), 2)


def _ensure_batch_exists_for_upload(batch_id, creator_id, batch_name="", location=""):
    """Guarantee video_batch row exists before inserting videos."""
    try:
        from app.models import VideoBatch
        existing = VideoBatch.query.get(batch_id)
        if existing:
            return existing
        b = VideoBatch(
            id=batch_id,
            creator_id=creator_id,
            batch_name=batch_name or f"Batch {batch_id}",
            location=location or "",
            status="uploaded"
        )
        db.session.add(b)
        db.session.commit()
        return b
    except Exception as e:
        db.session.rollback()
        print("VideoBatch ensure warning:", e)
        try:
            db.session.execute(db.text("""
                INSERT INTO video_batch (id, creator_id, batch_name, location, status, created_at)
                VALUES (:id, :creator_id, :name, :location, 'uploaded', NOW())
                ON CONFLICT (id) DO NOTHING
            """), {
                "id": batch_id,
                "creator_id": creator_id,
                "name": batch_name or f"Batch {batch_id}",
                "location": location or ""
            })
            db.session.commit()
        except Exception as e2:
            db.session.rollback()
            print("Raw video_batch ensure warning:", e2)
        return None


def _ensure_video_upload_columns():
    """Make sure old PostgreSQL video table has the columns required by the uploader before any SELECT."""
    statements = [
        "ALTER TABLE creator_profile ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE",
        "UPDATE creator_profile SET deleted = FALSE WHERE deleted IS NULL",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS price NUMERIC(10,2) DEFAULT 0",
        "UPDATE video SET price = 0 WHERE price IS NULL",
        "ALTER TABLE video ALTER COLUMN price SET DEFAULT 0",
        "ALTER TABLE video ALTER COLUMN price DROP NOT NULL",
        "DELETE FROM video WHERE batch_id IS NOT NULL AND batch_id NOT IN (SELECT id FROM video_batch)",
        "ALTER TABLE video DROP CONSTRAINT IF EXISTS video_batch_id_fkey",
        "ALTER TABLE video ADD CONSTRAINT video_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES video_batch(id) ON DELETE CASCADE",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS filename VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS file_path VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS thumbnail_path VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS internal_filename VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS r2_video_key VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS r2_thumbnail_key VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS public_thumbnail_url VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS file_size_bytes BIGINT DEFAULT 0",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS original_price NUMERIC(10,2) DEFAULT 0",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS edited_price NUMERIC(10,2) DEFAULT 0",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS bundle_price NUMERIC(10,2) DEFAULT 0",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS recorded_at TIMESTAMP",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS recorded_date DATE",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS recorded_time TIME",
        "UPDATE video SET filename = COALESCE(NULLIF(filename, ''), internal_filename, split_part(r2_video_key, '/', array_length(string_to_array(r2_video_key, '/'), 1)), 'video.mp4') WHERE filename IS NULL OR filename = ''",
        "UPDATE video SET file_path = COALESCE(NULLIF(file_path, ''), r2_video_key, filename, internal_filename, 'video.mp4') WHERE file_path IS NULL OR file_path = ''",
        "UPDATE video SET thumbnail_path = COALESCE(NULLIF(thumbnail_path, ''), r2_thumbnail_key, public_thumbnail_url) WHERE thumbnail_path IS NULL OR thumbnail_path = ''",
        "ALTER TABLE video ALTER COLUMN filename SET DEFAULT ''",
        "ALTER TABLE video ALTER COLUMN file_path SET DEFAULT ''"
    ]
    try:
        for sql in statements:
            db.session.execute(db.text(sql))
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        print("Video uploader column repair warning:", e)


@creator_bp.route("/upload/r2/prepare", methods=["POST"])
def upload_r2_prepare():
    _ensure_video_upload_columns()
    creator = current_creator()
    storage_limit_gb, max_batch_gb = _creator_plan_limits(creator)
    storage_used_gb = _creator_storage_used_gb(creator.id)
    if not creator:
        return jsonify({"ok": False, "error": "Creator login required."}), 401

    from app.models import VideoBatch
    from app.services.r2 import r2_configured, create_presigned_put_url
    import uuid
    from werkzeug.utils import secure_filename

    if not r2_configured():
        return jsonify({"ok": False, "error": "Cloudflare R2 is not configured. Missing R2 variables in Railway."}), 400

    data = request.get_json(silent=True) or {}
    files = data.get("files", [])
    location = (data.get("location") or "").strip()
    batch_name = (data.get("batch_name") or "").strip() or "New video batch"

    try:
        original_price = float(data.get("original_price") or 0)
        edited_price = float(data.get("edited_price") or 0)
        bundle_price = float(data.get("bundle_price") or 0)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid price."}), 400

    if not files:
        return jsonify({"ok": False, "error": "Choose at least one video file."}), 400

    # Safe plan validation. Never fail because plan fields are empty.
    total_upload_bytes = sum(int(f.get("size") or 0) for f in files)
    total_upload_gb = total_upload_bytes / (1024 ** 3)
    if total_upload_gb > float(max_batch_gb):
        return jsonify({"ok": False, "error": f"Batch exceeds your plan limit. Maximum per batch is {max_batch_gb:g} GB."}), 400
    if (float(storage_used_gb) + total_upload_gb) > float(storage_limit_gb):
        return jsonify({"ok": False, "error": f"Upload exceeds your storage plan. Used {storage_used_gb:g} GB of {storage_limit_gb:g} GB."}), 400

    # Prevent duplicate uploads by same creator using original filename.
    from app.models import Video
    duplicate_messages = []
    for f in files:
        original_name_for_check = secure_filename(f.get("name") or "video.mp4")
        existing = db.session.query(Video.id, Video.batch_id).filter(
            Video.creator_id == creator.id,
            Video.status != "deleted",
            db.or_(
                Video.filename == original_name_for_check,
                Video.internal_filename == original_name_for_check
            )
        ).order_by(Video.id.desc()).first()
        if existing:
            duplicate_messages.append(
                f"{original_name_for_check} already exists in batch #{existing.batch_id}. Please check that batch before uploading again."
            )
    if duplicate_messages:
        return jsonify({"ok": False, "error": "Duplicate file found: " + " | ".join(duplicate_messages)}), 409

    total_size = 0
    for f in files:
        total_size += int(f.get("size") or 0)

    if total_size <= 0:
        return jsonify({"ok": False, "error": "Invalid file size."}), 400

    if total_size > BATCH_LIMIT_BYTES:
        return jsonify({"ok": False, "error": "This batch is over 128 GB. Please split it into smaller batches."}), 400

    used = _creator_used_storage_bytes(creator.id)
    limit = _creator_storage_limit_bytes(creator)
    if used + total_size > limit:
        return jsonify({"ok": False, "error": "This upload exceeds your plan storage limit. Upgrade your plan or delete old videos."}), 400

    batch = VideoBatch(
        creator_id=creator.id,
        location=location,
        batch_name=batch_name,
        total_size_bytes=total_size,
        file_count=len(files),
        status="uploading"
    )
    db.session.add(batch)
    db.session.commit()
    try:
        db.session.refresh(batch)
    except Exception:
        pass
    _ensure_batch_exists_for_upload(batch.id, creator.id, getattr(batch, 'name', ''), location or getattr(batch, 'location', ''))

    uploads = []
    for f in files:
        original_name = secure_filename(f.get("name") or "video.mp4")
        content_type = f.get("type") or "application/octet-stream"
        key = f"creators/{creator.id}/batches/{batch.id}/{uuid.uuid4().hex}_{original_name}"
        thumb_key = f"creators/{creator.id}/batches/{batch.id}/thumbs/{uuid.uuid4().hex}_{original_name}.jpg"
        url = create_presigned_put_url(key, content_type=content_type, expires=60 * 60 * 6)
        thumb_url = create_presigned_put_url(thumb_key, content_type="image/jpeg", expires=60 * 60 * 6)
        uploads.append({
            "name": original_name,
            "size": int(f.get("size") or 0),
            "type": content_type,
            "key": key,
            "upload_url": url,
            "thumbnail_key": thumb_key,
            "thumbnail_upload_url": thumb_url,
            "last_modified": f.get("last_modified")
        })

    return jsonify({
        "ok": True,
        "batch_id": batch.id,
        "uploads": uploads,
        "message": "Upload prepared. Browser will upload directly to Cloudflare R2."
    })



@creator_bp.route("/upload/r2/thumbnail/presign", methods=["POST"])
def upload_r2_thumbnail_presign():
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401

    import uuid
    from werkzeug.utils import secure_filename
    from app.services.r2 import create_presigned_put_url_stable, public_url_for_key

    data = request.get_json(silent=True) or {}
    batch_id = data.get("batch_id") or "pending"
    filename = secure_filename(data.get("filename") or "thumb.jpg")
    if not filename.lower().endswith(".jpg"):
        filename = filename + ".jpg"

    key = f"creators/{creator.id}/batches/{batch_id}/thumbs/browser_{uuid.uuid4().hex}_{filename}"
    url = create_presigned_put_url_stable(key, "image/jpeg")
    public_url = public_url_for_key(key)
    return jsonify({"ok": True, "url": url, "key": key, "public_url": public_url})


@creator_bp.route("/upload/r2/complete", methods=["POST"])
def upload_r2_complete():
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401

    data = request.get_json(silent=True) or {}
    batch_id = data.get("batch_id") or data.get("batchId")
    files = _normalize_completed_upload_files(data)

    if not files:
        return jsonify({
            "ok": False,
            "error": "No uploaded videos received.",
            "debug_keys": list(data.keys())
        }), 400

    try:
        from app.models import VideoBatch
    except Exception:
        VideoBatch = None

    batch = None
    if VideoBatch is not None and batch_id:
        batch = VideoBatch.query.get(batch_id)

    if batch is None:
        return jsonify({"ok": False, "error": "Upload batch was not found. Please refresh and try again."}), 400

    try:
        created = []
        for file_info in files:
            v = _safe_video_create_from_upload(creator.id, batch, file_info, location=getattr(batch, "location", None))
            db.session.add(v)
            created.append(v)

        # Mark batch active/complete when those columns exist.
        try:
            if hasattr(batch, "status"):
                batch.status = "active"
            if hasattr(batch, "completed_at"):
                batch.completed_at = datetime.utcnow()
            if hasattr(batch, "total_size_bytes"):
                batch.total_size_bytes = sum(int(f.get("file_size") or 0) for f in files)
        except Exception:
            pass

        db.session.commit()

        # Generate thumbnails and read video metadata from the actual uploaded R2 video.
        try:
            for v in created:
                has_thumb = bool(getattr(v, "public_thumbnail_url", None) or getattr(v, "thumbnail_url", None) or getattr(v, "thumbnail_path", None) or getattr(v, "r2_thumbnail_key", None))
                if not has_thumb:
                    _generate_and_attach_thumbnail_for_video(v)
                _fill_public_thumbnail_url(v)
            db.session.commit()
        except Exception:
            db.session.rollback()

        try:
            _recalculate_creator_storage(creator.id)
        except Exception:
            db.session.rollback()

        return jsonify({"ok": True, "message": "Videos saved.", "count": len(created), "batch_id": getattr(batch, "id", None)})
    except Exception as e:
        db.session.rollback()
        _cleanup_uploaded_r2_files(files if "files" in locals() else uploaded if "uploaded" in locals() else [])
        return jsonify({"ok": False, "error": "Videos uploaded, but database save failed. Uploaded R2 files were cleaned up automatically: " + str(e)}), 500


def upload_r2_complete():
    _ensure_video_upload_columns()
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Creator login required."}), 401

    from app.models import Video, VideoBatch
    from app.services.r2 import public_url_for_key

    data = request.get_json(silent=True) or {}
    batch_id = data.get("batch_id")
    uploaded = data.get("uploaded", [])
    location = (data.get("location") or "").strip()

    try:
        original_price = float(data.get("original_price") or 0)
        edited_price = float(data.get("edited_price") or 0)
        bundle_price = float(data.get("bundle_price") or 0)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid price."}), 400

    batch = VideoBatch.query.filter_by(id=batch_id, creator_id=creator.id).first()
    if not batch:
        return jsonify({"ok": False, "error": "Batch not found."}), 404

    if not uploaded:
        return jsonify({"ok": False, "error": "No uploaded videos received."}), 400

    _ensure_batch_exists_for_upload(batch.id, creator.id, getattr(batch, 'name', ''), location or getattr(batch, 'location', ''))

    default_original_price, default_edited_price, default_bundle_price = _creator_default_prices(creator)
    for item in uploaded:
        key = item.get("key")
        if not key:
            continue

        recorded_at = None
        recorded_date = None
        recorded_time = None
        try:
            lm = item.get("last_modified")
            if lm:
                recorded_at = datetime.fromtimestamp(int(lm) / 1000, tz=timezone.utc).replace(tzinfo=None)
                recorded_date = recorded_at.date()
                recorded_time = recorded_at.time()
        except Exception:
            recorded_at = None

        if recorded_date is None or recorded_time is None:
            fallback_date, fallback_time = _safe_recorded_date_time_from_file_info(item if 'item' in locals() else {})
            recorded_date = recorded_date or fallback_date
            recorded_time = recorded_time or fallback_time
        if recorded_at is None:
            recorded_at = datetime.combine(recorded_date, recorded_time)

        thumb_key = item.get("thumbnail_key")
        v = Video(
            creator_id=creator.id,
            batch_id=batch.id,
            location=location or batch.location,
            file_path=key,
            r2_video_key=key,
            thumbnail_path=thumb_key,
            r2_thumbnail_key=thumb_key,
            public_thumbnail_url=public_url_for_key(thumb_key) if thumb_key else "",
            recorded_at=recorded_at,
            recorded_date=recorded_date,
            recorded_time=recorded_time,
            file_size_bytes=int(item.get("size") or 0),
            original_price=original_price,
            edited_price=edited_price,
            bundle_price=bundle_price,
            status="active",
            filename=item.get("name") or key.split("/")[-1],
            internal_filename=item.get("name") or key.split("/")[-1]
        )
        db.session.add(v)

    batch.status = "uploaded"
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        _cleanup_uploaded_r2_files(files if "files" in locals() else uploaded if "uploaded" in locals() else [])
        return jsonify({"ok": False, "error": "Videos uploaded, but database save failed. Uploaded R2 files were cleaned up automatically: " + str(e)}), 500

    return jsonify({
        "ok": True,
        "message": "Batch uploaded successfully to BoatSpotMedia Storage. You can view it in Batches.",
        "batch_id": batch.id
    })


@creator_bp.route("/batches")
def batches():
    creator = current_creator()
    if not creator:
        return redirect("/creator/login")
    from app.models import VideoBatch
    batches = VideoBatch.query.filter_by(creator_id=creator.id).order_by(VideoBatch.id.desc()).all()
    return render_template("creator/batches.html", batches=batches)


@creator_bp.route("/batches/<int:batch_id>")
def batch_detail(batch_id):
    creator = current_creator()
    if not creator:
        return redirect("/creator/login")
    from app.models import Video, VideoBatch
    batch = VideoBatch.query.filter_by(id=batch_id, creator_id=creator.id).first_or_404()
    videos = Video.query.filter_by(batch_id=batch.id, creator_id=creator.id).order_by(Video.id.desc()).all()
    return render_template("creator/batch_detail.html", batch=batch, videos=videos)


@creator_bp.route("/batches/<int:batch_id>/delete", methods=["POST"])
def delete_batch(batch_id):
    creator = current_creator()
    if not creator:
        return redirect("/creator/login")
    from app.models import Video, VideoBatch
    batch = VideoBatch.query.filter_by(id=batch_id, creator_id=creator.id).first_or_404()
    videos = Video.query.filter_by(batch_id=batch.id, creator_id=creator.id).all()
    for v in videos:
        v.status = "deleted"
    batch.status = "deleted"
    db.session.commit()
    return redirect(url_for("creator.batches"))


@creator_bp.route("/videos/<int:video_id>/delete", methods=["POST"])
def delete_video(video_id):
    creator = current_creator()
    if not creator:
        return redirect("/creator/login")
    from app.models import Video
    v = Video.query.filter_by(id=video_id, creator_id=creator.id).first_or_404()
    v.status = "deleted"
    db.session.commit()
    return redirect(request.referrer or url_for("creator.batches"))




@creator_bp.route("/apply", methods=["GET", "POST"])
def apply():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        social = (request.form.get("social") or "").strip()
        message = (request.form.get("message") or "").strip()

        try:
            db.session.execute(db.text("""
                CREATE TABLE IF NOT EXISTS creator_application (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    email VARCHAR(255),
                    social VARCHAR(500),
                    message TEXT,
                    status VARCHAR(50) DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            db.session.execute(db.text("""
                INSERT INTO creator_application (name, email, social, message, status, created_at)
                VALUES (:name, :email, :social, :message, 'pending', NOW())
            """), {"name": name, "email": email, "social": social, "message": message})
            db.session.commit()
            flash("Application submitted. BoatSpotMedia will review your request.", "success")
            return redirect(url_for("creator.login"))
        except Exception as e:
            db.session.rollback()
            print("creator apply warning:", e)
            flash("Application could not be submitted right now. Please try again.", "error")

    return render_template("creator/apply.html")


@creator_bp.route("/apply/google")
def apply_with_google():
    try:
        return redirect(url_for("public.auth_google_register", account_type="creator"))
    except Exception:
        flash("Google application/login is not fully configured yet. Please apply with email for now.", "info")
        return redirect(url_for("creator.apply"))



@creator_bp.route("/apply/apple")
def apply_with_apple_under_construction():
    flash("Apple login is under construction. You'll be able to apply or log in with Apple soon.", "info")
    return redirect(url_for("creator.apply"))



@creator_bp.route("/upload/batch/<int:batch_id>/cancel", methods=["POST"])
def cancel_upload_batch(batch_id):
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401
    try:
        from app.models import Video, VideoBatch
        videos = Video.query.filter_by(creator_id=creator.id, batch_id=batch_id).all()
        try:
            from app.services import r2
            for v in videos:
                for key in [getattr(v, "r2_video_key", None), getattr(v, "file_path", None), getattr(v, "r2_thumbnail_key", None), getattr(v, "thumbnail_path", None)]:
                    if key and hasattr(r2, "delete"):
                        r2.delete(key)
        except Exception as e:
            print("R2 cancel delete warning:", e)
        for v in videos:
            db.session.delete(v)
        batch = VideoBatch.query.filter_by(id=batch_id, creator_id=creator.id).first()
        if batch:
            db.session.delete(batch)
        db.session.commit()
        _recalculate_creator_storage(creator.id)
        return jsonify({"ok": True, "message": "Upload cancelled and uploaded files were removed."})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500



@creator_bp.route("/videos/<int:video_id>/regenerate-thumbnail", methods=["POST"])
def regenerate_video_thumbnail(video_id):
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401

    try:
        from app.models import Video
        import tempfile, subprocess, os, json, urllib.request, uuid
        from app.services.r2 import public_url_for_key
        try:
            from app.services.r2 import upload_file as r2_upload_file
        except Exception:
            r2_upload_file = None

        v = Video.query.filter_by(id=video_id, creator_id=creator.id).first()
        if not v:
            return jsonify({"ok": False, "error": "Video not found."}), 404

        public_video_url = public_url_for_key(v.r2_video_key or v.file_path)
        if not public_video_url:
            return jsonify({"ok": False, "error": "Video public URL unavailable for thumbnail generation."}), 400

        tmpdir = tempfile.mkdtemp()
        video_path = os.path.join(tmpdir, "video_original")
        thumb_path = os.path.join(tmpdir, "thumb.jpg")

        urllib.request.urlretrieve(public_video_url, video_path)

        def duration(path):
            try:
                r = subprocess.run(["ffprobe","-v","error","-show_entries","format=duration","-of","json",path], capture_output=True, text=True, timeout=60)
                return float(json.loads(r.stdout or "{}").get("format",{}).get("duration") or 0)
            except Exception:
                return 0

        d = duration(video_path)
        points = [0.50,0.45,0.55,0.60,0.40,0.65,0.35,0.70,0.25,0.75]
        made = False
        for pct in points:
            t = max(1.0, d * pct if d else 2.0)
            cmd = [
                "ffmpeg","-y","-ss",str(t),"-i",video_path,
                "-frames:v","1",
                "-vf","crop=iw*0.70:ih*0.70:iw*0.15:ih*0.15,scale=1280:-2,eq=brightness=0.02:saturation=1.1",
                "-q:v","2",thumb_path
            ]
            subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 4000:
                # basic black check using ffmpeg signalstats would be heavy; size threshold + multiple points is enough here.
                made = True
                break

        if not made:
            return jsonify({"ok": False, "error": "Could not generate a usable thumbnail."}), 500

        thumb_key = f"creators/{creator.id}/batches/{v.batch_id}/thumbs/backend_{uuid.uuid4().hex}_{v.filename or 'thumb'}.jpg"
        if r2_upload_file:
            r2_upload_file(thumb_path, thumb_key, content_type="image/jpeg")
        else:
            try:
                from app.services import r2
                if hasattr(r2, "upload"):
                    r2.upload(thumb_path, thumb_key, content_type="image/jpeg")
                else:
                    return jsonify({"ok": False, "error": "R2 upload helper not available."}), 500
            except Exception as e:
                return jsonify({"ok": False, "error": "R2 upload helper not available: " + str(e)}), 500

        v.r2_thumbnail_key = thumb_key
        v.thumbnail_path = thumb_key
        v.public_thumbnail_url = public_url_for_key(thumb_key)
        db.session.commit()
        return jsonify({"ok": True, "thumbnail_url": v.public_thumbnail_url})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500



@creator_bp.route("/upload/r2/multipart/init", methods=["POST"])
def upload_r2_multipart_init():
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401

    data = request.get_json(silent=True) or {}
    filename = _safe_secure_filename(data.get("filename") or "video")
    key = data.get("key")
    batch_id = data.get("batch_id")
    content_type = data.get("content_type") or "application/octet-stream"

    if not key:
        import uuid
        key = f"creators/{creator.id}/batches/{batch_id or 'pending'}/{uuid.uuid4().hex}_{filename}"

    try:
        from app.services.r2 import create_multipart_upload
        result = create_multipart_upload(key, content_type=content_type)
        return jsonify({"ok": True, "upload_id": result["UploadId"], "key": key})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def upload_r2_multipart_init():
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401

    data = request.get_json(silent=True) or {}
    filename = _safe_secure_filename(data.get("filename") or "video")
    key = data.get("key")
    batch_id = data.get("batch_id")
    content_type = data.get("content_type") or "application/octet-stream"

    if not key:
        import uuid
        key = f"creators/{creator.id}/batches/{batch_id or 'pending'}/{uuid.uuid4().hex}_{filename}"

    try:
        from app.services.r2 import create_multipart_upload
        result = create_multipart_upload(key, content_type=content_type)
        return jsonify({"ok": True, "upload_id": result["UploadId"], "key": key})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@creator_bp.route("/upload/r2/multipart/part", methods=["POST"])
def upload_r2_multipart_part():
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401

    data = request.get_json(silent=True) or {}
    key = data.get("key")
    upload_id = data.get("upload_id")
    part_number = int(data.get("part_number") or 1)

    try:
        from app.services.r2 import presign_upload_part
        url = presign_upload_part(key, upload_id, part_number)
        return jsonify({"ok": True, "url": url})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@creator_bp.route("/upload/r2/multipart/complete", methods=["POST"])
def upload_r2_multipart_complete():
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401

    data = request.get_json(silent=True) or {}
    key = data.get("key")
    upload_id = data.get("upload_id")
    parts = data.get("parts") or []

    try:
        from app.services.r2 import complete_multipart_upload
        complete_multipart_upload(key, upload_id, parts)
        return jsonify({"ok": True, "key": key})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@creator_bp.route("/upload/r2/multipart/abort", methods=["POST"])
def upload_r2_multipart_abort():
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401

    data = request.get_json(silent=True) or {}
    try:
        from app.services.r2 import abort_multipart_upload
        abort_multipart_upload(data.get("key"), data.get("upload_id"))
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500



@creator_bp.route("/upload/r2/multipart/status", methods=["GET"])
def upload_r2_multipart_status():
    try:
        from app.services.r2 import create_multipart_upload, presign_upload_part, complete_multipart_upload, abort_multipart_upload
        return jsonify({"ok": True, "multipart": True})
    except Exception as e:
        return jsonify({"ok": True, "multipart": False, "error": str(e)})
