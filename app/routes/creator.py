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
    """Creator display name for menus: Instagram/business/name first, email last."""
    try:
        user = getattr(creator, "user", None)
        for obj in (creator, user):
            if not obj:
                continue
            for attr in ("instagram_username", "instagram", "instagram_handle", "business_name", "display_name", "name", "username"):
                val = getattr(obj, attr, None)
                if val:
                    val = str(val).strip()
                    if val:
                        if attr in ("instagram_username", "instagram", "instagram_handle"):
                            return "@" + val.lstrip("@")
                        return val
        for obj in (user, creator):
            if obj and getattr(obj, "email", None):
                return str(getattr(obj, "email"))
    except Exception:
        pass
    return "Dashboard"

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
    def num(v, default):
        try:
            if v is not None and str(v).strip() != "":
                return float(v)
        except Exception:
            pass
        return float(default)
    plan = getattr(creator, "plan", None)
    original = num(getattr(creator, "default_original_price", None), 40)
    edited = num(getattr(creator, "default_edited_price", None), 60)
    bundle = num(getattr(creator, "default_bundle_price", None), 80)
    if plan:
        original = num(getattr(plan, "default_original_price", None), original)
        edited = num(getattr(plan, "default_edited_price", None), edited)
        bundle = num(getattr(plan, "default_bundle_price", None), bundle)
    return original, edited, bundle

def _recalculate_creator_storage(creator_id):
    """Recalculate storage from active videos only and update profile."""
    try:
        used = _creator_used_storage_bytes(creator_id)
        try:
            from app.models import CreatorProfile
            c = CreatorProfile.query.get(creator_id)
            if c and hasattr(c, "storage_used_bytes"):
                c.storage_used_bytes = int(used)
                db.session.add(c)
                db.session.commit()
        except Exception:
            db.session.rollback()
        return int(used)
    except Exception as e:
        print("storage recalculation warning:", e)
        try:
            db.session.rollback()
        except Exception:
            pass
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
    return render_template(template_name, **kwargs)


@creator_bp.app_context_processor
def inject_creator_menu_helpers():
    return {
        "creator_display_name": _creator_display_name
    }



def _creator_display_name(creator):
    """Prefer Instagram/business/display name instead of email."""
    try:
        user = getattr(creator, "user", None)
        for obj in (creator, user):
            if not obj:
                continue
            for attr in ("instagram", "instagram_handle", "business_name", "display_name", "name", "username"):
                val = getattr(obj, attr, None)
                if val:
                    val = str(val).strip()
                    if val:
                        return ("@" + val.lstrip("@")) if attr in ("instagram", "instagram_handle") else val
        for obj in (user, creator):
            if obj and getattr(obj, "email", None):
                return str(getattr(obj, "email"))
    except Exception:
        pass
    return "Dashboard"


def _active_storage_used_bytes(creator_id):
    """Count only active videos, excluding deleted/cancelled."""
    try:
        from sqlalchemy import text
        used = db.session.execute(text("""
            SELECT COALESCE(SUM(COALESCE(file_size_bytes, 0)), 0)
            FROM video
            WHERE creator_id = :cid
              AND COALESCE(status, '') NOT IN ('deleted','cancelled','canceled')
        """), {"cid": creator_id}).scalar() or 0
        return int(used)
    except Exception as e:
        try:
            print("active storage used warning:", e)
            db.session.rollback()
        except Exception:
            pass
        return 0


def _available_buyer_locations():
    """Buyer location list comes only from active videos uploaded by creators."""
    try:
        from sqlalchemy import text
        rows = db.session.execute(text("""
            SELECT DISTINCT TRIM(location) AS location
            FROM video
            WHERE location IS NOT NULL
              AND TRIM(location) <> ''
              AND COALESCE(status, '') NOT IN ('deleted','cancelled','canceled')
            ORDER BY TRIM(location)
        """)).fetchall()
        return [r[0] for r in rows if r and r[0]]
    except Exception as e:
        try:
            print("buyer locations warning:", e)
            db.session.rollback()
        except Exception:
            pass
        return []


def _creator_known_locations():
    return _available_buyer_locations()


def _delete_batch_files_from_r2(batch):
    """Delete all batch files/thumbnails from R2 and mark DB rows deleted. No recursion."""
    try:
        from app.services.r2 import delete_r2_object, delete_r2_prefix
        deleted = 0
        batch_id = getattr(batch, "id", None)
        creator_id = getattr(batch, "creator_id", None) or getattr(batch, "creator_profile_id", None)

        try:
            from app.models import Video
            videos = Video.query.filter_by(batch_id=batch_id).all()
            for v in videos:
                for attr in ("r2_video_key", "file_path", "r2_thumbnail_key", "thumbnail_path"):
                    key = getattr(v, attr, None)
                    if key and not str(key).startswith("http"):
                        try:
                            delete_r2_object(key)
                            deleted += 1
                        except Exception as e:
                            try:
                                print("R2 object delete warning:", key, e)
                            except Exception:
                                pass
                if hasattr(v, "status"):
                    v.status = "deleted"
                    db.session.add(v)
                else:
                    db.session.delete(v)
        except Exception as e:
            try:
                print("R2 DB key cleanup warning:", e)
            except Exception:
                pass

        if creator_id and batch_id:
            for prefix in (
                f"creators/{creator_id}/batches/{batch_id}/",
                f"creator/{creator_id}/batch/{batch_id}/",
                f"batches/{batch_id}/",
            ):
                try:
                    deleted += delete_r2_prefix(prefix)
                except Exception as e:
                    try:
                        print("R2 prefix delete warning:", prefix, e)
                    except Exception:
                        pass

        if batch:
            if hasattr(batch, "status"):
                try:
                    _r2_delete_batch_strong(batch.id, creator.id)
                except Exception:
                    pass
                try:
                    _r2_delete_batch_strong(batch.id, creator.id)
                except Exception:
                    pass
                try:
                    _r2_delete_batch_strong(batch.id, creator.id)
                except Exception:
                    pass
                batch.status = "deleted"
                db.session.add(batch)
            else:
                try:
                    _r2_delete_batch_strong(batch.id, creator.id)
                except Exception:
                    pass
                try:
                    _r2_delete_batch_strong(batch.id, creator.id)
                except Exception:
                    pass
                try:
                    _r2_delete_batch_strong(batch.id, creator.id)
                except Exception:
                    pass
                db.session.delete(batch)
        return deleted
    except Exception as e:
        try:
            print("R2 batch cleanup warning:", e)
        except Exception:
            pass
        return 0

def _cleanup_upload_prefix(batch_id, creator_id):
    try:
        from app.services.r2 import delete_r2_prefix
        if batch_id and creator_id:
            return delete_r2_prefix(f"creators/{creator_id}/batches/{batch_id}/")
    except Exception:
        pass
    return 0


def _ny_dt(dt):
    if not dt:
        return None
    try:
        from zoneinfo import ZoneInfo
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        try:
            from datetime import timedelta
            return dt - timedelta(hours=5)
        except Exception:
            return dt


def _creator_current_batch_id_from_request():
    """Find batch id from URL, JSON, form, session, or existing active batch."""
    for source in (
        (request.json if request.is_json else None),
        request.form,
        request.args,
    ):
        try:
            if source:
                for k in ("batch_id", "batchId", "id"):
                    val = source.get(k)
                    if val:
                        return int(val)
        except Exception:
            pass
    try:
        val = session.get("current_upload_batch_id") or session.get("upload_batch_id")
        if val:
            return int(val)
    except Exception:
        pass
    return None





def _r2_collect_video_keys(video):
    keys = set()
    for attr in (
        "r2_video_key", "r2_thumbnail_key", "r2_key", "thumbnail_key",
        "video_key", "thumb_key", "file_path", "thumbnail_path",
        "r2_video_path", "r2_thumbnail_path", "storage_key", "storage_path",
        "object_key", "preview_key", "preview_path", "public_thumbnail_url", "thumbnail_url"
    ):
        try:
            val = getattr(video, attr, None)
            if val:
                val = str(val)
                if not val.startswith("http") and "/" in val:
                    keys.add(val)
        except Exception:
            pass
    try:
        for val in vars(video).values():
            if isinstance(val, str) and "/" in val and not val.startswith("http"):
                low = val.lower()
                if any(x in low for x in ("creator", "batch", "upload", "thumb", "preview", ".mp4", ".mov", ".m4v", ".jpg", ".jpeg", ".png", ".heic")):
                    keys.add(val)
    except Exception:
        pass
    return list(keys)


def _r2_batch_prefixes(batch_id, creator_id=None):
    prefixes = [
        f"batches/{batch_id}/",
        f"batch/{batch_id}/",
        f"videos/batches/{batch_id}/",
        f"uploads/batches/{batch_id}/",
        f"thumbs/batches/{batch_id}/",
        f"previews/batches/{batch_id}/",
    ]
    if creator_id:
        prefixes += [
            f"creators/{creator_id}/batches/{batch_id}/",
            f"creators/{creator_id}/batch/{batch_id}/",
            f"creator/{creator_id}/batches/{batch_id}/",
            f"creator/{creator_id}/batch/{batch_id}/",
            f"uploads/{creator_id}/{batch_id}/",
            f"uploads/creators/{creator_id}/batches/{batch_id}/",
            f"videos/{creator_id}/{batch_id}/",
            f"thumbs/{creator_id}/{batch_id}/",
            f"previews/{creator_id}/{batch_id}/",
        ]
    return prefixes


def _r2_delete_batch_strong(batch_id, creator_id=None):
    deleted = 0
    try:
        from app.models import Video
        from app.services.r2 import delete_r2_candidates
        keys = []
        for v in Video.query.filter_by(batch_id=batch_id).all():
            keys.extend(_r2_collect_video_keys(v))
        deleted += delete_r2_candidates(keys=list(set(keys)), prefixes=_r2_batch_prefixes(batch_id, creator_id))
    except Exception as e:
        try:
            print("R2 strong batch delete warning:", e)
        except Exception:
            pass
    return deleted


def _mark_batch_and_videos_deleted(batch_id):
    try:
        from app.models import VideoBatch, Video
        for v in Video.query.filter_by(batch_id=batch_id).all():
            if hasattr(v, "status"):
                v.status = "deleted"
                db.session.add(v)
            else:
                db.session.delete(v)
        batch = VideoBatch.query.get(batch_id)
        if batch:
            if hasattr(batch, "status"):
                batch.status = "deleted"
                db.session.add(batch)
            else:
                db.session.delete(batch)
        return batch
    except Exception:
        return None


def _current_upload_batch_id():
    # 1. Request body/args/form
    for source in (
        request.get_json(silent=True) if request.is_json else None,
        request.form,
        request.args,
    ):
        try:
            if source:
                for k in ("batch_id", "batchId", "id"):
                    val = source.get(k)
                    if val:
                        return int(val)
        except Exception:
            pass

    # 2. Session
    try:
        val = session.get("current_upload_batch_id") or session.get("upload_batch_id") or session.get("last_batch_id")
        if val:
            return int(val)
    except Exception:
        pass

    # 3. Latest ghost/uploading batch for this creator. This is the key fix.
    try:
        creator = current_creator()
        if creator:
            from app.models import VideoBatch
            possible_cols = []
            if hasattr(VideoBatch, "creator_id"):
                possible_cols.append(VideoBatch.creator_id == creator.id)
            if hasattr(VideoBatch, "creator_profile_id"):
                possible_cols.append(VideoBatch.creator_profile_id == creator.id)
            if hasattr(VideoBatch, "user_id"):
                possible_cols.append(VideoBatch.user_id == getattr(creator, "user_id", None))

            q = VideoBatch.query
            if possible_cols:
                q = q.filter(db.or_(*possible_cols))
            if hasattr(VideoBatch, "status"):
                q = q.filter(db.or_(VideoBatch.status == None, ~VideoBatch.status.in_(["deleted", "cancelled", "canceled"])))
            b = q.order_by(VideoBatch.id.desc()).first()
            if b:
                return int(b.id)
    except Exception as e:
        try:
            print("current upload batch lookup warning:", e)
        except Exception:
            pass
    return None



# BoatSpotMedia v40 compatibility aliases for older/newer menu links.
@creator_bp.route("/creator/dashboard")
def creator_dashboard_alias_v40():
    try:
        return redirect(url_for("creator.dashboard"))
    except Exception:
        try:
            return redirect("/dashboard")
        except Exception:
            return redirect("/login")


@creator_bp.route("/creator/login")
def creator_login_alias_v40():
    try:
        return redirect(url_for("creator.login"))
    except Exception:
        return redirect("/login")

@creator_bp.route("/r2-clean/batch/<int:batch_id>", methods=["POST"])
@creator_bp.route("/upload/batch/<int:batch_id>/cancel-clean", methods=["POST"])
@creator_bp.route("/creator/upload/batch/<int:batch_id>/cancel-clean", methods=["POST"])
@creator_bp.route("/batch/<int:batch_id>/delete-full", methods=["POST"])
@creator_bp.route("/creator/batch/<int:batch_id>/delete-full", methods=["POST"])
def r2_clean_batch_v399(batch_id):
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401
    try:
        from app.models import VideoBatch
        batch = VideoBatch.query.get(batch_id)
        creator_id = getattr(batch, "creator_id", None) or getattr(batch, "creator_profile_id", None) or getattr(creator, "id", None)
        deleted = _r2_delete_batch_strong(batch_id, creator_id)
        _mark_batch_and_videos_deleted(batch_id)
        db.session.commit()
        try:
            session.pop("current_upload_batch_id", None)
            session.pop("upload_batch_id", None)
            session.pop("last_batch_id", None)
        except Exception:
            pass
        try:
            _recalculate_creator_storage(getattr(creator, "id", None))
        except Exception:
            pass
        return jsonify({"ok": True, "batch_id": batch_id, "deleted_objects": deleted})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500


@creator_bp.route("/cancel-upload-delete-current", methods=["POST"])
@creator_bp.route("/r2-clean/current-upload", methods=["POST"])
@creator_bp.route("/cancel-upload", methods=["POST"])
def cancel_upload_delete_current_v399():
    batch_id = _current_upload_batch_id()
    if not batch_id:
        return jsonify({"ok": False, "error": "No active batch id found"}), 400
    return r2_clean_batch_v399(batch_id)

