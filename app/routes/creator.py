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
                    _delete_batch_r2_objects(batch)
                except Exception:
                    pass
                batch.status = "deleted"
                db.session.add(batch)
            else:
                try:
                    _delete_batch_r2_objects(batch)
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


def _r2_collect_video_keys_for_batch(batch_id):
    keys = set()
    try:
        from app.models import Video
        videos = Video.query.filter_by(batch_id=batch_id).all()
        for v in videos:
            # known likely fields
            for attr in (
                "r2_video_key", "r2_thumbnail_key",
                "r2_key", "thumbnail_key",
                "video_key", "thumb_key",
                "file_path", "thumbnail_path",
                "storage_key", "storage_path",
                "object_key", "preview_key", "preview_path",
            ):
                val = getattr(v, attr, None)
                if val:
                    val = str(val)
                    if not val.startswith("http") and "/" in val:
                        keys.add(val)

            # scan every string field as fallback
            try:
                for val in vars(v).values():
                    if isinstance(val, str) and "/" in val and not val.startswith("http"):
                        low = val.lower()
                        if any(x in low for x in ("thumb", "preview", "upload", "creator", "batch", ".mp4", ".mov", ".m4v", ".jpg", ".jpeg", ".png")):
                            keys.add(val)
            except Exception:
                pass
    except Exception as e:
        try:
            print("R2 collect keys warning:", e)
        except Exception:
            pass
    return list(keys)


def _r2_prefixes_for_batch(batch_id, creator_id=None):
    prefixes = [
        f"batches/{batch_id}/",
        f"batch/{batch_id}/",
        f"uploads/batches/{batch_id}/",
        f"videos/batches/{batch_id}/",
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
            f"videos/{creator_id}/{batch_id}/",
            f"thumbs/{creator_id}/{batch_id}/",
            f"previews/{creator_id}/{batch_id}/",
        ]
    return prefixes


def _delete_batch_r2_objects(batch):
    """Delete videos/thumbnails/previews for batch from R2 only."""
    try:
        from app.services.r2 import delete_r2_candidates
        batch_id = getattr(batch, "id", None)
        creator_id = getattr(batch, "creator_id", None) or getattr(batch, "creator_profile_id", None)
        keys = _r2_collect_video_keys_for_batch(batch_id)
        prefixes = _r2_prefixes_for_batch(batch_id, creator_id)
        deleted = delete_r2_candidates(keys=keys, prefixes=prefixes)
        try:
            print("R2 delete batch cleanup:", {"batch_id": batch_id, "creator_id": creator_id, "keys": len(keys), "deleted": deleted})
        except Exception:
            pass
        return deleted
    except Exception as e:
        try:
            print("R2 delete batch cleanup warning:", e)
        except Exception:
            pass
        return 0




def _latest_creator_batch_id_for_cancel():
    """Find the latest active/ghost batch for Cancel Upload."""
    # request/session first
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
    try:
        val = session.get("current_upload_batch_id") or session.get("upload_batch_id") or session.get("last_batch_id")
        if val:
            return int(val)
    except Exception:
        pass

    # fallback: newest active batch for this creator
    try:
        creator = current_creator()
        if not creator:
            return None
        from app.models import VideoBatch
        q = VideoBatch.query
        filters = []
        if hasattr(VideoBatch, "creator_id"):
            filters.append(VideoBatch.creator_id == creator.id)
        if hasattr(VideoBatch, "creator_profile_id"):
            filters.append(VideoBatch.creator_profile_id == creator.id)
        if filters:
            q = q.filter(db.or_(*filters))
        if hasattr(VideoBatch, "status"):
            q = q.filter(db.or_(VideoBatch.status == None, ~VideoBatch.status.in_(["deleted", "cancelled", "canceled"])))
        batch = q.order_by(VideoBatch.id.desc()).first()
        if batch:
            return int(batch.id)
    except Exception as e:
        try:
            print("latest cancel batch lookup warning:", e)
        except Exception:
            pass
    return None


@creator_bp.route("/cancel-upload-run-delete-batch", methods=["POST"])
def cancel_upload_run_delete_batch_v388():
    """
    Cancel Upload uses the same cleanup logic as Delete Batch.
    This removes the ghost batch and R2 files when the user confirms cancellation.
    """
    batch_id = _latest_creator_batch_id_for_cancel()
    if not batch_id:
        return jsonify({"ok": False, "error": "No active batch found"}), 400

    # Prefer the existing R2 delete endpoint from the R2 delete fix.
    try:
        if "r2_clean_batch_v388_delete_only" in globals():
            return r2_clean_batch_v388_delete_only(batch_id)
    except Exception as e:
        try:
            print("delegated delete batch cleanup warning:", e)
        except Exception:
            pass

    # Fallback: direct cleanup with the same helper.
    try:
        from app.models import VideoBatch, Video
        batch = VideoBatch.query.get_or_404(batch_id)
        deleted = 0
        try:
            deleted = _delete_batch_r2_objects(batch)
        except Exception as e:
            try:
                print("fallback R2 cleanup warning:", e)
            except Exception:
                pass

        for v in Video.query.filter_by(batch_id=batch_id).all():
            if hasattr(v, "status"):
                v.status = "deleted"
                db.session.add(v)
            else:
                db.session.delete(v)

        if hasattr(batch, "status"):
            batch.status = "deleted"
            db.session.add(batch)
        else:
            db.session.delete(batch)

        db.session.commit()
        try:
            session.pop("current_upload_batch_id", None)
            session.pop("upload_batch_id", None)
            session.pop("last_batch_id", None)
        except Exception:
            pass
        return jsonify({"ok": True, "batch_id": batch_id, "deleted_objects": deleted})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500

@creator_bp.route("/r2-clean/batch/<int:batch_id>", methods=["POST"])
def r2_clean_batch_v388_delete_only(batch_id):
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401
    try:
        from app.models import VideoBatch, Video
        batch = VideoBatch.query.get_or_404(batch_id)
        deleted = _delete_batch_r2_objects(batch)
        for v in Video.query.filter_by(batch_id=batch_id).all():
            if hasattr(v, "status"):
                v.status = "deleted"
                db.session.add(v)
            else:
                db.session.delete(v)
        if hasattr(batch, "status"):
            batch.status = "deleted"
            db.session.add(batch)
        else:
            db.session.delete(batch)
        db.session.commit()
        return jsonify({"ok": True, "deleted_objects": deleted})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500


@creator_bp.route("/upload/batch/<int:batch_id>/cancel-clean", methods=["POST"])
@creator_bp.route("/creator/upload/batch/<int:batch_id>/cancel-clean", methods=["POST"])
def cancel_upload_batch_clean(batch_id):
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401
    deleted = _cleanup_upload_prefix(batch_id, getattr(creator, "id", None))
    try:
        from app.models import VideoBatch, Video
        batch = VideoBatch.query.get(batch_id)
        if batch:
            for v in Video.query.filter_by(batch_id=batch_id).all():
                db.session.delete(v)
            try:
                _delete_batch_files_from_r2(batch)
            except Exception:
                pass
            try:
                _delete_batch_files_from_r2(batch)
            except Exception:
                pass
            try:
                _delete_batch_r2_objects(batch)
            except Exception:
                pass
            db.session.delete(batch)
            db.session.commit()
    except Exception as e:
        db.session.rollback()
        try: print("cancel upload DB cleanup warning:", e)
        except Exception: pass
    return jsonify({"ok": True, "deleted_objects": deleted})


@creator_bp.route("/batch/<int:batch_id>/delete-full", methods=["POST"])
@creator_bp.route("/creator/batch/<int:batch_id>/delete-full", methods=["POST"])
def delete_batch_full_cleanup(batch_id):
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401
    try:
        from app.models import VideoBatch, Video
        batch = VideoBatch.query.get_or_404(batch_id)
        deleted = _delete_batch_files_from_r2(batch)
        for v in Video.query.filter_by(batch_id=batch_id).all():
            db.session.delete(v)
        try:
            _delete_batch_files_from_r2(batch)
        except Exception:
            pass
        try:
            _delete_batch_files_from_r2(batch)
        except Exception:
            pass
        try:
            _delete_batch_r2_objects(batch)
        except Exception:
            pass
        db.session.delete(batch)
        db.session.commit()
        return jsonify({"ok": True, "deleted_objects": deleted})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500


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
        session["user_role"] = "creator"
        session["display_name"] = creator_display_name(creator)
        session["creator_name"] = creator_display_name(creator)
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
        _recalculate_creator_storage(creator.id)
    storage_limit_gb, max_batch_gb = _creator_plan_limits(creator)
    storage_used_gb = _creator_storage_used_gb(creator.id)
    if not creator:
        flash('Please log in with an approved creator account.', 'error')
        return redirect(url_for('creator.login'))
    try:
        db.session.rollback()
    except Exception:
        pass
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
    total = db.session.query(db.func.coalesce(db.func.sum(Video.file_size_bytes), 0)).filter(
        Video.creator_id == creator_id,
        db.or_(Video.status == None, ~Video.status.in_(["deleted", "cancelled", "canceled"]))
    ).scalar()
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
        storage_limit_gb=storage_limit_gb,
        max_batch_gb=max_batch_gb,
        storage_used_gb=storage_used_gb,
        limit_gb=round(limit / 1024 / 1024 / 1024, 2),
        batch_limit_gb=128,
    )





def _creator_plan_limits(creator):
    """Return safe storage/batch limits for creator panel and uploads.
    Defaults: 500 GB storage, 128 GB per batch.
    Reads multiple legacy/new field names so Owner panel changes still show in Creator panel.
    """
    def first_number(*values, default=0):
        for v in values:
            try:
                if v is not None and str(v).strip() != "":
                    return float(v)
            except Exception:
                pass
        return float(default)

    # Try direct creator columns first
    storage_limit = first_number(
        getattr(creator, "storage_limit_gb", None),
        getattr(creator, "plan_storage_limit_gb", None),
        getattr(creator, "storage_gb", None),
        getattr(creator, "plan_storage_gb", None),
        default=500
    )
    batch_limit = first_number(
        getattr(creator, "max_batch_gb", None),
        getattr(creator, "max_batch_size_gb", None),
        getattr(creator, "batch_limit_gb", None),
        getattr(creator, "plan_batch_limit_gb", None),
        default=128
    )

    # Try related plan object if present
    plan = getattr(creator, "plan", None)
    if plan:
        storage_limit = first_number(
            getattr(plan, "storage_limit_gb", None),
            getattr(plan, "plan_storage_limit_gb", None),
            getattr(plan, "storage_gb", None),
            getattr(plan, "included_storage_gb", None),
            storage_limit,
            default=500
        )
        batch_limit = first_number(
            getattr(plan, "max_batch_gb", None),
            getattr(plan, "max_batch_size_gb", None),
            getattr(plan, "batch_limit_gb", None),
            batch_limit,
            default=128
        )

    if storage_limit <= 0:
        storage_limit = 500
    if batch_limit <= 0:
        batch_limit = 128
    return storage_limit, batch_limit


def _creator_storage_used_gb(creator_id):
    """Storage shown in dashboard/upload: active videos only."""
    try:
        used = _creator_used_storage_bytes(creator_id)
        return round(float(used) / (1024 ** 3), 2)
    except Exception as e:
        try:
            print("storage used gb warning:", e)
            db.session.rollback()
        except Exception:
            pass
        return 0

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


@creator_bp.route("/upload/r2/complete", methods=["POST"])
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
    from datetime import datetime, timezone
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
        return jsonify({"ok": False, "error": "Videos uploaded, but database save failed: " + str(e)}), 500

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
    batches = VideoBatch.query.filter(VideoBatch.creator_id == creator.id, db.or_(VideoBatch.status == None, ~VideoBatch.status.in_(["deleted", "cancelled", "canceled"]))).order_by(VideoBatch.id.desc()).all()
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
    try:
        _delete_batch_files_from_r2(batch)
    except Exception:
        pass
    try:
        _delete_batch_r2_objects(batch)
    except Exception:
        pass
    batch.status = "deleted"
    db.session.commit()
    try:
        _recalculate_creator_storage(creator.id)
    except Exception:
        pass
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
            try:
                _delete_batch_files_from_r2(batch)
            except Exception:
                pass
            try:
                _delete_batch_files_from_r2(batch)
            except Exception:
                pass
            try:
                _delete_batch_r2_objects(batch)
            except Exception:
                pass
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


@creator_bp.route("/r2-clean/batch/<int:batch_id>", methods=["POST"])
def r2_clean_batch_safe(batch_id):
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401

    deleted = 0
    try:
        from app.models import VideoBatch, Video
        from app.services.r2 import delete_r2_object, delete_r2_prefix

        batch = VideoBatch.query.get(batch_id)
        creator_id = getattr(batch, "creator_id", None) or getattr(batch, "creator_profile_id", None) or getattr(creator, "id", None)

        for v in Video.query.filter_by(batch_id=batch_id).all():
            for attr in ("r2_video_key", "r2_thumbnail_key", "file_path", "thumbnail_path"):
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
            try:
                if hasattr(v, "status"):
                    v.status = "deleted"
                    db.session.add(v)
                else:
                    db.session.delete(v)
            except Exception:
                pass

        if creator_id:
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
                    _delete_batch_r2_objects(batch)
                except Exception:
                    pass
                batch.status = "deleted"
                db.session.add(batch)
            else:
                try:
                    _delete_batch_r2_objects(batch)
                except Exception:
                    pass
                db.session.delete(batch)

        db.session.commit()
        return jsonify({"ok": True, "deleted_objects": deleted})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
