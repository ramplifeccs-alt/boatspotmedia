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

def current_creator():
    creator = CreatorProfile.query.first()

    if not creator:
        user = User(
            email="creator@test.com",
            role="creator",
            display_name="Test Creator",
            is_active=True
        )
        db.session.add(user)
        db.session.flush()

        creator = CreatorProfile(
            user_id=user.id,
            approved=True,
            storage_limit_gb=512,
            commission_rate=20,
            product_commission_rate=20
        )
        db.session.add(creator)
        db.session.flush()
        db.session.add(CreatorClickStats(creator_id=creator.id))
        db.session.commit()

    try:
        if creator.user and (not creator.user.display_name or creator.user.display_name == "None"):
            creator.user.display_name = "Creator"
            db.session.commit()
    except Exception:
        db.session.rollback()

    return creator

def render_creator_template(template_name, **kwargs):
    creator = kwargs.get("creator") or current_creator()
    kwargs["creator"] = creator
    kwargs["creator_name"] = creator_display_name(creator)
    kwargs["creator_instagram"] = creator_instagram(creator)
    return render_template(template_name, **kwargs)

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
    if request.method == "POST":
        return redirect(url_for("creator.dashboard"))
    return render_template("creator/login.html")

@creator_bp.route("/dashboard")
def dashboard():
    creator = current_creator()
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
    creator = current_creator()
    if not creator:
        return redirect("/creator/login")

    used = _creator_used_storage_bytes(creator.id)
    limit = _creator_storage_limit_bytes(creator)
    return render_template(
        "creator/upload.html",
        used_bytes=used,
        limit_bytes=limit,
        used_gb=round(used / 1024 / 1024 / 1024, 2),
        limit_gb=round(limit / 1024 / 1024 / 1024, 2),
        batch_limit_gb=128,
    )



def _ensure_video_upload_columns():
    """Make sure old PostgreSQL video table has the columns required by the uploader before any SELECT."""
    statements = [
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS price NUMERIC(10,2) DEFAULT 0",
        "UPDATE video SET price = 0 WHERE price IS NULL",
        "ALTER TABLE video ALTER COLUMN price SET DEFAULT 0",
        "ALTER TABLE video ALTER COLUMN price DROP NOT NULL",
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
