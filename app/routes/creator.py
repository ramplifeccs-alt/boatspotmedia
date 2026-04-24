import os, tempfile, uuid
from flask import Blueprint, render_template, request, redirect, url_for, current_app
from app.models import User, CreatorProfile, Batch, Video, Location, CreatorClickStats, Product, VideoPricingPreset, OrderItem
from app.services.db import db
from app.services.media import extract_creation_time, generate_center_thumbnail
from app.services.r2 import upload as r2_upload

creator_bp = Blueprint("creator", __name__)

def current_creator():
    creator = CreatorProfile.query.first()
    if not creator:
        user = User(email="creator@test.com", role="creator", display_name="Test Creator", is_active=True)
        db.session.add(user); db.session.flush()
        creator = CreatorProfile(user_id=user.id, approved=True, storage_limit_gb=512, commission_rate=20, product_commission_rate=20)
        db.session.add(creator); db.session.flush()
        db.session.add(CreatorClickStats(creator_id=creator.id))
        db.session.commit()
    if creator.user and not creator.user.display_name:
        creator.user.display_name = "Creator"
        db.session.commit()
    return creator

@creator_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        return redirect(url_for("creator.dashboard"))
    return render_template("creator/login.html")

@creator_bp.route("/dashboard")
def dashboard():
    creator = current_creator()
    stats = CreatorClickStats.query.filter_by(creator_id=creator.id).first()
    videos_count = Video.query.filter_by(creator_id=creator.id, status="active").count()
    return render_template("creator/dashboard.html", creator=creator, stats=stats, videos_count=videos_count)

@creator_bp.route("/upload", methods=["GET", "POST"])
def upload():
    creator = current_creator()
    suggestions = [l.name for l in Location.query.order_by(Location.name.asc()).all()]
    if request.method == "POST":
        files = request.files.getlist("videos")
        location = (request.form.get("location") or "").strip()
        total_size = 0
        temp_paths = []
        for f in files:
            f.seek(0, os.SEEK_END); size = f.tell(); f.seek(0)
            total_size += size
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(f.filename)[1] or ".mp4")
            f.save(tmp.name)
            temp_paths.append((tmp.name, f.filename, size))
        if total_size > current_app.config["MAX_BATCH_GB"] * 1024**3:
            return render_template("creator/upload.html", suggestions=suggestions, error="Maximum upload size per batch is 128GB.")
        if creator.storage_used_bytes + total_size > creator.storage_limit_gb * 1024**3:
            return render_template("creator/upload.html", suggestions=suggestions, error="Storage limit reached. Delete videos or upgrade your plan.")
        batch = Batch(creator_id=creator.id, location=location, total_size_bytes=total_size)
        db.session.add(batch); db.session.flush()
        preset = VideoPricingPreset.query.filter_by(creator_id=creator.id, is_default=True, active=True).first()
        price = float(preset.price) if preset else 40.00
        for path, filename, size in temp_paths:
            recorded_at = extract_creation_time(path)
            video_key = f"creator_{creator.id}/batch_{batch.id}/{uuid.uuid4()}_{filename}"
            r2_upload(path, current_app.config["R2_BUCKET_VIDEOS"], video_key)
            thumb_url = ""; thumb_key = None
            try:
                thumb_path = path + ".jpg"
                generate_center_thumbnail(path, thumb_path)
                thumb_key = f"video_thumbs/{uuid.uuid4()}.jpg"
                thumb_url = r2_upload(thumb_path, current_app.config["R2_BUCKET_THUMBNAILS"], thumb_key)
            except Exception:
                pass
            db.session.add(Video(
                creator_id=creator.id, batch_id=batch.id, location=location,
                recorded_at=recorded_at, r2_video_key=video_key, r2_thumbnail_key=thumb_key,
                public_thumbnail_url=thumb_url, file_size_bytes=size,
                original_price=price, edited_price=price, bundle_price=price,
                internal_filename=filename
            ))
            try: os.unlink(path)
            except Exception: pass
        creator.storage_used_bytes += total_size
        db.session.commit()
        return render_template("creator/upload.html", suggestions=suggestions, success=True, batch_id=batch.id)
    return render_template("creator/upload.html", suggestions=suggestions)

@creator_bp.route("/batches")
def batches():
    creator = current_creator()
    batches = Batch.query.filter_by(creator_id=creator.id).order_by(Batch.created_at.desc()).all()
    return render_template("creator/batches.html", batches=batches)

@creator_bp.route("/batches/<int:batch_id>")
def batch_detail(batch_id):
    creator = current_creator()
    batch = Batch.query.get_or_404(batch_id)
    videos = Video.query.filter_by(batch_id=batch.id, creator_id=creator.id).order_by(Video.id.asc()).all()
    return render_template("creator/batch_detail.html", batch=batch, videos=videos)

@creator_bp.route("/batches/<int:batch_id>/delete", methods=["POST"])
def delete_batch(batch_id):
    creator = current_creator()
    videos = Video.query.filter_by(batch_id=batch_id, creator_id=creator.id, status="active").all()
    for v in videos:
        v.status = "deleted"
        creator.storage_used_bytes = max(0, creator.storage_used_bytes - (v.file_size_bytes or 0))
    db.session.commit()
    return redirect(url_for("creator.batches"))

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

@creator_bp.route("/pricing", methods=["GET", "POST"])
def pricing():
    creator = current_creator()
    if request.method == "POST":
        if request.form.get("is_default"):
            VideoPricingPreset.query.filter_by(creator_id=creator.id).update({"is_default": False})
        db.session.add(VideoPricingPreset(
            creator_id=creator.id,
            title=request.form.get("title") or "Default Video Price",
            description=request.form.get("description"),
            price=float(request.form.get("price") or 40),
            delivery_type=request.form.get("delivery_type") or "instant",
            is_default=bool(request.form.get("is_default")),
            active=True
        ))
        creator.second_clip_discount_percent = int(request.form.get("second_clip_discount_percent") or creator.second_clip_discount_percent or 0)
        db.session.commit()
        return redirect(url_for("creator.pricing"))
    presets = VideoPricingPreset.query.filter_by(creator_id=creator.id, active=True).order_by(VideoPricingPreset.id.desc()).all()
    return render_template("creator/pricing.html", creator=creator, presets=presets)

@creator_bp.route("/orders")
def orders():
    creator = current_creator()
    status = request.args.get("status", "pending")
    q = OrderItem.query.filter_by(creator_id=creator.id)
    if status == "pending":
        q = q.filter(OrderItem.edited_status == "pending")
    elif status == "completed":
        q = q.filter(OrderItem.edited_status.in_(["ready", "not_required"]))
    items = q.order_by(OrderItem.id.desc()).all()
    return render_template("creator/orders.html", items=items, status=status)

@creator_bp.route("/orders/<int:item_id>/upload-edited", methods=["GET", "POST"])
def upload_edited(item_id):
    creator = current_creator()
    item = OrderItem.query.filter_by(id=item_id, creator_id=creator.id).first_or_404()
    if request.method == "POST":
        f = request.files.get("edited_video")
        if f:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(f.filename)[1] or ".mp4")
            f.save(tmp.name)
            key = f"edited/creator_{creator.id}/order_item_{item.id}/{uuid.uuid4()}_{f.filename}"
            r2_upload(tmp.name, current_app.config["R2_BUCKET_VIDEOS"], key)
            item.edited_r2_key = key
            item.edited_status = "ready"
            db.session.commit()
            return render_template("creator/upload_edited.html", item=item, success=True)
    return render_template("creator/upload_edited.html", item=item)

@creator_bp.route("/products", methods=["GET", "POST"])
def products():
    creator = current_creator()
    if request.method == "POST":
        db.session.add(Product(
            creator_id=creator.id, title=request.form.get("title"), description=request.form.get("description"),
            price=float(request.form.get("price") or 0), shipping_cost=float(request.form.get("shipping_cost") or 0),
            processing_time=request.form.get("processing_time"), shipping_method=request.form.get("shipping_method"), active=True
        ))
        db.session.commit()
        return redirect(url_for("creator.products"))
    products = Product.query.filter_by(creator_id=creator.id).all()
    return render_template("creator/products.html", products=products)

@creator_bp.route("/settings", methods=["GET", "POST"])
def settings():
    creator = current_creator()
    if request.method == "POST":
        if creator.user:
            creator.user.display_name = request.form.get("display_name") or creator.user.display_name
            creator.user.email = request.form.get("email") or creator.user.email
        db.session.commit()
        return redirect(url_for("creator.settings"))
    return render_template("creator/settings.html", creator=creator)
