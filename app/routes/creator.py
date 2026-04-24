import os, tempfile, uuid
from flask import Blueprint, render_template, request, redirect, url_for, current_app
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

@creator_bp.route("/upload", methods=["GET", "POST"])
def upload():
    creator = current_creator()

    try:
        suggestions = [l.name for l in Location.query.order_by(Location.name.asc()).all()]
    except Exception:
        db.session.rollback()
        suggestions = [
            "Boca Raton Inlet",
            "Hillsboro Inlet",
            "Boynton Inlet",
            "Haulover Inlet",
            "Port Everglades"
        ]

    if request.method == "POST":
        files = request.files.getlist("videos")
        location = (request.form.get("location") or "").strip()
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
            return render_creator_template("creator/upload.html", creator=creator, suggestions=suggestions, error="Maximum upload size per batch is 128GB.")

        if creator.storage_used_bytes + total_size > creator.storage_limit_gb * 1024**3:
            return render_creator_template("creator/upload.html", creator=creator, suggestions=suggestions, error="Storage limit reached. Delete videos or upgrade your plan.")

        batch = Batch(creator_id=creator.id, location=location, total_size_bytes=total_size)
        db.session.add(batch)
        db.session.flush()

        preset = VideoPricingPreset.query.filter_by(creator_id=creator.id, is_default=True, active=True).first()
        price = float(preset.price) if preset else 40.00

        for path, filename, size in temp_paths:
            recorded_at = extract_creation_time(path)
            video_key = f"creator_{creator.id}/batch_{batch.id}/{uuid.uuid4()}_{filename}"
            r2_upload(path, current_app.config["R2_BUCKET_VIDEOS"], video_key)

            thumb_url = ""
            thumb_key = None

            try:
                thumb_path = path + ".jpg"
                generate_center_thumbnail(path, thumb_path)
                thumb_key = f"video_thumbs/{uuid.uuid4()}.jpg"
                thumb_url = r2_upload(thumb_path, current_app.config["R2_BUCKET_THUMBNAILS"], thumb_key)
            except Exception:
                pass

            db.session.add(Video(
                creator_id=creator.id,
                batch_id=batch.id,
                location=location,
                recorded_at=recorded_at,
                r2_video_key=video_key,
                r2_thumbnail_key=thumb_key,
                public_thumbnail_url=thumb_url,
                file_size_bytes=size,
                original_price=price,
                edited_price=price,
                bundle_price=price,
                internal_filename=filename,
                status="active"
            ))

            try:
                os.unlink(path)
            except Exception:
                pass

        creator.storage_used_bytes += total_size
        db.session.commit()

        return render_creator_template("creator/upload.html", creator=creator, suggestions=suggestions, success=True, batch_id=batch.id)

    return render_creator_template("creator/upload.html", creator=creator, suggestions=suggestions)

@creator_bp.route("/batches")
def batches():
    creator = current_creator()
    try:
        batches = Batch.query.filter_by(creator_id=creator.id).order_by(Batch.created_at.desc()).all()
    except Exception:
        db.session.rollback()
        batches = []
    return render_creator_template("creator/batches.html", creator=creator, batches=batches)

@creator_bp.route("/batches/<int:batch_id>")
def batch_detail(batch_id):
    creator = current_creator()
    batch = Batch.query.get_or_404(batch_id)
    videos = Video.query.filter_by(batch_id=batch.id, creator_id=creator.id).order_by(Video.id.asc()).all()
    return render_creator_template("creator/batch_detail.html", creator=creator, batch=batch, videos=videos)

@creator_bp.route("/batches/<int:batch_id>/delete", methods=["POST"])
def delete_batch(batch_id):
    creator = current_creator()
    for v in Video.query.filter_by(batch_id=batch_id, creator_id=creator.id, status="active").all():
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
