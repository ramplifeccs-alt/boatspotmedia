import os
from flask import Blueprint, render_template, request, redirect, session
from werkzeug.security import generate_password_hash, check_password_hash
from app import db
from app.models import User

buyer_bp = Blueprint("buyer", __name__)


def _find_user_by_email(email):
    if not email:
        return None
    try:
        return User.query.filter(db.func.lower(User.email) == email.lower().strip()).first()
    except Exception:
        db.session.rollback()
        return None


def _set_login_session(user):
    session["user_id"] = user.id
    session["user_email"] = user.email
    session["user_role"] = user.role
    session["display_name"] = getattr(user, "display_name", None) or user.email
    session.modified = True


def _password_ok(stored_hash, password):
    if not stored_hash:
        return False
    try:
        if check_password_hash(stored_hash, password):
            return True
    except Exception:
        pass
    return stored_hash == password


def _buyer_orders_for_email(email):
    if not email:
        return []
    try:
        return db.session.execute(db.text("""
            SELECT id, stripe_session_id, amount_total, currency, status, created_at, pending_discount_review
            FROM bsm_cart_order
            WHERE lower(buyer_email)=lower(:email)
            ORDER BY created_at DESC
        """), {"email": email}).mappings().all()
    except Exception:
        db.session.rollback()
        return []


def _buyer_orders_for_user_v424(user_id, email):
    try:
        db.session.execute(db.text("ALTER TABLE bsm_cart_order ADD COLUMN IF NOT EXISTS buyer_user_id INTEGER"))
        db.session.commit()

        # bsm_v466_pending_edit_status
        try:
            db.session.execute(db.text("""
                UPDATE bsm_cart_order_item
                SET delivery_status = 'pending_edit'
                WHERE package IN ('edited','edit','instagram_edit','tiktok_edit','reel_edit','short_edit','bundle','combo','original_plus_edited','original_edited','original+edited','original_edit')
                  AND (edited_r2_key IS NULL OR edited_r2_key = '')
                  AND delivery_status IN ('ready_to_download','ready','delivered','paid','')
            """))
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            try: print("pending edit status correction v46.6:", e)
            except Exception: pass

        _bsm_fix_order_item_creator_id_v460()
    except Exception:
        db.session.rollback()
    try:
        return db.session.execute(db.text("""
            SELECT id, stripe_session_id, amount_total, currency, status, created_at, pending_discount_review
            FROM bsm_cart_order
            WHERE (buyer_user_id = :uid)
               OR (:email IS NOT NULL AND lower(buyer_email)=lower(:email))
            ORDER BY created_at DESC
        """), {"uid": user_id or 0, "email": email}).mappings().all()
    except Exception:
        db.session.rollback()
        return []


def _buyer_order_items(order_id):
    try:
        return db.session.execute(db.text("""
            SELECT i.*, v.location, v.filename, v.internal_filename, v.thumbnail_path, v.public_thumbnail_url, v.r2_thumbnail_key
            FROM bsm_cart_order_item i
            LEFT JOIN video v ON v.id=i.video_id
            WHERE i.cart_order_id=:oid
            ORDER BY i.id ASC
        """), {"oid": order_id}).mappings().all()
    except Exception:
        db.session.rollback()
        return []



def _bsm_media_url_v427(row, kind="thumb"):
    try:
        keys = ["thumbnail_path", "public_thumbnail_url", "r2_thumbnail_key"] if kind == "thumb" else ["file_path", "r2_video_key", "public_url", "preview_url"]
        for key in keys:
            val = row.get(key) if hasattr(row, "get") else row[key]
            if val:
                val = str(val)
                if val.startswith("http") or val.startswith("/"):
                    return val
                return "/media/" + val.lstrip("/")
    except Exception:
        pass
    return None


def _bsm_eastern_time_v427(value):
    try:
        from zoneinfo import ZoneInfo
        from datetime import timezone
        if not value:
            return ""
        if getattr(value, "tzinfo", None) is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(ZoneInfo("America/New_York")).strftime("%m/%d/%Y %I:%M %p")
    except Exception:
        try:
            return value.strftime("%m/%d/%Y %I:%M %p")
        except Exception:
            return str(value or "")



def _claim_guest_orders_v428(user_id, email):
    try:
        db.session.execute(db.text("ALTER TABLE bsm_cart_order ADD COLUMN IF NOT EXISTS buyer_user_id INTEGER"))
        db.session.execute(db.text("""
            UPDATE bsm_cart_order
            SET buyer_user_id = :uid
            WHERE (buyer_user_id IS NULL OR buyer_user_id = 0)
              AND buyer_email IS NOT NULL
              AND lower(buyer_email) = lower(:email)
        """), {"uid": user_id, "email": email})
        db.session.commit()
    except Exception:
        db.session.rollback()



def _bsm_item_is_downloadable_v431(item):
    try:
        package = str(item.get("package") or "").lower()
        status = str(item.get("delivery_status") or "").lower()
        if status in ["pending_edit", "editing", "not_ready", "pending"]:
            return False
        if package in ["original", "instant", "instant_download", "download", "4k", "original_4k"]:
            return True
        if status in ["ready_to_download", "ready", "download_ready", "paid"]:
            return True
        # If package/status is empty but this is a paid order item, show download;
        # the download route will still protect access and return unavailable if file is missing.
        return True
    except Exception:
        return True



def _bsm_item_download_locked_v439(item):
    status = str(item.get("discount_status") or "").lower()
    delivery = str(item.get("delivery_status") or "").lower()
    if status in ["pending_review", "pending", "awaiting_creator", "needs_approval"]:
        return True
    if delivery in ["pending_discount_review", "pending_edit", "editing", "not_ready", "pending"]:
        return True
    return False



def _bsm_download_timer_v441(item, order_created_at=None):
    try:
        from datetime import datetime, timezone, timedelta
        package = str(item.get("package") or "").lower()
        is_edited = package in ["edited", "edit", "instagram_edit", "tiktok_edit", "reel_edit", "short_edit"]
        start = item.get("edited_uploaded_at") if is_edited and item.get("edited_uploaded_at") else order_created_at
        if not start:
            return {"expired": False, "expires_at": "", "remaining_seconds": 72*3600}
        if getattr(start, "tzinfo", None) is None:
            start = start.replace(tzinfo=timezone.utc)
        expires = start + timedelta(hours=72)
        remaining = int((expires - datetime.now(timezone.utc)).total_seconds())
        try:
            from zoneinfo import ZoneInfo
            expires_display = expires.astimezone(ZoneInfo("America/New_York")).strftime("%m/%d/%Y %I:%M %p")
        except Exception:
            expires_display = expires.strftime("%m/%d/%Y %I:%M %p")
        return {"expired": remaining <= 0, "expires_at": expires_display, "remaining_seconds": max(0, remaining)}
    except Exception:
        return {"expired": False, "expires_at": "", "remaining_seconds": 72*3600}



def _bsm_is_edited_package_v443(package):
    return str(package or "").lower() in ["edited", "edit", "instagram_edit", "tiktok_edit", "reel_edit", "short_edit"]

def _bsm_is_bundle_package_v443(package):
    return str(package or "").lower() in ["bundle", "combo", "original_plus_edited", "original_edited", "original+edited", "original_edit"]


def _bsm_public_r2_url_v468(key):
    import os
    base = (os.environ.get("R2_PUBLIC_URL") or os.environ.get("R2_PUBLIC_BASE_URL") or "").strip().rstrip("/")
    if base and key:
        return base + "/" + str(key).lstrip("/")
    return ""



def _bsm_make_delivery_v443(ix, delivery_type, order_created_at=None):
    """
    Build buyer delivery rows.
    Edited delivery must use edited_r2_key public R2 URL and must not be overwritten.
    Original side of a bundle must remain downloadable even if edited side is pending_edit.
    """
    delivery = dict(ix)
    delivery["item_id"] = delivery.get("item_id") or delivery.get("order_item_id") or delivery.get("id")
    delivery["delivery_type"] = delivery_type
    delivery["delivery_label"] = "Edited Version" if delivery_type == "edited" else "Original Clip / Instant Download"

    discount_status = str(delivery.get("discount_status") or "").lower()
    delivery_status = str(delivery.get("delivery_status") or "").lower()

    if delivery_type == "edited":
        ready = bool(delivery.get("edited_r2_key")) and delivery_status in ["ready_to_download", "ready", "delivered"]
        delivery["download_locked"] = not ready
        if ready:
            delivery["download_url"] = _bsm_public_r2_url_v468(delivery.get("edited_r2_key"))
            delivery["status_label"] = "Ready"
        else:
            delivery["download_url"] = None
            delivery["status_label"] = "Pending edit"
        timer = _bsm_download_timer_v441(delivery, order_created_at) if "_bsm_download_timer_v441" in globals() else _bsm_download_timer_v442(delivery, order_created_at)
    else:
        locked_by_discount = discount_status in ["pending_review", "pending", "awaiting_creator", "needs_approval"]
        delivery["download_locked"] = bool(locked_by_discount)
        delivery["download_url"] = None if delivery["download_locked"] else "/download-video/" + str(delivery.get("item_id") or delivery.get("id") or delivery.get("video_id")) + "?delivery=original"
        delivery["status_label"] = "Pending approval" if locked_by_discount else "Ready"
        delivery["package"] = "original"
        timer = _bsm_download_timer_v441(delivery, order_created_at) if "_bsm_download_timer_v441" in globals() else _bsm_download_timer_v442(delivery, order_created_at)

    delivery["download_expired"] = timer.get("expired", False)
    delivery["download_expires_at"] = timer.get("expires_at", "")
    delivery["download_remaining_seconds"] = timer.get("remaining_seconds", 72*3600)
    if delivery["download_expired"]:
        delivery["download_url"] = None
    return delivery


def _bsm_group_order_items_for_display_v443(order_items, order_created_at=None):
    """
    Combines original+edited rows for the same video into one display item.
    Also expands a bundle row into original + edited deliveries.
    """
    grouped = {}
    singles = []

    for raw in order_items or []:
        ix = dict(raw)
        video_id = ix.get("video_id") or ix.get("id")
        package = str(ix.get("package") or "").lower()
        key = str(video_id)

        if _bsm_is_bundle_package_v443(package):
            display = dict(ix)
            display["display_package"] = "bundle"
            display["deliveries"] = [
                _bsm_make_delivery_v443(ix, "original", order_created_at),
                _bsm_make_delivery_v443(ix, "edited", order_created_at),
            ]
            singles.append(display)
            continue

        if key not in grouped:
            grouped[key] = dict(ix)
            grouped[key]["deliveries"] = []

        if _bsm_is_edited_package_v443(package):
            grouped[key]["deliveries"].append(_bsm_make_delivery_v443(ix, "edited", order_created_at))
        else:
            grouped[key]["deliveries"].append(_bsm_make_delivery_v443(ix, "original", order_created_at))

    # If same video has original + edited separate, show one display item with two deliveries.
    result = singles
    for _, item in grouped.items():
        # sort original first, edited second
        item["deliveries"] = sorted(item.get("deliveries", []), key=lambda d: 1 if d.get("delivery_type") == "edited" else 0)
        result.append(item)
    return result



def _bsm_fix_order_item_creator_id_v460(order_id=None):
    """
    After checkout, attach creator_id to order items from the purchased videos.
    This prevents creator orders dashboard from showing empty sales.
    """
    try:
        params = {}
        where_order = ""
        if order_id:
            params["order_id"] = order_id
            where_order = " AND i.cart_order_id = :order_id "
        db.session.execute(db.text(f"""
            UPDATE bsm_cart_order_item i
            SET creator_id = v.creator_id
            FROM video v
            WHERE i.video_id = v.id
              AND (i.creator_id IS NULL OR i.creator_id = 0)
              AND v.creator_id IS NOT NULL
              {where_order}
        """), params)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try:
            print("order item creator_id fix warning v46.0:", e)
        except Exception:
            pass


@buyer_bp.route("/buyer/register", methods=["GET", "POST"])
def buyer_register():
    if request.method == "POST":
        display_name = (request.form.get("display_name") or request.form.get("full_name") or request.form.get("name") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""

        if not email or not password:
            return render_template("public/generic_register.html", role="buyer", error="Email and password are required.")

        if len(password) < 6:
            return render_template("public/generic_register.html", role="buyer", error="Password must be at least 6 characters.")

        user = _find_user_by_email(email)
        if user:
            # If this buyer already exists, refresh password and log in.
            # If email belongs to another role, do not take over that account.
            if getattr(user, "role", None) != "buyer":
                return render_template("public/generic_register.html", role="buyer", error="This email already exists under another account type. Please use login or another email.")
            user.password_hash = generate_password_hash(password)
            user.display_name = display_name or user.display_name or email
            user.is_active = True
        else:
            user = User(
                email=email,
                password_hash=generate_password_hash(password),
                display_name=display_name or email,
                role="buyer",
                is_active=True,
            )
            db.session.add(user)

        try:
            db.session.commit()
            _set_login_session(user)
            return redirect("/buyer/dashboard")
        except Exception as e:
            db.session.rollback()
            return render_template("public/generic_register.html", role="buyer", error="Could not create account. Please try again.")

    return render_template("public/generic_register.html", role="buyer")


@buyer_bp.route("/buyer/login", methods=["GET", "POST"])
def buyer_login():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""

        user = _find_user_by_email(email)
        if not user or getattr(user, "role", None) != "buyer" or not _password_ok(getattr(user, "password_hash", None), password):
            return render_template("public/generic_login.html", title="Buyer Login", subtitle="Access your orders and downloads.", register_url="/buyer/register", role="buyer", error="Invalid email or password.")

        if not getattr(user, "is_active", True):
            return render_template("public/generic_login.html", title="Buyer Login", subtitle="Access your orders and downloads.", register_url="/buyer/register", role="buyer", error="Account is not active.")

        _set_login_session(user)
        return redirect("/buyer/dashboard")

    return render_template("public/generic_login.html", title="Buyer Login", subtitle="Access your orders and downloads.", register_url="/buyer/register", role="buyer")


@buyer_bp.route("/buyer/dashboard")
def buyer_dashboard():
    if not session.get("user_id") or session.get("user_role") != "buyer":
        return redirect("/buyer/login")

    email = session.get("user_email")
    _claim_guest_orders_v428(session.get('user_id'), email)
    orders = []
    for order in _buyer_orders_for_user_v424(session.get('user_id'), email):
        d = dict(order)
        items = []
        for x in _buyer_order_items(order["id"]):
            ix = dict(x)
            timer = _bsm_download_timer_v441(ix, d.get("created_at"))
            ix["download_expired"] = timer["expired"]
            ix["download_expires_at"] = timer["expires_at"]
            ix["download_remaining_seconds"] = timer["remaining_seconds"]
            ix["download_locked"] = _bsm_item_download_locked_v439(ix)
            timer = _bsm_download_timer_v441(ix, d.get("created_at"))
            ix["download_expired"] = timer["expired"]
            ix["download_expires_at"] = timer["expires_at"]
            ix["download_remaining_seconds"] = timer["remaining_seconds"]
            ix["download_url"] = None if ix["download_locked"] or ix["download_expired"] else "/download-video/" + str(ix.get("id") or ix.get("video_id"))
            ix["thumbnail_url"] = _bsm_media_url_v427(ix, "thumb")
            ix["download_locked"] = _bsm_item_download_locked_v439(ix)
            timer = _bsm_download_timer_v441(ix, d.get("created_at"))
            ix["download_expired"] = timer["expired"]
            ix["download_expires_at"] = timer["expires_at"]
            ix["download_remaining_seconds"] = timer["remaining_seconds"]
            ix["download_url"] = None if ix["download_locked"] or ix["download_expired"] else "/download-video/" + str(ix.get("id") or ix.get("video_id"))
            items.append(ix)
        if not items:
            d["order_items"] = []
        else:
            d["order_items"] = _bsm_group_order_items_for_display_v443(items, d.get("created_at"))
        d["created_at_et"] = _bsm_eastern_time_v427(d.get("created_at"))
        d["recover_download_url"] = "/buyer/order-downloads/" + str(d.get("id"))
        orders.append(d)

    return render_template(
        "buyer/dashboard.html",
        display_name=session.get("display_name") or email or "Buyer",
        email=email,
        buyer_email=email,
        orders=orders,
    )


@buyer_bp.route("/buyer/orders")
def buyer_orders():
    return redirect("/buyer/dashboard")


@buyer_bp.route("/buyer/downloads")
def buyer_downloads():
    return redirect("/buyer/dashboard")



def _bsm_direct_download_video_response_v435(video_id):
    """
    Direct purchased video download.
    The ID is the actual video.id. This does not depend on cart item id.
    """
    if not session.get("user_id") or session.get("user_role") != "buyer":
        session["after_login_redirect"] = "/buyer/dashboard"
        session.modified = True
        return redirect("/buyer/login?next=/buyer/dashboard")

    uid = session.get("user_id")
    email = (session.get("user_email") or "").lower()

    try:
        db.session.execute(db.text("ALTER TABLE bsm_cart_order ADD COLUMN IF NOT EXISTS buyer_user_id INTEGER"))
        db.session.commit()
    except Exception:
        db.session.rollback()

    # Verify this buyer bought this video.
    try:
        purchased = db.session.execute(db.text("""
            SELECT i.id AS order_item_id, i.video_id, i.delivery_status, i.package,
                   o.id AS order_id, o.buyer_user_id, o.buyer_email, o.status
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            WHERE i.video_id = :vid
              AND (
                    o.buyer_user_id = :uid
                    OR lower(coalesce(o.buyer_email,'')) = lower(:email)
                  )
            ORDER BY o.created_at DESC
            LIMIT 1
        """), {"vid": video_id, "uid": uid or 0, "email": email}).mappings().first()
    except Exception:
        db.session.rollback()
        purchased = None

    # Also allow if caller passed order item id instead of video id.
    if not purchased:
        try:
            purchased = db.session.execute(db.text("""
                SELECT i.id AS order_item_id, i.video_id, i.delivery_status, i.package,
                       o.id AS order_id, o.buyer_user_id, o.buyer_email, o.status
                FROM bsm_cart_order_item i
                JOIN bsm_cart_order o ON o.id = i.cart_order_id
                WHERE i.id = :item_id
                  AND (
                        o.buyer_user_id = :uid
                        OR lower(coalesce(o.buyer_email,'')) = lower(:email)
                      )
                ORDER BY o.created_at DESC
                LIMIT 1
            """), {"item_id": video_id, "uid": uid or 0, "email": email}).mappings().first()
        except Exception:
            db.session.rollback()
            purchased = None

    if not purchased:
        return "Download not found: this video is not linked to your paid orders.", 404

    if str(purchased.get("delivery_status") or "").lower() in ["pending_edit", "editing", "not_ready", "pending"]:
        return "This edited video is not ready for download yet.", 400

    real_video_id = purchased.get("video_id") or video_id

    try:
        video = db.session.execute(db.text("SELECT * FROM video WHERE id=:vid LIMIT 1"), {"vid": real_video_id}).mappings().first()
    except Exception:
        db.session.rollback()
        video = None

    if not video:
        return "Video record not found.", 404

    # Exact known columns across app versions.
    candidate_keys = [
        "public_url",
        "download_url",
        "file_url",
        "original_url",
        "video_url",
        "r2_public_url",
        "file_path",
        "r2_video_key",
        "r2_key",
        "video_key",
        "storage_key",
        "original_file_path",
        "original_path",
        "internal_filename",
        "filename",
    ]

    for key in candidate_keys:
        try:
            val = video.get(key)
        except Exception:
            val = None
        if not val:
            continue

        val = str(val).strip()
        if not val:
            continue

        if val.startswith("http://") or val.startswith("https://"):
            return redirect(val)
        if val.startswith("/"):
            return redirect(val)

        # R2/media keys used by this app.
        return redirect((os.environ.get("R2_PUBLIC_URL") or "").rstrip("/") + "/" + val.lstrip("/"))

    return "Video file path was not found in the video record. Contact support with Order #" + str(purchased.get("order_id")), 404


@buyer_bp.route("/download-video/<int:video_id>")
@buyer_bp.route("/download-item/<int:video_id>")
@buyer_bp.route("/buyer/download-item/<int:video_id>")
def bsm_download_video_buyer_v435(video_id):
    return _bsm_direct_download_video_response_v435(video_id)
