import os
import secrets
from flask import Blueprint, render_template, request, redirect, session, flash
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




# v50.4O Buyer auth upgrade helpers
def _bsm_public_base_url_v504o():
    return (os.environ.get("PUBLIC_BASE_URL") or os.environ.get("BASE_URL") or "https://boatspotmedia.com").rstrip("/")

def _bsm_normalize_phone_v504o(phone):
    phone = (phone or "").strip()
    if not phone:
        return ""
    cleaned = "".join(ch for ch in phone if ch.isdigit() or ch == "+")
    if cleaned and not cleaned.startswith("+"):
        if len(cleaned) == 10:
            cleaned = "+1" + cleaned
        elif len(cleaned) == 11 and cleaned.startswith("1"):
            cleaned = "+" + cleaned
    return cleaned

def _bsm_ensure_buyer_auth_columns_v504o():
    try:
        db.session.execute(db.text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS phone_number TEXT'))
        db.session.execute(db.text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS sms_notifications_enabled BOOLEAN DEFAULT TRUE'))
        db.session.execute(db.text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS password_reset_token TEXT'))
        db.session.execute(db.text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS password_reset_expires_at TIMESTAMP'))
        db.session.execute(db.text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS google_id TEXT'))
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try:
            print("buyer auth columns v50.4O warning:", e)
        except Exception:
            pass

def _bsm_send_password_reset_email_v504o(to_email, reset_url):
    try:
        import requests
        api_key = os.environ.get("SENDGRID_API_KEY")
        from_email = os.environ.get("SENDGRID_FROM_EMAIL") or os.environ.get("FROM_EMAIL") or os.environ.get("MAIL_FROM")
        if not api_key or not from_email:
            print("SendGrid missing for buyer password reset v50.4O")
            return False
        payload = {
            "personalizations": [{"to": [{"email": to_email}]}],
            "from": {"email": from_email},
            "subject": "Reset your BoatSpotMedia password",
            "content": [{"type": "text/html", "value": f"""
                <h2>Reset your BoatSpotMedia password</h2>
                <p>Click the button below to reset your password. This link expires in 1 hour.</p>
                <p><a href="{reset_url}" style="background:#2563eb;color:#fff;padding:12px 16px;border-radius:8px;text-decoration:none;font-weight:700;">Reset Password</a></p>
                <p>If the button does not work, copy and paste this link:</p>
                <p>{reset_url}</p>
            """}],
        }
        r = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": "Bearer " + api_key, "Content-Type": "application/json"},
            json=payload,
            timeout=12,
        )
        return r.status_code in (200, 202)
    except Exception as e:
        try:
            print("send buyer reset email v50.4O warning:", e)
        except Exception:
            pass
        return False


@buyer_bp.route("/buyer/register", methods=["GET", "POST"])
def buyer_register():
    _bsm_ensure_buyer_auth_columns_v504o()
    if request.method == "POST":
        first_name = (request.form.get("first_name") or "").strip()
        last_name = (request.form.get("last_name") or "").strip()
        display_name = (request.form.get("display_name") or request.form.get("full_name") or (first_name + " " + last_name).strip() or request.form.get("name") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        phone = _bsm_normalize_phone_v504o(request.form.get("phone_number") or request.form.get("phone") or "")
        sms_enabled = bool(request.form.get("sms_notifications_enabled"))
        password = request.form.get("password") or ""
        confirm_password = request.form.get("confirm_password") or ""

        if not email or not password:
            return render_template("buyer/register.html", error="Email and password are required.")
        if len(password) < 6:
            return render_template("buyer/register.html", error="Password must be at least 6 characters.")
        if confirm_password and password != confirm_password:
            return render_template("buyer/register.html", error="Passwords do not match.")

        user = _find_user_by_email(email)
        if user:
            if getattr(user, "role", None) != "buyer":
                return render_template("buyer/register.html", error="This email already exists under another account type. Please use login or another email.")
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
                db.session.flush()
            except Exception:
                pass

        try:
            db.session.commit()
            db.session.execute(db.text("""
                UPDATE "user"
                SET phone_number=:phone,
                    sms_notifications_enabled=:sms_enabled
                WHERE lower(email)=lower(:email)
            """), {"phone": phone, "sms_enabled": sms_enabled, "email": email})
            db.session.commit()
            user = _find_user_by_email(email)
            _set_login_session(user)
            return redirect("/buyer/dashboard")
        except Exception as e:
            db.session.rollback()
            try:
                print("buyer register v50.4O warning:", e)
            except Exception:
                pass
            return render_template("buyer/register.html", error="Could not create account. Please try again.")

    return render_template("buyer/register.html")


@buyer_bp.route("/buyer/login", methods=["GET", "POST"])
def buyer_login():
    _bsm_ensure_buyer_auth_columns_v504o()
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""

        user = _find_user_by_email(email)
        if not user or getattr(user, "role", None) != "buyer" or not _password_ok(getattr(user, "password_hash", None), password):
            return render_template("buyer/login.html", error="Invalid email or password.")

        if not getattr(user, "is_active", True):
            return render_template("buyer/login.html", error="Account is not active.")

        _set_login_session(user)
        return redirect("/buyer/dashboard")

    return render_template("buyer/login.html")




@buyer_bp.route("/buyer/forgot-password", methods=["GET", "POST"])
def buyer_forgot_password_v504o():
    _bsm_ensure_buyer_auth_columns_v504o()
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        user = _find_user_by_email(email)
        if user and getattr(user, "role", None) == "buyer":
            token = secrets.token_urlsafe(32)
            try:
                db.session.execute(db.text("""
                    UPDATE "user"
                    SET password_reset_token=:token,
                        password_reset_expires_at=NOW() + INTERVAL '1 hour'
                    WHERE id=:uid
                """), {"token": token, "uid": user.id})
                db.session.commit()
                reset_url = _bsm_public_base_url_v504o() + "/buyer/reset-password/" + token
                _bsm_send_password_reset_email_v504o(user.email, reset_url)
            except Exception as e:
                db.session.rollback()
                try:
                    print("buyer forgot password v50.4O warning:", e)
                except Exception:
                    pass
        return render_template("buyer/forgot_password.html", sent=True)

    return render_template("buyer/forgot_password.html")


@buyer_bp.route("/buyer/reset-password/<token>", methods=["GET", "POST"])
def buyer_reset_password_v504o(token):
    _bsm_ensure_buyer_auth_columns_v504o()
    token = (token or "").strip()
    try:
        row = db.session.execute(db.text("""
            SELECT id, email
            FROM "user"
            WHERE password_reset_token=:token
              AND password_reset_expires_at IS NOT NULL
              AND password_reset_expires_at > NOW()
              AND role='buyer'
            LIMIT 1
        """), {"token": token}).mappings().first()
    except Exception:
        db.session.rollback()
        row = None

    if not row:
        return render_template("buyer/reset_password.html", invalid=True)

    if request.method == "POST":
        password = request.form.get("password") or ""
        confirm_password = request.form.get("confirm_password") or ""
        if len(password) < 6:
            return render_template("buyer/reset_password.html", error="Password must be at least 6 characters.")
        if password != confirm_password:
            return render_template("buyer/reset_password.html", error="Passwords do not match.")
        try:
            db.session.execute(db.text("""
                UPDATE "user"
                SET password_hash=:password_hash,
                    password_reset_token=NULL,
                    password_reset_expires_at=NULL
                WHERE id=:uid
            """), {"password_hash": generate_password_hash(password), "uid": row.get("id")})
            db.session.commit()
            return render_template("buyer/reset_password.html", success=True)
        except Exception:
            db.session.rollback()
            return render_template("buyer/reset_password.html", error="Could not reset password. Please try again.")

    return render_template("buyer/reset_password.html")


@buyer_bp.route("/dashboard")
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


@buyer_bp.route("/downloads")
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



# v50.5C Internal Support Center helpers
def _bsm_ensure_support_tables_v505c():
    try:
        db.session.execute(db.text("""
            CREATE TABLE IF NOT EXISTS support_thread (
                id SERIAL PRIMARY KEY,
                thread_type TEXT NOT NULL,
                subject TEXT NOT NULL,
                buyer_user_id INTEGER,
                buyer_email TEXT,
                creator_id INTEGER,
                order_id INTEGER,
                status TEXT DEFAULT 'open',
                last_message_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.execute(db.text("""
            CREATE TABLE IF NOT EXISTS support_message (
                id SERIAL PRIMARY KEY,
                thread_id INTEGER NOT NULL,
                sender_role TEXT NOT NULL,
                sender_id INTEGER,
                sender_email TEXT,
                body TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.execute(db.text("CREATE INDEX IF NOT EXISTS idx_support_thread_creator ON support_thread(creator_id)"))
        db.session.execute(db.text("CREATE INDEX IF NOT EXISTS idx_support_thread_buyer ON support_thread(buyer_user_id)"))
        db.session.execute(db.text("CREATE INDEX IF NOT EXISTS idx_support_msg_thread ON support_message(thread_id)"))
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try: print("support tables v50.5C warning:", e)
        except Exception: pass

def _bsm_thread_messages_v505c(thread_id):
    try:
        return db.session.execute(db.text("""
            SELECT *
            FROM support_message
            WHERE thread_id=:tid
            ORDER BY created_at ASC, id ASC
        """), {"tid": thread_id}).mappings().all()
    except Exception:
        db.session.rollback()
        return []

@buyer_bp.route("/support", methods=["GET", "POST"])
def buyer_support_center_v505c():
    if not session.get("user_id") or session.get("user_role") != "buyer":
        session["after_login_redirect"] = "/buyer/support"
        session.modified = True
        return redirect("/buyer/login")

    _bsm_ensure_support_tables_v505c()
    buyer_id = session.get("user_id")
    buyer_email = session.get("user_email")

    if request.method == "POST":
        order_id = request.form.get("order_id") or None
        creator_id = request.form.get("creator_id") or None
        subject = (request.form.get("subject") or "Support request").strip()[:180]
        body = (request.form.get("message") or "").strip()
        if not body:
            flash("Please enter a message.")
            return redirect("/buyer/support")

        # Verify buyer owns the order and get creator if not provided.
        try:
            row = db.session.execute(db.text("""
                SELECT o.id AS order_id, COALESCE(i.creator_id, v.creator_id) AS creator_id
                FROM bsm_cart_order o
                JOIN bsm_cart_order_item i ON i.cart_order_id=o.id
                LEFT JOIN video v ON v.id=i.video_id
                WHERE o.id=:order_id
                  AND (o.buyer_user_id=:buyer_id OR lower(o.buyer_email)=lower(:buyer_email))
                LIMIT 1
            """), {"order_id": order_id, "buyer_id": buyer_id, "buyer_email": buyer_email}).mappings().first()
        except Exception:
            db.session.rollback()
            row = None

        if row:
            creator_id = row.get("creator_id") or creator_id
        if not creator_id:
            flash("Could not identify the creator for this support request.")
            return redirect("/buyer/support")

        try:
            res = db.session.execute(db.text("""
                INSERT INTO support_thread
                (thread_type, subject, buyer_user_id, buyer_email, creator_id, order_id, status, last_message_at, created_at, updated_at)
                VALUES ('buyer_creator', :subject, :buyer_id, :buyer_email, :creator_id, :order_id, 'open', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                RETURNING id
            """), {"subject": subject, "buyer_id": buyer_id, "buyer_email": buyer_email, "creator_id": creator_id, "order_id": order_id})
            thread_id = res.scalar()
            db.session.execute(db.text("""
                INSERT INTO support_message(thread_id, sender_role, sender_id, sender_email, body, created_at)
                VALUES (:thread_id, 'buyer', :sender_id, :sender_email, :body, CURRENT_TIMESTAMP)
            """), {"thread_id": thread_id, "sender_id": buyer_id, "sender_email": buyer_email, "body": body})
            db.session.commit()
            flash("Support request sent.")
            return redirect(f"/buyer/support/{thread_id}")
        except Exception as e:
            db.session.rollback()
            try: print("buyer support create v50.5C warning:", e)
            except Exception: pass
            flash("Could not send support request.")
            return redirect("/buyer/support")

    try:
        threads = db.session.execute(db.text("""
            SELECT st.*,
                   cp.public_name AS creator_name,
                   (SELECT body FROM support_message sm WHERE sm.thread_id=st.id ORDER BY sm.created_at DESC, sm.id DESC LIMIT 1) AS last_body
            FROM support_thread st
            LEFT JOIN creator_profile cp ON cp.id=st.creator_id
            WHERE st.thread_type='buyer_creator'
              AND (st.buyer_user_id=:buyer_id OR lower(st.buyer_email)=lower(:buyer_email))
            ORDER BY st.last_message_at DESC, st.id DESC
        """), {"buyer_id": buyer_id, "buyer_email": buyer_email}).mappings().all()
    except Exception:
        db.session.rollback()
        threads=[]

    try:
        orders = db.session.execute(db.text("""
            SELECT DISTINCT o.id, o.created_at, COALESCE(i.creator_id, v.creator_id) AS creator_id,
                   COALESCE(cp.public_name, cpa.public_name, 'Creator') AS creator_name,
                   COALESCE(v.location, v.internal_filename, 'Video') AS title
            FROM bsm_cart_order o
            JOIN bsm_cart_order_item i ON i.cart_order_id=o.id
            LEFT JOIN video v ON v.id=i.video_id
            LEFT JOIN creator_profile cp ON cp.id=i.creator_id
            LEFT JOIN creator_profile cpa ON cpa.id=v.creator_id
            WHERE (o.buyer_user_id=:buyer_id OR lower(o.buyer_email)=lower(:buyer_email))
            ORDER BY o.created_at DESC
            LIMIT 50
        """), {"buyer_id": buyer_id, "buyer_email": buyer_email}).mappings().all()
    except Exception:
        db.session.rollback()
        orders=[]

    return render_template("buyer/support.html", threads=threads, orders=orders, email=buyer_email)

@buyer_bp.route("/support/<int:thread_id>", methods=["GET", "POST"])
def buyer_support_thread_v505c(thread_id):
    if not session.get("user_id") or session.get("user_role") != "buyer":
        return redirect("/buyer/login")
    _bsm_ensure_support_tables_v505c()
    buyer_id=session.get("user_id")
    buyer_email=session.get("user_email")

    try:
        thread=db.session.execute(db.text("""
            SELECT st.*, cp.public_name AS creator_name
            FROM support_thread st
            LEFT JOIN creator_profile cp ON cp.id=st.creator_id
            WHERE st.id=:tid AND st.thread_type='buyer_creator'
              AND (st.buyer_user_id=:buyer_id OR lower(st.buyer_email)=lower(:buyer_email))
            LIMIT 1
        """), {"tid":thread_id,"buyer_id":buyer_id,"buyer_email":buyer_email}).mappings().first()
    except Exception:
        db.session.rollback()
        thread=None
    if not thread:
        return "Support thread not found", 404

    if request.method=="POST":
        body=(request.form.get("message") or "").strip()
        if body:
            try:
                db.session.execute(db.text("""
                    INSERT INTO support_message(thread_id, sender_role, sender_id, sender_email, body, created_at)
                    VALUES (:tid, 'buyer', :sid, :email, :body, CURRENT_TIMESTAMP)
                """), {"tid":thread_id,"sid":buyer_id,"email":buyer_email,"body":body})
                db.session.execute(db.text("""
                    UPDATE support_thread SET status='open', last_message_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id=:tid
                """), {"tid":thread_id})
                db.session.commit()
            except Exception:
                db.session.rollback()
                flash("Could not send message.")
        return redirect(f"/buyer/support/{thread_id}")

    messages=_bsm_thread_messages_v505c(thread_id)
    return render_template("buyer/support_thread.html", thread=thread, messages=messages, email=buyer_email)
