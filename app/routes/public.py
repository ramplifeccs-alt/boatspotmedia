import os
from datetime import datetime
from flask import Blueprint, redirect, render_template, request, url_for, session, jsonify, flash
from sqlalchemy import text
from werkzeug.security import generate_password_hash, check_password_hash
from app.models import Video, Location, ServiceAd, CharterListing, User
from app.services.db import db

public_bp = Blueprint("public", __name__)


def clean_instagram(value):
    value = (value or "").strip()
    for prefix in ["https://www.instagram.com/", "https://instagram.com/", "http://www.instagram.com/", "http://instagram.com/"]:
        value = value.replace(prefix, "")
    value = value.strip().strip("/")
    if value.startswith("@"):
        value = value[1:]
    return value.strip()



def _public_video_locations():
    """Buyer locations come only from active videos uploaded by creators."""
    try:
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
            print("public locations warning:", e)
            db.session.rollback()
        except Exception:
            pass
        return []


def _ny_dt(dt):
    """Display DB UTC timestamps in America/New_York."""
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


def _session_dashboard_url():
    role = session.get("user_role") or session.get("role")
    if role == "creator":
        return "/creator/dashboard"
    if role == "buyer":
        return "/buyer/dashboard"
    if role in ["services", "service"]:
        return "/service-account/dashboard"
    if role in ["charter", "charters", "charter_provider"]:
        return "https://charters.boatspotmedia.com"
    return "/login"


def _session_display_name():
    return session.get("display_name") or session.get("creator_name") or session.get("user_email") or "Dashboard"



def _buyer_purchase_options_for_video(video):
    """
    Buyer preview options must use the creator's Pricing page.
    If preset lookup fails, fallback to prices already stored on video.
    """
    options = []
    try:
        from app.models import VideoPricingPreset

        creator_ids = []
        for attr in ("creator_id", "creator_profile_id"):
            val = getattr(video, attr, None)
            if val and val not in creator_ids:
                creator_ids.append(val)

        for rel in ("creator", "creator_profile"):
            obj = getattr(video, rel, None)
            cid = getattr(obj, "id", None)
            if cid and cid not in creator_ids:
                creator_ids.append(cid)

        presets = []
        for cid in creator_ids:
            try:
                found = VideoPricingPreset.query.filter_by(
                    creator_id=cid,
                    active=True
                ).order_by(
                    VideoPricingPreset.is_default.desc(),
                    VideoPricingPreset.id.asc()
                ).all()
                if found:
                    presets = found
                    break
            except Exception:
                pass

        for p in presets:
            try:
                price = float(p.price or 0)
            except Exception:
                price = 0.0
            if price <= 0:
                continue

            dtype = (getattr(p, "delivery_type", "") or "").lower().strip()
            title_text = (getattr(p, "title", "") or "").strip() or "Video option"
            desc_text = (getattr(p, "description", "") or "").strip()
            label_check = (dtype + " " + title_text).lower()

            if any(x in label_check for x in ["bundle", "combo", "original +", "original plus"]):
                package = "bundle"
            elif any(x in label_check for x in ["edit", "edited", "instagram", "reel", "short"]):
                package = "edited"
            else:
                package = "original"

            options.append({
                "id": getattr(p, "id", None),
                "package": package,
                "title": title_text,
                "description": desc_text,
                "price": price,
                "delivery_type": getattr(p, "delivery_type", "") or "",
            })

        if not options:
            for attr, title_text, package in (
                ("original_price", "Original 4K download", "original"),
                ("edited_price", "Edited video", "edited"),
                ("bundle_price", "Original + edited combo", "bundle"),
            ):
                try:
                    price = float(getattr(video, attr, 0) or 0)
                except Exception:
                    price = 0.0
                if price > 0:
                    options.append({
                        "id": None,
                        "package": package,
                        "title": title_text,
                        "description": "",
                        "price": price,
                        "delivery_type": package,
                    })
    except Exception as e:
        try:
            print("buyer purchase options warning:", e)
        except Exception:
            pass
    return options


@public_bp.app_context_processor
def inject_public_helpers():
    return {
        "ny_dt": _ny_dt,
        "session_dashboard_url": _session_dashboard_url,
        "session_display_name": _session_display_name
    }



def _ensure_video_tracking_columns_raw():
    """
    Create tracking columns safely. These columns are NOT part of the SQLAlchemy Video model,
    so uploads will not fail if the DB has not been migrated yet.
    """
    try:
        db.session.execute(db.text("ALTER TABLE video ADD COLUMN IF NOT EXISTS preview_views INTEGER DEFAULT 0"))
        db.session.execute(db.text("ALTER TABLE video ADD COLUMN IF NOT EXISTS preview_clicks INTEGER DEFAULT 0"))
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
    except Exception as e:
        db.session.rollback()
        try:
            print("tracking column ensure warning:", e)
        except Exception:
            pass


def _track_video_event_raw(video_id, event_name):
    try:
        _ensure_video_tracking_columns_raw()
        if event_name == "preview_view":
            db.session.execute(db.text("UPDATE video SET preview_views = COALESCE(preview_views, 0) + 1 WHERE id = :vid"), {"vid": video_id})
        elif event_name == "preview_click":
            db.session.execute(db.text("UPDATE video SET preview_clicks = COALESCE(preview_clicks, 0) + 1 WHERE id = :vid"), {"vid": video_id})
        else:
            return False
        db.session.commit()
        return True
    except Exception as e:
        db.session.rollback()
        try:
            print("video event tracking warning:", e)
        except Exception:
            pass
        return False




def _login_user_by_role_v420(role, dashboard_url, title="Login", subtitle=""):
    email = (request.form.get("email") or "").lower().strip()
    password = request.form.get("password") or ""
    if not email or not password:
        return render_template("public/generic_login.html", title=title, subtitle=subtitle, register_url=f"/{role}/register" if role != "buyer" else "/buyer/register", role=role, error="Email and password are required.")

    user = User.query.filter(db.func.lower(User.email) == email.lower()).first()
    if not user or user.role != role:
        return render_template("public/generic_login.html", title=title, subtitle=subtitle, register_url=f"/{role}/register" if role != "buyer" else "/buyer/register", role=role, error="Invalid email or password.")

    if not getattr(user, "is_active", True):
        return render_template("public/generic_login.html", title=title, subtitle=subtitle, register_url=f"/{role}/register" if role != "buyer" else "/buyer/register", role=role, error="Account is not active.")

    stored_hash = getattr(user, "password_hash", None) or ""
    ok = False
    try:
        ok = check_password_hash(stored_hash, password)
    except Exception:
        ok = False

    if not ok and stored_hash == password:
        ok = True

    if not ok:
        return render_template("public/generic_login.html", title=title, subtitle=subtitle, register_url=f"/{role}/register" if role != "buyer" else "/buyer/register", role=role, error="Invalid email or password.")

    session["user_id"] = user.id
    session["user_email"] = user.email
    session["user_role"] = user.role
    session["display_name"] = user.display_name or user.email
    session.modified = True
    return redirect(dashboard_url)



# Buyer routes registered on public_bp v42.2
# These are placed here because public_bp is already confirmed working (/ and /login return 200).

def _bsm_find_user_by_email_v422(email):
    if not email:
        return None
    try:
        return User.query.filter(db.func.lower(User.email) == email.lower().strip()).first()
    except Exception:
        db.session.rollback()
        return None


def _bsm_set_buyer_session_v422(user):
    session["user_id"] = user.id
    session["user_email"] = user.email
    session["user_role"] = user.role
    session["display_name"] = getattr(user, "display_name", None) or user.email
    session.modified = True


def _bsm_password_ok_v422(stored_hash, password):
    if not stored_hash:
        return False
    try:
        if check_password_hash(stored_hash, password):
            return True
    except Exception:
        pass
    try:
        return stored_hash == password
    except Exception:
        return False


def _bsm_buyer_orders_for_email_v422(email):
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


def _bsm_buyer_orders_for_user_v424(user_id, email):
    try:
        db.session.execute(db.text("ALTER TABLE bsm_cart_order ADD COLUMN IF NOT EXISTS buyer_user_id INTEGER"))
        db.session.commit()
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


def _bsm_buyer_order_items_v422(order_id):
    try:
        return db.session.execute(db.text("""
            SELECT i.*, v.location, v.filename, v.internal_filename, v.thumbnail_path, v.public_thumbnail_url, v.r2_thumbnail_key, v.file_path, v.r2_video_key, v.public_url, v.preview_url
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



def _bsm_claim_guest_orders_v428(user):
    """
    After buyer registration/login, attach paid guest orders with same email to this buyer user_id.
    This supports guest checkout and Apple Pay/Stripe email-based orders.
    """
    try:
        db.session.execute(db.text("ALTER TABLE bsm_cart_order ADD COLUMN IF NOT EXISTS buyer_user_id INTEGER"))
        db.session.execute(db.text("""
            UPDATE bsm_cart_order
            SET buyer_user_id = :uid
            WHERE (buyer_user_id IS NULL OR buyer_user_id = 0)
              AND buyer_email IS NOT NULL
              AND lower(buyer_email) = lower(:email)
        """), {"uid": user.id, "email": user.email})
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try:
            print("claim guest orders warning v42.8:", e)
        except Exception:
            pass



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




# v49.1F original/instant download URL helper.
# For instant/original purchases, buyer dashboard should point directly to the original R2/public file,
# not to a guessed internal route.
def _bsm_original_download_url_v491f(row):
    import os
    base = (
        os.environ.get("R2_PUBLIC_URL")
        or os.environ.get("R2_PUBLIC_BASE_URL")
        or os.environ.get("PUBLIC_R2_URL")
        or ""
    ).strip().rstrip("/")

    keys = [
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
    ]

    for key in keys:
        try:
            val = row.get(key)
        except Exception:
            val = None
        if not val:
            continue

        val = str(val).strip()
        if not val:
            continue

        if val.startswith("http://") or val.startswith("https://"):
            return val

        # Some old records may store local media paths.
        if val.startswith("/media/") or val.startswith("/static/"):
            return val

        # R2 object keys.
        if base:
            return base + "/" + val.lstrip("/")

    return None


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
        delivery["download_url"] = None if delivery["download_locked"] else (_bsm_original_download_url_v491f(delivery) or ("/download-video/" + str(delivery.get("item_id") or delivery.get("id") or delivery.get("video_id")) + "?delivery=original"))
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


@public_bp.route("/buyer/register", methods=["GET", "POST"])
def buyer_register_public_v422():
    if request.method == "POST":
        display_name = (request.form.get("display_name") or request.form.get("full_name") or request.form.get("name") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""

        if not email or not password:
            return render_template("public/generic_register.html", role="buyer", error="Email and password are required.")

        if len(password) < 6:
            return render_template("public/generic_register.html", role="buyer", error="Password must be at least 6 characters.")

        # buyer_terms_required_v444
        if role == "buyer" and request.form.get("accept_terms") not in ["on", "true", "1", "yes"]:
            return render_template("public/generic_register.html", role="buyer", error="You must accept the Buyer Terms and Privacy Policy to create an account.")

        user = _bsm_find_user_by_email_v422(email)
        if user:
            if getattr(user, "role", None) != "buyer":
                return render_template("public/generic_register.html", role="buyer", error="This email already exists under another account type. Please login or use another email.")
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
            _bsm_set_buyer_session_v422(user)
            _bsm_claim_guest_orders_v428(user)
            return redirect(session.pop("after_login_redirect", None) or request.args.get("next") or "/buyer/dashboard")
        except Exception as e:
            db.session.rollback()
            try:
                print("buyer register warning:", e)
            except Exception:
                pass
            return render_template("public/generic_register.html", role="buyer", error="Could not create account. Please try again.")

    return render_template("public/generic_register.html", role="buyer")


@public_bp.route("/buyer/login", methods=["GET", "POST"])
def buyer_login_public_v422():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""

        user = _bsm_find_user_by_email_v422(email)
        if not user or getattr(user, "role", None) != "buyer" or not _bsm_password_ok_v422(getattr(user, "password_hash", None), password):
            return render_template("public/generic_login.html", title="Buyer Login", subtitle="Access your orders and downloads.", register_url="/buyer/register", role="buyer", error="Invalid email or password.")

        if not getattr(user, "is_active", True):
            return render_template("public/generic_login.html", title="Buyer Login", subtitle="Access your orders and downloads.", register_url="/buyer/register", role="buyer", error="Account is not active.")

        _bsm_set_buyer_session_v422(user)
        _bsm_claim_guest_orders_v428(user)
        return redirect(session.pop("after_login_redirect", None) or request.args.get("next") or "/buyer/dashboard")

    return render_template("public/generic_login.html", title="Buyer Login", subtitle="Access your orders and downloads.", register_url="/buyer/register", role="buyer")


@public_bp.route("/buyer/dashboard")
def buyer_dashboard_public_v422():
    if not session.get("user_id") or session.get("user_role") != "buyer":
        return redirect("/buyer/login")

    email = session.get("user_email")
    orders = []
    for order in _bsm_buyer_orders_for_user_v424(session.get("user_id"), email):
        d = dict(order)
        items = []
        for x in _bsm_buyer_order_items_v422(order["id"]):
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
            ix["download_url"] = None if ix["download_locked"] or ix["download_expired"] else (_bsm_original_download_url_v491f(ix) or ("/download-video/" + str(ix.get("id") or ix.get("video_id"))))
            ix["thumbnail_url"] = _bsm_media_url_v427(ix, "thumb")
            ix["download_locked"] = _bsm_item_download_locked_v439(ix)
            timer = _bsm_download_timer_v441(ix, d.get("created_at"))
            ix["download_expired"] = timer["expired"]
            ix["download_expires_at"] = timer["expires_at"]
            ix["download_remaining_seconds"] = timer["remaining_seconds"]
            ix["download_url"] = None if ix["download_locked"] or ix["download_expired"] else (_bsm_original_download_url_v491f(ix) or ("/download-video/" + str(ix.get("id") or ix.get("video_id"))))
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


@public_bp.route("/payment/success-public-fallback")
def payment_success_public_fallback_v415():
    """
    Fallback route so Stripe success_url never returns Not Found.
    Full order persistence is handled by the payments route/webhook when available.
    """
    session_id = request.args.get("session_id")
    try:
        return render_template(
            "buyer/payment_success.html",
            download_url=None,
            download_urls=[],
            buyer_email=None,
            safe_message="Payment received. Your order is being processed. Please check your email for your download link. If you purchased an edited video, the creator will deliver it after editing."
        )
    except Exception:
        return """<!doctype html><html><head><title>Payment Received - BoatSpotMedia</title></head>
        <body style="font-family:Arial;padding:30px;">
        <p><a href="/"><img src="/static/img/logo-header.png" alt="BoatSpotMedia" style="height:60px;max-width:300px;object-fit:contain;"></a></p>
        <h1>Payment received</h1>
        <p>Your order is being processed. Please check your email for your download link.</p>
        <p><a href="/">Back to BoatSpotMedia</a></p>
        </body></html>"""


@public_bp.route("/track/video/<int:video_id>/<event_name>", methods=["POST"])
def track_video_event_v403(video_id, event_name):
    ok = _track_video_event_raw(video_id, event_name)
    return jsonify({"ok": bool(ok)})


@public_bp.route("/")
def home():
    try:
        latest = Video.query.filter_by(status="active").order_by(Video.created_at.desc()).limit(20).all()
    except Exception:
        db.session.rollback(); latest = []
    selected, used = [], set()
    for v in latest:
        cid = getattr(v, "creator_id", None)
        if cid not in used:
            selected.append(v); used.add(cid)
        if len(selected) == 3: break
    for v in latest:
        if len(selected) == 3: break
        if v not in selected: selected.append(v)
    return render_template("public/home.html", videos=selected)


@public_bp.route("/search")
def search_page():
    try: locations = _public_video_locations()
    except Exception: db.session.rollback(); locations = []
    return render_template("public/search.html", locations=locations, video_locations=locations, results=None)


@public_bp.route("/search/results")
def search_results():
    location = request.args.get("location"); date_s = request.args.get("date"); start_s = request.args.get("start_time"); end_s = request.args.get("end_time")
    results = []
    try:
        q = Video.query.filter_by(status="active")
        if location: q = q.filter(Video.location == location)
        if date_s and start_s and end_s:
            from zoneinfo import ZoneInfo
            d = datetime.strptime(date_s, "%Y-%m-%d").date()
            start_local = datetime.combine(d, datetime.strptime(start_s, "%H:%M").time()).replace(tzinfo=ZoneInfo("America/New_York"))
            end_local = datetime.combine(d, datetime.strptime(end_s, "%H:%M").time()).replace(tzinfo=ZoneInfo("America/New_York"))
            start_dt = start_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
            end_dt = end_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
            q = q.filter(Video.recorded_at >= start_dt, Video.recorded_at <= end_dt)
        results = q.order_by(Video.recorded_at.asc()).limit(200).all()
    except Exception:
        db.session.rollback()
    try: locations = _public_video_locations()
    except Exception: db.session.rollback(); locations = []
    return render_template("public/search.html", locations=locations, video_locations=locations, results=results)


@public_bp.route("/preview/<int:video_id>")
def preview_video(video_id):
    from app.models import CreatorClickStats
    v = Video.query.get_or_404(video_id)
    stats = CreatorClickStats.query.filter_by(creator_id=v.creator_id).first()
    if not stats:
        stats = CreatorClickStats(creator_id=v.creator_id); db.session.add(stats)
    stats.clicks_today += 1; stats.clicks_week += 1; stats.clicks_month += 1; stats.clicks_lifetime += 1
    db.session.commit()
    _track_video_event_raw(v.id, "preview_view")
    return render_template("public/preview.html", video=v, purchase_options=_buyer_purchase_options_for_video(v))




@public_bp.route("/apply-creator", methods=["GET","POST"], endpoint="apply_creator_v488")
@public_bp.route("/creator/apply", methods=["GET","POST"])
@public_bp.route("/apply", methods=["GET","POST"])
def apply_creator_v488():
    if request.method == "POST":
        first_name = request.form.get("first_name") or ""
        last_name = request.form.get("last_name") or ""
        brand_name = request.form.get("brand_name") or request.form.get("company_name") or ""
        email = request.form.get("email") or ""
        phone = request.form.get("phone") or ""
        instagram = request.form.get("instagram") or ""
        try:
            db.session.execute(db.text("ALTER TABLE creator_application ADD COLUMN IF NOT EXISTS phone TEXT"))
            db.session.execute(db.text("""
                INSERT INTO creator_application
                (first_name, last_name, brand_name, email, phone, instagram, status, submitted_at)
                VALUES (:first_name, :last_name, :brand_name, :email, :phone, :instagram, 'pending', CURRENT_TIMESTAMP)
            """), {
                "first_name": first_name,
                "last_name": last_name,
                "brand_name": brand_name,
                "email": email,
                "phone": phone,
                "instagram": instagram,
            })
            db.session.commit()
            return render_template("public/apply_creator.html", submitted=True)
        except Exception:
            db.session.rollback()
            flash("Could not submit application. Please try again.")
    return render_template("public/apply_creator.html", submitted=False)




# v49.1 compatibility: preserve old creator login Google button endpoint.
@public_bp.route("/auth/google/register/<account_type>", endpoint="auth_google_register")
@public_bp.route("/auth/google/<account_type>")
def auth_google_register(account_type="buyer"):
    """
    Compatibility route for old templates using:
    url_for('public.auth_google_register', account_type='creator')
    It redirects to the existing Google/OAuth route if present, otherwise to the correct login.
    """
    account_type = account_type or request.args.get("account_type") or "buyer"
    # Try common existing OAuth routes without assuming one exact name.
    for endpoint in [
        "public.google_login",
        "public.auth_google",
        "public.google_auth",
        "public.oauth_google",
        "public.login_google",
        "public.google_register",
    ]:
        try:
            return redirect(url_for(endpoint, account_type=account_type))
        except Exception:
            pass
    # Fallback keeps the page working instead of crashing.
    if account_type == "creator":
        return redirect("/creator/login")
    return redirect("/buyer/login")
