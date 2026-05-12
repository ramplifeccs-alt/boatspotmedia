import os
import secrets
import stripe
from datetime import datetime, timedelta
from flask import Blueprint, redirect, render_template, request, url_for, session, jsonify, flash
from sqlalchemy import text
from werkzeug.security import generate_password_hash, check_password_hash
from app.models import Video, Location, ServiceAd, CharterListing, User
from app.services.db import db

public_bp = Blueprint("public", __name__)


# BoatSpotMedia public/legal/time helpers v50.5AM
BSM_LOCAL_TZ = "America/New_York"


def _bsm_local_tz_v505am():
    from zoneinfo import ZoneInfo
    return ZoneInfo(BSM_LOCAL_TZ)


def _bsm_dt_for_input_v505am(value):
    """DB timestamps are stored naive UTC; owner forms show Miami/New York local time."""
    if not value:
        return ""
    try:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        from zoneinfo import ZoneInfo
        if getattr(value, "tzinfo", None) is None:
            value = value.replace(tzinfo=ZoneInfo("UTC"))
        return value.astimezone(_bsm_local_tz_v505am()).strftime("%Y-%m-%dT%H:%M")
    except Exception:
        try:
            return value.strftime("%Y-%m-%dT%H:%M")
        except Exception:
            return ""


def _bsm_dt_display_v505am(value, date_only=False):
    if not value:
        return ""
    try:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        from zoneinfo import ZoneInfo
        if getattr(value, "tzinfo", None) is None:
            value = value.replace(tzinfo=ZoneInfo("UTC"))
        fmt = "%m/%d/%Y" if date_only else "%m/%d/%Y %I:%M %p ET"
        return value.astimezone(_bsm_local_tz_v505am()).strftime(fmt)
    except Exception:
        try:
            return value.strftime("%m/%d/%Y") if date_only else value.strftime("%m/%d/%Y %I:%M %p ET")
        except Exception:
            return ""


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
        "session_display_name": _session_display_name,
        "bsm_dt_for_input": _bsm_dt_for_input_v505am,
        "bsm_dt_display": _bsm_dt_display_v505am
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



# v50.5A buyer phone/SMS helpers for public buyer register
def _bsm_normalize_phone_public_v505a(phone):
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

def _bsm_ensure_buyer_sms_columns_public_v505a():
    try:
        db.session.execute(db.text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS phone_number TEXT'))
        db.session.execute(db.text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS sms_notifications_enabled BOOLEAN DEFAULT TRUE'))
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try: print("public buyer sms columns v50.5A warning:", e)
        except Exception: pass


@public_bp.route("/buyer/register", methods=["GET", "POST"])
def buyer_register_public_v422():
    if request.method == "POST":
        display_name = (request.form.get("display_name") or request.form.get("full_name") or request.form.get("name") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""
        phone_number = _bsm_normalize_phone_public_v505a(request.form.get("phone_number") or request.form.get("phone") or "")
        sms_notifications_enabled = bool(request.form.get("sms_notifications_enabled"))

        if not email or not password:
            return render_template("public/generic_register.html", role="buyer", error="Email and password are required.")

        if len(password) < 6:
            return render_template("public/generic_register.html", role="buyer", error="Password must be at least 6 characters.")

        # buyer_terms_required_v444
        if request.form.get("accept_terms") not in ["on", "true", "1", "yes"]:
            return render_template("public/generic_register.html", role="buyer", error="You must accept the Buyer Terms and Privacy Policy to create an account.")

        _bsm_ensure_buyer_sms_columns_public_v505a()
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
            try:
                db.session.execute(db.text("""
                    UPDATE "user"
                    SET phone_number=:phone_number,
                        sms_notifications_enabled=:sms_enabled
                    WHERE lower(email)=lower(:email)
                """), {
                    "phone_number": phone_number,
                    "sms_enabled": sms_notifications_enabled,
                    "email": email,
                })
                db.session.commit()
            except Exception as sms_e:
                db.session.rollback()
                try: print("public buyer phone save v50.5A warning:", sms_e)
                except Exception: pass
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



def _bsm_sendgrid_send_v505ad(to_email, subject, html_body, text_body=None):
    try:
        import os, requests
        api_key = os.environ.get("SENDGRID_API_KEY")
        from_email = (
            os.environ.get("SENDGRID_FROM_EMAIL")
            or os.environ.get("MAIL_FROM")
            or os.environ.get("FROM_EMAIL")
            or "noreply@boatspotmedia.com"
        )
        from_name = os.environ.get("SENDGRID_FROM_NAME") or "BoatSpotMedia"
        if not api_key:
            try:
                print("buyer forgot password v50.5AD: SENDGRID_API_KEY missing")
            except Exception:
                pass
            return False
        payload = {
            "personalizations": [{"to": [{"email": to_email}]}],
            "from": {"email": from_email, "name": from_name},
            "subject": subject,
            "content": [
                {"type": "text/plain", "value": text_body or "Reset your BoatSpotMedia password."},
                {"type": "text/html", "value": html_body},
            ],
        }
        r = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=12,
        )
        if r.status_code not in (200, 202):
            try:
                print("buyer forgot password SendGrid v50.5AD failed:", r.status_code, r.text[:500])
            except Exception:
                pass
            return False
        return True
    except Exception as e:
        try:
            print("buyer forgot password SendGrid v50.5AD exception:", e)
        except Exception:
            pass
        return False

def _bsm_public_base_url_v505ad():
    try:
        import os
        base = (os.environ.get("PUBLIC_BASE_URL") or os.environ.get("APP_BASE_URL") or os.environ.get("BASE_URL") or "").strip()
        if base:
            return base.rstrip("/")
        return request.url_root.rstrip("/")
    except Exception:
        return "https://boatspotmedia.com"

def _bsm_ensure_password_reset_table_v505ad():
    try:
        db.session.execute(db.text("""
            CREATE TABLE IF NOT EXISTS password_reset_token (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                email TEXT NOT NULL,
                token TEXT NOT NULL UNIQUE,
                role TEXT DEFAULT 'buyer',
                expires_at TIMESTAMP NOT NULL,
                used_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.execute(db.text("CREATE INDEX IF NOT EXISTS idx_password_reset_token_token ON password_reset_token(token)"))
        db.session.execute(db.text("CREATE INDEX IF NOT EXISTS idx_password_reset_token_email ON password_reset_token(email)"))
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try:
            print("buyer forgot password table v50.5AD warning:", e)
        except Exception:
            pass

@public_bp.route("/buyer/forgot-password", methods=["GET", "POST"])
@public_bp.route("/buyer/forgot_password", methods=["GET", "POST"])
def buyer_forgot_password_v505ad():
    _bsm_ensure_password_reset_table_v505ad()
    sent = False
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        if email:
            try:
                user = db.session.execute(db.text("""
                    SELECT id, email
                    FROM "user"
                    WHERE lower(COALESCE(email,'')) = lower(:email)
                      AND COALESCE(role,'') = 'buyer'
                    LIMIT 1
                """), {"email": email}).mappings().first()
                if user:
                    token = secrets.token_urlsafe(40)
                    expires_at = datetime.utcnow() + timedelta(hours=2)
                    db.session.execute(db.text("""
                        INSERT INTO password_reset_token (user_id, email, token, role, expires_at)
                        VALUES (:user_id, :email, :token, 'buyer', :expires_at)
                    """), {
                        "user_id": user.get("id"),
                        "email": email,
                        "token": token,
                        "expires_at": expires_at,
                    })
                    db.session.commit()

                    link = f"{_bsm_public_base_url_v505ad()}/buyer/reset-password/{token}"
                    html = f"""
                    <div style="font-family:Arial,sans-serif;color:#0f172a">
                      <h2>Reset your BoatSpotMedia password</h2>
                      <p>Click the button below to reset your buyer account password. This link expires in 2 hours.</p>
                      <p><a href="{link}" style="display:inline-block;background:#2563eb;color:#fff;padding:12px 18px;border-radius:10px;text-decoration:none;font-weight:bold">Reset Password</a></p>
                      <p>If the button does not work, copy and paste this link:</p>
                      <p>{link}</p>
                    </div>
                    """
                    text = f"Reset your BoatSpotMedia password: {link}\nThis link expires in 2 hours."
                    _bsm_sendgrid_send_v505ad(email, "Reset your BoatSpotMedia password", html, text)
                else:
                    # Keep same UX for privacy.
                    db.session.rollback()
            except Exception as e:
                db.session.rollback()
                try:
                    print("buyer forgot password v50.5AD warning:", e)
                except Exception:
                    pass
        sent = True
    return render_template("buyer/forgot_password.html", sent=sent)

@public_bp.route("/buyer/reset-password/<token>", methods=["GET", "POST"])
@public_bp.route("/buyer/reset_password/<token>", methods=["GET", "POST"])
def buyer_reset_password_v505ad(token):
    _bsm_ensure_password_reset_table_v505ad()
    invalid = False
    success = False
    error = None
    token = (token or "").strip()

    try:
        row = db.session.execute(db.text("""
            SELECT prt.id, prt.user_id, prt.email, prt.expires_at, prt.used_at
            FROM password_reset_token prt
            JOIN "user" u ON u.id = prt.user_id
            WHERE prt.token=:token
              AND COALESCE(prt.role,'buyer')='buyer'
              AND COALESCE(u.role,'')='buyer'
            LIMIT 1
        """), {"token": token}).mappings().first()
    except Exception:
        db.session.rollback()
        row = None

    if not row:
        invalid = True
        return render_template("buyer/reset_password.html", invalid=invalid, success=success, error=error)

    try:
        exp = row.get("expires_at")
        used = row.get("used_at")
        expired = False
        if used:
            expired = True
        elif hasattr(exp, "replace"):
            expired = exp < datetime.utcnow()
        else:
            expired = str(exp) < datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        if expired:
            invalid = True
            return render_template("buyer/reset_password.html", invalid=invalid, success=success, error=error)
    except Exception:
        invalid = True
        return render_template("buyer/reset_password.html", invalid=invalid, success=success, error=error)

    if request.method == "POST":
        password = request.form.get("password") or ""
        confirm = request.form.get("confirm_password") or ""
        if len(password) < 8:
            error = "Password must be at least 8 characters."
        elif password != confirm:
            error = "Passwords do not match."
        else:
            hashed = generate_password_hash(password)
            try:
                # IMPORTANT v50.5AO FIX:
                # The buyer login checks the password_hash column. The previous reset flow
                # updated password_hash first, then tried to update an optional legacy
                # "password" column. On databases where that legacy column does not exist,
                # SQLAlchemy rolled back the full transaction, so the page showed success
                # while the old password stayed active. Keep this flow focused on the real
                # login column only, then mark the token as used in the same transaction.
                result = db.session.execute(
                    db.text("UPDATE \"user\" SET password_hash=:ph WHERE id=:uid AND role='buyer'"),
                    {"ph": hashed, "uid": row.get("user_id")}
                )
                if getattr(result, "rowcount", 0) != 1:
                    raise Exception("Buyer password was not updated")
                db.session.execute(db.text("""
                    UPDATE password_reset_token
                    SET used_at=CURRENT_TIMESTAMP
                    WHERE id=:id
                """), {"id": row.get("id")})
                db.session.commit()
                success = True
            except Exception as e:
                db.session.rollback()
                try:
                    print("buyer reset password v50.5AD warning:", e)
                except Exception:
                    pass
                error = "Could not reset password. Please request a new link."

    return render_template("buyer/reset_password.html", invalid=invalid, success=success, error=error)


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



def _bsm_ensure_home_ad_campaign_table_public_v505ak():
    try:
        db.session.execute(db.text("""
            CREATE TABLE IF NOT EXISTS homepage_ad_campaign (
                id SERIAL PRIMARY KEY,
                token TEXT UNIQUE NOT NULL,
                advertiser_name TEXT,
                advertiser_email TEXT,
                title TEXT,
                image_url TEXT,
                target_url TEXT,
                price_amount NUMERIC(10,2) DEFAULT 0,
                currency TEXT DEFAULT 'usd',
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                is_active BOOLEAN DEFAULT FALSE,
                payment_status TEXT DEFAULT 'draft',
                stripe_session_id TEXT,
                stripe_payment_intent_id TEXT,
                open_new_tab BOOLEAN DEFAULT TRUE,
                display_order INTEGER DEFAULT 0,
                notes TEXT,
                terms_accepted_at TIMESTAMP,
                paid_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.commit()
    except Exception:
        db.session.rollback()

def _bsm_public_base_url_home_ads_v505ak():
    try:
        base = os.environ.get("PUBLIC_BASE_URL") or os.environ.get("APP_BASE_URL") or os.environ.get("BASE_URL")
        if base:
            return base.rstrip("/")
        return request.host_url.rstrip("/")
    except Exception:
        return "https://boatspotmedia.com"

@public_bp.route("/sponsored/<token>")
@public_bp.route("/ad-campaign/<token>")
def public_home_ad_campaign_preview_v505ak(token):
    _bsm_ensure_home_ad_campaign_table_public_v505ak()
    try:
        campaign = db.session.execute(db.text("""
            SELECT *
            FROM homepage_ad_campaign
            WHERE token=:token
            LIMIT 1
        """), {"token": token}).mappings().first()
    except Exception:
        db.session.rollback()
        campaign = None
    if not campaign:
        return render_template("public/home_ad_not_found.html"), 404
    return render_template("public/home_ad_checkout.html", campaign=campaign)

@public_bp.route("/sponsored/<token>/checkout", methods=["POST"])
@public_bp.route("/ad-campaign/<token>/checkout", methods=["POST"])
def public_home_ad_campaign_checkout_v505ak(token):
    _bsm_ensure_home_ad_campaign_table_public_v505ak()
    try:
        campaign = db.session.execute(db.text("""
            SELECT *
            FROM homepage_ad_campaign
            WHERE token=:token
            LIMIT 1
        """), {"token": token}).mappings().first()
    except Exception:
        db.session.rollback()
        campaign = None

    if not campaign:
        return render_template("public/home_ad_not_found.html"), 404

    if request.form.get("accept_terms") not in ("on", "true", "1", "yes"):
        flash("You must accept the advertising terms before payment.")
        return redirect(f"/sponsored/{token}")

    amount_cents = int(round(float(campaign.get("price_amount") or 0) * 100))
    if amount_cents < 50:
        flash("This campaign does not have a valid payment amount.")
        return redirect(f"/sponsored/{token}")

    stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")
    if not stripe.api_key:
        return "Stripe is not configured.", 500

    base = _bsm_public_base_url_home_ads_v505ak()
    try:
        session = stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card"],
            automatic_tax={"enabled": True},
            billing_address_collection="required",
            customer_email=campaign.get("advertiser_email") or None,
            line_items=[{
                "price_data": {
                    "currency": campaign.get("currency") or "usd",
                    "product_data": {
                        "name": "BoatSpotMedia Homepage Advertising Campaign",
                        "description": (campaign.get("title") or "Homepage Banner")[:500],
                    },
                    "unit_amount": amount_cents,
                },
                "quantity": 1,
            }],
            metadata={
                "homepage_ad_campaign": "1",
                "campaign_id": str(campaign.get("id")),
                "campaign_token": str(campaign.get("token")),
                "advertiser_email": str(campaign.get("advertiser_email") or ""),
                "campaign_title": str(campaign.get("title") or ""),
            },
            success_url=base + f"/sponsored/{token}?paid=1&session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=base + f"/sponsored/{token}?canceled=1",
        )

        db.session.execute(db.text("""
            UPDATE homepage_ad_campaign
            SET stripe_session_id=:sid,
                terms_accepted_at=CURRENT_TIMESTAMP,
                payment_status=CASE WHEN payment_status='paid' THEN payment_status ELSE 'checkout_created' END,
                updated_at=CURRENT_TIMESTAMP
            WHERE token=:token
        """), {"sid": session.id, "token": token})
        db.session.commit()

        return redirect(session.url, code=303)
    except Exception as e:
        db.session.rollback()
        try:
            print("homepage ad Stripe checkout v50.5AK warning:", e)
        except Exception:
            pass
        flash("Could not create payment link. Please contact BoatSpotMedia.")
        return redirect(f"/sponsored/{token}")



@public_bp.route("/terms")
def public_terms_v505am():
    return render_template("public/legal.html", page_key="terms")

@public_bp.route("/privacy")
def public_privacy_v505am():
    return render_template("public/legal.html", page_key="privacy")

@public_bp.route("/refund-policy")
def public_refund_policy_v505am():
    return render_template("public/legal.html", page_key="refund")

@public_bp.route("/copyright-dmca")
@public_bp.route("/dmca")
def public_copyright_dmca_v505am():
    return render_template("public/legal.html", page_key="dmca")

@public_bp.route("/advertising-terms")
def public_advertising_terms_v505am():
    return render_template("public/legal.html", page_key="advertising")

@public_bp.route("/buyer-terms")
def public_buyer_terms_v505am():
    return render_template("public/legal.html", page_key="buyer")

@public_bp.route("/contact-support")
@public_bp.route("/support")
def public_contact_support_v505am():
    return render_template("public/legal.html", page_key="support")


@public_bp.route("/language/<lang>")
def public_set_language_v505al(lang):
    lang = (lang or "en").lower()
    if lang not in ("en", "es"):
        lang = "en"
    session["site_lang"] = lang
    return redirect(request.referrer or "/")

def _bsm_t_v505al(key):
    lang = (session.get("site_lang") or request.args.get("lang") or "en").lower()
    translations = {
        "en": {
            "find_title": "Find your boat video",
            "find_subtitle": "Search by date, location, creator, or boat details.",
            "search_placeholder": "Search your video...",
            "search_button": "Find Videos",
            "latest_uploads": "Latest Uploads",
            "home": "Home",
            "find_your_video": "Find Your Video",
            "login": "Login",
            "sponsored": "Sponsored",
            "hero_title": "Find your boat video",
            "hero_subtitle": "Search by inlet, date, and approximate time. Buy the original video or request an edited version.",
            "preview": "Preview",
            "time_pending": "Time pending",
            "captured_by": "Captured by",
            "creator": "Creator",
            "no_videos": "No videos uploaded yet.",
            "marketplace": "Marketplace",
            "company": "Company",
            "legal": "Legal",
            "browse_videos": "Browse Videos",
            "sell_your_videos": "Sell Your Videos",
            "contact_support": "Contact / Support",
            "terms_conditions": "Terms & Conditions",
            "privacy_policy": "Privacy Policy",
            "refund_policy": "Refund Policy",
            "copyright_dmca": "Copyright / DMCA",
            "advertising_terms": "Advertising Terms",
            "buyer_terms": "Buyer Terms",
            "footer_note": "BoatSpotMedia is an independent marketplace platform for boat video creators and buyers.",
        },
        "es": {
            "find_title": "Encuentra el video de tu bote",
            "find_subtitle": "Busca por fecha, lugar, creador o detalles del bote.",
            "search_placeholder": "Busca tu video...",
            "search_button": "Buscar Videos",
            "latest_uploads": "Videos Recientes",
            "home": "Inicio",
            "find_your_video": "Buscar Video",
            "login": "Ingresar",
            "sponsored": "Publicidad",
            "hero_title": "Encuentra el video de tu bote",
            "hero_subtitle": "Busca por inlet, fecha y hora aproximada. Compra el video original o solicita una versión editada.",
            "preview": "Vista previa",
            "time_pending": "Hora pendiente",
            "captured_by": "Grabado por",
            "creator": "Creador",
            "no_videos": "Todavía no hay videos subidos.",
            "marketplace": "Marketplace",
            "company": "Compañía",
            "legal": "Legal",
            "browse_videos": "Buscar videos",
            "sell_your_videos": "Vender tus videos",
            "contact_support": "Contacto / Soporte",
            "terms_conditions": "Términos y Condiciones",
            "privacy_policy": "Política de Privacidad",
            "refund_policy": "Política de Reembolsos",
            "copyright_dmca": "Copyright / DMCA",
            "advertising_terms": "Términos de Publicidad",
            "buyer_terms": "Términos del Comprador",
            "footer_note": "BoatSpotMedia es una plataforma marketplace independiente para creadores y compradores de videos de botes.",
        }
    }
    return translations.get(lang, translations["en"]).get(key, key)

@public_bp.context_processor
def inject_public_language_v505al():
    return {
        "site_lang": (session.get("site_lang") or "en"),
        "t": _bsm_t_v505al
    }


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





def _bsm_normalize_us_phone_v505t(phone):
    phone = (phone or "").strip()
    if not phone:
        return ""
    cleaned = "".join(ch for ch in phone if ch.isdigit() or ch == "+")
    if cleaned.startswith("+"):
        digits = "".join(ch for ch in cleaned if ch.isdigit())
        if len(digits) == 11 and digits.startswith("1"):
            return "+" + digits
        if len(digits) == 10:
            return "+1" + digits
        return "+" + digits if digits else ""
    digits = "".join(ch for ch in cleaned if ch.isdigit())
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    return "+" + digits if digits else ""


@public_bp.route("/apply-creator", methods=["GET","POST"], endpoint="apply_creator_v488")
@public_bp.route("/creator/apply", methods=["GET","POST"])
@public_bp.route("/apply", methods=["GET","POST"])
def apply_creator_v488():
    if request.method == "POST":
        first_name = request.form.get("first_name") or ""
        last_name = request.form.get("last_name") or ""
        brand_name = request.form.get("brand_name") or request.form.get("company_name") or ""
        email = request.form.get("email") or ""
        phone = _bsm_normalize_us_phone_v505t(request.form.get("phone") or "")
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






# v50.5B Google OAuth buyer login/register
def _bsm_base_url_v505b():
    return (os.environ.get("PUBLIC_BASE_URL") or os.environ.get("BASE_URL") or "https://boatspotmedia.com").rstrip("/")

def _bsm_google_redirect_uri_v505b():
    return _bsm_base_url_v505b() + "/auth/callback/google"

def _bsm_google_create_or_login_buyer_v505b(email, name="", google_id=""):
    email = (email or "").strip().lower()
    if not email:
        return None

    try:
        db.session.execute(db.text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS google_id TEXT'))
        db.session.execute(db.text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS phone_number TEXT'))
        db.session.execute(db.text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS sms_notifications_enabled BOOLEAN DEFAULT TRUE'))
        db.session.commit()
    except Exception:
        db.session.rollback()

    user = _bsm_find_user_by_email_v422(email)
    if user:
        if getattr(user, "role", None) != "buyer":
            return None
        try:
            db.session.execute(db.text("""
                UPDATE "user"
                SET google_id=COALESCE(:google_id, google_id),
                    is_active=TRUE
                WHERE id=:uid
            """), {"google_id": google_id or None, "uid": user.id})
            db.session.commit()
        except Exception:
            db.session.rollback()
        return user

    try:
        user = User(
            email=email,
            password_hash=generate_password_hash(secrets.token_urlsafe(24)),
            display_name=name or email,
            role="buyer",
            is_active=True,
        )
        db.session.add(user)
        db.session.commit()

        try:
            db.session.execute(db.text("""
                UPDATE "user"
                SET google_id=:google_id,
                    sms_notifications_enabled=TRUE
                WHERE id=:uid
            """), {"google_id": google_id or None, "uid": user.id})
            db.session.commit()
        except Exception:
            db.session.rollback()

        return user
    except Exception as e:
        db.session.rollback()
        try:
            print("google buyer create v50.5B warning:", e)
        except Exception:
            pass
        return None


@public_bp.route("/auth/google")
@public_bp.route("/auth/google/buyer")
@public_bp.route("/login/google")
@public_bp.route("/buyer/google")
def google_buyer_login_v505b():
    client_id = os.environ.get("GOOGLE_CLIENT_ID") or os.environ.get("GOOGLE_OAUTH_CLIENT_ID")
    if not client_id:
        flash("Google login is not configured. Missing GOOGLE_CLIENT_ID.")
        return redirect("/buyer/login")

    state = secrets.token_urlsafe(24)
    session["google_oauth_state"] = state
    session["google_oauth_role"] = "buyer"
    session.modified = True

    params = {
        "client_id": client_id,
        "redirect_uri": _bsm_google_redirect_uri_v505b(),
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "online",
        "prompt": "select_account",
    }

    import urllib.parse
    return redirect("https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params))


@public_bp.route("/auth/callback/google")
@public_bp.route("/auth/google/callback")
def google_oauth_callback_v505b():
    if request.args.get("error"):
        flash("Google login was cancelled or rejected.")
        return redirect("/buyer/login")

    state = request.args.get("state") or ""
    code = request.args.get("code") or ""

    if not code:
        return "Google callback route is working. Start login from /auth/google.", 200

    if not state or state != session.get("google_oauth_state"):
        flash("Invalid Google login session. Please try again.")
        return redirect("/buyer/login")

    client_id = os.environ.get("GOOGLE_CLIENT_ID") or os.environ.get("GOOGLE_OAUTH_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET") or os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET")
    if not client_id or not client_secret:
        flash("Google login is not configured. Missing Google client secret.")
        return redirect("/buyer/login")

    try:
        import requests

        token_res = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": _bsm_google_redirect_uri_v505b(),
                "grant_type": "authorization_code",
            },
            timeout=12,
        )

        if token_res.status_code != 200:
            try:
                print("google token error v50.5B:", token_res.status_code, token_res.text[:500])
            except Exception:
                pass
            flash("Could not verify Google login.")
            return redirect("/buyer/login")

        token_data = token_res.json()
        access_token = token_data.get("access_token")

        if not access_token:
            flash("Could not get Google access token.")
            return redirect("/buyer/login")

        userinfo_res = requests.get(
            "https://www.googleapis.com/oauth2/v3/userinfo",
            headers={"Authorization": "Bearer " + access_token},
            timeout=12,
        )

        if userinfo_res.status_code != 200:
            try:
                print("google userinfo error v50.5B:", userinfo_res.status_code, userinfo_res.text[:500])
            except Exception:
                pass
            flash("Could not get Google profile.")
            return redirect("/buyer/login")

        profile = userinfo_res.json()
        email = (profile.get("email") or "").strip().lower()

        if not email or not profile.get("email_verified", True):
            flash("Google account email is not verified.")
            return redirect("/buyer/login")

        user = _bsm_google_create_or_login_buyer_v505b(
            email=email,
            name=profile.get("name") or email,
            google_id=profile.get("sub") or "",
        )

        if not user:
            flash("Could not create or access buyer account with this Google email.")
            return redirect("/buyer/login")

        _bsm_set_buyer_session_v422(user)
        _bsm_claim_guest_orders_v428(user)

        session.pop("google_oauth_state", None)
        session.pop("google_oauth_role", None)
        session.modified = True

        return redirect(session.pop("after_login_redirect", None) or "/buyer/dashboard")

    except Exception as e:
        try:
            print("google oauth callback v50.5B warning:", e)
        except Exception:
            pass
        flash("Google login failed. Please try again.")
        return redirect("/buyer/login")


# v49.1K safe download fallback using R2_PUBLIC_URL + video.file_path.
@public_bp.route("/download-video/<int:item_id>")
def public_download_video_v491i(item_id):
    delivery = (request.args.get("delivery") or "original").lower().strip()

    # Correct public R2 base variable. Keep fallbacks for Railway env naming.
    r2_public_url = (
        os.environ.get("R2_PUBLIC_URL")
        or os.environ.get("R2_PUBLIC_BASE_URL")
        or os.environ.get("PUBLIC_R2_URL")
        or "https://pub-ac294ba2f7794c37848062239f41227d.r2.dev"
    ).strip().rstrip("/")

    def make_url(key):
        if not key:
            return None
        key = str(key).strip()
        if not key:
            return None
        if key.startswith("http://") or key.startswith("https://"):
            return key
        if key.startswith("/media/") or key.startswith("/static/"):
            return key
        return r2_public_url + "/" + key.lstrip("/")

    try:
        # First: item_id is bsm_cart_order_item.id.
        row = db.session.execute(db.text("""
            SELECT i.id AS item_id,
                   i.video_id,
                   i.edited_r2_key,
                   v.file_path,
                   v.r2_video_key,
                   v.internal_filename
            FROM bsm_cart_order_item i
            LEFT JOIN video v ON v.id = i.video_id
            WHERE i.id=:item_id
            LIMIT 1
        """), {"item_id": item_id}).mappings().first()

        # Fallback: item_id may be video.id in older buttons.
        if not row:
            row = db.session.execute(db.text("""
                SELECT NULL AS item_id,
                       v.id AS video_id,
                       NULL AS edited_r2_key,
                       v.file_path,
                       v.r2_video_key,
                       v.internal_filename
                FROM video v
                WHERE v.id=:item_id
                LIMIT 1
            """), {"item_id": item_id}).mappings().first()

        if not row:
            return "Video not found", 404

        if delivery in ["edited", "edit"]:
            url = make_url(row.get("edited_r2_key"))
            if url:
                return redirect(url)

        # Original / instant: use file_path first. This stores:
        # creators/<creator_id>/batches/<batch_id>/<filename>.MP4
        for key in ["file_path", "r2_video_key", "internal_filename"]:
            url = make_url(row.get(key))
            if url:
                return redirect(url)

        return "Video file not found", 404
    except Exception as e:
        db.session.rollback()
        try:
            print("download-video v49.1K warning:", e)
        except Exception:
            pass
        return "Download error", 500




# v50.2 tracking: views/clicks/conversion events
def _bsm_ensure_analytics_events_v502():
    try:
        db.session.execute(db.text("""
            CREATE TABLE IF NOT EXISTS analytics_event (
                id SERIAL PRIMARY KEY,
                event_type TEXT NOT NULL,
                video_id INTEGER,
                creator_id INTEGER,
                buyer_user_id INTEGER,
                session_id TEXT,
                path TEXT,
                user_agent TEXT,
                ip_hash TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.execute(db.text("CREATE INDEX IF NOT EXISTS idx_analytics_event_video_type ON analytics_event(video_id, event_type)"))
        db.session.execute(db.text("CREATE INDEX IF NOT EXISTS idx_analytics_event_creator_type ON analytics_event(creator_id, event_type)"))
        db.session.commit()
    except Exception:
        db.session.rollback()

def _bsm_tracking_session_id_v502():
    try:
        sid=session.get("bsm_tracking_sid")
        if not sid:
            import uuid
            sid=uuid.uuid4().hex
            session["bsm_tracking_sid"]=sid
        return sid
    except Exception:
        return ""

@public_bp.route("/track", methods=["POST"])
def public_track_event_v502():
    _bsm_ensure_analytics_events_v502()
    data=request.get_json(silent=True) or request.form or {}
    event_type=(data.get("event_type") or data.get("type") or "").strip().lower()
    if event_type not in ["view","click","purchase_click"]:
        return jsonify({"ok":False,"error":"invalid event"}),400
    try:
        video_id=int(data.get("video_id") or 0) or None
    except Exception:
        video_id=None
    creator_id=None
    if video_id:
        try:
            row=db.session.execute(db.text("SELECT creator_id FROM video WHERE id=:id LIMIT 1"),{"id":video_id}).mappings().first()
            creator_id=row.get("creator_id") if row else None
        except Exception:
            db.session.rollback()
    try:
        buyer_user_id=session.get("buyer_user_id") or session.get("user_id")
    except Exception:
        buyer_user_id=None
    try:
        import hashlib
        ip=(request.headers.get("CF-Connecting-IP") or request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip()
        ip_hash=hashlib.sha256(ip.encode("utf-8")).hexdigest()[:24] if ip else ""
    except Exception:
        ip_hash=""
    try:
        db.session.execute(db.text("""
            INSERT INTO analytics_event
            (event_type, video_id, creator_id, buyer_user_id, session_id, path, user_agent, ip_hash)
            VALUES (:event_type,:video_id,:creator_id,:buyer_user_id,:session_id,:path,:user_agent,:ip_hash)
        """),{
            "event_type":event_type,
            "video_id":video_id,
            "creator_id":creator_id,
            "buyer_user_id":buyer_user_id,
            "session_id":_bsm_tracking_session_id_v502(),
            "path":data.get("path") or request.referrer or "",
            "user_agent":(request.headers.get("User-Agent") or "")[:250],
            "ip_hash":ip_hash
        })
        db.session.commit()
        return jsonify({"ok":True})
    except Exception as e:
        db.session.rollback()
        try: print("track event v50.2 warning:", e)
        except Exception: pass
        return jsonify({"ok":False}),500

