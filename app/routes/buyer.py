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
            SELECT i.*,
                   v.location,
                   v.filename,
                   v.internal_filename,
                   v.thumbnail_path,
                   v.public_thumbnail_url,
                   v.r2_thumbnail_key,
                   v.file_path,
                   v.r2_video_key,
                   v.public_url,
                   v.preview_url
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
            ix["download_url"] = "/buyer/download-item/" + str(ix.get("id")) if _bsm_item_is_downloadable_v431(ix) else None
            ix["thumbnail_url"] = _bsm_media_url_v427(ix, "thumb")
            ix["download_url"] = "/buyer/download-item/" + str(ix.get("id")) if _bsm_item_is_downloadable_v431(ix) else None
            items.append(ix)
        d["order_items"] = items
        d["created_at_et"] = _bsm_eastern_time_v427(d.get("created_at"))
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
