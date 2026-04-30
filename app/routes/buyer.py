from flask import Blueprint, render_template, request, redirect, session
from werkzeug.security import generate_password_hash, check_password_hash
from app import db

buyer_bp = Blueprint("buyer", __name__)


def _ensure_buyer_tables():
    """
    Robust buyer account table.
    Handles fresh DB and older partial tables from placeholder versions.
    """
    try:
        db.session.execute(db.text("""
            CREATE TABLE IF NOT EXISTS buyer_account (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) UNIQUE,
                password_hash TEXT,
                full_name VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.execute(db.text("ALTER TABLE buyer_account ADD COLUMN IF NOT EXISTS email VARCHAR(255)"))
        db.session.execute(db.text("ALTER TABLE buyer_account ADD COLUMN IF NOT EXISTS password_hash TEXT"))
        db.session.execute(db.text("ALTER TABLE buyer_account ADD COLUMN IF NOT EXISTS full_name VARCHAR(255)"))
        db.session.execute(db.text("ALTER TABLE buyer_account ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"))
        try:
            db.session.execute(db.text("CREATE UNIQUE INDEX IF NOT EXISTS idx_buyer_account_email_lower ON buyer_account (lower(email))"))
        except Exception:
            db.session.rollback()
            db.session.execute(db.text("CREATE INDEX IF NOT EXISTS idx_buyer_account_email_lower_nonunique ON buyer_account (lower(email))"))
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try:
            print("buyer table ensure warning:", e)
        except Exception:
            pass


def _buyer_by_email(email):
    _ensure_buyer_tables()
    if not email:
        return None
    try:
        return db.session.execute(
            db.text("SELECT * FROM buyer_account WHERE lower(email)=lower(:email) ORDER BY id DESC LIMIT 1"),
            {"email": email.strip().lower()},
        ).mappings().first()
    except Exception:
        db.session.rollback()
        return None


def _create_or_update_buyer(email, password, full_name=""):
    """
    If buyer exists, update password and name. This prevents old placeholder accounts
    from blocking a real login.
    """
    _ensure_buyer_tables()
    email = (email or "").strip().lower()
    full_name = (full_name or "").strip()
    password_hash = generate_password_hash(password)

    existing = _buyer_by_email(email)
    try:
        if existing:
            db.session.execute(
                db.text("""
                    UPDATE buyer_account
                    SET password_hash=:password_hash,
                        full_name=COALESCE(NULLIF(:full_name,''), full_name)
                    WHERE id=:id
                """),
                {"password_hash": password_hash, "full_name": full_name, "id": existing["id"]},
            )
        else:
            db.session.execute(
                db.text("""
                    INSERT INTO buyer_account (email, password_hash, full_name)
                    VALUES (:email, :password_hash, :full_name)
                """),
                {"email": email, "password_hash": password_hash, "full_name": full_name},
            )
        db.session.commit()
        return True
    except Exception as e:
        db.session.rollback()
        try:
            print("buyer create/update warning:", e)
        except Exception:
            pass
        return False


def _password_matches(stored_hash, password):
    if not stored_hash:
        return False
    try:
        if check_password_hash(stored_hash, password):
            return True
    except Exception:
        pass
    # Legacy fallback if any old version saved plaintext by mistake.
    try:
        return stored_hash == password
    except Exception:
        return False


def _buyer_orders_for_email(email):
    if not email:
        return []
    try:
        rows = db.session.execute(db.text("""
            SELECT id, stripe_session_id, amount_total, currency, status, created_at, pending_discount_review
            FROM bsm_cart_order
            WHERE lower(buyer_email)=lower(:email)
            ORDER BY created_at DESC
        """), {"email": email}).mappings().all()
        return rows
    except Exception:
        db.session.rollback()
        return []


def _buyer_order_items(order_id):
    try:
        return db.session.execute(db.text("""
            SELECT i.*, v.location, v.filename, v.internal_filename, v.thumbnail_path
            FROM bsm_cart_order_item i
            LEFT JOIN video v ON v.id=i.video_id
            WHERE i.cart_order_id=:oid
            ORDER BY i.id ASC
        """), {"oid": order_id}).mappings().all()
    except Exception:
        db.session.rollback()
        return []


@buyer_bp.route("/buyer/register", methods=["GET", "POST"])
def buyer_register_v418():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""
        full_name = (request.form.get("full_name") or "").strip()

        if not email or not password:
            return render_template("buyer/register.html", error="Email and password are required.", email=email, full_name=full_name)

        if len(password) < 6:
            return render_template("buyer/register.html", error="Password must be at least 6 characters.", email=email, full_name=full_name)

        ok = _create_or_update_buyer(email, password, full_name)
        if not ok:
            return render_template("buyer/register.html", error="Could not save account. Please try again.", email=email, full_name=full_name)

        session["buyer_email"] = email
        session.modified = True
        return redirect("/buyer/dashboard")

    return render_template("buyer/register.html")


@buyer_bp.route("/buyer/login", methods=["GET", "POST"])
def buyer_login_v418():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""

        buyer = _buyer_by_email(email)
        if not buyer or not _password_matches(buyer.get("password_hash"), password):
            return render_template("buyer/login.html", error="Invalid email or password. If you just registered before this fix, create the account again with the same email to update the password.", email=email)

        session["buyer_email"] = buyer["email"]
        session.modified = True
        return redirect("/buyer/dashboard")

    return render_template("buyer/login.html")


@buyer_bp.route("/buyer/logout")
def buyer_logout_v418():
    session.pop("buyer_email", None)
    session.modified = True
    return redirect("/")


@buyer_bp.route("/buyer/dashboard")
def buyer_order_dashboard_v418():
    email = session.get("buyer_email")
    if not email:
        return redirect("/buyer/login")

    orders = []
    for order in _buyer_orders_for_email(email):
        d = dict(order)
        d["items"] = [dict(x) for x in _buyer_order_items(order["id"])]
        orders.append(d)

    return render_template("buyer/dashboard.html", buyer_email=email, orders=orders)
