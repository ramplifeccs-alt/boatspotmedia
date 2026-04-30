from flask import Blueprint, render_template, request, redirect, session
from werkzeug.security import generate_password_hash, check_password_hash
from app import db

buyer_bp = Blueprint("buyer", __name__)

def _ensure_buyer_tables():
    try:
        db.session.execute(db.text("""
            CREATE TABLE IF NOT EXISTS buyer_account (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                full_name VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.commit()
    except Exception:
        db.session.rollback()

def _buyer_by_email(email):
    _ensure_buyer_tables()
    if not email:
        return None
    try:
        return db.session.execute(
            db.text("SELECT * FROM buyer_account WHERE lower(email)=lower(:email)"),
            {"email": email.strip()},
        ).mappings().first()
    except Exception:
        db.session.rollback()
        return None

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
def buyer_register_v417():
    _ensure_buyer_tables()
    if request.method == "POST":
        email=(request.form.get("email") or "").strip().lower()
        password=request.form.get("password") or ""
        full_name=(request.form.get("full_name") or "").strip()
        if not email or not password:
            return render_template("buyer/register.html", error="Email and password are required.", email=email, full_name=full_name)
        if len(password) < 6:
            return render_template("buyer/register.html", error="Password must be at least 6 characters.", email=email, full_name=full_name)
        if _buyer_by_email(email):
            return render_template("buyer/register.html", error="This email already has an account. Please login.", email=email, full_name=full_name)
        try:
            db.session.execute(db.text("INSERT INTO buyer_account (email,password_hash,full_name) VALUES (:email,:password_hash,:full_name)"),
                               {"email":email,"password_hash":generate_password_hash(password),"full_name":full_name})
            db.session.commit()
            session["buyer_email"]=email
            return redirect("/buyer/dashboard")
        except Exception:
            db.session.rollback()
            return render_template("buyer/register.html", error="Could not create account. Please try again.", email=email, full_name=full_name)
    return render_template("buyer/register.html")

@buyer_bp.route("/buyer/login", methods=["GET", "POST"])
def buyer_login_v417():
    _ensure_buyer_tables()
    if request.method == "POST":
        email=(request.form.get("email") or "").strip().lower()
        password=request.form.get("password") or ""
        buyer=_buyer_by_email(email)
        if not buyer or not check_password_hash(buyer["password_hash"], password):
            return render_template("buyer/login.html", error="Invalid email or password.", email=email)
        session["buyer_email"]=buyer["email"]
        return redirect("/buyer/dashboard")
    return render_template("buyer/login.html")

@buyer_bp.route("/buyer/logout")
def buyer_logout_v417():
    session.pop("buyer_email", None)
    return redirect("/")

@buyer_bp.route("/buyer/dashboard")
def buyer_order_dashboard_v417():
    email=session.get("buyer_email")
    if not email:
        return redirect("/buyer/login")
    orders=[]
    for order in _buyer_orders_for_email(email):
        d=dict(order)
        d["items"]=[dict(x) for x in _buyer_order_items(order["id"])]
        orders.append(d)
    return render_template("buyer/dashboard.html", buyer_email=email, orders=orders)
