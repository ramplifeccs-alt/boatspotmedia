from flask import Blueprint, render_template, session, redirect
from app import db

buyer_bp = Blueprint("buyer", __name__)

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

@buyer_bp.route("/buyer/dashboard")
def dashboard():
    if not session.get("user_id"):
        return redirect("/buyer/login")
    email = session.get("user_email")
    orders = []
    for order in _buyer_orders_for_email(email):
        d = dict(order)
        d["items"] = [dict(x) for x in _buyer_order_items(order["id"])]
        orders.append(d)
    return render_template(
        "buyer/dashboard.html",
        display_name=session.get("display_name") or email or "Buyer",
        email=email,
        buyer_email=email,
        orders=orders,
    )

@buyer_bp.route("/buyer/orders")
def orders():
    if not session.get("user_id"):
        return redirect("/buyer/login")
    return redirect("/buyer/dashboard")

@buyer_bp.route("/buyer/downloads")
def downloads():
    if not session.get("user_id"):
        return redirect("/buyer/login")
    return redirect("/buyer/dashboard")
