from datetime import datetime, timedelta
import secrets
from flask import Blueprint, render_template, request, redirect, url_for
from app.models import Video, Order, OrderItem, DownloadToken, CreatorProfile
from app.services.db import db

buyer_bp = Blueprint("buyer", __name__)

@buyer_bp.route("/cart")
def cart():
    ids = request.args.get("videos", "")
    video_ids = [int(x) for x in ids.split(",") if x.isdigit()]
    videos = Video.query.filter(Video.id.in_(video_ids)).all() if video_ids else []
    total = 0
    by_creator = {}
    discounts = {}
    for v in videos:
        by_creator.setdefault(v.creator_id, []).append(v)
    for creator_id, items in by_creator.items():
        creator = CreatorProfile.query.get(creator_id)
        discount = creator.second_clip_discount_percent if creator else 0
        for i, v in enumerate(items):
            price = float(v.original_price)
            if i >= 1 and discount:
                price = price * (100 - discount) / 100
                discounts[v.id] = discount
            total += price
    return render_template("buyer/cart.html", videos=videos, total=total, discounts=discounts)

@buyer_bp.route("/checkout", methods=["POST"])
def checkout():
    buyer_email = request.form.get("buyer_email")
    ids = request.form.getlist("video_id")
    videos = Video.query.filter(Video.id.in_([int(i) for i in ids])).all()
    order = Order(buyer_email=buyer_email, status="paid")
    db.session.add(order); db.session.flush()
    total = 0
    grouped = {}
    for v in videos:
        grouped.setdefault(v.creator_id, []).append(v)
    for creator_id, items in grouped.items():
        creator = CreatorProfile.query.get(creator_id)
        discount = creator.second_clip_discount_percent if creator else 0
        for i, v in enumerate(items):
            price = float(v.original_price)
            if i >= 1 and discount:
                price = price * (100 - discount) / 100
            item = OrderItem(order_id=order.id, video_id=v.id, creator_id=v.creator_id, purchase_type="original", price=price)
            db.session.add(item); db.session.flush()
            token = DownloadToken(
                order_item_id=item.id,
                token=secrets.token_urlsafe(32),
                expires_at=datetime.utcnow() + timedelta(days=7)
            )
            db.session.add(token)
            total += price
    order.total_price = total
    db.session.commit()
    return redirect(url_for("buyer.orders", email=buyer_email))

@buyer_bp.route("/orders")
def orders():
    email = request.args.get("email")
    orders = Order.query.filter_by(buyer_email=email).order_by(Order.created_at.desc()).all() if email else []
    now = datetime.utcnow()
    return render_template("buyer/orders.html", orders=orders, now=now)

@buyer_bp.route("/download/<token>")
def download(token):
    dt = DownloadToken.query.filter_by(token=token).first_or_404()
    if dt.expires_at < datetime.utcnow():
        return "Download expired", 403
    dt.download_count += 1
    db.session.commit()
    # In production this should generate a signed R2 URL.
    return f"Download allowed for video #{dt.item.video_id}. Replace this with signed R2 URL redirect."


@buyer_bp.route("/dashboard")
def dashboard():
    return "Buyer Dashboard - login successful with Google."
