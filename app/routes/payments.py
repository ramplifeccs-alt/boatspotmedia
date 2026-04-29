from flask import session
import json
from flask import Blueprint
from flask import request, redirect, url_for, jsonify, render_template
from app import db
import os
import stripe
from flask import Blueprint, request, redirect, jsonify, session, url_for

payments_bp = Blueprint("payments", __name__)

def stripe_ready():
    return bool(os.getenv("STRIPE_SECRET_KEY"))

def dollars_to_cents(value):
    try:
        return int(round(float(value) * 100))
    except Exception:
        return 0

def get_base_url():
    domain = os.getenv("DOMAIN") or "https://boatspotmedia.com"
    if not domain.startswith("http"):
        domain = "https://" + domain
    return domain.rstrip("/")

def create_checkout_session(item_type, item_id, title, description, amount, metadata=None):
    if not stripe_ready():
        return None, "Stripe is not configured. Missing STRIPE_SECRET_KEY."

    cents = dollars_to_cents(amount)
    if cents < 50:
        return None, "Invalid price. Stripe minimum is $0.50."

    stripe.api_key = os.getenv("STRIPE_SECRET_KEY")
    metadata = metadata or {}
    metadata.update({
        "item_type": str(item_type),
        "item_id": str(item_id),
        "buyer_user_id": str(session.get("user_id") or ""),
    })

    base = get_base_url()
    checkout = stripe.checkout.Session.create(
        mode="payment",
        payment_method_types=["card"],
        line_items=[{
            "price_data": {
                "currency": "usd",
                "product_data": {
                    "name": title or "BoatSpotMedia Purchase",
                    "description": (description or "")[:500],
                },
                "unit_amount": cents,
            },
            "quantity": 1,
        }],
        metadata=metadata,
        success_url=f"{base}/checkout/success?session_id={{CHECKOUT_SESSION_ID}}",
        cancel_url=f"{base}/checkout/cancel",
    )
    return checkout.url, None



def _price_from_creator_preset(video, price_id):
    try:
        if not price_id:
            return None
        from app.models import VideoPricingPreset
        creator_id = getattr(video, "creator_id", None) or getattr(video, "creator_profile_id", None)
        preset = VideoPricingPreset.query.get(int(price_id))
        if not preset:
            return None
        if creator_id and getattr(preset, "creator_id", None) != creator_id:
            return None
        if getattr(preset, "active", True) is False:
            return None
        price = float(preset.price or 0)
        return price if price > 0 else None
    except Exception:
        return None



def _ensure_cart_order_tables():
    try:
        db.session.execute(db.text("""
            CREATE TABLE IF NOT EXISTS bsm_cart_order (
                id SERIAL PRIMARY KEY,
                cart_id VARCHAR(128),
                stripe_session_id VARCHAR(255),
                buyer_email VARCHAR(255),
                amount_total NUMERIC(10,2),
                currency VARCHAR(16),
                pending_discount_review BOOLEAN DEFAULT FALSE,
                status VARCHAR(64) DEFAULT 'paid',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.execute(db.text("""
            CREATE TABLE IF NOT EXISTS bsm_cart_order_item (
                id SERIAL PRIMARY KEY,
                cart_order_id INTEGER,
                video_id INTEGER,
                creator_id INTEGER,
                item_type VARCHAR(64),
                package VARCHAR(64),
                boat_key TEXT,
                unit_price NUMERIC(10,2),
                quantity INTEGER DEFAULT 1,
                discount_status VARCHAR(64) DEFAULT 'none',
                delivery_status VARCHAR(64) DEFAULT 'ready_to_download',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.commit()
    except Exception:
        db.session.rollback()


def _record_cart_order_from_session(stripe_session):
    """
    Persist each cart item as a sale so Creator Dashboard can see earnings.
    Also creates download links for instant video items.
    """
    from app.models import Video
    from app.services.cart import cart_summary
    from app.services.download_tokens import create_download_token
    from app.services.sendgrid_email import send_download_email

    _ensure_cart_order_tables()

    buyer_email = None
    try:
        buyer_email = stripe_session.customer_details.email if stripe_session.customer_details else stripe_session.customer_email
    except Exception:
        buyer_email = getattr(stripe_session, "customer_email", None)

    items = session.get("bsm_cart", []) or []
    if not items:
        return {"order_id": None, "download_urls": []}

    amount_total = float(getattr(stripe_session, "amount_total", 0) or 0) / 100.0
    currency = getattr(stripe_session, "currency", "usd")
    pending_review = str(getattr(stripe_session, "metadata", {}).get("pending_discount_review", "False")) == "True"

    db.session.execute(
        db.text("""
            INSERT INTO bsm_cart_order (cart_id, stripe_session_id, buyer_email, amount_total, currency, pending_discount_review, status)
            VALUES (:cart_id, :sid, :email, :amount, :currency, :pending, 'paid')
        """),
        {
            "cart_id": session.get("bsm_cart_id"),
            "sid": getattr(stripe_session, "id", None),
            "email": buyer_email,
            "amount": amount_total,
            "currency": currency,
            "pending": pending_review,
        },
    )
    order_id = db.session.execute(db.text("SELECT currval(pg_get_serial_sequence('bsm_cart_order','id')) AS id")).mappings().first()["id"]

    download_urls = []
    for item in items:
        if item.get("item_type") != "video":
            continue

        video_id = item.get("video_id")
        video = Video.query.get(video_id)
        if not video:
            continue

        creator_id = item.get("creator_id") or getattr(video, "creator_id", None) or getattr(video, "creator_profile_id", None)
        package = item.get("package", "original")
        unit_price = float(item.get("unit_price") or 0)
        qty = int(item.get("quantity") or 1)

        # Edited video is pending creator delivery; original/bundle can create instant link.
        delivery_status = "pending_edit" if package == "edited" else "ready_to_download"

        db.session.execute(
            db.text("""
                INSERT INTO bsm_cart_order_item
                (cart_order_id, video_id, creator_id, item_type, package, boat_key, unit_price, quantity, discount_status, delivery_status)
                VALUES (:oid, :vid, :cid, :itype, :package, :boat_key, :price, :qty, :discount, :delivery)
            """),
            {
                "oid": order_id,
                "vid": video_id,
                "cid": creator_id,
                "itype": item.get("item_type", "video"),
                "package": package,
                "boat_key": item.get("boat_key"),
                "price": unit_price,
                "qty": qty,
                "discount": "pending_review" if pending_review else "none",
                "delivery": delivery_status,
            },
        )

        # Keep old bsm_sale table in sync for dashboard analytics.
        try:
            _record_sale_best_effort(video, buyer_email, unit_price * qty, package, stripe_session_id=getattr(stripe_session, "id", None))
        except Exception:
            pass

        if delivery_status == "ready_to_download":
            token = create_download_token(video_id=video_id, buyer_email=buyer_email, order_id=getattr(stripe_session, "id", None), package=package)
            download_urls.append({"video_id": video_id, "title": getattr(video, "filename", None) or getattr(video, "internal_filename", None) or "Video", "token": token})

    db.session.commit()

    # Send one email with all instant links.
    try:
        if buyer_email and download_urls:
            host = os.environ.get("PUBLIC_BASE_URL")
            # fallback will be built in route if needed; email here uses env base.
            base = (host or "").rstrip("/")
            if base:
                links_html = "<br>".join([f'{d["title"]}: {base}/download/{d["token"]}' for d in download_urls])
                send_download_email(buyer_email, links_html, video_title="Your BoatSpotMedia videos", order_id=getattr(stripe_session, "id", None))
    except Exception:
        pass

    # Clear cart after successful persistence.
    try:
        session["bsm_cart"] = []
        session.modified = True
    except Exception:
        pass

    return {"order_id": order_id, "download_urls": download_urls}


@payments_bp.route("/checkout/product/<int:product_id>")
def checkout_product(product_id):
    from app.models import Product
    product = Product.query.get_or_404(product_id)
    title = getattr(product, "title", None) or getattr(product, "name", None) or "Product"
    description = getattr(product, "description", "") or ""
    price = getattr(product, "price", None) or getattr(product, "base_price", None) or 0
    url, err = create_checkout_session("product", product_id, title, description, price)
    if err:
        return err, 400
    return redirect(url)


@payments_bp.route("/checkout/video/<int:video_id>")
def checkout_video(video_id):
    from app.models import Video
    video = Video.query.get_or_404(video_id)
    package = request.args.get("package", "original")
    preset_amount = _price_from_creator_preset(video, request.args.get("price_id"))
    if package == "edited":
        amount = getattr(video, "edited_price", None) or 0
        title = "Edited Video"
    elif package == "bundle":
        amount = getattr(video, "bundle_price", None) or 0
        title = "Original + Edited Video"
    else:
        amount = getattr(video, "original_price", None) or 0
        title = "Original 4K Video"
    desc = f"BoatSpotMedia video #{video_id}"
    url, err = create_checkout_session("video", video_id, title, desc, amount, {"package": package})
    if err:
        return err, 400
    return redirect(url)


@payments_bp.route("/checkout/service/<int:service_id>")
def checkout_service(service_id):
    from app.models import ServiceAd
    service = ServiceAd.query.get_or_404(service_id)
    title = getattr(service, "title", None) or "Boat Service"
    description = getattr(service, "description", "") or ""
    amount = getattr(service, "price", None) or getattr(service, "starting_price", None) or 0
    url, err = create_checkout_session("service", service_id, title, description, amount)
    if err:
        return err, 400
    return redirect(url)


@payments_bp.route("/checkout/charter/<int:charter_id>")
def checkout_charter(charter_id):
    from app.models import CharterListing
    charter = CharterListing.query.get_or_404(charter_id)
    title = getattr(charter, "title", None) or getattr(charter, "boat_name", None) or "Boat Charter"
    description = getattr(charter, "description", "") or ""
    amount = getattr(charter, "price", None) or getattr(charter, "hourly_rate", None) or 0
    url, err = create_checkout_session("charter", charter_id, title, description, amount)
    if err:
        return err, 400
    return redirect(url)


@payments_bp.route("/checkout/success")
def checkout_success():
    return """
    <h1>Payment successful</h1>
    <p>Thank you. Your order was received by BoatSpotMedia.</p>
    <p><a href="/">Back to home</a></p>
    """


@payments_bp.route("/checkout/cancel")
def checkout_cancel():
    return """
    <h1>Checkout canceled</h1>
    <p>Your payment was not completed.</p>
    <p><a href="/">Back to home</a></p>
    """


@payments_bp.route("/stripe/webhook", methods=["POST"])
def stripe_webhook():
    payload = request.data
    sig_header = request.headers.get("Stripe-Signature")
    secret = os.getenv("STRIPE_WEBHOOK_SECRET")
    stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

    if not secret:
        return "Missing STRIPE_WEBHOOK_SECRET", 400

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, secret)
    except ValueError:
        return "Invalid payload", 400
    except stripe.error.SignatureVerificationError:
        return "Invalid signature", 400

    event_type = event.get("type")
    obj = event.get("data", {}).get("object", {})

    if event_type == "checkout.session.completed":
        # Basic order record hook.
        # This keeps the system safe even if detailed Order models differ by version.
        metadata = obj.get("metadata", {}) or {}
        print("STRIPE CHECKOUT COMPLETED:", {
            "session_id": obj.get("id"),
            "amount_total": obj.get("amount_total"),
            "currency": obj.get("currency"),
            "customer_email": obj.get("customer_details", {}).get("email"),
            "metadata": dict(metadata),
        })

    elif event_type == "payment_intent.succeeded":
        print("STRIPE PAYMENT SUCCEEDED:", obj.get("id"))

    elif event_type == "payment_intent.payment_failed":
        print("STRIPE PAYMENT FAILED:", obj.get("id"))

    return jsonify({"received": True})


@payments_bp.route("/cart/checkout")
def checkout_cart():
    from app.services.cart import cart_summary, build_cart_display_items
    summary = cart_summary()
    items = build_cart_display_items()
    if not items:
        return redirect("/cart")
    stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")
    if not stripe.api_key:
        return "Stripe is not configured.", 500
    line_items = []
    for item in items:
        amount = float(item.get("unit_price") or 0)
        if amount <= 0:
            continue
        line_items.append({
            "price_data": {
                "currency": os.environ.get("STRIPE_CURRENCY", "usd"),
                "product_data": {"name": item.get("title") or "BoatSpotMedia item", "description": f"{item.get('item_type')} - {item.get('package')}"},
                "unit_amount": int(round(amount * 100)),
            },
            "quantity": int(item.get("quantity") or 1),
        })
    if not line_items:
        return "Cart has no purchasable items.", 400
    session = stripe.checkout.Session.create(
        mode="payment",
        payment_method_types=["card"],
        line_items=line_items,
        metadata={"cart_checkout":"1", "cart_id": session.get("bsm_cart_id", ""), "pending_discount_review": str(summary.get("pending_discount_review", False))},
        success_url=request.host_url.rstrip() + "/payment/success?session_id={CHECKOUT_SESSION_ID}",
        cancel_url=request.host_url.rstrip() + "/cart",
    )
    return redirect(session.url, code=303)
