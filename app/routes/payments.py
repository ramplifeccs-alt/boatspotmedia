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
        metadata={"cart_checkout":"1", "pending_discount_review": str(summary.get("pending_discount_review", False))},
        success_url=request.host_url.rstrip() + "/payment/success?session_id={CHECKOUT_SESSION_ID}",
        cancel_url=request.host_url.rstrip() + "/cart",
    )
    return redirect(session.url, code=303)
