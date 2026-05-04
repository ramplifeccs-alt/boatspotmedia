from flask import session as flask_session, render_template, request, redirect, flash
import json
from flask import Blueprint
from flask import request, redirect, url_for, jsonify, render_template
from app import db
import os
import stripe
from flask import Blueprint, request, redirect, jsonify, url_for

payments_bp = Blueprint("payments", __name__)


def _normalize_paid_video_package_v491e(video, item):
    package = (item.get("package") or "original").lower().strip()
    price_id = item.get("price_id")
    try:
        from app.services.cart import _normalize_video_package_v491e
        return _normalize_video_package_v491e(video, package, price_id)
    except Exception:
        if package in ["instant", "instant_download", "download", "original", "4k", "original_4k"]:
            return "original"
        if package in ["bundle", "combo", "original_plus_edited", "original_edited", "original+edited", "original_edit"]:
            return "bundle"
        if package in ["edited", "edit", "instagram_edit", "tiktok_edit", "reel_edit", "short_edit"]:
            try:
                if float(getattr(video, "edited_price", 0) or 0) <= 0 and float(getattr(video, "original_price", 0) or 0) > 0:
                    return "original"
            except Exception:
                pass
            return "edited"
        return "original"



# v49.1Q Stripe Connect split payments for video sales

# v49.1S multi-creator cart guard.


# v49.1V final enforcement: use current visible cart only, not stale pending snapshots.
def _bsm_cart_items_for_guard_v491t():
    """
    Load only the current cart stored in session.
    Do NOT use old bsm_pending_cart snapshots here, because they can contain removed items
    and falsely trigger multi-creator block after the buyer removes a video.
    """
    for key in ["cart", "bsm_cart", "cart_items", "bsm_cart_items"]:
        try:
            val = flask_session.get(key)
            if isinstance(val, list):
                return val
            if isinstance(val, dict):
                return list(val.values())
        except Exception:
            pass
    return []

def _bsm_item_video_id_v491t(item):
    try:
        if not isinstance(item, dict):
            return 0
        # Prefer explicit video_id. Some carts use id as cart line id, so only use id as fallback.
        return int(item.get("video_id") or item.get("videoId") or item.get("video") or item.get("id") or 0)
    except Exception:
        return 0

def _bsm_enforce_single_creator_connected_v491t(items):
    # If checkout route passes live items, use them. Otherwise load current session cart.
    items = items if items else _bsm_cart_items_for_guard_v491t()

    creator_ids = set()
    creator_names = {}
    missing_stripe = []

    try:
        for item in items or []:
            vid = _bsm_item_video_id_v491t(item)
            if not vid:
                continue
            row = db.session.execute(db.text("""
                SELECT v.creator_id,
                       COALESCE(u.display_name, u.public_name, u.primary_location, u.email, 'Creator') AS creator_name,
                       COALESCE(u.stripe_account_id,'') AS stripe_account_id
                FROM video v
                LEFT JOIN creator_profile cp ON cp.id = v.creator_id
                LEFT JOIN "user" u ON u.id = cp.user_id
                WHERE v.id=:vid
                LIMIT 1
            """), {"vid": vid}).mappings().first()
            if row and row.get("creator_id"):
                cid = int(row.get("creator_id"))
                creator_ids.add(cid)
                creator_names[cid] = row.get("creator_name") or ("Creator #" + str(cid))
                if not (row.get("stripe_account_id") or "").strip():
                    missing_stripe.append(cid)
    except Exception as e:
        db.session.rollback()
        try:
            print("cart guard v49.1V warning:", e)
        except Exception:
            pass
        return None

    if len(creator_ids) > 1:
        names = ", ".join([creator_names.get(cid, "Creator #" + str(cid)) for cid in sorted(creator_ids)])
        flash("Your cart has videos from multiple creators: " + names + ". Please purchase videos from one creator at a time so payouts and commissions are separated correctly.")
        return redirect("/cart")

    if len(creator_ids) == 1 and missing_stripe:
        cid = list(creator_ids)[0]
        flash((creator_names.get(cid) or "This creator") + " has not connected Stripe payouts yet. This video cannot be purchased until the creator connects Stripe.")
        return redirect("/cart")

    return None

def _bsm_abort_checkout_if_needed_v491t(items=None):
    resp = _bsm_enforce_single_creator_connected_v491t(items)
    if resp is not None:
        return resp
    return None



def _bsm_cart_creator_summary_v491s(items):
    """
    Returns creator IDs present in the current cart.
    For Stripe Connect destination charges, one Checkout Session should contain one creator only.
    """
    creator_ids = set()
    creator_names = {}
    try:
        for it in items or []:
            vid = int(it.get("video_id") or it.get("id") or 0)
            if not vid:
                continue
            row = db.session.execute(db.text("""
                SELECT v.creator_id,
                       COALESCE(u.display_name, u.public_name, u.primary_location, u.email, 'Creator') AS creator_name
                FROM video v
                LEFT JOIN creator_profile cp ON cp.id = v.creator_id
                LEFT JOIN "user" u ON u.id = cp.user_id
                WHERE v.id=:vid
                LIMIT 1
            """), {"vid": vid}).mappings().first()
            if row and row.get("creator_id"):
                cid = int(row.get("creator_id"))
                creator_ids.add(cid)
                creator_names[cid] = row.get("creator_name") or ("Creator #" + str(cid))
    except Exception as e:
        db.session.rollback()
        try:
            print("cart creator summary v49.1S warning:", e)
        except Exception:
            pass
    return {"creator_ids": sorted(list(creator_ids)), "creator_names": creator_names}

def _bsm_cart_has_multiple_creators_v491s(items):
    summary = _bsm_cart_creator_summary_v491s(items)
    return len(summary.get("creator_ids") or []) > 1, summary


def _bsm_connect_info_for_cart_v491q(items):
    """
    Returns Connect info for a cart when all items belong to one creator with Stripe connected.
    If multiple creators are in one cart, returns disabled so old checkout still works.
    """
    try:
        creator_ids = set()
        for it in items or []:
            vid = int(it.get("video_id") or it.get("id") or 0)
            if not vid:
                continue
            row = db.session.execute(db.text("""
                SELECT v.creator_id,
                       cp.commission_rate,
                       u.stripe_account_id
                FROM video v
                LEFT JOIN creator_profile cp ON cp.id = v.creator_id
                LEFT JOIN "user" u ON u.id = cp.user_id
                WHERE v.id=:vid
                LIMIT 1
            """), {"vid": vid}).mappings().first()
            if row and row.get("creator_id"):
                creator_ids.add(int(row.get("creator_id")))
        if len(creator_ids) != 1:
            return {"enabled": False, "reason": "multi_creator_or_missing"}

        creator_id = list(creator_ids)[0]
        row = db.session.execute(db.text("""
            SELECT cp.id AS creator_id,
                   COALESCE(cp.commission_rate, 25) AS commission_rate,
                   u.stripe_account_id
            FROM creator_profile cp
            LEFT JOIN "user" u ON u.id = cp.user_id
            WHERE cp.id=:creator_id
            LIMIT 1
        """), {"creator_id": creator_id}).mappings().first()

        if not row:
            return {"enabled": False, "reason": "creator_not_found", "creator_id": creator_id}

        acct = (row.get("stripe_account_id") or "").strip()
        if not acct:
            return {"enabled": False, "reason": "creator_stripe_not_connected", "creator_id": creator_id}

        commission_rate = float(row.get("commission_rate") or 25)
        if commission_rate < 0:
            commission_rate = 0
        if commission_rate > 95:
            commission_rate = 95

        return {
            "enabled": True,
            "creator_id": creator_id,
            "stripe_account_id": acct,
            "commission_rate": commission_rate,
        }
    except Exception as e:
        db.session.rollback()
        try:
            print("connect info v49.1Q warning:", e)
        except Exception:
            pass
        return {"enabled": False, "reason": "error"}

def _bsm_application_fee_cents_v491q(amount_total_cents, commission_rate):
    try:
        return int(round(int(amount_total_cents or 0) * (float(commission_rate or 0) / 100.0)))
    except Exception:
        return 0

def _bsm_record_order_connect_fields_v491q(order_id, stripe_account_id=None, application_fee_amount=None, creator_id=None, commission_rate=None):
    try:
        db.session.execute(db.text("ALTER TABLE bsm_cart_order ADD COLUMN IF NOT EXISTS creator_id INTEGER"))
        db.session.execute(db.text("ALTER TABLE bsm_cart_order ADD COLUMN IF NOT EXISTS creator_stripe_account_id TEXT"))
        db.session.execute(db.text("ALTER TABLE bsm_cart_order ADD COLUMN IF NOT EXISTS platform_fee_amount NUMERIC DEFAULT 0"))
        db.session.execute(db.text("ALTER TABLE bsm_cart_order ADD COLUMN IF NOT EXISTS creator_gross_amount NUMERIC DEFAULT 0"))
        db.session.execute(db.text("ALTER TABLE bsm_cart_order ADD COLUMN IF NOT EXISTS commission_rate NUMERIC DEFAULT 0"))
        if order_id:
            platform_fee = float(application_fee_amount or 0) / 100.0
            db.session.execute(db.text("""
                UPDATE bsm_cart_order
                SET creator_id=COALESCE(:creator_id, creator_id),
                    creator_stripe_account_id=COALESCE(:acct, creator_stripe_account_id),
                    platform_fee_amount=:platform_fee,
                    creator_gross_amount=GREATEST(COALESCE(amount_total,0)-:platform_fee, 0),
                    commission_rate=COALESCE(:commission_rate, commission_rate)
                WHERE id=:order_id
            """), {
                "order_id": order_id,
                "creator_id": creator_id,
                "acct": stripe_account_id,
                "platform_fee": platform_fee,
                "commission_rate": commission_rate,
            })
            db.session.commit()
    except Exception as e:
        db.session.rollback()
        try:
            print("order connect fields v49.1Q warning:", e)
        except Exception:
            pass


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
        "buyer_user_id": str(flask_session.get("user_id") or ""),
    })

    base = get_base_url()


    # v49.1S: Do not allow a single cart checkout with videos from multiple creators.
    # Stripe Connect destination charges route funds to one connected account per Checkout Session.
    _multi_creator_cart_v491s, _creator_summary_v491s = _bsm_cart_has_multiple_creators_v491s(items if 'items' in locals() else [])
    if _multi_creator_cart_v491s:
        names = ", ".join([_creator_summary_v491s.get("creator_names", {}).get(cid, "Creator #" + str(cid)) for cid in _creator_summary_v491s.get("creator_ids", [])])
        flash("Your cart has videos from multiple creators: " + names + ". Please purchase one creator at a time so payouts and commissions are separated correctly.")
        return redirect("/cart")


    # v49.1S: If this is a one-creator cart but that creator has no Stripe connected, block checkout.
    # This protects creator payouts and avoids manual settlement.
    if not _multi_creator_cart_v491s:
        _connect_check_v491s = _bsm_connect_info_for_cart_v491q(items if 'items' in locals() else []) if '_bsm_connect_info_for_cart_v491q' in globals() else {"enabled": False}
        if _creator_summary_v491s.get("creator_ids") and not _connect_check_v491s.get("enabled"):
            flash("This creator has not connected Stripe payouts yet. The video cannot be purchased until the creator connects Stripe.")
            return redirect("/cart")

    # v49.1Q Stripe Connect split payment calculation
    connect_info_v491q = _bsm_connect_info_for_cart_v491q(items if 'items' in locals() else [])
    payment_intent_data_v491q = None
    if connect_info_v491q.get("enabled"):
        total_cents_v491q = 0
        try:
            for li in line_items:
                price_data = li.get("price_data") or {}
                total_cents_v491q += int(price_data.get("unit_amount") or 0) * int(li.get("quantity") or 1)
        except Exception:
            total_cents_v491q = int(round(float(total or amount_total or 0) * 100)) if ('total' in locals() or 'amount_total' in locals()) else 0
        fee_cents_v491q = _bsm_application_fee_cents_v491q(total_cents_v491q, connect_info_v491q.get("commission_rate"))
        if fee_cents_v491q > 0:
            payment_intent_data_v491q = {
                "application_fee_amount": fee_cents_v491q,
                "transfer_data": {"destination": connect_info_v491q.get("stripe_account_id")},
                "metadata": {
                    "connect_split": "true",
                    "creator_id": str(connect_info_v491q.get("creator_id")),
                    "creator_stripe_account_id": str(connect_info_v491q.get("stripe_account_id")),
                    "commission_rate": str(connect_info_v491q.get("commission_rate")),
                    "platform_fee_cents": str(fee_cents_v491q),
                },
            }

    checkout = stripe.checkout.Session.create(
        mode="payment",
            payment_intent_data=payment_intent_data_v491q,
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
                buyer_user_id INTEGER,
                amount_total NUMERIC(10,2),
                currency VARCHAR(16),
                pending_discount_review BOOLEAN DEFAULT FALSE,
                status VARCHAR(64) DEFAULT 'paid',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.execute(db.text("ALTER TABLE bsm_cart_order ADD COLUMN IF NOT EXISTS buyer_user_id INTEGER"))
        db.session.execute(db.text("CREATE INDEX IF NOT EXISTS idx_bsm_cart_order_buyer_user_id ON bsm_cart_order (buyer_user_id)"))
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

    # Duplicate cart order guard v42.0
    try:
        sid = getattr(stripe_session, "id", None)
        if sid:
            existing = db.session.execute(db.text("SELECT id FROM bsm_cart_order WHERE stripe_session_id=:sid LIMIT 1"), {"sid": sid}).mappings().first()
            if existing:
                return {"order_id": existing["id"], "download_urls": []}
    except Exception:
        db.session.rollback()

    # buyer_user_id_v424: associate order with logged-in buyer, not only email.
    buyer_user_id = None
    try:
        buyer_user_id = int(stripe_session.metadata.get("buyer_user_id") or 0) or None
    except Exception:
        buyer_user_id = None
    if not buyer_user_id:
        try:
            buyer_user_id = int(flask_session.get("user_id") or 0) or None
        except Exception:
            buyer_user_id = None

    buyer_email = None
    try:
        buyer_email = (stripe_session.metadata.get("buyer_login_email") or None)
    except Exception:
        buyer_email = None
    if not buyer_email:
        try:
            buyer_email = stripe_session.customer_details.email if stripe_session.customer_details else stripe_session.customer_email
        except Exception:
            buyer_email = getattr(stripe_session, "customer_email", None)

    items = flask_session.get("bsm_cart", []) or []
    if not items:
        try:
            items = _load_pending_cart_snapshot(str(stripe_session.metadata.get("cart_id", "")))
        except Exception:
            items = []
    if not items:
        return {"order_id": None, "download_urls": []}

    amount_total = float(getattr(stripe_session, "amount_total", 0) or 0) / 100.0
    currency = getattr(stripe_session, "currency", "usd")
    pending_review = str(getattr(stripe_session, "metadata", {}).get("pending_discount_review", "False")) == "True"

    row = db.session.execute(
        db.text("""
            INSERT INTO bsm_cart_order (cart_id, stripe_session_id, buyer_email, buyer_user_id, amount_total, currency, pending_discount_review, status)
            VALUES (:cart_id, :sid, :email, :buyer_user_id, :amount, :currency, :pending, 'paid')
            RETURNING id
        """),
        {
            "cart_id": flask_session.get("bsm_cart_id") or str(stripe_session.metadata.get("cart_id", "")),
            "sid": getattr(stripe_session, "id", None),
            "email": buyer_email,
            "buyer_user_id": buyer_user_id,
            "amount": amount_total,
            "currency": currency,
            "pending": pending_review,
        },
    ).mappings().first()
    order_id = row["id"] if row else None

    download_urls = []
    for item in items:
        if item.get("item_type") != "video":
            continue

        video_id = item.get("video_id")
        video = Video.query.get(video_id)
        if not video:
            continue

        creator_id = item.get("creator_id") or getattr(video, "creator_id", None) or getattr(video, "creator_profile_id", None)
        package = _normalize_paid_video_package_v491e(video, item)
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
                "oid": order_id or 0,
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
            # v42.4: download token schema can differ. Do not let token creation break order saving.
            try:
                pass
            except Exception:
                pass

    db.session.commit()

    # SendGrid cart email will be handled in the next delivery workflow phase.
    # Clear cart after successful persistence.
    try:
        flask_flask_session["bsm_cart"] = []
        flask_flask_session.modified = True
    except Exception:
        pass

    return {"order_id": order_id, "download_urls": download_urls}



def _ensure_pending_cart_table():
    try:
        db.session.execute(db.text("""
            CREATE TABLE IF NOT EXISTS bsm_pending_cart (
                cart_id VARCHAR(128) PRIMARY KEY,
                buyer_email VARCHAR(255),
                cart_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.commit()
    except Exception:
        db.session.rollback()


def _save_pending_cart_snapshot(cart_id, cart_items, buyer_email=None):
    try:
        _ensure_pending_cart_table()
        db.session.execute(
            db.text("""
                INSERT INTO bsm_pending_cart (cart_id, buyer_email, cart_json)
                VALUES (:cart_id, :buyer_email, :cart_json)
                ON CONFLICT (cart_id) DO UPDATE SET
                    buyer_email = EXCLUDED.buyer_email,
                    cart_json = EXCLUDED.cart_json,
                    created_at = CURRENT_TIMESTAMP
            """),
            {
                "cart_id": cart_id,
                "buyer_email": buyer_email,
                "cart_json": json.dumps(cart_items or []),
            },
        )
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try:
            print("pending cart snapshot save warning:", e)
        except Exception:
            pass


def _load_pending_cart_snapshot(cart_id):
    try:
        _ensure_pending_cart_table()
        row = db.session.execute(
            db.text("SELECT cart_json FROM bsm_pending_cart WHERE cart_id = :cart_id"),
            {"cart_id": cart_id},
        ).mappings().first()
        if not row:
            return []
        return json.loads(row["cart_json"] or "[]")
    except Exception as e:
        db.session.rollback()
        try:
            print("pending cart snapshot load warning:", e)
        except Exception:
            pass
        return []



def _safe_record_cart_order_from_session(stripe_session):
    try:
        return _record_cart_order_from_session(stripe_session)
    except Exception as e:
        try:
            print("cart order record fatal warning:", e)
        except Exception:
            pass
        return {"order_id": None, "download_urls": []}



def _public_base_url():
    """
    Reliable public URL for Stripe redirects.
    Prefer PUBLIC_BASE_URL in Railway; fallback to current request host.
    """
    try:
        base = os.environ.get("PUBLIC_BASE_URL")
        if base:
            return base.rstrip("/")
    except Exception:
        pass
    return request.host_url.rstrip("/")



def _record_cart_order_from_webhook_v420(obj):
    """
    Persist cart order from Stripe webhook using bsm_pending_cart cart_id.
    This works even when buyer does not return to /payment/success.
    """
    from app.models import Video
    _ensure_cart_order_tables()
    metadata = obj.get("metadata", {}) or {}
    if str(metadata.get("cart_checkout", "")) != "1":
        return None

    sid = obj.get("id")
    try:
        if sid:
            existing = db.session.execute(db.text("SELECT id FROM bsm_cart_order WHERE stripe_session_id=:sid LIMIT 1"), {"sid": sid}).mappings().first()
            if existing:
                return existing["id"]
    except Exception:
        db.session.rollback()

    cart_id = str(metadata.get("cart_id", "") or "")
    items = _load_pending_cart_snapshot(cart_id)
    if not items:
        return None

    buyer_user_id = None
    try:
        buyer_user_id = int(metadata.get("buyer_user_id") or 0) or None
    except Exception:
        buyer_user_id = None

    buyer_email = metadata.get("buyer_login_email") or None
    if not buyer_email:
        try:
            buyer_email = (obj.get("customer_details") or {}).get("email") or obj.get("customer_email")
        except Exception:
            buyer_email = obj.get("customer_email")

    amount_total = float(obj.get("amount_total") or 0) / 100.0
    currency = obj.get("currency") or "usd"
    pending_review = str(metadata.get("pending_discount_review", "False")) == "True"

    try:
        row = db.session.execute(db.text("""
            INSERT INTO bsm_cart_order (cart_id, stripe_session_id, buyer_email, buyer_user_id, amount_total, currency, pending_discount_review, status)
            VALUES (:cart_id, :sid, :email, :buyer_user_id, :amount, :currency, :pending, 'paid')
            RETURNING id
        """), {"cart_id": cart_id, "sid": sid, "email": buyer_email, "buyer_user_id": buyer_user_id, "amount": amount_total, "currency": currency, "pending": pending_review}).mappings().first()
        order_id = row["id"] if row else None

        for item in items:
            if item.get("item_type") != "video":
                continue
            video = Video.query.get(item.get("video_id"))
            if not video:
                continue
            creator_id = item.get("creator_id") or getattr(video, "creator_id", None) or getattr(video, "creator_profile_id", None)
            package = _normalize_paid_video_package_v491e(video, item)
            unit_price = float(item.get("unit_price") or 0)
            qty = int(item.get("quantity") or 1)
            delivery_status = "pending_edit" if package == "edited" else "ready_to_download"
            db.session.execute(db.text("""
                INSERT INTO bsm_cart_order_item
                (cart_order_id, video_id, creator_id, item_type, package, boat_key, unit_price, quantity, discount_status, delivery_status)
                VALUES (:oid, :vid, :cid, :itype, :package, :boat_key, :price, :qty, :discount, :delivery)
            """), {
                "oid": order_id or 0,
                "vid": item.get("video_id"),
                "cid": creator_id,
                "itype": item.get("item_type", "video"),
                "package": package,
                "boat_key": item.get("boat_key"),
                "price": unit_price,
                "qty": qty,
                "discount": "pending_review" if pending_review else "none",
                "delivery": delivery_status,
            })
            try:
                _record_sale_best_effort(video, buyer_email, unit_price * qty, package, stripe_session_id=sid)
            except Exception:
                pass

        try:
            pi = getattr(stripe_session, "payment_intent", None)
            md = getattr(stripe_session, "metadata", {}) or {}
            fee_cents = int((md.get("connect_platform_fee_cents") if hasattr(md, "get") else 0) or 0)
            creator_id = int((md.get("connect_creator_id") if hasattr(md, "get") else 0) or 0) or None
            acct = None
            if pi:
                try:
                    import stripe
                    stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")
                    pi_obj = stripe.PaymentIntent.retrieve(pi)
                    pi_md = getattr(pi_obj, "metadata", {}) or {}
                    acct = pi_md.get("creator_stripe_account_id") if hasattr(pi_md, "get") else None
                    fee_cents = int((pi_md.get("platform_fee_cents") if hasattr(pi_md, "get") else fee_cents) or fee_cents or 0)
                except Exception:
                    pass
            _bsm_record_order_connect_fields_v491q(order_id, acct, fee_cents, creator_id, None)
        except Exception:
            pass
        db.session.commit()
        return order_id
    except Exception as e:
        db.session.rollback()
        try:
            print("webhook cart order record warning:", e)
        except Exception:
            pass
        return None




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



# v47.2 creator subscription webhook helper
def _bsm_creator_apply_subscription_v472(creator_id, plan_key, status, stripe_customer_id=None, stripe_subscription_id=None, current_period_end=0, storage_limit_gb=5, cancel_at_period_end=False):
    try:
        db.session.execute(db.text("""
            CREATE TABLE IF NOT EXISTS creator_subscription (
              id SERIAL PRIMARY KEY, creator_id INTEGER UNIQUE NOT NULL, plan_key TEXT NOT NULL DEFAULT 'free',
              status TEXT NOT NULL DEFAULT 'active', storage_limit_gb INTEGER NOT NULL DEFAULT 5,
              stripe_customer_id TEXT, stripe_subscription_id TEXT, current_period_end TIMESTAMP,
              cancel_at_period_end BOOLEAN DEFAULT FALSE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)); db.session.commit()
    except Exception: db.session.rollback()
    try:
        db.session.execute(db.text("""
            INSERT INTO creator_subscription (creator_id,plan_key,status,storage_limit_gb,stripe_customer_id,stripe_subscription_id,current_period_end,cancel_at_period_end,updated_at)
            VALUES (:cid,:plan,:status,:gb,:cust,:sub,to_timestamp(:period),:cancel,CURRENT_TIMESTAMP)
            ON CONFLICT (creator_id) DO UPDATE SET plan_key=EXCLUDED.plan_key,status=EXCLUDED.status,storage_limit_gb=EXCLUDED.storage_limit_gb,
            stripe_customer_id=COALESCE(EXCLUDED.stripe_customer_id,creator_subscription.stripe_customer_id),
            stripe_subscription_id=COALESCE(EXCLUDED.stripe_subscription_id,creator_subscription.stripe_subscription_id),
            current_period_end=EXCLUDED.current_period_end,cancel_at_period_end=EXCLUDED.cancel_at_period_end,updated_at=CURRENT_TIMESTAMP
        """),{"cid":creator_id,"plan":plan_key,"status":status,"gb":int(storage_limit_gb or 5),"cust":stripe_customer_id,"sub":stripe_subscription_id,"period":int(current_period_end or 0),"cancel":bool(cancel_at_period_end)})
        db.session.execute(db.text("""
            UPDATE creator_profile
            SET storage_limit_gb=:gb
            WHERE id=:cid
        """), {"cid": creator_id, "gb": int(storage_limit_gb or 5)})
        if commission_percent is not None:
            db.session.execute(db.text("UPDATE creator_profile SET commission_rate=:commission WHERE id=:cid"), {"cid": creator_id, "commission": commission_percent})
        # Optional commission from metadata, if supplied by checkout.
        try:
            commission_percent = None
            if hasattr(storage_limit_gb, "get"):
                commission_percent = storage_limit_gb.get("commission_percent")
            if commission_percent is not None:
                db.session.execute(db.text("UPDATE creator_profile SET commission_rate=:commission WHERE id=:cid"), {"cid": creator_id, "commission": commission_percent})
        except Exception:
            pass
        db.session.commit()
    except Exception as e:
        db.session.rollback(); print("creator subscription apply warning v47.2:", e)

def _bsm_handle_creator_subscription_event_v472(event):
    obj=event.get("data",{}).get("object",{})
    typ=event.get("type")
    if typ=="checkout.session.completed" and obj.get("mode")=="subscription":
        md=obj.get("metadata") or {}
        if md.get("billing_type")=="creator_subscription":
            _bsm_creator_apply_subscription_v472(int(md.get("creator_id") or obj.get("client_reference_id") or 0), md.get("plan_key") or "pro", "active", obj.get("customer"), obj.get("subscription"), 0, int(md.get("storage_limit_gb") or 5, md.get("commission_percent")))
    elif typ in ["invoice.payment_failed","invoice.paid"]:
        status="past_due" if typ=="invoice.payment_failed" else "active"
        try:
            db.session.execute(db.text("UPDATE creator_subscription SET status=:s, updated_at=CURRENT_TIMESTAMP WHERE stripe_subscription_id=:sub"),{"s":status,"sub":obj.get("subscription")})
            db.session.commit()
        except Exception: db.session.rollback()
    elif typ=="customer.subscription.deleted":
        md=obj.get("metadata") or {}
        cid=int(md.get("creator_id") or 0)
        if cid: _bsm_creator_apply_subscription_v472(cid,"free","active",obj.get("customer"),obj.get("id"),obj.get("current_period_end") or 0,5)


@payments_bp.route("/payment/success")
def payment_success_v423():
    """
    Robust payment success page:
    - verifies Stripe session_id when available
    - records cart order before rendering
    - hides buyer login/register buttons if buyer is already logged in
    """
    session_id = request.args.get("session_id")
    download_urls = []
    buyer_email = flask_session.get("user_email")
    safe_message = "Payment received. Your order is being processed."

    if session_id:
        try:
            stripe.api_key = os.environ.get("STRIPE_SECRET_KEY")
            stripe_session = stripe.checkout.Session.retrieve(session_id)
            buyer_email = None
            try:
                buyer_email = stripe_session.customer_details.email if stripe_session.customer_details else stripe_session.customer_email
            except Exception:
                buyer_email = getattr(stripe_session, "customer_email", None)
            if buyer_email:
                # keep buyer session email if logged in, but use checkout email for display/order match
                pass
            if getattr(stripe_session, "payment_status", None) == "paid":
                try:
                    result = _safe_record_cart_order_from_session(stripe_session)
                except Exception:
                    try:
                        result = _record_cart_order_from_session(stripe_session)
                    except Exception as e:
                        try:
                            print("payment success record warning v42.3:", e)
                        except Exception:
                            pass
                        result = {"download_urls": []}
                for d in result.get("download_urls", []) or []:
                    token = d.get("token")
                    if token:
                        download_urls.append({"title": d.get("title") or "video", "url": request.host_url.rstrip("/") + "/download/" + token})
                # clear cart v42.4 after successful payment/order processing
                try:
                    flask_flask_session["bsm_cart"] = []
                    flask_flask_session.modified = True
                except Exception:
                    pass
                safe_message = "Payment received. Your order has been saved."
            else:
                safe_message = "Payment is still processing. Please check again shortly."
        except Exception as e:
            try:
                print("payment success retrieve warning v42.3:", e)
            except Exception:
                pass
            safe_message = "Payment received. Your order is being processed. Please check your email shortly."

    is_logged_in_buyer = bool(flask_session.get("user_id") and flask_session.get("user_role") == "buyer")
    return render_template(
        "buyer/payment_success.html",
        download_url=(download_urls[0]["url"] if download_urls else None),
        download_urls=download_urls,
        buyer_email=buyer_email or flask_session.get("user_email"),
        safe_message=safe_message,
        is_logged_in_buyer=is_logged_in_buyer,
    )


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



# v49.1N robust creator subscription activation from Stripe webhook.
def _bsm_creator_plan_lookup_v491n(plan_key):
    plan_key = (plan_key or "free").strip().lower()
    try:
        row = db.session.execute(db.text("""
            SELECT plan_key, storage_gb, commission_percent
            FROM creator_plan
            WHERE plan_key=:plan_key
            LIMIT 1
        """), {"plan_key": plan_key}).mappings().first()
        if row:
            return {
                "storage_gb": float(row.get("storage_gb") or 5),
                "commission_percent": float(row.get("commission_percent") or 25),
            }
    except Exception:
        db.session.rollback()
    # Safe defaults matching current product plan rules.
    defaults = {
        "free": {"storage_gb": 5, "commission_percent": 25},
        "starter": {"storage_gb": 150, "commission_percent": 25},
        "pro": {"storage_gb": 512, "commission_percent": 22},
        "studio": {"storage_gb": 2048, "commission_percent": 17},
    }
    return defaults.get(plan_key, defaults["free"])

def _bsm_apply_creator_subscription_v491n(creator_id, plan_key, status="active", stripe_customer_id=None, stripe_subscription_id=None, current_period_end=None, storage_limit_gb=None, commission_percent=None):
    creator_id = int(creator_id or 0)
    if creator_id <= 0:
        return False

    plan_key = (plan_key or "free").strip().lower()
    plan = _bsm_creator_plan_lookup_v491n(plan_key)
    storage_limit_gb = float(storage_limit_gb or plan["storage_gb"] or 5)
    commission_percent = float(commission_percent or plan["commission_percent"] or 25)

    try:
        db.session.execute(db.text("""
            CREATE TABLE IF NOT EXISTS creator_subscription (
                id SERIAL PRIMARY KEY,
                creator_id INTEGER UNIQUE,
                plan_key TEXT,
                status TEXT,
                storage_limit_gb NUMERIC DEFAULT 5,
                stripe_customer_id TEXT,
                stripe_subscription_id TEXT,
                current_period_end TIMESTAMP,
                cancel_at_period_end BOOLEAN DEFAULT false,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.execute(db.text("ALTER TABLE creator_subscription ADD COLUMN IF NOT EXISTS storage_limit_gb NUMERIC DEFAULT 5"))
        db.session.execute(db.text("ALTER TABLE creator_subscription ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT"))
        db.session.execute(db.text("ALTER TABLE creator_subscription ADD COLUMN IF NOT EXISTS stripe_subscription_id TEXT"))
        db.session.execute(db.text("ALTER TABLE creator_subscription ADD COLUMN IF NOT EXISTS current_period_end TIMESTAMP"))
        db.session.execute(db.text("ALTER TABLE creator_subscription ADD COLUMN IF NOT EXISTS cancel_at_period_end BOOLEAN DEFAULT false"))
        db.session.execute(db.text("ALTER TABLE creator_subscription ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP"))

        db.session.execute(db.text("""
            INSERT INTO creator_subscription
                (creator_id, plan_key, status, storage_limit_gb, stripe_customer_id, stripe_subscription_id, current_period_end, updated_at)
            VALUES
                (:creator_id, :plan_key, :status, :storage_limit_gb, :stripe_customer_id, :stripe_subscription_id,
                 to_timestamp(NULLIF(:current_period_end,'')::double precision), CURRENT_TIMESTAMP)
            ON CONFLICT (creator_id) DO UPDATE SET
                plan_key=EXCLUDED.plan_key,
                status=EXCLUDED.status,
                storage_limit_gb=EXCLUDED.storage_limit_gb,
                stripe_customer_id=COALESCE(EXCLUDED.stripe_customer_id, creator_subscription.stripe_customer_id),
                stripe_subscription_id=COALESCE(EXCLUDED.stripe_subscription_id, creator_subscription.stripe_subscription_id),
                current_period_end=COALESCE(EXCLUDED.current_period_end, creator_subscription.current_period_end),
                updated_at=CURRENT_TIMESTAMP
        """), {
            "creator_id": creator_id,
            "plan_key": plan_key,
            "status": status or "active",
            "storage_limit_gb": storage_limit_gb,
            "stripe_customer_id": stripe_customer_id,
            "stripe_subscription_id": stripe_subscription_id,
            "current_period_end": str(current_period_end or ""),
        })

        db.session.execute(db.text("""
            UPDATE creator_profile
            SET storage_limit_gb=:storage_limit_gb,
                commission_rate=:commission_percent
            WHERE id=:creator_id
        """), {
            "creator_id": creator_id,
            "storage_limit_gb": storage_limit_gb,
            "commission_percent": commission_percent,
        })

        db.session.commit()
        return True
    except Exception as e:
        db.session.rollback()
        try:
            print("creator subscription apply v49.1N warning:", e)
        except Exception:
            pass
        return False

def _bsm_handle_creator_subscription_event_v491n(event):
    typ = event.get("type")
    obj = event.get("data", {}).get("object", {}) or {}

    if typ == "checkout.session.completed" and obj.get("mode") == "subscription":
        md = obj.get("metadata") or {}
        if md.get("billing_type") == "creator_subscription":
            return _bsm_apply_creator_subscription_v491n(
                creator_id=md.get("creator_id") or obj.get("client_reference_id"),
                plan_key=md.get("plan_key") or "free",
                status="active",
                stripe_customer_id=obj.get("customer"),
                stripe_subscription_id=obj.get("subscription"),
                current_period_end=None,
                storage_limit_gb=md.get("storage_limit_gb"),
                commission_percent=md.get("commission_percent"),
            )

    if typ in ("customer.subscription.created", "customer.subscription.updated"):
        md = obj.get("metadata") or {}
        if md.get("billing_type") == "creator_subscription":
            stripe_status = obj.get("status") or "active"
            status = "active" if stripe_status in ("active", "trialing") else stripe_status
            return _bsm_apply_creator_subscription_v491n(
                creator_id=md.get("creator_id"),
                plan_key=md.get("plan_key") or "free",
                status=status,
                stripe_customer_id=obj.get("customer"),
                stripe_subscription_id=obj.get("id"),
                current_period_end=obj.get("current_period_end"),
                storage_limit_gb=md.get("storage_limit_gb"),
                commission_percent=md.get("commission_percent"),
            )

    if typ == "customer.subscription.deleted":
        md = obj.get("metadata") or {}
        cid = md.get("creator_id")
        if cid:
            return _bsm_apply_creator_subscription_v491n(
                creator_id=cid,
                plan_key="free",
                status="active",
                stripe_customer_id=obj.get("customer"),
                stripe_subscription_id=obj.get("id"),
                current_period_end=obj.get("current_period_end"),
                storage_limit_gb=5,
                commission_percent=25,
            )
    return False



# v49.1O robust creator subscription webhook handler.
def _bsm_creator_plan_lookup_v491o_payments(plan_key):
    plan_key = (plan_key or "free").strip().lower()
    try:
        row = db.session.execute(db.text("""
            SELECT plan_key, storage_gb, commission_percent
            FROM creator_plan
            WHERE plan_key=:plan_key
            LIMIT 1
        """), {"plan_key": plan_key}).mappings().first()
        if row:
            return float(row.get("storage_gb") or 5), float(row.get("commission_percent") or 25)
    except Exception:
        db.session.rollback()
    defaults = {
        "free": (5,25),
        "starter": (150,25),
        "pro": (512,22),
        "studio": (2048,17),
    }
    return defaults.get(plan_key, defaults["free"])

def _bsm_apply_creator_subscription_v491o_payments(creator_id, plan_key, status="active", stripe_customer_id=None, stripe_subscription_id=None, current_period_end=None, storage_limit_gb=None, commission_percent=None):
    creator_id = int(creator_id or 0)
    if creator_id <= 0:
        return False
    plan_key = (plan_key or "free").strip().lower()
    plan_gb, plan_commission = _bsm_creator_plan_lookup_v491o_payments(plan_key)
    storage_limit_gb = float(storage_limit_gb or plan_gb or 5)
    commission_percent = float(commission_percent or plan_commission or 25)
    try:
        db.session.execute(db.text("""
            CREATE TABLE IF NOT EXISTS creator_subscription (
                id SERIAL PRIMARY KEY,
                creator_id INTEGER UNIQUE,
                plan_key TEXT,
                status TEXT,
                storage_limit_gb NUMERIC DEFAULT 5,
                stripe_customer_id TEXT,
                stripe_subscription_id TEXT,
                current_period_end TIMESTAMP,
                cancel_at_period_end BOOLEAN DEFAULT false,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.execute(db.text("ALTER TABLE creator_subscription ADD COLUMN IF NOT EXISTS storage_limit_gb NUMERIC DEFAULT 5"))
        db.session.execute(db.text("ALTER TABLE creator_subscription ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT"))
        db.session.execute(db.text("ALTER TABLE creator_subscription ADD COLUMN IF NOT EXISTS stripe_subscription_id TEXT"))
        db.session.execute(db.text("ALTER TABLE creator_subscription ADD COLUMN IF NOT EXISTS current_period_end TIMESTAMP"))
        db.session.execute(db.text("ALTER TABLE creator_subscription ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP"))

        try:
            period_epoch = int(current_period_end) if current_period_end else None
        except Exception:
            period_epoch = None

        db.session.execute(db.text("""
            INSERT INTO creator_subscription
                (creator_id, plan_key, status, storage_limit_gb, stripe_customer_id, stripe_subscription_id, current_period_end, updated_at)
            VALUES
                (:creator_id, :plan_key, :status, :storage_limit_gb, :stripe_customer_id, :stripe_subscription_id,
                 CASE WHEN :period_epoch IS NULL THEN NULL ELSE to_timestamp(:period_epoch) END, CURRENT_TIMESTAMP)
            ON CONFLICT (creator_id) DO UPDATE SET
                plan_key=EXCLUDED.plan_key,
                status=EXCLUDED.status,
                storage_limit_gb=EXCLUDED.storage_limit_gb,
                stripe_customer_id=COALESCE(EXCLUDED.stripe_customer_id, creator_subscription.stripe_customer_id),
                stripe_subscription_id=COALESCE(EXCLUDED.stripe_subscription_id, creator_subscription.stripe_subscription_id),
                current_period_end=COALESCE(EXCLUDED.current_period_end, creator_subscription.current_period_end),
                updated_at=CURRENT_TIMESTAMP
        """), {
            "creator_id": creator_id,
            "plan_key": plan_key,
            "status": status or "active",
            "storage_limit_gb": storage_limit_gb,
            "stripe_customer_id": stripe_customer_id,
            "stripe_subscription_id": stripe_subscription_id,
            "period_epoch": period_epoch,
        })
        db.session.execute(db.text("""
            UPDATE creator_profile
            SET storage_limit_gb=:storage_limit_gb,
                commission_rate=:commission_percent
            WHERE id=:creator_id
        """), {"creator_id": creator_id, "storage_limit_gb": storage_limit_gb, "commission_percent": commission_percent})
        db.session.commit()
        return True
    except Exception as e:
        db.session.rollback()
        try:
            print("creator subscription webhook apply v49.1O warning:", e)
        except Exception:
            pass
        return False

def _bsm_handle_creator_subscription_event_v491o_payments(event):
    typ = event.get("type")
    obj = (event.get("data") or {}).get("object") or {}
    md = obj.get("metadata") or {}

    if typ == "checkout.session.completed" and obj.get("mode") == "subscription":
        if md.get("billing_type") == "creator_subscription":
            return _bsm_apply_creator_subscription_v491o_payments(
                creator_id=md.get("creator_id") or obj.get("client_reference_id"),
                plan_key=md.get("plan_key") or "free",
                status="active",
                stripe_customer_id=obj.get("customer"),
                stripe_subscription_id=obj.get("subscription"),
                current_period_end=None,
                storage_limit_gb=md.get("storage_limit_gb"),
                commission_percent=md.get("commission_percent"),
            )

    if typ in ("customer.subscription.created", "customer.subscription.updated"):
        if md.get("billing_type") == "creator_subscription":
            stripe_status = obj.get("status") or "active"
            status = "active" if stripe_status in ("active", "trialing") else stripe_status
            return _bsm_apply_creator_subscription_v491o_payments(
                creator_id=md.get("creator_id"),
                plan_key=md.get("plan_key") or "free",
                status=status,
                stripe_customer_id=obj.get("customer"),
                stripe_subscription_id=obj.get("id"),
                current_period_end=obj.get("current_period_end"),
                storage_limit_gb=md.get("storage_limit_gb"),
                commission_percent=md.get("commission_percent"),
            )

    if typ == "customer.subscription.deleted":
        if md.get("creator_id"):
            return _bsm_apply_creator_subscription_v491o_payments(
                creator_id=md.get("creator_id"),
                plan_key="free",
                status="active",
                stripe_customer_id=obj.get("customer"),
                stripe_subscription_id=obj.get("id"),
                current_period_end=obj.get("current_period_end"),
                storage_limit_gb=5,
                commission_percent=25,
            )
    return False


@payments_bp.route("/stripe/webhook", methods=["POST"])
@payments_bp.route("/webhooks/stripe", methods=["POST"])
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
        _bsm_fix_order_item_creator_id_v460()
        
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
    # enforce checkout guard v49.1T
    _guard_resp_v491t = _bsm_abort_checkout_if_needed_v491t(items if 'items' in locals() else None)
    if _guard_resp_v491t is not None:
        return _guard_resp_v491t

    # Checkout login guard v42.9
    if not flask_session.get("user_id") or flask_session.get("user_role") != "buyer":
        flask_session["after_login_redirect"] = "/cart"
        flask_session.modified = True
        return redirect("/buyer/login?next=/cart")
    from app.services.cart import cart_summary, build_cart_display_items, current_cart_id
    summary = cart_summary()
    items = build_cart_display_items()
    cart_id = current_cart_id()
    _save_pending_cart_snapshot(cart_id, summary.get('items') or [], buyer_email=request.args.get('email'))
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

    # v49.1Q Stripe Connect split payment calculation
    connect_info_v491q = _bsm_connect_info_for_cart_v491q(items if 'items' in locals() else [])
    payment_intent_data_v491q = None
    if connect_info_v491q.get("enabled"):
        total_cents_v491q = 0
        try:
            for li in line_items:
                price_data = li.get("price_data") or {}
                total_cents_v491q += int(price_data.get("unit_amount") or 0) * int(li.get("quantity") or 1)
        except Exception:
            total_cents_v491q = int(round(float(total or amount_total or 0) * 100)) if ('total' in locals() or 'amount_total' in locals()) else 0
        fee_cents_v491q = _bsm_application_fee_cents_v491q(total_cents_v491q, connect_info_v491q.get("commission_rate"))
        if fee_cents_v491q > 0:
            payment_intent_data_v491q = {
                "application_fee_amount": fee_cents_v491q,
                "transfer_data": {"destination": connect_info_v491q.get("stripe_account_id")},
                "metadata": {
                    "connect_split": "true",
                    "creator_id": str(connect_info_v491q.get("creator_id")),
                    "creator_stripe_account_id": str(connect_info_v491q.get("stripe_account_id")),
                    "commission_rate": str(connect_info_v491q.get("commission_rate")),
                    "platform_fee_cents": str(fee_cents_v491q),
                },
            }

    session = stripe.checkout.Session.create(
        mode="payment",
            payment_intent_data=payment_intent_data_v491q,
        payment_method_types=["card"],
        line_items=line_items,
        metadata={"cart_checkout":"1", "cart_id": cart_id, "pending_discount_review": "False", "buyer_user_id": str(flask_session.get("user_id", "")), "buyer_login_email": str(flask_session.get("user_email", ""))},
        success_url=_public_base_url() + "/payment/success?session_id={CHECKOUT_SESSION_ID}",
        cancel_url=_public_base_url() + "/cart",
    )
    return redirect(session.url, code=303)


@payments_bp.route("/payment/received")
def payment_received_fallback():
    return _payment_success_safe_page("Payment received. If your purchase includes instant downloads, use the download buttons on this page. We will also email the link. Edited videos will be delivered after the creator uploads the final file.", [], None)
