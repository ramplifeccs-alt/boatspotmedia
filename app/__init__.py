from flask import Flask
from config import Config
from .services.db import db


def create_app():
    flask_app = Flask(__name__, template_folder="templates", static_folder="static")
    flask_app.config.from_object(Config)
    db.init_app(flask_app)

    from .routes.public import public_bp
    from .routes.creator import creator_bp
    from .routes.owner import owner_bp

    flask_app.register_blueprint(public_bp)
    flask_app.register_blueprint(creator_bp, url_prefix="/creator")
    flask_app.register_blueprint(owner_bp, url_prefix="/owner")

    for import_path, prefix, label in [
        (".routes.buyer", "/buyer", "buyer"),
        (".routes.advertiser", "/advertiser", "advertiser"),
        (".routes.charters", "/charters", "charters"),
    ]:
        try:
            module = __import__(__name__ + import_path, fromlist=["*"])
            bp = getattr(module, f"{label}_bp")
            flask_app.register_blueprint(bp, url_prefix=prefix)
        except Exception as e:
            print(f"{label} blueprint skipped:", e)

    try:
        from .routes.services_panel import services_bp
        flask_app.register_blueprint(services_bp)
    except Exception as e:
        print("services blueprint skipped:", e)

    with flask_app.app_context():
        try:
            db.create_all()
        except Exception as e:
            print("db.create_all warning:", e)
        try:
            from .services.db_repair import repair_all_known_tables
            repair_all_known_tables()
        except Exception as e:
            print("DB repair warning:", e)
        try:
            seed_owner_and_default_data()
        except Exception as e:
            print("Seed warning:", e)

    @flask_app.context_processor
    def inject_latest_previews():
        try:
            from app.routes.public import _home_preview_videos
            return {"latest_previews": _home_preview_videos()}
        except Exception:
            return {"latest_previews": []}


    # BoatSpotMedia v43.7 create_app R2 presigned download routes
    def _bsm_download_video_v437(video_ref):
        from flask import session, redirect, request
        import os

        if not session.get("user_id") or session.get("user_role") != "buyer":
            session["after_login_redirect"] = "/buyer/dashboard"
            session.modified = True
            return redirect("/buyer/login?next=/buyer/dashboard")

        try:
            ref = int(str(video_ref).strip())
        except Exception:
            return "Invalid download link.", 404

        uid = session.get("user_id")
        email = (session.get("user_email") or "").lower()

        try:
            db.session.execute(db.text("ALTER TABLE bsm_cart_order ADD COLUMN IF NOT EXISTS buyer_user_id INTEGER"))
            db.session.commit()
        except Exception:
            db.session.rollback()

        purchased = None

        # Try ref as video_id in cart order system.
        try:
            purchased = db.session.execute(db.text("""
                SELECT i.*, i.id AS order_item_id, i.video_id, i.delivery_status, i.package,
                       o.id AS order_id, o.buyer_user_id, o.buyer_email, o.status
                FROM bsm_cart_order_item i
                JOIN bsm_cart_order o ON o.id = i.cart_order_id
                WHERE i.video_id = :ref
                  AND (
                        o.buyer_user_id = :uid
                        OR lower(coalesce(o.buyer_email,'')) = lower(:email)
                      )
                ORDER BY o.created_at DESC
                LIMIT 1
            """), {"ref": ref, "uid": uid or 0, "email": email}).mappings().first()
        except Exception:
            db.session.rollback()

        # Try ref as order_item_id in cart order system.
        if not purchased:
            try:
                purchased = db.session.execute(db.text("""
                    SELECT i.*, i.id AS order_item_id, i.video_id, i.delivery_status, i.package,
                           o.id AS order_id, o.buyer_user_id, o.buyer_email, o.status
                    FROM bsm_cart_order_item i
                    JOIN bsm_cart_order o ON o.id = i.cart_order_id
                    WHERE i.id = :ref
                      AND (
                            o.buyer_user_id = :uid
                            OR lower(coalesce(o.buyer_email,'')) = lower(:email)
                          )
                    ORDER BY o.created_at DESC
                    LIMIT 1
                """), {"ref": ref, "uid": uid or 0, "email": email}).mappings().first()
            except Exception:
                db.session.rollback()

        # Legacy order/order_item fallback.
        if not purchased:
            try:
                purchased = db.session.execute(db.text("""
                    SELECT oi.id AS order_item_id, oi.video_id, oi.edited_status AS delivery_status, oi.purchase_type AS package,
                           o.id AS order_id, o.buyer_id AS buyer_user_id, o.buyer_email, o.status
                    FROM order_item oi
                    JOIN "order" o ON o.id = oi.order_id
                    WHERE (oi.video_id = :ref OR oi.id = :ref)
                      AND (
                            o.buyer_id = :uid
                            OR lower(coalesce(o.buyer_email,'')) = lower(:email)
                          )
                    ORDER BY o.created_at DESC
                    LIMIT 1
                """), {"ref": ref, "uid": uid or 0, "email": email}).mappings().first()
            except Exception:
                db.session.rollback()

        if not purchased:
            return "Download not found: this video is not linked to your paid orders.", 404

        if str(purchased.get("delivery_status") or "").lower() in ["pending_edit","editing","not_ready","pending"]:
            return "This video is not ready for download yet.", 400

        video_id = purchased.get("video_id") or ref

        try:
            video = db.session.execute(db.text("SELECT * FROM video WHERE id=:vid LIMIT 1"), {"vid": video_id}).mappings().first()
        except Exception:
            db.session.rollback()
            video = None

        if not video:
            return "Video record not found.", 404

        # Prefer actual R2 object key/path.
        r2_key = None
        for key in ["r2_video_key", "file_path", "r2_key", "video_key", "storage_key", "original_file_path", "original_path"]:
            if key in video and video.get(key):
                value = str(video.get(key)).strip()
                if value and not value.startswith("http://") and not value.startswith("https://"):
                    r2_key = value.lstrip("/")
                    break

        # If public URL exists, use it.
        for key in ["public_url", "download_url", "file_url", "original_url", "video_url", "r2_public_url"]:
            if key in video and video.get(key):
                value = str(video.get(key)).strip()
                if value.startswith("http://") or value.startswith("https://"):
                    return redirect(value)

        if not r2_key:
            # Some DB rows may only have filename/internal_filename, but those are not enough unless they are full R2 keys.
            for key in ["internal_filename", "filename"]:
                if key in video and video.get(key):
                    value = str(video.get(key)).strip()
                    if "/" in value:
                        r2_key = value.lstrip("/")
                        break

        if not r2_key:
            return "R2 video key was not found in this video record. Contact support with Order #" + str(purchased.get("order_id")), 404

        # v43.9: Force download on mobile/desktop with R2 presigned URL and attachment header.
        filename = str(video.get("filename") or video.get("internal_filename") or r2_key.split("/")[-1])
        try:
            from app.services.r2 import r2_client, _bucket_name
            client = r2_client()
            bucket = _bucket_name()
            signed_url = client.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": bucket,
                    "Key": r2_key.lstrip("/"),
                    "ResponseContentDisposition": f'attachment; filename="{filename}"'
                },
                ExpiresIn=60 * 20,
            )
            return redirect(signed_url)
        except Exception as e:
            try:
                print("R2 forced download presign warning v43.9:", e)
            except Exception:
                pass
            # Fallback to public URL if presign fails.
            public_base = (os.environ.get("R2_PUBLIC_URL") or os.environ.get("R2_PUBLIC_BASE_URL") or "").strip().rstrip("/")
            if public_base:
                return redirect(public_base + "/" + r2_key.lstrip("/"))
            return "Could not create forced R2 download link. Contact support with Order #" + str(purchased.get("order_id")), 500

    flask_app.add_url_rule("/download-video/<path:video_ref>", "bsm_download_video_v437", _bsm_download_video_v437)
    flask_app.add_url_rule("/download-item/<path:video_ref>", "bsm_download_item_v437", _bsm_download_video_v437)
    flask_app.add_url_rule("/buyer/download-item/<path:video_ref>", "bsm_buyer_download_item_v437", _bsm_download_video_v437)



    # BoatSpotMedia v44.1 download route: original vs edited + 72h timer
    def _bsm_download_video_v441(video_ref):
        from flask import session, redirect, request
        from datetime import datetime, timezone, timedelta
        import os

        if not session.get("user_id") or session.get("user_role") != "buyer":
            session["after_login_redirect"] = "/buyer/dashboard"
            session.modified = True
            return redirect("/buyer/login?next=/buyer/dashboard")

        try:
            ref = int(str(video_ref).strip())
        except Exception:
            return "Invalid download link.", 404

        uid = session.get("user_id")
        email = (session.get("user_email") or "").lower()

        try:
            db.session.execute(db.text("ALTER TABLE bsm_cart_order ADD COLUMN IF NOT EXISTS buyer_user_id INTEGER"))
            db.session.execute(db.text("ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_r2_key TEXT"))
            db.session.execute(db.text("ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_uploaded_at TIMESTAMP"))
            db.session.commit()
        except Exception:
            db.session.rollback()

        purchased = None

        # Prefer exact order item id, because same video can have original + edited.
        try:
            purchased = db.session.execute(db.text("""
                SELECT i.*, i.id AS order_item_id,
                       o.id AS order_id, o.buyer_user_id, o.buyer_email, o.status, o.created_at AS order_created_at
                FROM bsm_cart_order_item i
                JOIN bsm_cart_order o ON o.id = i.cart_order_id
                WHERE i.id = :ref
                  AND (
                        o.buyer_user_id = :uid
                        OR lower(coalesce(o.buyer_email,'')) = lower(:email)
                      )
                ORDER BY o.created_at DESC
                LIMIT 1
            """), {"ref": ref, "uid": uid or 0, "email": email}).mappings().first()
        except Exception:
            db.session.rollback()

        # Fallback as video id.
        if not purchased:
            try:
                purchased = db.session.execute(db.text("""
                    SELECT i.*, i.id AS order_item_id,
                           o.id AS order_id, o.buyer_user_id, o.buyer_email, o.status, o.created_at AS order_created_at
                    FROM bsm_cart_order_item i
                    JOIN bsm_cart_order o ON o.id = i.cart_order_id
                    WHERE i.video_id = :ref
                      AND (
                            o.buyer_user_id = :uid
                            OR lower(coalesce(o.buyer_email,'')) = lower(:email)
                          )
                    ORDER BY
                      CASE WHEN lower(coalesce(i.package,'')) IN ('original','instant','instant_download','download','4k','original_4k') THEN 0 ELSE 1 END,
                      o.created_at DESC
                    LIMIT 1
                """), {"ref": ref, "uid": uid or 0, "email": email}).mappings().first()
            except Exception:
                db.session.rollback()

        if not purchased:
            return "Download not found: this video is not linked to your paid orders.", 404

        package = str(purchased.get("package") or "").lower()
        requested_delivery_v443 = str(request.args.get("delivery") or "").lower()
        is_bundle_v443 = package in ["bundle", "combo", "original_plus_edited", "original_edited", "original+edited", "original_edit"]
        if is_bundle_v443 and requested_delivery_v443 in ["original", "edited"]:
            is_edited = requested_delivery_v443 == "edited"
        else:
            is_edited = package in ["edited", "edit", "instagram_edit", "tiktok_edit", "reel_edit", "short_edit"]

        discount_status = str(purchased.get("discount_status") or "").lower()
        if discount_status in ["pending_review", "pending", "awaiting_creator", "needs_approval"]:
            return "Download is pending creator approval.", 403

        # 72h window start: original from purchase; edited from edited upload.
        if is_edited:
            edited_key = str(purchased.get("edited_r2_key") or "").strip()
            if not edited_key or str(purchased.get("delivery_status") or "").lower() in ["pending_edit", "editing", "not_ready", "pending"]:
                return "This edited video is not ready for download yet.", 400
            start_time = purchased.get("edited_uploaded_at") or purchased.get("order_created_at")
        else:
            start_time = purchased.get("order_created_at")

        try:
            if start_time:
                if getattr(start_time, "tzinfo", None) is None:
                    start_time = start_time.replace(tzinfo=timezone.utc)
                expires_at = start_time + timedelta(hours=72)
                if datetime.now(timezone.utc) > expires_at:
                    return "Download expired. This file was available for 72 hours.", 403
        except Exception:
            pass

        video_id = purchased.get("video_id") or ref
        try:
            video = db.session.execute(db.text("SELECT * FROM video WHERE id=:vid LIMIT 1"), {"vid": video_id}).mappings().first()
        except Exception:
            db.session.rollback()
            video = None

        if not video:
            return "Video record not found.", 404

        r2_key = None
        if is_edited:
            r2_key = str(purchased.get("edited_r2_key") or "").strip().lstrip("/")
        else:
            for key in ["r2_video_key", "file_path", "r2_key", "video_key", "storage_key", "original_file_path", "original_path"]:
                if key in video and video.get(key):
                    value = str(video.get(key)).strip()
                    if value and not value.startswith("http://") and not value.startswith("https://"):
                        r2_key = value.lstrip("/")
                        break

        if not r2_key and not is_edited:
            for key in ["public_url", "download_url", "file_url", "original_url", "video_url", "r2_public_url"]:
                if key in video and video.get(key):
                    value = str(video.get(key)).strip()
                    if value.startswith("http://") or value.startswith("https://"):
                        return redirect(value)

        if not r2_key:
            return "R2 video key was not found for this item. Contact support with Order #" + str(purchased.get("order_id")), 404

        filename = str(video.get("filename") or video.get("internal_filename") or r2_key.split("/")[-1])
        if is_edited:
            filename = "edited_" + filename

        try:
            from app.services.r2 import r2_client, _bucket_name
            signed_url = r2_client().generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": _bucket_name(),
                    "Key": r2_key,
                    "ResponseContentDisposition": f'attachment; filename="{filename}"'
                },
                ExpiresIn=60 * 20,
            )
            return redirect(signed_url)
        except Exception as e:
            try:
                print("R2 forced download warning v44.1:", e)
            except Exception:
                pass
            public_base = (os.environ.get("R2_PUBLIC_URL") or os.environ.get("R2_PUBLIC_BASE_URL") or "").strip().rstrip("/")
            if public_base:
                return redirect(public_base + "/" + r2_key)
            return "Could not create R2 download link. Contact support with Order #" + str(purchased.get("order_id")), 500

    flask_app.add_url_rule("/download-video/<path:video_ref>", "bsm_download_video_v441", _bsm_download_video_v441)
    flask_app.add_url_rule("/download-item/<path:video_ref>", "bsm_download_item_v441", _bsm_download_video_v441)
    flask_app.add_url_rule("/buyer/download-item/<path:video_ref>", "bsm_buyer_download_item_v441", _bsm_download_video_v441)


    return flask_app


def seed_owner_and_default_data():
    from .models import User, StoragePlan, Location
    from werkzeug.security import generate_password_hash
    if not User.query.filter_by(role="owner").first():
        db.session.add(User(email="owner@boatspotmedia.com", password_hash=generate_password_hash("ChangeMe123!"), role="owner", display_name="BoatSpotMedia Owner", is_active=True))
    if not StoragePlan.query.first():
        db.session.add_all([
            StoragePlan(name="Starter 128GB", storage_limit_gb=128, monthly_price=29.00, commission_rate=30),
            StoragePlan(name="Creator 512GB", storage_limit_gb=512, monthly_price=79.00, commission_rate=20),
            StoragePlan(name="Studio 2TB", storage_limit_gb=2048, monthly_price=199.00, commission_rate=10),
        ])
    if not Location.query.first():
        db.session.add_all([Location(name=n) for n in ["Boca Raton Inlet","Hillsboro Inlet","Boynton Inlet","Haulover Inlet","Port Everglades","Government Cut","Jupiter Inlet","Lake Worth Inlet","Palm Beach Inlet"]])
    db.session.commit()


app = create_app()

try:
    from app.routes.payments import payments_bp
    app.register_blueprint(payments_bp)
except Exception as e:
    print("payments blueprint registration warning:", e)

try:
    from app.routes.cart import cart_bp
    app.register_blueprint(cart_bp)
except Exception as e:
    print("cart blueprint registration warning:", e)



# BoatSpotMedia buyer routes registration v42.1
try:
    from app.routes.buyer import buyer_bp
    if "buyer" not in app.blueprints:
        app.register_blueprint(buyer_bp)
except Exception as e:
    try:
        print("buyer blueprint registration warning:", e)
    except Exception:
        pass

