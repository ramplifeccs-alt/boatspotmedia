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



# BoatSpotMedia v43.6 direct app-level R2 download routes
def _bsm_r2_download_response_v436(video_id):
    from flask import session, redirect
    try:
        from app import db
    except Exception:
        from __main__ import db

    if not session.get("user_id") or session.get("user_role") != "buyer":
        session["after_login_redirect"] = "/buyer/dashboard"
        session.modified = True
        return redirect("/buyer/login?next=/buyer/dashboard")

    uid = session.get("user_id")
    email = (session.get("user_email") or "").lower()

    try:
        db.session.execute(db.text("ALTER TABLE bsm_cart_order ADD COLUMN IF NOT EXISTS buyer_user_id INTEGER"))
        db.session.commit()
    except Exception:
        db.session.rollback()

    purchased = None

    # First treat id as video_id.
    try:
        purchased = db.session.execute(db.text("""
            SELECT i.id AS order_item_id, i.video_id, i.delivery_status, i.package,
                   o.id AS order_id, o.buyer_user_id, o.buyer_email, o.status
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            WHERE i.video_id = :vid
              AND (
                    o.buyer_user_id = :uid
                    OR lower(coalesce(o.buyer_email,'')) = lower(:email)
                  )
            ORDER BY o.created_at DESC
            LIMIT 1
        """), {"vid": video_id, "uid": uid or 0, "email": email}).mappings().first()
    except Exception:
        db.session.rollback()
        purchased = None

    # Fallback treat id as order_item_id.
    if not purchased:
        try:
            purchased = db.session.execute(db.text("""
                SELECT i.id AS order_item_id, i.video_id, i.delivery_status, i.package,
                       o.id AS order_id, o.buyer_user_id, o.buyer_email, o.status
                FROM bsm_cart_order_item i
                JOIN bsm_cart_order o ON o.id = i.cart_order_id
                WHERE i.id = :item_id
                  AND (
                        o.buyer_user_id = :uid
                        OR lower(coalesce(o.buyer_email,'')) = lower(:email)
                      )
                ORDER BY o.created_at DESC
                LIMIT 1
            """), {"item_id": video_id, "uid": uid or 0, "email": email}).mappings().first()
        except Exception:
            db.session.rollback()
            purchased = None

    if not purchased:
        return "Download not found: this video is not linked to your paid orders.", 404

    if str(purchased.get("delivery_status") or "").lower() in ["pending_edit","editing","not_ready","pending"]:
        return "This edited video is not ready for download yet.", 400

    real_video_id = purchased.get("video_id") or video_id

    try:
        video = db.session.execute(db.text("SELECT * FROM video WHERE id=:vid LIMIT 1"), {"vid": real_video_id}).mappings().first()
    except Exception:
        db.session.rollback()
        video = None

    if not video:
        return "Video record not found.", 404

    # IMPORTANT: use R2 key/path columns first.
    r2_keys = [
        "r2_video_key",
        "r2_key",
        "video_key",
        "storage_key",
        "file_path",
        "original_file_path",
        "original_path",
        "internal_filename",
        "filename",
    ]
    url_keys = [
        "public_url",
        "download_url",
        "file_url",
        "original_url",
        "video_url",
        "r2_public_url",
    ]

    # If a full public/signed URL exists, use it.
    for key in url_keys:
        val = video.get(key) if key in video else None
        if val:
            val = str(val).strip()
            if val.startswith("http://") or val.startswith("https://") or val.startswith("/"):
                return redirect(val)

    # For R2 object keys, reuse app's /media/<key> route.
    # This app already serves thumbnails/previews/videos from R2 through /media.
    for key in r2_keys:
        val = video.get(key) if key in video else None
        if val:
            val = str(val).strip()
            if not val:
                continue
            if val.startswith("http://") or val.startswith("https://") or val.startswith("/"):
                return redirect(val)
            return redirect("/media/" + val.lstrip("/"))

    return "R2 video key was not found in the video record. Contact support with Order #" + str(purchased.get("order_id")), 404


try:
    @app.route("/download-video/<int:video_id>")
    @app.route("/download-item/<int:video_id>")
    @app.route("/buyer/download-item/<int:video_id>")
    def bsm_app_download_video_v436(video_id):
        return _bsm_r2_download_response_v436(video_id)
except Exception as e:
    try:
        print("v43.6 app-level download route warning:", e)
    except Exception:
        pass

