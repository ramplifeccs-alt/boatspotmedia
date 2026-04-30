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

# BoatSpotMedia buyer routes registration v41.6
try:
    from app.routes.buyer import buyer_bp
    app.register_blueprint(buyer_bp)
except Exception as e:
    try:
        print("buyer blueprint registration warning:", e)
    except Exception:
        pass
