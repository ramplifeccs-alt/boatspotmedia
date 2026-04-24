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
    from .routes.services_panel import services_bp
    from .routes.buyer import buyer_bp
    from .routes.advertiser import advertiser_bp
    from .routes.charters import charters_bp

    flask_app.register_blueprint(public_bp)
    flask_app.register_blueprint(creator_bp, url_prefix="/creator")
    flask_app.register_blueprint(owner_bp, url_prefix="/owner")
    app.register_blueprint(services_bp)
    flask_app.register_blueprint(buyer_bp, url_prefix="/buyer")
    flask_app.register_blueprint(advertiser_bp, url_prefix="/advertiser")
    flask_app.register_blueprint(charters_bp, url_prefix="/charters")

    with flask_app.app_context():
        db.create_all()
        try:
            from .services.db_repair import repair_all_known_tables
            repair_all_known_tables()
        except Exception as e:
            print("DB repair warning:", e)
        seed_owner_and_default_data()

    return flask_app

def seed_owner_and_default_data():
    from .models import User, StoragePlan, Location
    from werkzeug.security import generate_password_hash

    owner = User.query.filter_by(role="owner").first()
    if not owner:
        db.session.add(User(
            email="owner@boatspotmedia.com",
            password_hash=generate_password_hash("ChangeMe123!"),
            role="owner",
            display_name="BoatSpotMedia Owner",
            is_active=True
        ))

    if not StoragePlan.query.first():
        db.session.add_all([
            StoragePlan(name="Starter 128GB", storage_limit_gb=128, monthly_price=29.00, commission_rate=30),
            StoragePlan(name="Creator 512GB", storage_limit_gb=512, monthly_price=79.00, commission_rate=20),
            StoragePlan(name="Studio 2TB", storage_limit_gb=2048, monthly_price=199.00, commission_rate=10),
        ])

    if not Location.query.first():
        db.session.add_all([Location(name=n) for n in [
            "Boca Raton Inlet", "Hillsboro Inlet", "Boynton Inlet", "Haulover Inlet",
            "Port Everglades", "Government Cut", "Jupiter Inlet", "Lake Worth Inlet",
            "St. Lucie Inlet", "Palm Beach Inlet"
        ]])

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        print("Seed warning:", e)

app = create_app()
