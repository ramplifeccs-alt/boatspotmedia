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
    from .routes.buyer import buyer_bp
    from .routes.advertiser import advertiser_bp
    from .routes.charters import charters_bp

    flask_app.register_blueprint(public_bp)
    flask_app.register_blueprint(creator_bp, url_prefix="/creator")
    flask_app.register_blueprint(owner_bp, url_prefix="/owner")
    flask_app.register_blueprint(buyer_bp, url_prefix="/buyer")
    flask_app.register_blueprint(advertiser_bp, url_prefix="/advertiser")
    flask_app.register_blueprint(charters_bp, url_prefix="/charters")

    with flask_app.app_context():
        db.create_all()
        run_safe_startup_migrations()
        seed_owner_and_default_data()

    return flask_app

def run_safe_startup_migrations():
    # This build is for testing. It safely adds missing columns if Railway PostgreSQL
    # already has older tables from previous builds.
    from sqlalchemy import text
    engine = db.engine
    dialect = engine.dialect.name

    def has_table(table_name):
        with engine.connect() as conn:
            if dialect == "postgresql":
                row = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema='public' AND table_name=:table
                    )
                """), {"table": table_name}).scalar()
                return bool(row)
            else:
                row = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table"), {"table": table_name}).first()
                return row is not None

    def has_column(table_name, column_name):
        with engine.connect() as conn:
            if dialect == "postgresql":
                row = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns
                        WHERE table_schema='public' AND table_name=:table AND column_name=:column
                    )
                """), {"table": table_name, "column": column_name}).scalar()
                return bool(row)
            else:
                rows = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
                return any(r[1] == column_name for r in rows)

    def add_column(table_name, column_sql):
        with engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS {column_sql}' if dialect == "postgresql" else f'ALTER TABLE {table_name} ADD COLUMN {column_sql}'))

    # User table can be named "user" by SQLAlchemy; keep compatibility.
    if has_table("user"):
        needed = {
            "display_name": "display_name VARCHAR(255)",
            "language": "language VARCHAR(10) DEFAULT 'en'",
            "is_active": "is_active BOOLEAN DEFAULT TRUE",
            "created_at": "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "password_hash": "password_hash VARCHAR(255)",
            "role": "role VARCHAR(50)"
        }
        for col, sql in needed.items():
            if not has_column("user", col):
                add_column("user", sql)

    # Add common missing columns in other tables from earlier builds.
    table_columns = {
        "creator_profile": {
            "storage_used_bytes": "storage_used_bytes BIGINT DEFAULT 0",
            "second_clip_discount_percent": "second_clip_discount_percent INTEGER DEFAULT 0",
            "commission_override_rate": "commission_override_rate INTEGER",
            "commission_override_until": "commission_override_until TIMESTAMP",
            "suspended": "suspended BOOLEAN DEFAULT FALSE",
            "approved": "approved BOOLEAN DEFAULT FALSE"
        },
        "video": {
            "public_thumbnail_url": "public_thumbnail_url VARCHAR(800)",
            "internal_filename": "internal_filename VARCHAR(500)",
            "original_price": "original_price NUMERIC(10,2) DEFAULT 40",
            "edited_price": "edited_price NUMERIC(10,2) DEFAULT 60",
            "bundle_price": "bundle_price NUMERIC(10,2) DEFAULT 80",
            "status": "status VARCHAR(50) DEFAULT 'active'"
        },
        "download_token": {
            "download_count": "download_count INTEGER DEFAULT 0"
        }
    }

    for table, cols in table_columns.items():
        if has_table(table):
            for col, sql in cols.items():
                if not has_column(table, col):
                    add_column(table, sql)

def seed_owner_and_default_data():
    from .models import User, StoragePlan, Location
    from werkzeug.security import generate_password_hash

    owner = User.query.filter_by(role="owner").first()
    if not owner:
        owner = User(
            email="owner@boatspotmedia.com",
            password_hash=generate_password_hash("ChangeMe123!"),
            role="owner",
            display_name="BoatSpotMedia Owner",
            is_active=True
        )
        db.session.add(owner)

    if not StoragePlan.query.first():
        plans = [
            StoragePlan(name="Starter 128GB", storage_limit_gb=128, monthly_price=29.00, commission_rate=30),
            StoragePlan(name="Creator 512GB", storage_limit_gb=512, monthly_price=79.00, commission_rate=20),
            StoragePlan(name="Studio 2TB", storage_limit_gb=2048, monthly_price=199.00, commission_rate=10),
        ]
        db.session.add_all(plans)

    if not Location.query.first():
        names = [
            "Boca Raton Inlet", "Hillsboro Inlet", "Boynton Inlet", "Haulover Inlet",
            "Port Everglades", "Government Cut", "Jupiter Inlet", "Lake Worth Inlet",
            "St. Lucie Inlet", "Palm Beach Inlet"
        ]
        db.session.add_all([Location(name=n) for n in names])

    db.session.commit()

app = create_app()
