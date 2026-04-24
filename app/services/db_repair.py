from sqlalchemy import text
from app.services.db import db

def repair_creator_application_table():
    with db.engine.begin() as conn:
        if db.engine.dialect.name == "postgresql":
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS creator_application (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(120),
                    last_name VARCHAR(120),
                    brand_name VARCHAR(255),
                    email VARCHAR(255),
                    instagram VARCHAR(255),
                    status VARCHAR(50) DEFAULT 'pending',
                    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reviewed_at TIMESTAMP
                )
            """))

def repair_basic_tables():
    if db.engine.dialect.name != "postgresql":
        return
    with db.engine.begin() as conn:
        conn.execute(text('CREATE TABLE IF NOT EXISTS "user" (id SERIAL PRIMARY KEY)'))
        for col, typ in [
            ("email", "VARCHAR(255)"), ("password_hash", "VARCHAR(255)"), ("role", "VARCHAR(50)"),
            ("display_name", "VARCHAR(255)"), ("language", "VARCHAR(10) DEFAULT 'en'"),
            ("is_active", "BOOLEAN DEFAULT TRUE"), ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        ]:
            conn.execute(text(f'ALTER TABLE "user" ADD COLUMN IF NOT EXISTS {col} {typ}'))

        conn.execute(text('CREATE TABLE IF NOT EXISTS creator_profile (id SERIAL PRIMARY KEY)'))
        for col, typ in [
            ("user_id", "INTEGER"), ("plan_id", "INTEGER"), ("storage_limit_gb", "INTEGER DEFAULT 512"),
            ("storage_used_bytes", "BIGINT DEFAULT 0"), ("commission_rate", "INTEGER DEFAULT 20"),
            ("commission_override_rate", "INTEGER"), ("commission_override_until", "TIMESTAMP"),
            ("product_commission_rate", "INTEGER DEFAULT 20"),
            ("product_commission_override_rate", "INTEGER"),
            ("product_commission_override_until", "TIMESTAMP"),
            ("second_clip_discount_percent", "INTEGER DEFAULT 0"),
            ("approved", "BOOLEAN DEFAULT FALSE"), ("suspended", "BOOLEAN DEFAULT FALSE"),
            ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        ]:
            conn.execute(text(f'ALTER TABLE creator_profile ADD COLUMN IF NOT EXISTS {col} {typ}'))

        conn.execute(text('CREATE TABLE IF NOT EXISTS video_pricing_preset (id SERIAL PRIMARY KEY)'))
        for col, typ in [
            ("creator_id", "INTEGER"), ("title", "VARCHAR(200) DEFAULT 'Default Video Price'"),
            ("description", "TEXT"), ("price", "NUMERIC(10,2) DEFAULT 40"),
            ("delivery_type", "VARCHAR(50) DEFAULT 'instant'"),
            ("is_default", "BOOLEAN DEFAULT FALSE"), ("active", "BOOLEAN DEFAULT TRUE"),
            ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        ]:
            conn.execute(text(f'ALTER TABLE video_pricing_preset ADD COLUMN IF NOT EXISTS {col} {typ}'))

        conn.execute(text('CREATE TABLE IF NOT EXISTS product (id SERIAL PRIMARY KEY)'))
        for col, typ in [
            ("creator_id", "INTEGER"), ("title", "VARCHAR(200)"), ("description", "TEXT"),
            ("price", "NUMERIC(10,2)"), ("shipping_cost", "NUMERIC(10,2) DEFAULT 0"),
            ("processing_time", "VARCHAR(120)"), ("shipping_method", "VARCHAR(120)"),
            ("active", "BOOLEAN DEFAULT TRUE")
        ]:
            conn.execute(text(f'ALTER TABLE product ADD COLUMN IF NOT EXISTS {col} {typ}'))

def repair_all_known_tables():
    repair_basic_tables()
    repair_creator_application_table()
