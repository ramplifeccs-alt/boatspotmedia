from sqlalchemy import text
from app.services.db import db

def _pg():
    return db.engine.dialect.name == "postgresql"

def _add(conn, table, col, typ):
    conn.execute(text(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {typ}'))

def repair_creator_application_table():
    if not _pg(): return
    with db.engine.begin() as conn:
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
    if not _pg(): return
    with db.engine.begin() as conn:
        conn.execute(text('CREATE TABLE IF NOT EXISTS "user" (id SERIAL PRIMARY KEY)'))
        for col, typ in [
            ("email","VARCHAR(255)"),("password_hash","VARCHAR(255)"),("role","VARCHAR(50)"),
            ("display_name","VARCHAR(255)"),("language","VARCHAR(10) DEFAULT 'en'"),
            ("is_active","BOOLEAN DEFAULT TRUE"),("created_at","TIMESTAMP DEFAULT CURRENT_TIMESTAMP")]:
            _add(conn, '"user"', col, typ)

        conn.execute(text('CREATE TABLE IF NOT EXISTS storage_plan (id SERIAL PRIMARY KEY)'))
        for col, typ in [("name","VARCHAR(120)"),("storage_limit_gb","INTEGER DEFAULT 512"),("monthly_price","NUMERIC(10,2) DEFAULT 0"),("commission_rate","INTEGER DEFAULT 20"),("active","BOOLEAN DEFAULT TRUE")]:
            _add(conn, "storage_plan", col, typ)

        conn.execute(text('CREATE TABLE IF NOT EXISTS creator_profile (id SERIAL PRIMARY KEY)'))
        for col, typ in [
            ("user_id","INTEGER"),("plan_id","INTEGER"),("storage_limit_gb","INTEGER DEFAULT 512"),("storage_used_bytes","BIGINT DEFAULT 0"),
            ("commission_rate","INTEGER DEFAULT 20"),("commission_override_rate","INTEGER"),("commission_override_until","TIMESTAMP"),("commission_override_reason","VARCHAR(500)"),
            ("product_commission_rate","INTEGER DEFAULT 20"),("product_commission_override_rate","INTEGER"),("product_commission_override_until","TIMESTAMP"),("product_commission_override_reason","VARCHAR(500)"),
            ("second_clip_discount_percent","INTEGER DEFAULT 0"),("approved","BOOLEAN DEFAULT FALSE"),("suspended","BOOLEAN DEFAULT FALSE"),("created_at","TIMESTAMP DEFAULT CURRENT_TIMESTAMP")]:
            _add(conn, "creator_profile", col, typ)

        conn.execute(text('CREATE TABLE IF NOT EXISTS product (id SERIAL PRIMARY KEY)'))
        for col, typ in [("creator_id","INTEGER"),("title","VARCHAR(200)"),("description","TEXT"),("price","NUMERIC(10,2)"),("shipping_cost","NUMERIC(10,2) DEFAULT 0"),("processing_time","VARCHAR(120)"),("shipping_method","VARCHAR(120)"),("active","BOOLEAN DEFAULT TRUE")]:
            _add(conn, "product", col, typ)

        conn.execute(text('CREATE TABLE IF NOT EXISTS product_variant (id SERIAL PRIMARY KEY)'))
        for col, typ in [("product_id","INTEGER"),("variant_name","VARCHAR(120)"),("variant_value","VARCHAR(200)"),("color_name","VARCHAR(80)"),("color_hex","VARCHAR(20)"),("price_adjustment","NUMERIC(10,2) DEFAULT 0"),("active","BOOLEAN DEFAULT TRUE"),("created_at","TIMESTAMP DEFAULT CURRENT_TIMESTAMP")]:
            _add(conn, "product_variant", col, typ)

        conn.execute(text('CREATE TABLE IF NOT EXISTS product_variant_image (id SERIAL PRIMARY KEY)'))
        for col, typ in [("variant_id","INTEGER"),("image_url","VARCHAR(800)"),("sort_order","INTEGER DEFAULT 0")]:
            _add(conn, "product_variant_image", col, typ)

        conn.execute(text('CREATE TABLE IF NOT EXISTS commission_override_log (id SERIAL PRIMARY KEY)'))
        for col, typ in [("creator_id","INTEGER"),("commission_type","VARCHAR(50)"),("old_rate","INTEGER"),("new_rate","INTEGER"),("days","INTEGER"),("reason","VARCHAR(500)"),("expires_at","TIMESTAMP"),("created_at","TIMESTAMP DEFAULT CURRENT_TIMESTAMP")]:
            _add(conn, "commission_override_log", col, typ)

def repair_all_known_tables():
    repair_basic_tables()
    repair_creator_application_table()
