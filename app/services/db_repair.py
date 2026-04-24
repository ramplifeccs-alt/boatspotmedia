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
            ("is_active","BOOLEAN DEFAULT TRUE"),("created_at","TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        ]: _add(conn, '"user"', col, typ)

        conn.execute(text('CREATE TABLE IF NOT EXISTS creator_profile (id SERIAL PRIMARY KEY)'))
        for col, typ in [
            ("user_id","INTEGER"),("plan_id","INTEGER"),("storage_limit_gb","INTEGER DEFAULT 512"),
            ("storage_used_bytes","BIGINT DEFAULT 0"),("commission_rate","INTEGER DEFAULT 20"),
            ("commission_override_rate","INTEGER"),("commission_override_until","TIMESTAMP"),
            ("product_commission_rate","INTEGER DEFAULT 20"),("product_commission_override_rate","INTEGER"),
            ("product_commission_override_until","TIMESTAMP"),("second_clip_discount_percent","INTEGER DEFAULT 0"),
            ("approved","BOOLEAN DEFAULT FALSE"),("suspended","BOOLEAN DEFAULT FALSE"),
            ("instagram","VARCHAR(255)"),("created_at","TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        ]: _add(conn, "creator_profile", col, typ)

        conn.execute(text('CREATE TABLE IF NOT EXISTS batch (id SERIAL PRIMARY KEY)'))
        for col, typ in [
            ("creator_id","INTEGER"),("location","VARCHAR(180)"),("total_size_bytes","BIGINT DEFAULT 0"),
            ("created_at","TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        ]: _add(conn, "batch", col, typ)

        conn.execute(text('CREATE TABLE IF NOT EXISTS video (id SERIAL PRIMARY KEY)'))
        for col, typ in [
            ("creator_id","INTEGER"),("batch_id","INTEGER"),("location","VARCHAR(180)"),("recorded_at","TIMESTAMP"),
            ("r2_video_key","VARCHAR(500)"),("r2_thumbnail_key","VARCHAR(500)"),("public_thumbnail_url","VARCHAR(800)"),
            ("file_size_bytes","BIGINT DEFAULT 0"),("internal_filename","VARCHAR(500)"),
            ("original_price","NUMERIC(10,2) DEFAULT 40"),("edited_price","NUMERIC(10,2) DEFAULT 60"),
            ("bundle_price","NUMERIC(10,2) DEFAULT 80"),("status","VARCHAR(50) DEFAULT 'active'"),
            ("created_at","TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        ]: _add(conn, "video", col, typ)

        conn.execute(text('CREATE TABLE IF NOT EXISTS video_pricing_preset (id SERIAL PRIMARY KEY)'))
        for col, typ in [
            ("creator_id","INTEGER"),("title","VARCHAR(200) DEFAULT 'Default Video Price'"),("description","TEXT"),
            ("price","NUMERIC(10,2) DEFAULT 40"),("delivery_type","VARCHAR(50) DEFAULT 'instant'"),
            ("is_default","BOOLEAN DEFAULT FALSE"),("active","BOOLEAN DEFAULT TRUE"),
            ("created_at","TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        ]: _add(conn, "video_pricing_preset", col, typ)

        conn.execute(text('CREATE TABLE IF NOT EXISTS product (id SERIAL PRIMARY KEY)'))
        for col, typ in [
            ("creator_id","INTEGER"),("title","VARCHAR(200)"),("description","TEXT"),("price","NUMERIC(10,2)"),
            ("shipping_cost","NUMERIC(10,2) DEFAULT 0"),("processing_time","VARCHAR(120)"),
            ("shipping_method","VARCHAR(120)"),("active","BOOLEAN DEFAULT TRUE")
        ]: _add(conn, "product", col, typ)

        conn.execute(text('CREATE TABLE IF NOT EXISTS "order" (id SERIAL PRIMARY KEY)'))
        for col, typ in [
            ("buyer_email","VARCHAR(255)"),("buyer_id","INTEGER"),("total_price","NUMERIC(10,2) DEFAULT 0"),
            ("status","VARCHAR(50) DEFAULT 'paid'"),("created_at","TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        ]: _add(conn, '"order"', col, typ)

        conn.execute(text('CREATE TABLE IF NOT EXISTS order_item (id SERIAL PRIMARY KEY)'))
        for col, typ in [
            ("order_id","INTEGER"),("video_id","INTEGER"),("creator_id","INTEGER"),("purchase_type","VARCHAR(50) DEFAULT 'original'"),
            ("price","NUMERIC(10,2) DEFAULT 0"),("edited_status","VARCHAR(50) DEFAULT 'not_required'"),("edited_r2_key","VARCHAR(500)")
        ]: _add(conn, "order_item", col, typ)

        conn.execute(text('CREATE TABLE IF NOT EXISTS creator_click_stats (id SERIAL PRIMARY KEY)'))
        for col, typ in [
            ("creator_id","INTEGER"),("clicks_today","INTEGER DEFAULT 0"),("clicks_week","INTEGER DEFAULT 0"),
            ("clicks_month","INTEGER DEFAULT 0"),("clicks_lifetime","INTEGER DEFAULT 0")
        ]: _add(conn, "creator_click_stats", col, typ)

def repair_all_known_tables():
    repair_basic_tables()
    repair_creator_application_table()
