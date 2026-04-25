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
        for col, typ in [("product_id","INTEGER"),("variant_name","VARCHAR(120)"),("variant_value","VARCHAR(200)"),("price_adjustment","NUMERIC(10,2) DEFAULT 0"),("active","BOOLEAN DEFAULT TRUE"),("created_at","TIMESTAMP DEFAULT CURRENT_TIMESTAMP")]:
            _add(conn, "product_variant", col, typ)

        conn.execute(text('CREATE TABLE IF NOT EXISTS product_variant_image (id SERIAL PRIMARY KEY)'))
        for col, typ in [("variant_id","INTEGER"),("image_url","VARCHAR(800)"),("sort_order","INTEGER DEFAULT 0")]:
            _add(conn, "product_variant_image", col, typ)

        conn.execute(text('CREATE TABLE IF NOT EXISTS commission_override_log (id SERIAL PRIMARY KEY)'))
        for col, typ in [("creator_id","INTEGER"),("commission_type","VARCHAR(50)"),("old_rate","INTEGER"),("new_rate","INTEGER"),("days","INTEGER"),("reason","VARCHAR(500)"),("expires_at","TIMESTAMP"),("created_at","TIMESTAMP DEFAULT CURRENT_TIMESTAMP")]:
            _add(conn, "commission_override_log", col, typ)

def repair_all_known_tables():
    repair_creator_upload_tables()
    repair_basic_tables()
    repair_creator_application_table()



def repair_creator_upload_tables():
    statements = [
        "ALTER TABLE video_batch ADD COLUMN IF NOT EXISTS total_size_bytes BIGINT DEFAULT 0",
        "ALTER TABLE video_batch ADD COLUMN IF NOT EXISTS file_count INTEGER DEFAULT 0",
        "ALTER TABLE video_batch ADD COLUMN IF NOT EXISTS status VARCHAR(50) DEFAULT 'uploaded'",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS creator_id INTEGER",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS batch_id INTEGER",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS location VARCHAR(255)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS r2_video_key VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS file_size_bytes BIGINT DEFAULT 0",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS original_price NUMERIC(10,2) DEFAULT 0",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS edited_price NUMERIC(10,2) DEFAULT 0",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS bundle_price NUMERIC(10,2) DEFAULT 0",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS status VARCHAR(50) DEFAULT 'active'",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS internal_filename VARCHAR(500)"
    ]
    for sql in statements:
        try:
            db.session.execute(db.text(sql))
        except Exception as e:
            print("Upload table repair warning:", sql, e)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()



def repair_video_preview_search_columns():
    statements = [
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS r2_thumbnail_key VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS public_thumbnail_url VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS recorded_at TIMESTAMP",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS recorded_date DATE",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS recorded_time TIME"
    ]
    for sql in statements:
        try:
            db.session.execute(db.text(sql))
        except Exception as e:
            print("Video preview/search repair warning:", sql, e)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()



def repair_video_filename_column():
    statements = [
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS filename VARCHAR(500)",
        "UPDATE video SET filename = COALESCE(filename, internal_filename, split_part(r2_video_key, '/', array_length(string_to_array(r2_video_key, '/'), 1)), 'video.mp4') WHERE filename IS NULL OR filename = ''",
        "ALTER TABLE video ALTER COLUMN filename SET DEFAULT ''"
    ]
    for sql in statements:
        try:
            db.session.execute(db.text(sql))
        except Exception as e:
            print("Video filename repair warning:", sql, e)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()



def repair_video_file_path_columns():
    statements = [
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS file_path VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS thumbnail_path VARCHAR(500)",
        "UPDATE video SET file_path = COALESCE(NULLIF(file_path, ''), r2_video_key, filename, internal_filename, 'video.mp4') WHERE file_path IS NULL OR file_path = ''",
        "UPDATE video SET thumbnail_path = COALESCE(NULLIF(thumbnail_path, ''), r2_thumbnail_key, public_thumbnail_url) WHERE thumbnail_path IS NULL OR thumbnail_path = ''",
        "ALTER TABLE video ALTER COLUMN file_path SET DEFAULT ''"
    ]
    for sql in statements:
        try:
            db.session.execute(db.text(sql))
        except Exception as e:
            print('Video file_path repair warning:', sql, e)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()



def repair_video_price_column():
    statements = [
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS price NUMERIC(10,2) DEFAULT 0",
        "UPDATE video SET price = 0 WHERE price IS NULL",
        "ALTER TABLE video ALTER COLUMN price SET DEFAULT 0",
        "ALTER TABLE video ALTER COLUMN price DROP NOT NULL"
    ]
    for sql in statements:
        try:
            db.session.execute(db.text(sql))
        except Exception as e:
            print("Video price repair warning:", sql, e)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()

def repair_video_batch_fk():
    statements = [
        "DELETE FROM video WHERE batch_id IS NOT NULL AND batch_id NOT IN (SELECT id FROM batch)",
        "ALTER TABLE video DROP CONSTRAINT IF EXISTS video_batch_id_fkey",
        "ALTER TABLE video ADD CONSTRAINT video_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES batch(id) ON DELETE CASCADE"
    ]
    for sql in statements:
        try:
            db.session.execute(db.text(sql))
        except Exception as e:
            print("Video batch FK repair warning:", sql, e)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()



def repair_creator_plan_columns():
    statements = [
        "ALTER TABLE creator ADD COLUMN IF NOT EXISTS storage_limit_gb NUMERIC(10,2) DEFAULT 500",
        "ALTER TABLE creator ADD COLUMN IF NOT EXISTS max_batch_gb NUMERIC(10,2) DEFAULT 128",
        "UPDATE creator SET storage_limit_gb = 500 WHERE storage_limit_gb IS NULL OR storage_limit_gb <= 0",
        "UPDATE creator SET max_batch_gb = 128 WHERE max_batch_gb IS NULL OR max_batch_gb <= 0"
    ]
    for sql in statements:
        try:
            db.session.execute(db.text(sql))
        except Exception as e:
            print("Creator plan repair warning:", sql, e)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()



def repair_creator_delete_and_video_batch_fk():
    statements = [
        "ALTER TABLE creator_profile ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE",
        "UPDATE creator_profile SET deleted = FALSE WHERE deleted IS NULL",
        "UPDATE creator_application SET status='deleted' WHERE lower(email) IN (SELECT lower(u.email) FROM creator_profile c JOIN \"user\" u ON u.id=c.user_id WHERE c.deleted = TRUE)",
        "DELETE FROM video WHERE batch_id IS NOT NULL AND batch_id NOT IN (SELECT id FROM video_batch)",
        "ALTER TABLE video DROP CONSTRAINT IF EXISTS video_batch_id_fkey",
        "ALTER TABLE video ADD CONSTRAINT video_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES video_batch(id) ON DELETE CASCADE"
    ]
    for sql in statements:
        try:
            db.session.execute(db.text(sql))
        except Exception as e:
            print("Creator delete / video batch FK repair warning:", sql, e)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
