from sqlalchemy import text
from app.services.db import db

def repair_creator_application_table():
    dialect = db.engine.dialect.name
    with db.engine.begin() as conn:
        if dialect == "postgresql":
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS creator_application (
                    id SERIAL PRIMARY KEY
                )
            """))
            columns = [
                ("first_name", "VARCHAR(120)"),
                ("last_name", "VARCHAR(120)"),
                ("email", "VARCHAR(255)"),
                ("instagram", "VARCHAR(255)"),
                ("facebook", "VARCHAR(255)"),
                ("youtube", "VARCHAR(255)"),
                ("tiktok", "VARCHAR(255)"),
                ("status", "VARCHAR(50) DEFAULT 'pending'"),
                ("submitted_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
                ("reviewed_at", "TIMESTAMP")
            ]
            for name, coltype in columns:
                conn.execute(text(f'ALTER TABLE creator_application ADD COLUMN IF NOT EXISTS {name} {coltype}'))
        else:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS creator_application (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first_name TEXT,
                    last_name TEXT,
                    email TEXT,
                    instagram TEXT,
                    facebook TEXT,
                    youtube TEXT,
                    tiktok TEXT,
                    status TEXT DEFAULT 'pending',
                    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reviewed_at TIMESTAMP
                )
            """))

def repair_video_table():
    dialect = db.engine.dialect.name
    with db.engine.begin() as conn:
        if dialect == "postgresql":
            conn.execute(text("CREATE TABLE IF NOT EXISTS video (id SERIAL PRIMARY KEY)"))
            columns = [
                ("creator_id", "INTEGER"),
                ("batch_id", "INTEGER"),
                ("location", "VARCHAR(180)"),
                ("recorded_at", "TIMESTAMP"),
                ("r2_video_key", "VARCHAR(500)"),
                ("r2_thumbnail_key", "VARCHAR(500)"),
                ("public_thumbnail_url", "VARCHAR(800)"),
                ("file_size_bytes", "BIGINT DEFAULT 0"),
                ("internal_filename", "VARCHAR(500)"),
                ("original_price", "NUMERIC(10,2) DEFAULT 40"),
                ("edited_price", "NUMERIC(10,2) DEFAULT 60"),
                ("bundle_price", "NUMERIC(10,2) DEFAULT 80"),
                ("status", "VARCHAR(50) DEFAULT 'active'"),
                ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            ]
            for name, coltype in columns:
                conn.execute(text(f'ALTER TABLE video ADD COLUMN IF NOT EXISTS {name} {coltype}'))
            # copy old field if it exists
            conn.execute(text("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name='video' AND column_name='recorded_date'
                    ) THEN
                        UPDATE video SET recorded_at = recorded_date
                        WHERE recorded_at IS NULL AND recorded_date IS NOT NULL;
                    END IF;
                END $$;
            """))

def repair_user_table():
    dialect = db.engine.dialect.name
    with db.engine.begin() as conn:
        if dialect == "postgresql":
            conn.execute(text('CREATE TABLE IF NOT EXISTS "user" (id SERIAL PRIMARY KEY)'))
            cols = [
                ("email", "VARCHAR(255)"),
                ("password_hash", "VARCHAR(255)"),
                ("role", "VARCHAR(50)"),
                ("display_name", "VARCHAR(255)"),
                ("language", "VARCHAR(10) DEFAULT 'en'"),
                ("is_active", "BOOLEAN DEFAULT TRUE"),
                ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            ]
            for name, coltype in cols:
                conn.execute(text(f'ALTER TABLE "user" ADD COLUMN IF NOT EXISTS {name} {coltype}'))

def repair_all_known_tables():
    repair_user_table()
    repair_creator_application_table()
    repair_video_table()
