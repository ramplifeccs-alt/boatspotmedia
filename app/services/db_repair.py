from sqlalchemy import text
from app.services.db import db

def _dialect():
    return db.engine.dialect.name

def table_exists(table_name):
    dialect = _dialect()
    with db.engine.connect() as conn:
        if dialect == "postgresql":
            return bool(conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema='public' AND table_name=:table
                )
            """), {"table": table_name}).scalar())
        row = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table"), {"table": table_name}).first()
        return row is not None

def column_exists(table_name, column_name):
    dialect = _dialect()
    with db.engine.connect() as conn:
        if dialect == "postgresql":
            return bool(conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=:table AND column_name=:column
                )
            """), {"table": table_name, "column": column_name}).scalar())
        rows = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
        return any(r[1] == column_name for r in rows)

def add_column_if_missing(table_name, column_name, column_sql):
    if not table_exists(table_name):
        return
    if column_exists(table_name, column_name):
        return
    dialect = _dialect()
    with db.engine.begin() as conn:
        if dialect == "postgresql":
            conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS {column_sql}'))
        else:
            conn.execute(text(f'ALTER TABLE {table_name} ADD COLUMN {column_sql}'))

def repair_creator_application_table():
    # Explicit repair for older Railway DB created by previous builds.
    cols = {
        "first_name": "first_name VARCHAR(120)",
        "last_name": "last_name VARCHAR(120)",
        "email": "email VARCHAR(255)",
        "instagram": "instagram VARCHAR(255)",
        "facebook": "facebook VARCHAR(255)",
        "youtube": "youtube VARCHAR(255)",
        "tiktok": "tiktok VARCHAR(255)",
        "status": "status VARCHAR(50) DEFAULT 'pending'",
        "submitted_at": "submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        "reviewed_at": "reviewed_at TIMESTAMP"
    }
    for name, sql in cols.items():
        add_column_if_missing("creator_application", name, sql)

def repair_video_table():
    cols = {
        "creator_id": "creator_id INTEGER",
        "batch_id": "batch_id INTEGER",
        "location": "location VARCHAR(180)",
        "recorded_at": "recorded_at TIMESTAMP",
        "r2_video_key": "r2_video_key VARCHAR(500)",
        "r2_thumbnail_key": "r2_thumbnail_key VARCHAR(500)",
        "public_thumbnail_url": "public_thumbnail_url VARCHAR(800)",
        "file_size_bytes": "file_size_bytes BIGINT DEFAULT 0",
        "internal_filename": "internal_filename VARCHAR(500)",
        "original_price": "original_price NUMERIC(10,2) DEFAULT 40",
        "edited_price": "edited_price NUMERIC(10,2) DEFAULT 60",
        "bundle_price": "bundle_price NUMERIC(10,2) DEFAULT 80",
        "status": "status VARCHAR(50) DEFAULT 'active'",
        "created_at": "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    }
    for name, sql in cols.items():
        add_column_if_missing("video", name, sql)
    if table_exists("video") and column_exists("video", "recorded_date") and column_exists("video", "recorded_at"):
        with db.engine.begin() as conn:
            conn.execute(text("UPDATE video SET recorded_at = recorded_date WHERE recorded_at IS NULL AND recorded_date IS NOT NULL"))

def repair_user_table():
    cols = {
        "email": "email VARCHAR(255)",
        "password_hash": "password_hash VARCHAR(255)",
        "role": "role VARCHAR(50)",
        "display_name": "display_name VARCHAR(255)",
        "language": "language VARCHAR(10) DEFAULT 'en'",
        "is_active": "is_active BOOLEAN DEFAULT TRUE",
        "created_at": "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    }
    for name, sql in cols.items():
        add_column_if_missing("user", name, sql)

def repair_all_known_tables():
    repair_user_table()
    repair_creator_application_table()
    repair_video_table()
