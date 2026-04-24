from sqlalchemy import text
from app.services.db import db

def repair_creator_application_table():
    dialect = db.engine.dialect.name
    with db.engine.begin() as conn:
        if dialect == "postgresql":
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
        else:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS creator_application (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first_name TEXT,
                    last_name TEXT,
                    brand_name TEXT,
                    email TEXT,
                    instagram TEXT,
                    status TEXT DEFAULT 'pending',
                    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reviewed_at TIMESTAMP
                )
            """))

def repair_basic_tables():
    # Minimal safe creation for test mode if tables are missing.
    with db.engine.begin() as conn:
        if db.engine.dialect.name == "postgresql":
            conn.execute(text('CREATE TABLE IF NOT EXISTS "user" (id SERIAL PRIMARY KEY)'))
            for col, coltype in [
                ("email", "VARCHAR(255)"),
                ("password_hash", "VARCHAR(255)"),
                ("role", "VARCHAR(50)"),
                ("display_name", "VARCHAR(255)"),
                ("language", "VARCHAR(10) DEFAULT 'en'"),
                ("is_active", "BOOLEAN DEFAULT TRUE"),
                ("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            ]:
                conn.execute(text(f'ALTER TABLE "user" ADD COLUMN IF NOT EXISTS {col} {coltype}'))

def repair_all_known_tables():
    repair_basic_tables()
    repair_creator_application_table()
