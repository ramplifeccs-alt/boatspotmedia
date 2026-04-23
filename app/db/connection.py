
import psycopg2
from flask import current_app, g

def get_db():
    if "db" not in g:
        g.db = psycopg2.connect(current_app.config["DATABASE_URL"])
    return g.db

def init_db(app):
    @app.teardown_appcontext
    def close_db(exception):
        db = g.pop("db", None)
        if db is not None:
            db.close()
