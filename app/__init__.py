
from flask import Flask
from config.settings import Config
from app.db.connection import init_db

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    init_db(app)

    from app.routes.public import public_bp
    from app.routes.upload import upload_bp
    from app.routes.search import search_bp
    from app.routes.analytics import analytics_bp

    app.register_blueprint(public_bp)
    app.register_blueprint(upload_bp, url_prefix="/creator")
    app.register_blueprint(search_bp, url_prefix="/search")
    app.register_blueprint(analytics_bp, url_prefix="/analytics")

    return app
