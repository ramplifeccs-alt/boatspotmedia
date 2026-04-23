
from flask import Flask
from config.settings import Config

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    from app.routes.public import public_bp
    from app.routes.creator import creator_bp
    from app.routes.owner import owner_bp
    from app.routes.buyer import buyer_bp
    from app.routes.search import search_bp
    from app.routes.ads import ads_bp
    from app.routes.charters import charters_bp

    app.register_blueprint(public_bp)
    app.register_blueprint(creator_bp, url_prefix="/creator")
    app.register_blueprint(owner_bp, url_prefix="/owner")
    app.register_blueprint(buyer_bp, url_prefix="/buyer")
    app.register_blueprint(search_bp, url_prefix="/search")
    app.register_blueprint(ads_bp, url_prefix="/services")
    app.register_blueprint(charters_bp, url_prefix="/charters")

    return app
