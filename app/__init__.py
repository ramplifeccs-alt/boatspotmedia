from flask import Flask, session, url_for
from config import Config
from .services.db import db


def create_app():
    flask_app = Flask(__name__, template_folder="templates", static_folder="static")
    flask_app.config.from_object(Config)
    db.init_app(flask_app)

    from .routes.public import public_bp
    from .routes.creator import creator_bp
    from .routes.owner import owner_bp

    flask_app.register_blueprint(public_bp)
    flask_app.register_blueprint(creator_bp, url_prefix="/creator")
    flask_app.register_blueprint(owner_bp, url_prefix="/owner")

    for import_path, prefix, label in [
        (".routes.buyer", "/buyer", "buyer"),
        (".routes.advertiser", "/advertiser", "advertiser"),
        (".routes.charters", "/charters", "charters"),
    ]:
        try:
            module = __import__(__name__ + import_path, fromlist=["*"])
            bp = getattr(module, f"{label}_bp")
            flask_app.register_blueprint(bp, url_prefix=prefix)
        except Exception as e:
            print(f"{label} blueprint skipped:", e)

    try:
        from .routes.services_panel import services_bp
        flask_app.register_blueprint(services_bp)
    except Exception as e:
        print("services blueprint skipped:", e)

    with flask_app.app_context():
        try:
            db.create_all()
        except Exception as e:
            print("db.create_all warning:", e)
        try:
            from .services.db_repair import repair_all_known_tables
            repair_all_known_tables()
        except Exception as e:
            print("DB repair warning:", e)
        try:
            seed_owner_and_default_data()
        except Exception as e:
            print("Seed warning:", e)

    @flask_app.context_processor
    def inject_latest_previews():
        try:
            from app.routes.public import _home_preview_videos
            return {"latest_previews": _home_preview_videos()}
        except Exception:
            return {"latest_previews": []}


    @flask_app.context_processor
    def inject_account_menu():
        def account_display_name():
            try:
                # Prefer business/creator display names when available.
                role = session.get("role") or session.get("account_type") or ""
                email = session.get("user_email") or session.get("email") or ""
                user_id = session.get("user_id")
                creator_id = session.get("creator_id")

                if role == "creator" or creator_id:
                    try:
                        from app.models import CreatorProfile, User
                        q = CreatorProfile.query
                        c = None
                        if creator_id:
                            c = q.get(creator_id)
                        if not c and user_id:
                            c = q.filter_by(user_id=user_id).first()
                        if c:
                            for attr in ["business_name", "brand_name", "display_name", "instagram", "name"]:
                                val = getattr(c, attr, None)
                                if val:
                                    return str(val)
                            if getattr(c, "user", None) and getattr(c.user, "email", None):
                                return c.user.email
                    except Exception:
                        pass

                # Generic user fallback.
                try:
                    from app.models import User
                    if user_id:
                        u = User.query.get(user_id)
                        if u:
                            for attr in ["business_name", "company_name", "display_name", "name", "full_name", "username", "email"]:
                                val = getattr(u, attr, None)
                                if val:
                                    return str(val)
                except Exception:
                    pass

                return email or "My Account"
            except Exception:
                return "My Account"

        def account_dashboard_url():
            role = (session.get("role") or session.get("account_type") or "").lower()

            try:
                if role == "creator" or session.get("creator_id"):
                    return url_for("creator.dashboard")
                if role == "owner":
                    return url_for("owner.applications")
                if role == "buyer":
                    return url_for("public.buyer_dashboard")
                if role == "service":
                    return url_for("public.services_dashboard")
                if role == "charter":
                    return url_for("public.charters_dashboard")
            except Exception:
                pass

            # Fallbacks to routes that often exist in the project.
            for endpoint in [
                "creator.dashboard",
                "public.buyer_dashboard",
                "public.services_dashboard",
                "public.charters_dashboard",
                "public.home"
            ]:
                try:
                    return url_for(endpoint)
                except Exception:
                    continue
            return "/"

        return {
            "account_display_name": account_display_name,
            "account_dashboard_url": account_dashboard_url
        }

    return flask_app


def seed_owner_and_default_data():
    from .models import User, StoragePlan, Location
    from werkzeug.security import generate_password_hash
    if not User.query.filter_by(role="owner").first():
        db.session.add(User(email="owner@boatspotmedia.com", password_hash=generate_password_hash("ChangeMe123!"), role="owner", display_name="BoatSpotMedia Owner", is_active=True))
    if not StoragePlan.query.first():
        db.session.add_all([
            StoragePlan(name="Starter 128GB", storage_limit_gb=128, monthly_price=29.00, commission_rate=30),
            StoragePlan(name="Creator 512GB", storage_limit_gb=512, monthly_price=79.00, commission_rate=20),
            StoragePlan(name="Studio 2TB", storage_limit_gb=2048, monthly_price=199.00, commission_rate=10),
        ])
    if not Location.query.first():
        db.session.add_all([Location(name=n) for n in ["Boca Raton Inlet","Hillsboro Inlet","Boynton Inlet","Haulover Inlet","Port Everglades","Government Cut","Jupiter Inlet","Lake Worth Inlet","Palm Beach Inlet"]])
    db.session.commit()


app = create_app()
