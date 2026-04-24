import os
from datetime import datetime
from flask import Blueprint, redirect, render_template, request, url_for
from sqlalchemy import text
from werkzeug.security import generate_password_hash
from app.models import Video, Location, ServiceAd, CharterListing, User
from app.services.db import db

public_bp = Blueprint("public", __name__)


def clean_instagram(value):
    value = (value or "").strip()
    for prefix in ["https://www.instagram.com/", "https://instagram.com/", "http://www.instagram.com/", "http://instagram.com/"]:
        value = value.replace(prefix, "")
    value = value.strip().strip("/")
    if value.startswith("@"):
        value = value[1:]
    return value.strip()


@public_bp.route("/")
def home():
    try:
        latest = Video.query.filter_by(status="active").order_by(Video.created_at.desc()).limit(20).all()
    except Exception:
        db.session.rollback(); latest = []
    selected, used = [], set()
    for v in latest:
        cid = getattr(v, "creator_id", None)
        if cid not in used:
            selected.append(v); used.add(cid)
        if len(selected) == 3: break
    for v in latest:
        if len(selected) == 3: break
        if v not in selected: selected.append(v)
    return render_template("public/home.html", videos=selected)


@public_bp.route("/search")
def search_page():
    try: locations = Location.query.order_by(Location.name.asc()).all()
    except Exception: db.session.rollback(); locations = []
    return render_template("public/search.html", locations=locations, results=None)


@public_bp.route("/search/results")
def search_results():
    location = request.args.get("location"); date_s = request.args.get("date"); start_s = request.args.get("start_time"); end_s = request.args.get("end_time")
    results = []
    try:
        q = Video.query.filter_by(status="active")
        if location: q = q.filter(Video.location == location)
        if date_s and start_s and end_s:
            d = datetime.strptime(date_s, "%Y-%m-%d").date()
            start_dt = datetime.combine(d, datetime.strptime(start_s, "%H:%M").time())
            end_dt = datetime.combine(d, datetime.strptime(end_s, "%H:%M").time())
            q = q.filter(Video.recorded_at >= start_dt, Video.recorded_at <= end_dt)
        results = q.order_by(Video.recorded_at.asc()).limit(200).all()
    except Exception:
        db.session.rollback()
    try: locations = Location.query.order_by(Location.name.asc()).all()
    except Exception: db.session.rollback(); locations = []
    return render_template("public/search.html", locations=locations, results=results)


@public_bp.route("/preview/<int:video_id>")
def preview_video(video_id):
    from app.models import CreatorClickStats
    v = Video.query.get_or_404(video_id)
    stats = CreatorClickStats.query.filter_by(creator_id=v.creator_id).first()
    if not stats:
        stats = CreatorClickStats(creator_id=v.creator_id); db.session.add(stats)
    stats.clicks_today += 1; stats.clicks_week += 1; stats.clicks_month += 1; stats.clicks_lifetime += 1
    db.session.commit()
    return render_template("public/preview.html", video=v)


@public_bp.route("/apply-creator", methods=["GET", "POST"])
def apply_creator():
    if request.method == "POST":
        instagram = clean_instagram(request.form.get("instagram", ""))
        email = (request.form.get("email") or "").lower().strip()
        if not instagram: return render_template("public/apply_creator.html", error="Instagram is required.")
        if not email: return render_template("public/apply_creator.html", error="Email is required.")
        try:
            from app.services.db_repair import repair_creator_application_table
            repair_creator_application_table()
            existing_user = User.query.filter_by(email=email).first()
            existing_application = db.session.execute(text("SELECT id FROM creator_application WHERE LOWER(email)=:email LIMIT 1"), {"email": email}).first()
            if existing_user or existing_application:
                return render_template("public/apply_creator.html", error="This email already has an application or account. Please use the creator login.")
            with db.engine.begin() as conn:
                result = conn.execute(text("""
                    INSERT INTO creator_application (first_name, last_name, brand_name, email, instagram, status, submitted_at)
                    VALUES (:first_name, :last_name, :brand_name, :email, :instagram, 'pending', CURRENT_TIMESTAMP)
                    RETURNING id
                """), {
                    "first_name": request.form.get("first_name", ""), "last_name": request.form.get("last_name", ""),
                    "brand_name": instagram, "email": email, "instagram": instagram
                })
                app_id = result.scalar()
            return render_template("public/apply_creator.html", success=True, application_id=app_id)
        except Exception as e:
            db.session.rollback(); return render_template("public/apply_creator.html", error=f"Application could not be saved yet. Error: {str(e)[:500]}")
    return render_template("public/apply_creator.html")


@public_bp.route("/shop")
def shop():
    from app.models import Product
    try: products = Product.query.filter_by(active=True).all()
    except Exception: db.session.rollback(); products = []
    return render_template("public/shop.html", products=products)


@public_bp.route("/shop/product/<int:product_id>")
def product_detail(product_id):
    from app.models import Product, ProductVariant
    product = Product.query.get_or_404(product_id)
    variants = ProductVariant.query.filter_by(product_id=product.id, active=True).all()
    return render_template("public/product_detail.html", product=product, variants=variants)


@public_bp.route("/services")
def services_public():
    try: ads = ServiceAd.query.filter_by(active=True).order_by(ServiceAd.id.desc()).all()
    except Exception: db.session.rollback(); ads = []
    return render_template("public/services.html", ads=ads)


@public_bp.route("/services/ad/<int:ad_id>/click")
def service_ad_click(ad_id):
    from app.models import ServiceClickLog
    ad = ServiceAd.query.get_or_404(ad_id)
    ad.clicks = (ad.clicks or 0) + 1
    try:
        db.session.add(ServiceClickLog(service_ad_id=ad.id, ip_address=request.headers.get("X-Forwarded-For", request.remote_addr), user_agent=request.headers.get("User-Agent")))
    except Exception: pass
    db.session.commit()
    return redirect(ad.website_url or "/services")


@public_bp.route("/charters")
def charters_public():
    try: listings = CharterListing.query.filter_by(status="active").all()
    except Exception: db.session.rollback(); listings = []
    return render_template("public/charters.html", listings=listings)


@public_bp.route("/login")
def login_selector():
    return render_template("public/login_selector.html")


def _register_user(role, dashboard_url):
    email = (request.form.get("email") or "").lower().strip()
    password = request.form.get("password") or ""
    display_name = request.form.get("display_name") or request.form.get("business_name") or email
    if not email or not password:
        return render_template("public/generic_register.html", role=role, error="Email and password are required.")
    if User.query.filter_by(email=email).first():
        return render_template("public/generic_register.html", role=role, error="This email already exists. Please login.")
    user = User(email=email, password_hash=generate_password_hash(password), role=role, display_name=display_name, is_active=True)
    db.session.add(user); db.session.flush()
    if role == "service":
        from app.models import ServiceAccount
        db.session.add(ServiceAccount(business_name=display_name, contact_name=display_name, email=email, password_hash=user.password_hash, balance=0, is_active=True))
    db.session.commit()
    return redirect(dashboard_url)


@public_bp.route("/buyer/login", methods=["GET", "POST"])
def buyer_login():
    return render_template("public/generic_login.html", title="Buyer Login", subtitle="Access your orders and downloads.", register_url="/buyer/register", role="buyer")

@public_bp.route("/buyer/register", methods=["GET", "POST"])
def buyer_register():
    if request.method == "POST": return _register_user("buyer", "/buyer/login")
    return render_template("public/generic_register.html", title="Buyer Registration", role="buyer", login_url="/buyer/login")

@public_bp.route("/charter/login", methods=["GET", "POST"])
def charter_login():
    return render_template("public/generic_login.html", title="Charter Login", subtitle="Manage your charter listings.", register_url="/charter/register", role="charter_provider")

@public_bp.route("/charter/register", methods=["GET", "POST"])
def charter_register():
    if request.method == "POST": return _register_user("charter_provider", "/charter/login")
    return render_template("public/generic_register.html", title="Charter Registration", role="charter_provider", login_url="/charter/login")

@public_bp.route("/services/login", methods=["GET", "POST"])
def services_login():
    return render_template("public/generic_login.html", title="Services Login", subtitle="Manage your pay-per-click service ads.", register_url="/services/register", role="service")

@public_bp.route("/services/register", methods=["GET", "POST"])
def services_register():
    if request.method == "POST": return _register_user("service", "/service-account/dashboard")
    return render_template("public/generic_register.html", title="Services Registration", role="service", login_url="/services/login", business=True)


@public_bp.route("/auth/<provider>/<role>")
def oauth_start(provider, role):
    provider = provider.lower(); role = role.lower()
    if provider not in {"google", "apple"}:
        return redirect("/login")
    required = {
        "google": ["GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_SECRET"],
        "apple": ["APPLE_CLIENT_ID", "APPLE_TEAM_ID", "APPLE_KEY_ID", "APPLE_PRIVATE_KEY"]
    }[provider]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        return render_template("public/oauth_setup.html", provider=provider.title(), role=role, missing=missing)
    return render_template("public/oauth_setup.html", provider=provider.title(), role=role, missing=[], ready=True)
