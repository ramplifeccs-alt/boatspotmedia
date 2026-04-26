from app.services.pricing import creator_video_price_options
import os
from datetime import datetime
from flask import Blueprint, redirect, render_template, request, url_for, session, jsonify, Response, send_file, abort
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



def _dynamic_video_locations():
    """Buyer/search locations come only from existing creator uploads."""
    locations = []
    try:
        from app.models import Video
        rows = db.session.query(Video.location).filter(
            Video.location.isnot(None),
            Video.location != "",
            Video.status != "deleted"
        ).distinct().order_by(Video.location.asc()).all()
        locations += [r[0] for r in rows if r and r[0]]
    except Exception as e:
        print("video locations warning:", e)

    try:
        rows = db.session.execute(db.text("""
            SELECT DISTINCT location
            FROM video_batch
            WHERE location IS NOT NULL
              AND trim(location) <> ''
              AND COALESCE(status, '') <> 'deleted'
            ORDER BY location ASC
        """)).fetchall()
        locations += [r[0] for r in rows if r and r[0]]
    except Exception as e:
        db.session.rollback()
        print("batch locations warning:", e)

    seen = set()
    clean = []
    for loc in locations:
        value = " ".join(str(loc).strip().split())
        key = value.lower()
        if value and key not in seen:
            seen.add(key)
            clean.append(value)
    return clean



def _attach_price_options_to_videos(videos):
    try:
        from app.models import CreatorProfile
        for v in videos:
            creator = None
            try:
                creator = CreatorProfile.query.get(getattr(v, "creator_id", None))
            except Exception:
                creator = None
            try:
                v.price_options = creator_video_price_options(creator, v)
            except Exception:
                v.price_options = creator_video_price_options(None, v)
    except Exception as e:
        print("price option attach warning:", e)
    return videos




def _public_video_thumb_url(video):
    """Return the best available public thumbnail URL for home cards."""
    import os
    if not video:
        return None

    for attr in ["public_thumbnail_url", "thumbnail_url"]:
        try:
            value = getattr(video, attr, None)
            if value:
                return value
        except Exception:
            pass

    for attr in ["thumbnail_path", "r2_thumbnail_key"]:
        try:
            key = getattr(video, attr, None)
            if key:
                if str(key).startswith("http://") or str(key).startswith("https://"):
                    return key
                base = (os.getenv("R2_PUBLIC_BASE_URL") or "").rstrip("/")
                if base:
                    return f"{base}/{str(key).lstrip('/')}"
        except Exception:
            pass

    return None


def _public_active_video_query():
    from app.models import Video
    q = Video.query
    if "status" in Video.__table__.columns.keys():
        q = q.filter(Video.status != "deleted")
    return q

def _public_video_locations():
    from app.models import Video
    cols = Video.__table__.columns.keys()
    if "location" not in cols:
        return []
    q = _public_active_video_query().with_entities(Video.location).filter(Video.location.isnot(None))
    values = []
    seen = set()
    for row in q.all():
        loc = (row[0] or "").strip()
        if not loc:
            continue
        key = loc.lower()
        if key not in seen:
            seen.add(key)
            values.append(loc)
    return sorted(values, key=lambda x: x.lower())

def _public_latest_home_videos(limit=3):
    from app.models import Video
    q = _public_active_video_query()
    cols = Video.__table__.columns.keys()
    if "created_at" in cols:
        q = q.order_by(Video.created_at.desc())
    else:
        q = q.order_by(Video.id.desc())
    return q.limit(limit).all()



def _public_creator_name(video):
    try:
        creator = getattr(video, "creator", None)
        if creator:
            for attr in ["display_name", "business_name", "name", "username", "instagram"]:
                val = getattr(creator, attr, None)
                if val:
                    return val
        creator_id = getattr(video, "creator_id", None)
        if creator_id:
            try:
                from app.models import CreatorProfile
                c = CreatorProfile.query.get(creator_id)
                if c:
                    user = getattr(c, "user", None)
                    for obj in [c, user]:
                        if obj:
                            for attr in ["display_name", "business_name", "name", "username", "instagram", "email"]:
                                val = getattr(obj, attr, None)
                                if val:
                                    return val
            except Exception:
                pass
    except Exception:
        pass
    return "Creator"


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
    return render_template("public/home.html", latest_videos=_public_latest_home_videos(3), video_locations=_public_video_locations(), video_thumb_url=_public_video_thumb_url, creator_name=_public_creator_name)


@public_bp.route("/search")
def search_page():
    try: locations = Location.query.order_by(Location.name.asc()).all()
    except Exception: db.session.rollback(); locations = []
    return render_template("public/search.html", locations=locations, results=None, video_locations=_dynamic_video_locations())


@public_bp.route("/search/results")
def search_results():
    location = request.args.get("location"); date_s = request.args.get("date"); start_s = request.args.get("start_time"); end_s = request.args.get("end_time")
    results = []
    try:
        q = Video.query.filter_by(status="active")
        if location: q = q.filter(db.func.lower(Video.location) == location.strip().lower())
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


@public_bp.route("/auth/google/<account_type>")
def auth_google(account_type):
    import os, urllib.parse
    client_id = os.getenv("GOOGLE_CLIENT_ID")
    redirect_uri = os.getenv("GOOGLE_REDIRECT_URI")
    if not client_id or not redirect_uri:
        return "Google login is not configured yet. Missing GOOGLE_CLIENT_ID or GOOGLE_REDIRECT_URI.", 400
    scope = urllib.parse.quote("openid email profile")
    state = urllib.parse.quote(f"login:{account_type}")
    url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={urllib.parse.quote(client_id)}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
        "&response_type=code"
        f"&scope={scope}"
        f"&state={state}"
        "&access_type=offline"
        "&prompt=consent"
    )
    return redirect(url)


@public_bp.route("/auth/google-register/<account_type>")
def auth_google_register(account_type):
    import os, urllib.parse
    client_id = os.getenv("GOOGLE_CLIENT_ID")
    redirect_uri = os.getenv("GOOGLE_REDIRECT_URI")
    if not client_id or not redirect_uri:
        return "Google register is not configured yet. Missing GOOGLE_CLIENT_ID or GOOGLE_REDIRECT_URI.", 400
    scope = urllib.parse.quote("openid email profile")
    state = urllib.parse.quote(f"register:{account_type}")
    url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={urllib.parse.quote(client_id)}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
        "&response_type=code"
        f"&scope={scope}"
        f"&state={state}"
        "&access_type=offline"
        "&prompt=consent"
    )
    return redirect(url)


@public_bp.route("/auth/apple/<account_type>")
def auth_apple(account_type):
    import os, urllib.parse
    client_id = os.getenv("APPLE_CLIENT_ID")
    redirect_uri = os.getenv("APPLE_REDIRECT_URI")
    if not client_id or not redirect_uri:
        return "Apple login is not configured yet. Missing APPLE_CLIENT_ID or APPLE_REDIRECT_URI.", 400
    state = urllib.parse.quote(account_type)
    url = (
        "https://appleid.apple.com/auth/authorize"
        f"?client_id={urllib.parse.quote(client_id)}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
        "&response_type=code"
        "&scope=name%20email"
        f"&state={state}"
        "&response_mode=form_post"
    )
    return redirect(url)



@public_bp.route("/auth/callback/apple", methods=["GET", "POST"])
def auth_apple_callback():
    return "Apple callback received. Next step: verify Apple identity token and create/login user."

@public_bp.route("/auth/callback/google")
def auth_google_callback():
    import os
    import json
    import urllib.parse
    import urllib.request
    import urllib.error
    from werkzeug.security import generate_password_hash
    from app.models import User
    from app.services.db import db

    error = request.args.get("error")
    if error:
        return f"Google login error: {error}", 400

    code = request.args.get("code")
    raw_state = request.args.get("state") or "login:buyer"
    if ":" in raw_state:
        auth_mode, account_type = raw_state.split(":", 1)
    else:
        auth_mode, account_type = "login", raw_state

    if not code:
        return "Google login failed: missing authorization code.", 400

    client_id = os.getenv("GOOGLE_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
    redirect_uri = os.getenv("GOOGLE_REDIRECT_URI")

    if not client_id or not client_secret or not redirect_uri:
        return "Google login is not fully configured. Missing GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, or GOOGLE_REDIRECT_URI.", 400

    token_data = urllib.parse.urlencode({
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    }).encode("utf-8")

    try:
        token_req = urllib.request.Request(
            "https://oauth2.googleapis.com/token",
            data=token_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(token_req, timeout=20) as token_resp:
            token_json = json.loads(token_resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8")
        except Exception:
            body = ""
        return (
            "Google token exchange failed. "
            f"HTTP {e.code}: {e.reason}<br><br>"
            f"<b>Google response:</b><pre>{body}</pre><br>"
            f"<b>Redirect used by app:</b><pre>{redirect_uri}</pre><br>"
            "Check that GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET belong to the same OAuth Web Client, "
            "and that the redirect URI matches exactly in Google Cloud."
        ), 400
    except Exception as e:
        return f"Google token exchange failed: {e}", 400

    access_token = token_json.get("access_token")
    if not access_token:
        return f"Google token exchange failed: no access token returned. Response: {token_json}", 400

    try:
        user_req = urllib.request.Request(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
            method="GET",
        )
        with urllib.request.urlopen(user_req, timeout=20) as user_resp:
            google_user = json.loads(user_resp.read().decode("utf-8"))
    except Exception as e:
        return f"Google user info failed: {e}", 400

    email = google_user.get("email")
    display_name = google_user.get("name") or email

    if not email:
        return "Google login failed: Google did not return an email.", 400

    role_map = {
        "buyer": "buyer",
        "creator": "creator",
        "service": "services",
        "services": "services",
        "charter": "charter",
        "charters": "charter",
    }
    role = role_map.get(account_type, "buyer")

    user = User.query.filter_by(email=email).first()

    # Creator rule:
    # Creators are invite-only. They must be approved by Owner first.
    if role == "creator":
        if not user or user.role != "creator" or not user.is_active:
            return render_template("public/creator_invite_required.html"), 403

        try:
            from app.models import CreatorProfile
            creator = CreatorProfile.query.filter_by(user_id=user.id).first()
            if not creator or not creator.approved or creator.suspended:
                return render_template("public/creator_invite_required.html"), 403
        except Exception:
            pass

        user.display_name = user.display_name or display_name
        db.session.commit()

    else:
        # Login must NOT create accounts. User must register first.
        if auth_mode == "login":
            if not user:
                return render_template(
                    "public/register_required.html",
                    role=role,
                    account_type=account_type,
                    email=email,
                ), 403
            user.display_name = user.display_name or display_name
            user.is_active = True
            db.session.commit()

        # Register creates buyer/services/charter accounts.
        elif auth_mode == "register":
            if not user:
                user = User(
                    email=email,
                    password_hash=generate_password_hash(os.urandom(24).hex()),
                    role=role,
                    display_name=display_name,
                    is_active=True,
                )
                db.session.add(user)
                db.session.commit()
            else:
                user.display_name = user.display_name or display_name
                user.is_active = True
                db.session.commit()

    session["user_id"] = user.id
    session["user_email"] = user.email
    session["user_role"] = user.role
    session["display_name"] = user.display_name or user.email

    if user.role == "creator":
        return redirect("/creator/dashboard")
    if user.role in ["services", "service"]:
        return redirect("/service-account/dashboard")
    if user.role in ["charter", "charters"]:
        return redirect("/charters")
    return redirect("/buyer/dashboard")


@public_bp.route("/logout")
def logout():
    session.clear()
    return redirect("/")



# ===== Home previews and buyer video search v36 =====
def _home_preview_videos():
    from app.models import Video
    # latest active video per creator, max 3 creators.
    latest_per_creator = []
    creator_ids = [row[0] for row in db.session.query(Video.creator_id).filter(Video.status == "active", Video.creator_id.isnot(None)).group_by(Video.creator_id).order_by(db.func.max(Video.id).desc()).limit(3).all()]
    if len(creator_ids) >= 3:
        for cid in creator_ids[:3]:
            v = Video.query.filter_by(creator_id=cid, status="active").order_by(Video.id.desc()).first()
            if v:
                latest_per_creator.append(v)
        return latest_per_creator

    if len(creator_ids) == 2:
        newest = creator_ids[0]
        older = creator_ids[1]
        videos = Video.query.filter_by(creator_id=newest, status="active").order_by(Video.id.desc()).limit(2).all()
        one_old = Video.query.filter_by(creator_id=older, status="active").order_by(Video.id.desc()).first()
        if one_old:
            videos.append(one_old)
        return videos[:3]

    return Video.query.filter(Video.status == "active").order_by(Video.id.desc()).limit(3).all()


@public_bp.route("/video-search")
def video_search():
    from app.models import Video
    location = (request.args.get("location") or "").strip()
    date = (request.args.get("date") or "").strip()
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()

    q = Video.query.filter(Video.status == "active")
    if location:
        q = q.filter(db.func.lower(Video.location) == location.strip().lower())
    if date:
        q = q.filter(db.func.cast(Video.recorded_date, db.String) == date)
    if start_time:
        q = q.filter(Video.recorded_time >= start_time)
    if end_time:
        q = q.filter(Video.recorded_time <= end_time)
    videos = q.order_by(Video.recorded_at.desc().nullslast(), Video.id.desc()).limit(100).all()
    return render_template("public/video_search.html", videos=videos, video_locations=_dynamic_video_locations(), location=location, date=date, start_time=start_time, end_time=end_time)



@public_bp.route("/api/video-locations")
def api_video_locations():
    return jsonify({"locations": _dynamic_video_locations()})



@public_bp.route("/terms")
def terms():
    return render_template("public/terms.html")



@public_bp.route("/charters")
def charters_redirect():
    return redirect("https://charters.boatspotmedia.com", code=302)



@public_bp.route("/video-thumbnail/<int:video_id>")
def video_thumbnail(video_id):
    from app.models import Video
    video = Video.query.get_or_404(video_id)

    # Prefer direct public URL if present.
    for attr in ["public_thumbnail_url", "thumbnail_url"]:
        value = getattr(video, attr, None)
        if value:
            return redirect(value, code=302)

    key = getattr(video, "r2_thumbnail_key", None) or getattr(video, "thumbnail_path", None)

    # If no thumbnail exists, try to generate it server-side now.
    if not key:
        try:
            from app.routes.creator import _generate_and_attach_thumbnail_for_video
            from app import db
            ok = _generate_and_attach_thumbnail_for_video(video)
            if ok:
                db.session.commit()
                key = getattr(video, "r2_thumbnail_key", None) or getattr(video, "thumbnail_path", None)
        except Exception as e:
            try: print("on-demand thumbnail generation warning:", e)
            except Exception: pass

    if not key:
        abort(404)

    try:
        from app.services.r2 import get_r2_object_bytes
        data = get_r2_object_bytes(key)
        if not data:
            abort(404)
        return Response(data, mimetype="image/jpeg")
    except Exception:
        abort(404)


def video_thumbnail(video_id):
    from app.models import Video
    import io, os
    video = Video.query.get_or_404(video_id)

    # Prefer direct public URL if present.
    for attr in ["public_thumbnail_url", "thumbnail_url"]:
        value = getattr(video, attr, None)
        if value:
            return redirect(value, code=302)

    key = getattr(video, "r2_thumbnail_key", None) or getattr(video, "thumbnail_path", None)
    if not key:
        abort(404)

    try:
        from app.services.r2 import get_r2_client, _bucket_name
        obj = get_r2_client().get_object(Bucket=_bucket_name(), Key=key)
        data = obj["Body"].read()
        return Response(data, mimetype="image/jpeg")
    except Exception:
        abort(404)



@public_bp.route("/video/<int:video_id>")
def video_detail(video_id):
    from app.models import Video
    video = Video.query.get_or_404(video_id)
    return render_template("public/video_detail.html", video=video, creator_name=_public_creator_name, video_thumb_url=_public_video_thumb_url)
