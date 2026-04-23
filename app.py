import os
import uuid
import json
import random
import tempfile
import subprocess
import urllib.request
from datetime import datetime, date, time, timedelta
from functools import wraps
from pathlib import Path

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    session,
    send_from_directory,
    abort,
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

try:
    import stripe
except Exception:
    stripe = None

try:
    import boto3
except Exception:
    boto3 = None

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
VIDEO_DIR = UPLOAD_DIR / "videos"
THUMB_DIR = UPLOAD_DIR / "thumbs"
PREVIEW_DIR = UPLOAD_DIR / "previews"
LOGO_DIR = UPLOAD_DIR / "logos"

for p in [VIDEO_DIR, THUMB_DIR, PREVIEW_DIR, LOGO_DIR]:
    p.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "boatspotmedia-dev-secret")
db_url = os.getenv("DATABASE_URL", f"sqlite:///{BASE_DIR / 'boatspotmedia.db'}")
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql://", 1)
app.config["SQLALCHEMY_DATABASE_URI"] = db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["MAX_CONTENT_LENGTH"] = 4 * 1024 * 1024 * 1024

db = SQLAlchemy(app)

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_PUBLISHABLE_KEY = os.getenv("STRIPE_PUBLISHABLE_KEY", "")
if stripe and STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_BUCKET = os.getenv("R2_BUCKET", "")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_PUBLIC_BASE_URL = os.getenv("R2_PUBLIC_BASE_URL", "").rstrip("/")

r2_client = None
if boto3 and R2_ACCOUNT_ID and R2_BUCKET and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY:
    endpoint = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
    r2_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
    )

TRANSLATIONS = {
    "en": {
        "site_title": "BoatSpotMedia",
        "tagline": "Find your boat video by location, date and time range.",
        "home": "Home", "search": "Search", "services": "Services", "store": "Store",
        "creator_access": "Creator Access", "creator_login": "Creator Login", "buyer_login": "Buyer Login",
        "buyer_register": "Buyer Register", "support": "Support", "how_it_works": "How it works", "terms": "Terms & Conditions",
        "location": "Location", "date": "Date", "from": "From", "to": "To", "all_fields_required": "Please select Location, Date and Time range to search.",
        "latest_uploads": "Latest uploads", "featured_ad": "Advertise with us", "featured_ad_sub": "Promote your brand on BoatSpotMedia",
        "first_name": "First name", "last_name": "Last name", "brand_name": "Brand / public name", "email": "Email", "social_link": "Instagram / YouTube / Facebook", "primary_location": "Primary filming location",
        "submit": "Submit", "pending_review": "Your creator account is under review.", "creator_review_24h": "Creator applications are reviewed within 24 hours.",
        "dashboard": "Dashboard", "logout": "Logout", "login": "Login", "password": "Password", "search_results": "Search results",
        "buy": "Buy", "price": "Price", "cart": "Cart", "checkout": "Checkout", "add_to_cart": "Add to cart",
        "my_purchases": "My purchases", "creator_dashboard": "Creator Dashboard", "buyer_dashboard": "Buyer Dashboard",
        "upload_batch": "Upload batch", "batch_title": "Batch title", "upload_files": "Upload video files", "default_price": "Default price for this batch", "delivery_type": "Delivery type",
        "instant": "Instant", "edited": "Edited", "save_batch": "Save batch", "video_list": "Video list", "products": "Products", "orders": "Orders", "pricing": "Pricing", "settings": "Settings", "plans": "Plans",
        "connect_stripe": "Connect Stripe", "payout_status": "Payout status", "not_connected": "Not connected", "connected": "Connected",
        "owner_login": "Owner Login", "applications": "Applications", "creators": "Creators", "service_listings": "Service listings", "support_requests": "Support requests",
        "approve": "Approve", "reject": "Reject", "reset_password": "Reset password", "new_password": "New password",
        "language": "Language", "message": "Message", "send": "Send", "support_sent": "Support request sent.",
        "category": "Category", "city": "City", "website": "Website", "description": "Description", "register_service": "Advertise your marine business", "service_registered": "Service listing submitted.",
        "standard_listing": "Standard Listing", "monthly_price": "Monthly price", "public_name": "Public name", "logo": "Logo", "save": "Save", "save_settings": "Save settings",
        "creator_stripe_notice": "Complete your Stripe payout setup to receive earnings from your sales.", "creator_approved": "Approved creator", "status": "Status", "pending": "Pending", "approved": "Approved", "rejected": "Rejected",
        "rating_coming": "Rating coming soon", "new_creator": "New creator", "view_store": "View store", "view_creator": "View creator",
        "terms_body": "Digital downloads are non-refundable once delivered, except in cases of technical failure or incorrect delivery. Edited video orders may be refunded if not delivered within 72 hours or if the delivered file is incorrect or corrupted and reported within 24 hours.",
        "how_body": "Buyers search by location, date and time range. Approved creators upload videos, set prices per clip, and deliver edited orders within a maximum of 72 hours.",
        "delivery_72": "Edited delivery within 72 hours", "preview_note": "Preview is 8 seconds from the middle of the video, with animated watermark and no controls.",
        "starter": "Starter", "pro": "Pro", "elite": "Elite", "plan_pricing": "Publishing plans", "storage_notice": "Capacity and billing rules are prepared for testing.",
        "admin_users": "Admin users", "analytics": "Analytics", "control": "Control", "back": "Back",
        "service_categories": "Service categories", "go_to_dashboard": "Go to dashboard", "order_created": "Order created.", "socials":"Social links", "space_available":"Space available", "contact_for_access":"Contact us for creator access", "service_spotlight":"Marine services", "packages":"Packages", "logo_saved":"Logo uploaded successfully.", "support_email_notice":"We'll send you the private link by email.", "follow_creator":"Follow this creator", "videos": "Videos",
    },
    "es": {
        "site_title": "BoatSpotMedia",
        "tagline": "Encuentra tu video por ubicación, fecha y rango de hora.",
        "home": "Inicio", "search": "Buscar", "services": "Servicios", "store": "Tienda",
        "creator_access": "Acceso de creador", "creator_login": "Ingreso creador", "buyer_login": "Ingreso comprador",
        "buyer_register": "Registro comprador", "support": "Soporte", "how_it_works": "Cómo funciona", "terms": "Términos y condiciones",
        "location": "Ubicación", "date": "Fecha", "from": "Desde", "to": "Hasta", "all_fields_required": "Seleccione ubicación, fecha y rango de hora para buscar.",
        "latest_uploads": "Últimos videos", "featured_ad": "Anúnciate con nosotros", "featured_ad_sub": "Promociona tu marca en BoatSpotMedia",
        "first_name": "Nombre", "last_name": "Apellido", "brand_name": "Marca / nombre público", "email": "Correo", "social_link": "Instagram / YouTube / Facebook", "primary_location": "Ubicación principal de grabación",
        "submit": "Enviar", "pending_review": "Tu cuenta de creador está en revisión.", "creator_review_24h": "Las solicitudes de creador se revisan en 24 horas.",
        "dashboard": "Panel", "logout": "Salir", "login": "Entrar", "password": "Contraseña", "search_results": "Resultados",
        "buy": "Comprar", "price": "Precio", "cart": "Carrito", "checkout": "Pagar", "add_to_cart": "Agregar al carrito",
        "my_purchases": "Mis compras", "creator_dashboard": "Panel del creador", "buyer_dashboard": "Panel del comprador",
        "upload_batch": "Subir lote", "batch_title": "Nombre del lote", "upload_files": "Subir videos", "default_price": "Precio base del lote", "delivery_type": "Tipo de entrega",
        "instant": "Instantáneo", "edited": "Editado", "save_batch": "Guardar lote", "video_list": "Lista de videos", "products": "Productos", "orders": "Órdenes", "pricing": "Precios", "settings": "Ajustes", "plans": "Planes",
        "connect_stripe": "Conectar Stripe", "payout_status": "Estado de pago", "not_connected": "No conectado", "connected": "Conectado",
        "owner_login": "Ingreso Owner", "applications": "Solicitudes", "creators": "Creadores", "service_listings": "Servicios", "support_requests": "Solicitudes de soporte",
        "approve": "Aprobar", "reject": "Rechazar", "reset_password": "Resetear contraseña", "new_password": "Nueva contraseña",
        "language": "Idioma", "message": "Mensaje", "send": "Enviar", "support_sent": "Solicitud de soporte enviada.",
        "category": "Categoría", "city": "Ciudad", "website": "Sitio web", "description": "Descripción", "register_service": "Publica tu negocio náutico", "service_registered": "Anuncio enviado.",
        "standard_listing": "Anuncio estándar", "monthly_price": "Precio mensual", "public_name": "Nombre público", "logo": "Logo", "save": "Guardar", "save_settings": "Guardar ajustes",
        "creator_stripe_notice": "Completa Stripe para recibir tus ganancias.", "creator_approved": "Creador aprobado", "status": "Estado", "pending": "Pendiente", "approved": "Aprobado", "rejected": "Rechazado",
        "rating_coming": "Calificación próximamente", "new_creator": "Creador nuevo", "view_store": "Ver tienda", "view_creator": "Ver creador",
        "terms_body": "Las descargas digitales no son reembolsables una vez entregadas, salvo fallas técnicas o archivo incorrecto. Los videos editados pueden ser reembolsados si no se entregan en 72 horas o si el archivo entregado es incorrecto o corrupto y se reporta en 24 horas.",
        "how_body": "Los compradores buscan por ubicación, fecha y rango de hora. Los creadores aprobados suben videos, fijan precios por clip y entregan órdenes editadas dentro de 72 horas.",
        "delivery_72": "Entrega editada dentro de 72 horas", "preview_note": "El preview usa 8 segundos del centro del video, con watermark animado y sin controles.",
        "starter": "Starter", "pro": "Pro", "elite": "Elite", "plan_pricing": "Planes de publicación", "storage_notice": "La capacidad y cobros ya están preparados para pruebas.",
        "admin_users": "Admins", "analytics": "Analíticas", "control": "Control", "back": "Volver",
        "service_categories": "Categorías de servicios", "go_to_dashboard": "Ir al panel", "order_created": "Orden creada.", "socials":"Redes", "space_available":"Espacio disponible", "contact_for_access":"Contáctanos para acceso de creador", "service_spotlight":"Servicios náuticos", "packages":"Paquetes", "logo_saved":"Logo subido correctamente.", "support_email_notice":"Te enviaremos el enlace privado por correo.", "follow_creator":"Sigue a este creador", "videos": "Videos",
    }
}

SERVICE_CATEGORIES = [
    "Engine Repair", "Boat Maintenance", "Marine Mechanics", "Fiberglass Repair", "Hull Repair", "Propeller Service",
    "Boat Detailing", "Hull Cleaning", "Marine Wraps", "Ceramic Coating", "Bottom Cleaning", "Boat Polishing",
    "Marine Electronics", "GPS / Radar Installation", "Sound Systems", "Lighting Installation", "Battery Systems", "Trolling Motor Install",
    "Marine Upholstery", "Seat Repair", "Canvas & Covers", "Bimini Tops", "Boat Flooring", "SeaDek Installation",
    "Boat Transport", "Trailer Services", "Boat Storage", "Dry Storage Facilities", "Lift Installation",
    "Marina Slips", "Dock Services", "Fuel Docks", "Boat Clubs", "Private Dock Rental",
    "Licensed Captains", "Boat Delivery", "Boat Training", "Fishing Guides", "Charter Captains",
    "Boat Brokers", "Boat Dealers", "Boat Rentals", "Yacht Charters", "Consignment Sales",
    "Marine Parts", "Boat Accessories", "Safety Equipment", "Fishing Equipment", "Navigation Gear", "Propellers",
    "Boat Photography", "Drone Services", "Hull Inspections", "Surveyors", "Insurance Services", "Financing Services",
]

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    role = db.Column(db.String(20), nullable=False, default="buyer")
    email = db.Column(db.String(255), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    first_name = db.Column(db.String(80))
    last_name = db.Column(db.String(80))
    public_name = db.Column(db.String(120))
    approved = db.Column(db.Boolean, default=False)
    payout_connected = db.Column(db.Boolean, default=False)
    stripe_account_id = db.Column(db.String(120))
    plan = db.Column(db.String(20), default="starter")
    social_link = db.Column(db.String(255))
    social_link_2 = db.Column(db.String(255))
    primary_location = db.Column(db.String(120))
    logo_path = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class CreatorApplication(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String(80), nullable=False)
    last_name = db.Column(db.String(80), nullable=False)
    brand_name = db.Column(db.String(120), nullable=False)
    email = db.Column(db.String(255), unique=True, nullable=False)
    social_link = db.Column(db.String(255), nullable=False)
    primary_location = db.Column(db.String(120), nullable=False)
    status = db.Column(db.String(20), default="pending")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Batch(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    creator_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    title = db.Column(db.String(150), nullable=False)
    location = db.Column(db.String(120), nullable=False)
    recorded_date = db.Column(db.Date, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Video(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    batch_id = db.Column(db.Integer, db.ForeignKey("batch.id"), nullable=False)
    creator_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    filename = db.Column(db.String(255), nullable=False)
    file_path = db.Column(db.String(255), nullable=False)
    thumb_path = db.Column(db.String(255))
    preview_path = db.Column(db.String(255))
    location = db.Column(db.String(120), nullable=False)
    recorded_date = db.Column(db.Date, nullable=False)
    recorded_time = db.Column(db.Time, nullable=False)
    price = db.Column(db.Float, nullable=False, default=40.0)
    delivery_type = db.Column(db.String(20), nullable=False, default="instant")
    boat_model = db.Column(db.String(120))
    registration = db.Column(db.String(120))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Product(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    creator_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    title = db.Column(db.String(120), nullable=False)
    price = db.Column(db.Float, nullable=False)
    description = db.Column(db.Text)
    active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Package(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    creator_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    title = db.Column(db.String(120), nullable=False)
    description = db.Column(db.Text)
    price = db.Column(db.Float, nullable=False, default=40.0)
    delivery_type = db.Column(db.String(20), nullable=False, default="instant")
    turnaround_hours = db.Column(db.Integer, nullable=False, default=72)
    active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class CartItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    buyer_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    item_type = db.Column(db.String(20), nullable=False)
    item_id = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Order(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    buyer_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    total = db.Column(db.Float, nullable=False, default=0.0)
    status = db.Column(db.String(30), nullable=False, default="paid")
    payout_status = db.Column(db.String(30), nullable=False, default="hold")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class OrderItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    order_id = db.Column(db.Integer, db.ForeignKey("order.id"), nullable=False)
    item_type = db.Column(db.String(20), nullable=False)
    item_id = db.Column(db.Integer, nullable=False)
    price = db.Column(db.Float, nullable=False)
    delivery_status = db.Column(db.String(30), default="instant_ready")
    delivered_at = db.Column(db.DateTime)
    payout_release_at = db.Column(db.DateTime)
    edited_file_path = db.Column(db.String(255))
    download_expires_at = db.Column(db.DateTime)
    download_available = db.Column(db.Boolean, default=True)
    thumbnail_path_snapshot = db.Column(db.String(255))
    video_title_snapshot = db.Column(db.String(255))
    creator_name_snapshot = db.Column(db.String(120))
    recorded_date_snapshot = db.Column(db.Date)
    recorded_time_snapshot = db.Column(db.Time)
    location_snapshot = db.Column(db.String(120))

class ServiceListing(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    business_name = db.Column(db.String(120), nullable=False)
    category = db.Column(db.String(120), nullable=False)
    email = db.Column(db.String(255), nullable=False)
    city = db.Column(db.String(120), nullable=False)
    website = db.Column(db.String(255))
    description = db.Column(db.Text)
    status = db.Column(db.String(20), default="active")
    monthly_price = db.Column(db.Float, default=10.0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Review(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    creator_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    buyer_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    stars = db.Column(db.Integer, nullable=False)
    order_item_id = db.Column(db.Integer, db.ForeignKey("order_item.id"), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class SupportRequest(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120))
    email = db.Column(db.String(255))
    message = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class SiteSetting(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(100), unique=True)
    value = db.Column(db.String(255))

def t(key):
    lang = session.get("lang", "en")
    return TRANSLATIONS.get(lang, TRANSLATIONS["en"]).get(key, key)

def get_setting(key, default=""):
    s = SiteSetting.query.filter_by(key=key).first()
    return s.value if s else default

def set_setting(key, value):
    s = SiteSetting.query.filter_by(key=key).first()
    if not s:
        s = SiteSetting(key=key, value=value)
        db.session.add(s)
    else:
        s.value = value

def get_current_user():
    uid = session.get("user_id")
    return db.session.get(User, uid) if uid else None

@app.context_processor
def inject_globals():
    user = get_current_user()
    locations = [row[0] for row in db.session.query(Video.location).distinct().order_by(Video.location).all() if row[0]]
    cart_count = 0
    if user and user.role == "buyer":
        cart_count = CartItem.query.filter_by(buyer_id=user.id).count()
    return dict(t=t, lang=session.get("lang", "en"), current_user=user, creator_locations=locations, cart_count=cart_count, service_categories=SERVICE_CATEGORIES, get_setting=get_setting, db=db)

def parse_date(value):
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m %d %Y", "%m%d%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) == 8:
        try:
            return datetime.strptime(digits, "%m%d%Y").date()
        except Exception:
            return None
    return None

def parse_time(value):
    if not value:
        return None
    s = value.strip().upper().replace(".", "")
    for fmt in ("%H:%M", "%I:%M %p", "%I:%M%p"):
        try:
            return datetime.strptime(s, fmt).time()
        except Exception:
            pass
    return None

def login_required(role=None):
    def deco(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            u = get_current_user()
            if not u:
                return redirect(url_for("buyer_login"))
            if role and u.role != role:
                return redirect(url_for("index"))
            return fn(*args, **kwargs)
        return wrapped
    return deco

def owner_required(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        if not session.get("owner_auth"):
            return redirect(url_for("control_login"))
        return fn(*args, **kwargs)
    return wrapped

def media_url(path):
    if not path:
        return ""
    if path.startswith("http://") or path.startswith("https://"):
        return path
    parts = path.split("/", 1)
    if len(parts) != 2:
        return ""
    return url_for("uploaded_file", category=parts[0], filename=parts[1])

app.jinja_env.filters["media_url"] = media_url

def save_to_r2(src_path: Path, object_key: str) -> str:
    if r2_client and R2_BUCKET:
        r2_client.upload_file(str(src_path), R2_BUCKET, object_key)
        if R2_PUBLIC_BASE_URL:
            return f"{R2_PUBLIC_BASE_URL}/{object_key}"
    return object_key

def delete_r2_object(path_value: str):
    if not (r2_client and R2_BUCKET and path_value):
        return
    if path_value.startswith("http://") or path_value.startswith("https://"):
        if R2_PUBLIC_BASE_URL and path_value.startswith(R2_PUBLIC_BASE_URL + "/"):
            object_key = path_value.replace(R2_PUBLIC_BASE_URL + "/", "", 1)
        else:
            return
    else:
        object_key = path_value
    try:
        r2_client.delete_object(Bucket=R2_BUCKET, Key=object_key)
    except Exception as e:
        print("R2 DELETE ERROR:", e)

def delete_local_object(path_value: str):
    if not path_value or path_value.startswith("http://") or path_value.startswith("https://"):
        return
    try:
        category, filename = path_value.split("/", 1)
    except ValueError:
        return
    directory = {"videos": VIDEO_DIR, "thumbs": THUMB_DIR, "previews": PREVIEW_DIR, "logos": LOGO_DIR}.get(category)
    if not directory:
        return
    try:
        f = directory / filename
        if f.exists() and f.is_file():
            f.unlink()
    except Exception as e:
        print("LOCAL DELETE ERROR:", e)

def delete_asset(path_value: str):
    if not path_value:
        return
    delete_r2_object(path_value)
    delete_local_object(path_value)

def delete_video_assets(video: Video):
    if not video:
        return
    delete_asset(video.file_path)
    delete_asset(video.thumb_path)
    delete_asset(video.preview_path)

def download_temp_logo(url: str) -> Path | None:
    try:
        suffix = Path(url).suffix or ".png"
        fd, tmp_name = tempfile.mkstemp(suffix=suffix)
        os.close(fd)
        urllib.request.urlretrieve(url, tmp_name)
        return Path(tmp_name)
    except Exception as e:
        print("TEMP LOGO DOWNLOAD ERROR:", e)
        return None

def ffprobe_duration(path: Path):
    try:
        result = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "json", str(path)], capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        return float(data["format"]["duration"])
    except Exception as e:
        print("FFPROBE ERROR:", e)
        return 16.0


def ffprobe_created_datetime(path: Path):
    try:
        result = subprocess.run([
            "ffprobe", "-v", "error",
            "-show_entries", "format_tags=creation_time:stream_tags=creation_time",
            "-of", "json", str(path)
        ], capture_output=True, text=True, check=True)
        data = json.loads(result.stdout or '{}')
        candidates = []
        fmt_tags = data.get('format', {}).get('tags', {}) if isinstance(data.get('format'), dict) else {}
        if fmt_tags.get('creation_time'):
            candidates.append(fmt_tags.get('creation_time'))
        for stream in data.get('streams', []):
            tags = stream.get('tags', {})
            if tags.get('creation_time'):
                candidates.append(tags.get('creation_time'))
        for raw in candidates:
            try:
                return datetime.fromisoformat(raw.replace('Z', '+00:00')).astimezone().replace(tzinfo=None)
            except Exception:
                continue
    except Exception as e:
        print('FFPROBE CREATION TIME ERROR:', e)
    return None

def build_preview_assets(video_path: Path, creator_display: str, logo_path: Path | str | None = None):
    stem = video_path.stem + "_" + uuid.uuid4().hex[:8]
    thumb_file = THUMB_DIR / f"{stem}.jpg"
    preview_file = PREVIEW_DIR / f"{stem}.mp4"

    dur = ffprobe_duration(video_path)
    start = max(0.25, min(dur / 2, max(0.25, dur - 8.5)))
    thumb_time = max(0.25, min(start + 0.8, max(0.25, dur - 0.25)))

    try:
        thumb_cmd = [
            "ffmpeg", "-y",
            "-ss", str(thumb_time),
            "-i", str(video_path),
            "-frames:v", "1",
            "-vf", "thumbnail,scale=640:-2,format=yuvj420p",
            "-q:v", "3",
            str(thumb_file),
        ]
        thumb_run = subprocess.run(thumb_cmd, capture_output=True, text=True)
        if thumb_run.returncode != 0:
            print("THUMB FFMPEG ERROR:", thumb_run.stderr)

        preview_cmd = [
            "ffmpeg", "-y",
            "-ss", str(start),
            "-i", str(video_path),
            "-t", "8",
            "-vf", "scale=960:-2,format=yuv420p",
            "-an",
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-crf", "32",
            "-movflags", "+faststart",
            "-pix_fmt", "yuv420p",
            str(preview_file),
        ]
        preview_run = subprocess.run(preview_cmd, capture_output=True, text=True)
        if preview_run.returncode != 0:
            print("PREVIEW FFMPEG ERROR:", preview_run.stderr)
    except Exception as e:
        print("FFMPEG PROCESS ERROR:", e)

    thumb_result = thumb_file if thumb_file.exists() and thumb_file.stat().st_size > 0 else None
    preview_result = preview_file if preview_file.exists() and preview_file.stat().st_size > 0 else None
    print("THUMB GENERATED:", thumb_result)
    print("PREVIEW GENERATED:", preview_result)
    return thumb_result, preview_result

def creator_rating(creator_id):
    reviews = Review.query.filter_by(creator_id=creator_id).all()
    if len(reviews) < 3:
        return None, len(reviews)
    avg = round(sum(r.stars for r in reviews) / len(reviews), 1)
    return avg, len(reviews)

def ensure_order_item_columns():
    try:
        engine = db.engine
        dialect = engine.dialect.name
        if dialect == "sqlite":
            result = db.session.execute(text("PRAGMA table_info(order_item)")).fetchall()
            cols = {row[1] for row in result}
            statements = []
            if "download_expires_at" not in cols:
                statements.append("ALTER TABLE order_item ADD COLUMN download_expires_at DATETIME")
            if "download_available" not in cols:
                statements.append("ALTER TABLE order_item ADD COLUMN download_available BOOLEAN DEFAULT 1")
            if "thumbnail_path_snapshot" not in cols:
                statements.append("ALTER TABLE order_item ADD COLUMN thumbnail_path_snapshot VARCHAR(255)")
            if "video_title_snapshot" not in cols:
                statements.append("ALTER TABLE order_item ADD COLUMN video_title_snapshot VARCHAR(255)")
            if "creator_name_snapshot" not in cols:
                statements.append("ALTER TABLE order_item ADD COLUMN creator_name_snapshot VARCHAR(120)")
            if "recorded_date_snapshot" not in cols:
                statements.append("ALTER TABLE order_item ADD COLUMN recorded_date_snapshot DATE")
            if "recorded_time_snapshot" not in cols:
                statements.append("ALTER TABLE order_item ADD COLUMN recorded_time_snapshot TIME")
            if "location_snapshot" not in cols:
                statements.append("ALTER TABLE order_item ADD COLUMN location_snapshot VARCHAR(120)")
        else:
            result = db.session.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'order_item'" )).fetchall()
            cols = {row[0] for row in result}
            statements = []
            if "download_expires_at" not in cols:
                statements.append("ALTER TABLE order_item ADD COLUMN download_expires_at TIMESTAMP")
            if "download_available" not in cols:
                statements.append("ALTER TABLE order_item ADD COLUMN download_available BOOLEAN DEFAULT TRUE")
            if "thumbnail_path_snapshot" not in cols:
                statements.append("ALTER TABLE order_item ADD COLUMN thumbnail_path_snapshot VARCHAR(255)")
            if "video_title_snapshot" not in cols:
                statements.append("ALTER TABLE order_item ADD COLUMN video_title_snapshot VARCHAR(255)")
            if "creator_name_snapshot" not in cols:
                statements.append("ALTER TABLE order_item ADD COLUMN creator_name_snapshot VARCHAR(120)")
            if "recorded_date_snapshot" not in cols:
                statements.append("ALTER TABLE order_item ADD COLUMN recorded_date_snapshot DATE")
            if "recorded_time_snapshot" not in cols:
                statements.append("ALTER TABLE order_item ADD COLUMN recorded_time_snapshot TIME")
            if "location_snapshot" not in cols:
                statements.append("ALTER TABLE order_item ADD COLUMN location_snapshot VARCHAR(120)")
        for stmt in statements:
            db.session.execute(text(stmt))
        if statements:
            db.session.commit()
    except Exception as e:
        db.session.rollback()
        print("ensure_order_item_columns ERROR:", e)

@app.route('/set-language/<lang>')
def set_language(lang):
    if lang in ('en', 'es'):
        session['lang'] = lang
    return redirect(request.referrer or url_for('index'))

@app.route('/uploads/<category>/<path:filename>')
def uploaded_file(category, filename):
    directory = {"videos": VIDEO_DIR, "thumbs": THUMB_DIR, "previews": PREVIEW_DIR, "logos": LOGO_DIR}.get(category)
    if not directory:
        abort(404)
    return send_from_directory(directory, filename)

@app.route('/')
def index():
    latest_videos = Video.query.order_by(Video.recorded_date.desc(), Video.recorded_time.desc()).limit(12).all()
    creator_names = {}
    for v in latest_videos:
        creator = db.session.get(User, v.creator_id)
        creator_names[v.id] = creator.public_name if creator and creator.public_name else (creator.email.split('@')[0] if creator else 'Boat creator')
    all_services = ServiceListing.query.filter_by(status='active').all()
    latest_services = random.sample(all_services, min(3, len(all_services))) if all_services else []
    featured_ad = {"title": t('space_available'), "subtitle": t('featured_ad_sub')}
    return render_template('index.html', latest_videos=latest_videos, latest_services=latest_services, featured_ad=featured_ad, creator_names=creator_names)

@app.route('/search')
def search():
    location = request.args.get('location', '').strip().title()
    raw_date = request.args.get('date', '') or request.args.get('date_text', '')
    d = parse_date(raw_date)
    tf = parse_time(request.args.get('from', ''))
    tt = parse_time(request.args.get('to', ''))
    if not location or not d or not tf or not tt:
        flash(t('all_fields_required'))
        return redirect(url_for('index'))
    results = Video.query.filter(Video.location == location, Video.recorded_date == d, Video.recorded_time >= tf, Video.recorded_time <= tt).order_by(Video.recorded_time.asc()).all()
    creator_names = {}
    for v in results:
        creator = db.session.get(User, v.creator_id)
        creator_names[v.id] = creator.public_name if creator and creator.public_name else (creator.email.split('@')[0] if creator else 'Boat creator')
    return render_template('search.html', results=results, location=location, date=d, time_from=tf, time_to=tt, creator_names=creator_names)

@app.route('/video/<int:video_id>')
def video_detail(video_id):
    video = db.session.get(Video, video_id)
    if not video:
        flash('Video not found.')
        return redirect(url_for('index'))
    creator = db.session.get(User, video.creator_id)
    if not creator:
        flash('Creator not found.')
        return redirect(url_for('index'))
    rating, reviews = creator_rating(creator.id)
    creator_packages = Package.query.filter_by(creator_id=creator.id, active=True).order_by(Package.created_at.asc()).all()
    return render_template('video_detail.html', video=video, creator=creator, rating=rating, reviews=reviews, creator_packages=creator_packages)

@app.route('/creator-access', methods=['GET', 'POST'])
def creator_access():
    if request.method == 'POST':
        email = request.form['email'].strip().lower()
        if CreatorApplication.query.filter_by(email=email).first() or User.query.filter_by(email=email).first():
            flash('Email already exists.')
            return redirect(url_for('creator_access'))
        app_item = CreatorApplication(first_name=request.form['first_name'].strip(), last_name=request.form['last_name'].strip(), brand_name=request.form['brand_name'].strip(), email=email, social_link=request.form['social_link'].strip(), primary_location=request.form['primary_location'].strip().title())
        db.session.add(app_item)
        db.session.commit()
        flash(t('creator_review_24h'))
        return redirect(url_for('creator_login'))
    return render_template('creator_access.html')

@app.route('/creator-login', methods=['GET', 'POST'])
def creator_login():
    if request.method == 'POST':
        user = User.query.filter_by(email=request.form['email'].strip().lower(), role='creator').first()
        if not user or not check_password_hash(user.password_hash, request.form['password']):
            flash('Invalid credentials.')
            return redirect(url_for('creator_login'))
        if not user.approved:
            flash(t('pending_review'))
            return redirect(url_for('creator_login'))
        session['user_id'] = user.id
        return redirect(url_for('creator_dashboard'))
    return render_template('creator_login.html')

@app.route('/buyer-register', methods=['GET', 'POST'])
def buyer_register():
    if request.method == 'POST':
        email = request.form['email'].strip().lower()
        if User.query.filter_by(email=email).first():
            flash('Email already exists.')
            return redirect(url_for('buyer_register'))
        user = User(role='buyer', email=email, password_hash=generate_password_hash(request.form['password']), first_name=request.form.get('first_name', '').strip(), last_name=request.form.get('last_name', '').strip(), approved=True)
        db.session.add(user)
        db.session.commit()
        session['user_id'] = user.id
        return redirect(url_for('buyer_dashboard'))
    return render_template('buyer_register.html')

@app.route('/buyer-login', methods=['GET', 'POST'])
def buyer_login():
    if request.method == 'POST':
        user = User.query.filter_by(email=request.form['email'].strip().lower(), role='buyer').first()
        if not user or not check_password_hash(user.password_hash, request.form['password']):
            flash('Invalid credentials.')
            return redirect(url_for('buyer_login'))
        session['user_id'] = user.id
        return redirect(url_for('buyer_dashboard'))
    return render_template('buyer_login.html')

@app.route('/buyer')
@login_required('buyer')
def buyer_dashboard():
    user = get_current_user()
    orders = Order.query.filter_by(buyer_id=user.id).order_by(Order.created_at.desc()).all()
    enriched = []
    changed = False
    for o in orders:
        items = OrderItem.query.filter_by(order_id=o.id).all()
        for item in items:
            if item.download_available and item.download_expires_at and item.download_expires_at <= datetime.utcnow():
                item.download_available = False
                changed = True
        enriched.append((o, items))
    if changed:
        db.session.commit()
    cart_items = CartItem.query.filter_by(buyer_id=user.id).all()
    display_cart = []
    for c in cart_items:
        if c.item_type == 'video':
            v = db.session.get(Video, c.item_id)
            label = f"{v.location} · {v.recorded_time.strftime('%I:%M %p')}" if v else 'Video'
            display_cart.append((c, label, v.price if v else 0))
        else:
            p = db.session.get(Product, c.item_id)
            display_cart.append((c, p.title if p else 'Product', p.price if p else 0))
    return render_template('buyer_dashboard.html', orders=enriched, cart_items=display_cart, now=datetime.utcnow())

@app.route('/buyer/order-item/<int:item_id>/download')
@login_required('buyer')
def download_order_item(item_id):
    user = get_current_user()
    item = db.session.get(OrderItem, item_id)
    if not item:
        flash('Download not found.')
        return redirect(url_for('buyer_dashboard'))
    order = db.session.get(Order, item.order_id)
    if not order or order.buyer_id != user.id:
        flash('Unauthorized download.')
        return redirect(url_for('buyer_dashboard'))
    if item.item_type != 'video':
        flash('This item is not downloadable.')
        return redirect(url_for('buyer_dashboard'))
    if (not item.download_available or not item.download_expires_at or item.download_expires_at <= datetime.utcnow()):
        item.download_available = False
        db.session.commit()
        flash('Download expired.')
        return redirect(url_for('buyer_dashboard'))
    if item.delivery_status == 'delivered' and item.edited_file_path:
        path_value = item.edited_file_path
    else:
        video = db.session.get(Video, item.item_id)
        if not video or not video.file_path:
            flash('Original file is no longer available.')
            return redirect(url_for('buyer_dashboard'))
        path_value = video.file_path
    if path_value.startswith('http://') or path_value.startswith('https://'):
        return redirect(path_value)
    category, filename = path_value.split('/', 1)
    directory = {"videos": VIDEO_DIR, "thumbs": THUMB_DIR, "previews": PREVIEW_DIR, "logos": LOGO_DIR}.get(category)
    if not directory:
        flash('Invalid file location.')
        return redirect(url_for('buyer_dashboard'))
    return send_from_directory(directory, filename, as_attachment=True)

@app.route('/cart/add/video/<int:video_id>', methods=['POST'])
@login_required('buyer')
def add_video_cart(video_id):
    user = get_current_user()
    exists = CartItem.query.filter_by(buyer_id=user.id, item_type='video', item_id=video_id).first()
    if not exists:
        db.session.add(CartItem(buyer_id=user.id, item_type='video', item_id=video_id))
        db.session.commit()
    return redirect(url_for('buyer_dashboard'))

@app.route('/cart/add/product/<int:product_id>', methods=['POST'])
@login_required('buyer')
def add_product_cart(product_id):
    user = get_current_user()
    exists = CartItem.query.filter_by(buyer_id=user.id, item_type='product', item_id=product_id).first()
    if not exists:
        db.session.add(CartItem(buyer_id=user.id, item_type='product', item_id=product_id))
        db.session.commit()
    return redirect(url_for('buyer_dashboard'))

@app.route('/cart/remove/<int:item_id>', methods=['POST'])
@login_required('buyer')
def remove_cart_item(item_id):
    user = get_current_user()
    item = db.session.get(CartItem, item_id)
    if item and item.buyer_id == user.id:
        db.session.delete(item)
        db.session.commit()
    return redirect(url_for('buyer_dashboard'))

@app.route('/checkout', methods=['POST'])
@login_required('buyer')
def checkout():
    user = get_current_user()
    cart = CartItem.query.filter_by(buyer_id=user.id).all()
    if not cart:
        return redirect(url_for('buyer_dashboard'))
    total = 0.0
    order = Order(buyer_id=user.id, total=0.0, status='paid', payout_status='hold')
    db.session.add(order)
    db.session.flush()
    for c in cart:
        if c.item_type == 'video':
            video = db.session.get(Video, c.item_id)
            if not video:
                db.session.delete(c)
                continue
            creator = db.session.get(User, video.creator_id)
            creator_name = creator.public_name if creator and creator.public_name else 'Boat creator'
            price = video.price
            total += price
            status = 'instant_ready' if video.delivery_type == 'instant' else 'pending_edit'
            db.session.add(OrderItem(order_id=order.id, item_type='video', item_id=video.id, price=price, delivery_status=status, download_expires_at=datetime.utcnow() + timedelta(days=7), download_available=True, thumbnail_path_snapshot=video.thumb_path or '', video_title_snapshot=f"{video.location} · {video.recorded_time.strftime('%I:%M %p')}", creator_name_snapshot=creator_name, recorded_date_snapshot=video.recorded_date, recorded_time_snapshot=video.recorded_time, location_snapshot=video.location))
        elif c.item_type == 'product':
            product = db.session.get(Product, c.item_id)
            if not product:
                db.session.delete(c)
                continue
            price = product.price
            total += price
            db.session.add(OrderItem(order_id=order.id, item_type='product', item_id=product.id, price=price, delivery_status='processing', download_available=False))
        db.session.delete(c)
    order.total = total
    db.session.commit()
    flash(t('order_created'))
    return redirect(url_for('buyer_dashboard'))

@app.route('/creator')
@login_required('creator')
def creator_dashboard():
    user = get_current_user()
    batches = Batch.query.filter_by(creator_id=user.id).order_by(Batch.created_at.desc()).all()
    videos = Video.query.filter_by(creator_id=user.id).order_by(Video.recorded_date.desc(), Video.recorded_time.desc()).all()
    products = Product.query.filter_by(creator_id=user.id).order_by(Product.created_at.desc()).all()
    packages = Package.query.filter_by(creator_id=user.id).order_by(Package.created_at.desc()).all()
    order_items = db.session.query(OrderItem, Order).join(Order, OrderItem.order_id == Order.id).join(Video, db.and_(OrderItem.item_id == Video.id, OrderItem.item_type == 'video')).filter(Video.creator_id == user.id).order_by(Order.created_at.desc()).all()
    order_rows = []
    for item, order in order_items:
        video = db.session.get(Video, item.item_id)
        order_rows.append((item, order, video))
    rating, review_count = creator_rating(user.id)
    return render_template('creator_dashboard.html', user=user, batches=batches, videos=videos, products=products, packages=packages, order_rows=order_rows, rating=rating, review_count=review_count)

@app.route('/creator/upload', methods=['POST'])
@login_required('creator')
def creator_upload():
    user = get_current_user()
    title = request.form.get('batch_title', '').strip()
    location = request.form.get('location', '').strip().title()
    files = request.files.getlist('files')
    valid_files = [f for f in files if f and f.filename]

    if not location:
        flash('Missing location.')
        return redirect(url_for('creator_dashboard'))
    if not valid_files:
        flash('No files selected.')
        return redirect(url_for('creator_dashboard'))
    if not title:
        title = f"{location} Batch {datetime.utcnow().strftime('%m/%d/%Y %H:%M')}"

    existing_batch = Batch.query.filter_by(creator_id=user.id, title=title).first()
    if existing_batch:
        flash('Batch name already exists. Please choose another name.')
        return redirect(url_for('creator_dashboard'))

    batch_date = date.today()
    batch = Batch(creator_id=user.id, title=title, location=location, recorded_date=batch_date)
    db.session.add(batch)
    db.session.flush()

    logo_path = user.logo_path or None
    creator_name = user.public_name or user.email.split('@')[0]
    first_package = Package.query.filter_by(creator_id=user.id, active=True).order_by(Package.created_at.asc()).first()
    default_price = first_package.price if first_package else 40.0
    delivery_type = first_package.delivery_type if first_package else 'instant'

    cursor_time = datetime.strptime('12:00 PM', '%I:%M %p').time()
    count = 0
    errors = []

    for f in valid_files:
        orig = secure_filename(f.filename)
        unique = f"{uuid.uuid4().hex[:8]}_{orig}"
        local_path = VIDEO_DIR / unique
        thumb_file = None
        preview_file = None

        try:
            f.save(local_path)

            created_dt = ffprobe_created_datetime(local_path)
            video_date = created_dt.date() if created_dt else batch_date
            video_time = created_dt.time().replace(microsecond=0) if created_dt else cursor_time

            thumb_file, preview_file = build_preview_assets(local_path, creator_name, logo_path)

            file_path = f"videos/{unique}"
            thumb_path = f"thumbs/{thumb_file.name}" if thumb_file else ''
            preview_path = f"previews/{preview_file.name}" if preview_file else ''

            if r2_client and R2_BUCKET:
                try:
                    file_path = save_to_r2(local_path, f"videos/{unique}")
                    if thumb_file and thumb_file.exists():
                        thumb_path = save_to_r2(thumb_file, f"thumbs/{thumb_file.name}")
                    if preview_file and preview_file.exists():
                        preview_path = save_to_r2(preview_file, f"previews/{preview_file.name}")
                except Exception as e:
                    print('R2 UPLOAD ERROR:', e)

            vid = Video(
                batch_id=batch.id,
                creator_id=user.id,
                filename=orig,
                file_path=file_path,
                thumb_path=thumb_path,
                preview_path=preview_path,
                location=location,
                recorded_date=video_date,
                recorded_time=video_time,
                price=default_price,
                delivery_type=delivery_type,
            )
            db.session.add(vid)
            count += 1
            cursor_time = (datetime.combine(date.today(), cursor_time) + timedelta(minutes=3)).time()
        except Exception as e:
            print('UPLOAD ERROR:', e)
            errors.append(orig)
        finally:
            for temp_file in [local_path, thumb_file, preview_file]:
                try:
                    if temp_file and Path(temp_file).exists() and Path(temp_file).is_file():
                        Path(temp_file).unlink()
                except Exception as e:
                    print('TEMP DELETE ERROR:', e)

    if count == 0:
        db.session.delete(batch)
        db.session.commit()
        flash('No videos were saved. Please try again with a different file or name.')
        return redirect(url_for('creator_dashboard'))

    db.session.commit()
    if errors:
        flash(f"Batch saved with {count} videos. Failed: {', '.join(errors[:5])}")
    else:
        flash(f"Batch saved with {count} videos.")
    return redirect(url_for('creator_dashboard'))


@app.route('/creator/batch/<int:batch_id>/delete', methods=['POST'])
@login_required('creator')
def delete_batch(batch_id):
    user = get_current_user()
    batch = db.session.get(Batch, batch_id)
    if not batch or batch.creator_id != user.id:
        flash('Batch not found.')
        return redirect(url_for('creator_dashboard'))
    videos = Video.query.filter_by(batch_id=batch.id).all()
    video_ids = [v.id for v in videos]
    active_download = None
    if video_ids:
        active_download = OrderItem.query.filter(OrderItem.item_type == 'video', OrderItem.item_id.in_(video_ids), OrderItem.download_available == True, OrderItem.download_expires_at != None, OrderItem.download_expires_at > datetime.utcnow()).first()
    if active_download:
        flash('This batch cannot be deleted yet because one or more buyers still have an active download window.')
        return redirect(url_for('creator_dashboard'))
    try:
        if video_ids:
            CartItem.query.filter(CartItem.item_type == 'video', CartItem.item_id.in_(video_ids)).delete(synchronize_session=False)
        for video in videos:
            delete_video_assets(video)
            db.session.delete(video)
        db.session.delete(batch)
        db.session.commit()
        flash('Batch deleted successfully.')
    except Exception as e:
        db.session.rollback()
        print('DELETE BATCH ERROR:', e)
        flash('Could not delete batch.')
    return redirect(url_for('creator_dashboard'))



@app.route('/creator/batch/<int:batch_id>')
@login_required('creator')
def creator_batch_detail(batch_id):
    user = get_current_user()
    batch = db.session.get(Batch, batch_id)
    if not batch or batch.creator_id != user.id:
        flash('Batch not found.')
        return redirect(url_for('creator_dashboard'))
    videos = Video.query.filter_by(batch_id=batch.id, creator_id=user.id).order_by(Video.recorded_date.desc(), Video.recorded_time.desc()).all()
    return render_template('creator_batch_detail.html', batch=batch, videos=videos)

@app.route('/creator/batch/<int:batch_id>/delete-selected-videos', methods=['POST'])
@login_required('creator')
def delete_selected_videos(batch_id):
    user = get_current_user()
    batch = db.session.get(Batch, batch_id)
    if not batch or batch.creator_id != user.id:
        flash('Batch not found.')
        return redirect(url_for('creator_dashboard'))
    ids = request.form.getlist('video_ids')
    if not ids:
        flash('No videos selected.')
        return redirect(url_for('creator_batch_detail', batch_id=batch.id))
    videos = Video.query.filter(Video.batch_id == batch.id, Video.creator_id == user.id, Video.id.in_(ids)).all()
    try:
        for video in videos:
            active_download = OrderItem.query.filter(OrderItem.item_type == 'video', OrderItem.item_id == video.id, OrderItem.download_available == True, OrderItem.download_expires_at != None, OrderItem.download_expires_at > datetime.utcnow()).first()
            if active_download:
                continue
            CartItem.query.filter_by(item_type='video', item_id=video.id).delete(synchronize_session=False)
            delete_video_assets(video)
            db.session.delete(video)
        db.session.commit()
        flash('Selected videos deleted.')
    except Exception as e:
        db.session.rollback()
        print('DELETE SELECTED VIDEOS ERROR:', e)
        flash('Could not delete selected videos.')
    return redirect(url_for('creator_batch_detail', batch_id=batch.id))

@app.route('/creator/video/<int:video_id>/price', methods=['POST'])
@login_required('creator')
def update_video_price(video_id):
    user = get_current_user()
    video = db.session.get(Video, video_id)
    if video and video.creator_id == user.id:
        video.price = float(request.form['price'])
        db.session.commit()
    return redirect(url_for('creator_dashboard'))

@app.route('/creator/settings', methods=['POST'])
@login_required('creator')
def creator_settings():
    user = get_current_user()
    user.public_name = request.form.get('public_name', user.public_name or '').strip() or user.public_name
    user.social_link = request.form.get('social_link', user.social_link or '').strip()
    new_pw = request.form.get('new_password', '').strip()
    if 'logo' in request.files and request.files['logo'] and request.files['logo'].filename:
        logo = request.files['logo']
        fn = f"{uuid.uuid4().hex[:8]}_{secure_filename(logo.filename)}"
        path = LOGO_DIR / fn
        logo.save(path)
        logo_key = f"logos/{fn}"
        if r2_client and R2_BUCKET:
            try:
                user.logo_path = save_to_r2(path, logo_key)
            except Exception as e:
                print('LOGO R2 UPLOAD ERROR:', e)
                user.logo_path = logo_key
            finally:
                try:
                    if path.exists():
                        path.unlink()
                except Exception:
                    pass
        else:
            user.logo_path = logo_key
        flash(t('logo_saved'))
    if new_pw:
        user.password_hash = generate_password_hash(new_pw)
    db.session.commit()
    return redirect(url_for('creator_dashboard'))

@app.route('/creator/connect-stripe')
@login_required('creator')
def creator_connect_stripe():
    user = get_current_user()
    user.payout_connected = True
    db.session.commit()
    flash('Stripe test connection marked as ready. Replace this route with real Connect onboarding using your test keys.')
    return redirect(url_for('creator_dashboard'))

@app.route('/creator/package', methods=['POST'])
@login_required('creator')
def create_package():
    user = get_current_user()
    p = Package(creator_id=user.id, title=request.form['title'].strip(), description=request.form.get('description', '').strip(), price=float(request.form['price']), delivery_type=request.form.get('delivery_type', 'instant'), turnaround_hours=int(request.form.get('turnaround_hours', '72') or 72))
    db.session.add(p)
    db.session.commit()
    return redirect(url_for('creator_dashboard'))

@app.route('/creator/product', methods=['POST'])
@login_required('creator')
def create_product():
    user = get_current_user()
    p = Product(creator_id=user.id, title=request.form['title'].strip(), price=float(request.form['price']), description=request.form.get('description', '').strip())
    db.session.add(p)
    db.session.commit()
    return redirect(url_for('creator_dashboard'))

@app.route('/creator/order-item/<int:item_id>/deliver', methods=['POST'])
@login_required('creator')
def deliver_order_item(item_id):
    user = get_current_user()
    item = db.session.get(OrderItem, item_id)
    if not item or item.item_type != 'video':
        return redirect(url_for('creator_dashboard'))
    video = db.session.get(Video, item.item_id)
    if not video or video.creator_id != user.id:
        return redirect(url_for('creator_dashboard'))
    if 'edited_file' in request.files and request.files['edited_file'] and request.files['edited_file'].filename:
        ef = request.files['edited_file']
        fn = f"edited_{uuid.uuid4().hex[:8]}_{secure_filename(ef.filename)}"
        path = VIDEO_DIR / fn
        ef.save(path)
        if r2_client and R2_BUCKET:
            item.edited_file_path = save_to_r2(path, f'videos/{fn}')
            try:
                if path.exists():
                    path.unlink()
            except Exception:
                pass
        else:
            item.edited_file_path = f'videos/{fn}'
        item.delivery_status = 'delivered'
        item.delivered_at = datetime.utcnow()
        item.payout_release_at = datetime.utcnow() + timedelta(hours=24)
        db.session.commit()
        flash('Edited file delivered.')
    return redirect(url_for('creator_dashboard'))

@app.route('/product/<int:product_id>')
def product_detail(product_id):
    product = db.session.get(Product, product_id)
    if not product:
        flash('Product not found.')
        return redirect(url_for('store'))
    creator = db.session.get(User, product.creator_id)
    return render_template('product_detail.html', product=product, creator=creator)

@app.route('/store')
def store():
    products = Product.query.filter_by(active=True).order_by(Product.created_at.desc()).all()
    return render_template('store.html', products=products)

@app.route('/services-apply', methods=['GET', 'POST'])
def services_apply():
    if request.method == 'POST':
        sr = SupportRequest(name=request.form.get('name', '').strip(), email=request.form.get('email', '').strip(), message=f"SERVICE APPLY | Business: {request.form.get('business_name', '').strip()} | Category: {request.form.get('category', '').strip()} | City: {request.form.get('city', '').strip()} | Website: {request.form.get('website', '').strip()}")
        db.session.add(sr)
        db.session.commit()
        flash('Service listing request received.')
        return redirect(url_for('services_apply'))
    return render_template('services_apply.html', listing_price=get_setting('service_listing_price', '10'))

@app.route('/ad-home-apply', methods=['GET', 'POST'])
def ad_home_apply():
    if request.method == 'POST':
        sr = SupportRequest(name=request.form.get('name', '').strip(), email=request.form.get('email', '').strip(), message=f"HOME AD APPLY | Business: {request.form.get('business_name', '').strip()} | Website: {request.form.get('website', '').strip()} | Months: {request.form.get('months', '1').strip()}")
        db.session.add(sr)
        db.session.commit()
        flash('Home ad request received.')
        return redirect(url_for('ad_home_apply'))
    return render_template('ad_home_apply.html', ad_price=get_setting('home_ad_price', '50'))

@app.route('/services')
def services():
    listings = ServiceListing.query.filter_by(status='active').order_by(ServiceListing.created_at.desc()).all()
    return render_template('services.html', listings=listings)

@app.route('/creator/<int:creator_id>')
def public_creator(creator_id):
    creator = db.session.get(User, creator_id)
    if not creator:
        flash('Creator not found.')
        return redirect(url_for('index'))
    videos = Video.query.filter_by(creator_id=creator.id).order_by(Video.recorded_date.desc(), Video.recorded_time.desc()).limit(20).all()
    products = Product.query.filter_by(creator_id=creator.id, active=True).limit(6).all()
    rating, reviews = creator_rating(creator.id)
    return render_template('public_creator.html', creator=creator, videos=videos, products=products, rating=rating, reviews=reviews)

@app.route('/control', methods=['GET', 'POST'])
def control_login():
    if request.method == 'POST':
        if request.form['username'] == os.getenv('OWNER_USER', 'cp12517') and request.form['password'] == os.getenv('OWNER_PASS', '645231cp'):
            session['owner_auth'] = True
            return redirect(url_for('control_dashboard'))
        flash('Invalid owner credentials.')
    return render_template('control_login.html')

@app.route('/control/dashboard')
@owner_required
def control_dashboard():
    applications = CreatorApplication.query.order_by(CreatorApplication.created_at.desc()).all()
    creators = User.query.filter_by(role='creator').order_by(User.created_at.desc()).all()
    services = ServiceListing.query.order_by(ServiceListing.created_at.desc()).all()
    support_requests = SupportRequest.query.order_by(SupportRequest.created_at.desc()).limit(20).all()
    settings = {k.key: k.value for k in SiteSetting.query.all()}
    return render_template('control_dashboard.html', applications=applications, creators=creators, services=services, support_requests=support_requests, settings=settings)

@app.route('/control/application/<int:app_id>/<action>', methods=['POST'])
@owner_required
def application_action(app_id, action):
    item = db.session.get(CreatorApplication, app_id)
    if not item:
        return redirect(url_for('control_dashboard'))
    if action == 'approve':
        item.status = 'approved'
        temp_password = 'Creator123!'
        if not User.query.filter_by(email=item.email).first():
            user = User(role='creator', email=item.email, password_hash=generate_password_hash(temp_password), first_name=item.first_name, last_name=item.last_name, public_name=item.brand_name, approved=True, social_link=item.social_link, primary_location=item.primary_location, plan='starter')
            db.session.add(user)
        flash(f'Creator approved. Temporary password: {temp_password}')
    elif action == 'reject':
        item.status = 'rejected'
        flash('Application rejected.')
    db.session.commit()
    return redirect(url_for('control_dashboard'))

@app.route('/control/creator/<int:user_id>/reset-password', methods=['POST'])
@owner_required
def control_reset_password(user_id):
    user = db.session.get(User, user_id)
    if user and user.role == 'creator':
        user.password_hash = generate_password_hash(request.form['new_password'].strip())
        db.session.commit()
        flash('Creator password updated.')
    return redirect(url_for('control_dashboard'))

@app.route('/control/settings', methods=['POST'])
@owner_required
def control_settings():
    for key in ['starter_price', 'pro_price', 'elite_price', 'service_listing_price']:
        set_setting(key, request.form.get(key, ''))
    db.session.commit()
    return redirect(url_for('control_dashboard'))

@app.route('/support', methods=['POST'])
def support():
    sr = SupportRequest(name=request.form.get('name', '').strip(), email=request.form.get('email', '').strip(), message=request.form.get('message', '').strip())
    db.session.add(sr)
    db.session.commit()
    flash(t('support_sent'))
    return redirect(request.referrer or url_for('index'))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

def seed():
    if not User.query.filter_by(email='demo@boatspotmedia.com').first():
        creator = User(role='creator', email='demo@boatspotmedia.com', password_hash=generate_password_hash('demo'), first_name='Demo', last_name='Creator', public_name='RampLifeCCS', approved=True, social_link='https://instagram.com/ramplifeccs', primary_location='Boca Raton Inlet', payout_connected=False, plan='starter')
        buyer = User(role='buyer', email='buyer@boatspotmedia.com', password_hash=generate_password_hash('demo'), first_name='Demo', last_name='Buyer', approved=True)
        db.session.add_all([creator, buyer])
        db.session.flush()
        batch = Batch(creator_id=creator.id, title='Boca Sunday Batch', location='Boca Raton Inlet', recorded_date=date.today())
        db.session.add(batch)
        db.session.flush()
        for idx, tm in enumerate([time(14, 5), time(14, 18), time(14, 32), time(14, 44)]):
            db.session.add(Video(batch_id=batch.id, creator_id=creator.id, filename=f'GH01{idx + 1:03d}.MP4', file_path='', thumb_path='', preview_path='', location='Boca Raton Inlet', recorded_date=date.today(), recorded_time=tm, price=40.0 + idx * 10, delivery_type='instant' if idx < 2 else 'edited'))
        db.session.add(Product(creator_id=creator.id, title='RampLife Flag', price=35, description='Boat flag'))
        db.session.add(Package(creator_id=creator.id, title='Original video', description='Instant clean file', price=40, delivery_type='instant', turnaround_hours=0))
        db.session.add(Package(creator_id=creator.id, title='Edited video', description='Delivered within 72 hours', price=75, delivery_type='edited', turnaround_hours=72))
        db.session.add(ServiceListing(business_name='Atlantic Marine Wraps', category='Marine Wraps', email='hello@wraps.com', city='Pompano Beach', website='https://example.com', description='Premium wraps for boats and yachts.', status='active', monthly_price=10))
        for k, v in {'starter_price': '19', 'pro_price': '39', 'elite_price': '79', 'service_listing_price': '10'}.items():
            set_setting(k, v)
        db.session.commit()

with app.app_context():
    db.create_all()
    ensure_order_item_columns()
    seed()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)))
