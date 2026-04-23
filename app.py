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
LOGO_DIR = UPLOAD_DIR / "logos"

for p in [VIDEO_DIR, THUMB_DIR, LOGO_DIR]:
    p.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "boatspotmedia-dev-secret")
db_url = os.getenv("DATABASE_URL", f"sqlite:///{BASE_DIR / 'boatspotmedia.db'}")
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql://", 1)
app.config["SQLALCHEMY_DATABASE_URI"] = db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["MAX_CONTENT_LENGTH"] = 4 * 1024

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
        "delivery_72": "Edited delivery within 72 hours", "preview_note": "Only thumbnails are shown. Full video available after purchase.",
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
        "delivery_72": "Entrega editada dentro de 72 horas", "preview_note": "Solo se muestran miniaturas. Video completo disponible después de la compra.",
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
            if role and u.role!= role:
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
    if len(parts)!= 2:
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
    directory = {"videos": VIDEO_DIR, "thumbs": THUMB_DIR, "logos": LOGO_DIR}.get(category)
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
        creation = None
        fmt = data.get('format', {}).get('tags', {}).get('creation_time')
        if fmt:
            creation = fmt
        else:
            for stream in data.get('streams', []):
                tags = stream.get('tags', {}) or {}
                if tags.get('creation_time'):
                    creation = tags['creation_time']
                    break
        if not creation:
            return None
        creation = creation.replace('Z', '+00:00')
        dt = datetime.fromisoformat(creation)
        if dt.tzinfo:
            dt = dt.astimezone().replace(tzinfo=None)
        return dt
    except Exception as e:
        print('FFPROBE CREATION TIME ERROR:', e, flush=True)
        return None

def build_thumbnail(video_path: Path, creator_display: str, logo_path: Path | str | None = None):
    stem = video_path.stem + "_" + uuid.uuid4().hex[:8]
    thumb_file = THUMB_DIR / f"{stem}.jpg"

    dur = ffprobe_duration(video_path)
    thumb_time = min(max(1.0, dur / 2), max(1.0, dur - 0.5))

    thumb_cmd = [
        "ffmpeg",
        "-y",
        "-i", str(video_path),
        "-ss", str(thumb_time),
        "-frames:v", "1",
        "-vf", "scale=640:-2",
        "-q:v", "3",
        str(thumb_file),
    ]
    thumb_run = subprocess.run(thumb_cmd, capture_output=True, text=True)
    if thumb_run.returncode!= 0:
        print("THUMB FFMPEG ERROR:", thumb_run.stderr, flush=True)

    thumb_result = thumb_file if thumb_file.exists() and thumb_file.stat().st_size > 0 else None
    print("THUMB GENERATED:", thumb_result, flush=True)
    return thumb_result

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
    directory = {"videos": VIDEO_DIR, "thumbs": THUMB_DIR, "logos": LOGO_DIR}.get(category)
    if not directory:
        abort(404)
    return send_from_directory(directory, filename)

@app.route('/')
def index():
    latest_videos = Video.query.order_by(Video.recorded_date.desc(), Video.recorded_time.desc()).limit(5).all()
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
        db.session
