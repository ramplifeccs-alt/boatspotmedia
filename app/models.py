from datetime import datetime, timedelta
from .services.db import db

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False)
    password_hash = db.Column(db.String(255))
    role = db.Column(db.String(50), nullable=False)  # buyer, creator, owner, advertiser, charter_provider
    display_name = db.Column(db.String(255))
    language = db.Column(db.String(10), default="en")
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class StoragePlan(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    storage_limit_gb = db.Column(db.Integer, nullable=False)
    monthly_price = db.Column(db.Numeric(10,2), nullable=False)
    commission_rate = db.Column(db.Integer, nullable=False)
    active = db.Column(db.Boolean, default=True)

class CreatorApplication(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String(120))
    last_name = db.Column(db.String(120))
    email = db.Column(db.String(255), nullable=False)
    instagram = db.Column(db.String(255))
    facebook = db.Column(db.String(255))
    youtube = db.Column(db.String(255))
    tiktok = db.Column(db.String(255))
    status = db.Column(db.String(50), default="pending")
    submitted_at = db.Column(db.DateTime, default=datetime.utcnow)
    reviewed_at = db.Column(db.DateTime)

class CreatorProfile(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"))
    plan_id = db.Column(db.Integer, db.ForeignKey("storage_plan.id"))
    storage_limit_gb = db.Column(db.Integer, default=512)
    storage_used_bytes = db.Column(db.BigInteger, default=0)
    commission_rate = db.Column(db.Integer, default=20)
    commission_override_rate = db.Column(db.Integer)
    commission_override_until = db.Column(db.DateTime)
    second_clip_discount_percent = db.Column(db.Integer, default=0)
    approved = db.Column(db.Boolean, default=False)
    suspended = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    user = db.relationship("User")
    plan = db.relationship("StoragePlan")

    @property
    def storage_used_gb(self):
        return round((self.storage_used_bytes or 0) / (1024**3), 2)

    @property
    def storage_remaining_gb(self):
        return max(0, self.storage_limit_gb - self.storage_used_gb)

    def active_commission_rate(self):
        now = datetime.utcnow()
        if self.commission_override_rate is not None and self.commission_override_until and self.commission_override_until > now:
            return self.commission_override_rate
        return self.commission_rate

class Location(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(180), unique=True, nullable=False)

class Batch(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    creator_id = db.Column(db.Integer, db.ForeignKey("creator_profile.id"))
    location = db.Column(db.String(180))
    total_size_bytes = db.Column(db.BigInteger, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    creator = db.relationship("CreatorProfile")

class Video(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    creator_id = db.Column(db.Integer, db.ForeignKey("creator_profile.id"))
    batch_id = db.Column(db.Integer, db.ForeignKey("batch.id"))
    location = db.Column(db.String(180))
    recorded_at = db.Column(db.DateTime)
    r2_video_key = db.Column(db.String(500))
    r2_thumbnail_key = db.Column(db.String(500))
    public_thumbnail_url = db.Column(db.String(800))
    file_size_bytes = db.Column(db.BigInteger, default=0)
    original_price = db.Column(db.Numeric(10,2), default=40.00)
    edited_price = db.Column(db.Numeric(10,2), default=60.00)
    bundle_price = db.Column(db.Numeric(10,2), default=80.00)
    status = db.Column(db.String(50), default="active")
    internal_filename = db.Column(db.String(500))  # hidden from buyer
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    creator = db.relationship("CreatorProfile")
    batch = db.relationship("Batch")

class Order(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    buyer_email = db.Column(db.String(255), nullable=False)
    buyer_id = db.Column(db.Integer, db.ForeignKey("user.id"))
    total_price = db.Column(db.Numeric(10,2), default=0)
    status = db.Column(db.String(50), default="paid")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class OrderItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    order_id = db.Column(db.Integer, db.ForeignKey("order.id"))
    video_id = db.Column(db.Integer, db.ForeignKey("video.id"))
    creator_id = db.Column(db.Integer, db.ForeignKey("creator_profile.id"))
    purchase_type = db.Column(db.String(50), default="original") # original, edited, bundle
    price = db.Column(db.Numeric(10,2), default=0)
    edited_status = db.Column(db.String(50), default="not_required") # pending, ready, not_required
    edited_r2_key = db.Column(db.String(500))
    order = db.relationship("Order", backref="items")
    video = db.relationship("Video")
    creator = db.relationship("CreatorProfile")

class DownloadToken(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    order_item_id = db.Column(db.Integer, db.ForeignKey("order_item.id"))
    token = db.Column(db.String(255), unique=True, nullable=False)
    expires_at = db.Column(db.DateTime, nullable=False)
    download_count = db.Column(db.Integer, default=0)
    item = db.relationship("OrderItem", backref="download_tokens")

class CreatorClickStats(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    creator_id = db.Column(db.Integer, db.ForeignKey("creator_profile.id"), unique=True)
    clicks_today = db.Column(db.Integer, default=0)
    clicks_week = db.Column(db.Integer, default=0)
    clicks_month = db.Column(db.Integer, default=0)
    clicks_lifetime = db.Column(db.Integer, default=0)

class Product(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    creator_id = db.Column(db.Integer, db.ForeignKey("creator_profile.id"))
    title = db.Column(db.String(200))
    description = db.Column(db.Text)
    price = db.Column(db.Numeric(10,2))
    shipping_cost = db.Column(db.Numeric(10,2), default=0)
    processing_time = db.Column(db.String(120))
    shipping_method = db.Column(db.String(120))
    active = db.Column(db.Boolean, default=True)

class ProductImage(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    product_id = db.Column(db.Integer, db.ForeignKey("product.id"))
    r2_key = db.Column(db.String(500))
    image_url = db.Column(db.String(800))

class AdvertiserProfile(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"))
    balance = db.Column(db.Numeric(10,2), default=0)
    user = db.relationship("User")

class ServiceAd(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    advertiser_id = db.Column(db.Integer, db.ForeignKey("advertiser_profile.id"))
    title = db.Column(db.String(200))
    description = db.Column(db.Text)
    website_url = db.Column(db.String(500))
    image_url = db.Column(db.String(800))
    target_location = db.Column(db.String(180))
    cost_per_click = db.Column(db.Numeric(10,2), default=0.15)
    status = db.Column(db.String(50), default="active") # active, paused, hidden
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class AdClick(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    ad_id = db.Column(db.Integer, db.ForeignKey("service_ad.id"))
    ip_area = db.Column(db.String(255))
    clicked_at = db.Column(db.DateTime, default=datetime.utcnow)

class CharterListing(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    provider_id = db.Column(db.Integer, db.ForeignKey("user.id"))
    title = db.Column(db.String(220))
    boat_name = db.Column(db.String(180))
    location = db.Column(db.String(180))
    capacity = db.Column(db.Integer)
    price_hour = db.Column(db.Numeric(10,2))
    price_trip = db.Column(db.Numeric(10,2))
    description = db.Column(db.Text)
    status = db.Column(db.String(50), default="active")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
