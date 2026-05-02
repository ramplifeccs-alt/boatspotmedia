from werkzeug.security import check_password_hash
import os, tempfile, uuid
from flask import Blueprint, render_template, request, redirect, url_for, current_app, flash, jsonify, session
from app.models import User, CreatorProfile, Batch, Video, Location, CreatorClickStats, Product, VideoPricingPreset, OrderItem, StoragePlan, ProductVariant
from app.services.db import db
from app.services.media import extract_creation_time, generate_center_thumbnail
from app.services.r2 import upload as r2_upload

creator_bp = Blueprint("creator", __name__)

def creator_instagram(creator):
    try:
        value = getattr(creator, "instagram", None)
        if value:
            return str(value).replace("@", "")
    except Exception:
        pass

    try:
        if creator.user and creator.user.display_name:
            return str(creator.user.display_name).replace("@", "")
    except Exception:
        pass

    return ""

def creator_display_name(creator):
    try:
        if creator.user and creator.user.display_name and creator.user.display_name != "None":
            return creator.user.display_name
    except Exception:
        pass

    ig = creator_instagram(creator)
    return ig or "Creator"


def _ensure_creator_profile_deleted_column():
    """Create creator_profile.deleted before ORM queries reference it."""
    try:
        db.session.execute(db.text("ALTER TABLE creator_profile ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE"))
        db.session.execute(db.text("UPDATE creator_profile SET deleted = FALSE WHERE deleted IS NULL"))
        db.session.commit()
        _bsm_fix_order_item_creator_id_v460()
    except Exception as e:
        db.session.rollback()
        print("creator_profile.deleted repair warning:", e)



ALLOWED_VIDEO_EXTENSIONS = {".mp4",".mov",".mxf",".avi",".mts",".m2ts",".3gp",".hevc",".h265",".h264",".m4v",".mpg",".mpeg",".wmv"}

def _is_allowed_video_filename(filename):
    return os.path.splitext((filename or "").lower())[1] in ALLOWED_VIDEO_EXTENSIONS

def _creator_default_prices(creator):
    return _creator_video_prices_from_pricing_page(creator)


def _recalculate_creator_storage(creator_id):
    """Recalculate storage from active videos only and update profile."""
    try:
        used = _creator_used_storage_bytes(creator_id)
        try:
            from app.models import CreatorProfile
            c = CreatorProfile.query.get(creator_id)
            if c and hasattr(c, "storage_used_bytes"):
                c.storage_used_bytes = int(used)
                db.session.add(c)
                db.session.commit()
                try:
                    _schedule_creator_pricing_update(creator)
                except Exception:
                    pass
        except Exception:
            db.session.rollback()
        return int(used)
    except Exception as e:
        print("storage recalculation warning:", e)
        try:
            db.session.rollback()
        except Exception:
            pass
        return 0

def current_creator():
    _ensure_creator_profile_deleted_column()
    user_id = session.get("user_id")
    user_email = session.get("user_email") or session.get("email")
    creator_id = session.get("creator_id")

    q = CreatorProfile.query.filter(
        CreatorProfile.approved == True,
        CreatorProfile.suspended == False,
        db.or_(CreatorProfile.deleted == False, CreatorProfile.deleted.is_(None))
    )

    if creator_id:
        creator = q.filter_by(id=creator_id).first()
        if creator:
            return creator

    if user_id:
        creator = q.filter_by(user_id=user_id).first()
        if creator:
            return creator

    if user_email:
        creator = q.join(User, CreatorProfile.user_id == User.id).filter(db.func.lower(User.email) == user_email.lower()).first()
        if creator:
            return creator

    return None


def render_creator_template(template_name, **kwargs):
    creator = kwargs.get("creator") or current_creator()
    kwargs["creator"] = creator
    kwargs["creator_name"] = creator_display_name(creator)
    kwargs["creator_instagram"] = creator_instagram(creator)
    return render_template(template_name, **kwargs)


@creator_bp.app_context_processor
def inject_creator_menu_helpers():
    return {
        "creator_display_name": _creator_display_name
    }



def _creator_display_name(creator):
    """Prefer Instagram/business/display name instead of email."""
    try:
        user = getattr(creator, "user", None)
        for obj in (creator, user):
            if not obj:
                continue
            for attr in ("instagram", "instagram_handle", "business_name", "display_name", "name", "username"):
                val = getattr(obj, attr, None)
                if val:
                    val = str(val).strip()
                    if val:
                        return ("@" + val.lstrip("@")) if attr in ("instagram", "instagram_handle") else val
        for obj in (user, creator):
            if obj and getattr(obj, "email", None):
                return str(getattr(obj, "email"))
    except Exception:
        pass
    return "Dashboard"


def _active_storage_used_bytes(creator_id):
    """Count only active videos, excluding deleted/cancelled."""
    try:
        from sqlalchemy import text
        used = db.session.execute(text("""
            SELECT COALESCE(SUM(COALESCE(file_size_bytes, 0)), 0)
            FROM video
            WHERE creator_id = :cid
              AND COALESCE(status, '') NOT IN ('deleted','cancelled','canceled')
        """), {"cid": creator_id}).scalar() or 0
        return int(used)
    except Exception as e:
        try:
            print("active storage used warning:", e)
            db.session.rollback()
        except Exception:
            pass
        return 0


def _available_buyer_locations():
    """Buyer location list comes only from active videos uploaded by creators."""
    try:
        from sqlalchemy import text
        rows = db.session.execute(text("""
            SELECT DISTINCT TRIM(location) AS location
            FROM video
            WHERE location IS NOT NULL
              AND TRIM(location) <> ''
              AND COALESCE(status, '') NOT IN ('deleted','cancelled','canceled')
            ORDER BY TRIM(location)
        """)).fetchall()
        return [r[0] for r in rows if r and r[0]]
    except Exception as e:
        try:
            print("buyer locations warning:", e)
            db.session.rollback()
        except Exception:
            pass
        return []


def _creator_known_locations():
    return _available_buyer_locations()


def _delete_batch_files_from_r2(batch):
    """Delete all batch files/thumbnails from R2 and mark DB rows deleted. No recursion."""
    try:
        from app.services.r2 import delete_r2_object, delete_r2_prefix
        deleted = 0
        batch_id = getattr(batch, "id", None)
        creator_id = getattr(batch, "creator_id", None) or getattr(batch, "creator_profile_id", None)

        try:
            from app.models import Video
            videos = Video.query.filter_by(batch_id=batch_id).all()
            for v in videos:
                for attr in ("r2_video_key", "file_path", "r2_thumbnail_key", "thumbnail_path"):
                    key = getattr(v, attr, None)
                    if key and not str(key).startswith("http"):
                        try:
                            delete_r2_object(key)
                            deleted += 1
                        except Exception as e:
                            try:
                                print("R2 object delete warning:", key, e)
                            except Exception:
                                pass
                if hasattr(v, "status"):
                    v.status = "deleted"
                    db.session.add(v)
                else:
                    db.session.delete(v)
        except Exception as e:
            try:
                print("R2 DB key cleanup warning:", e)
            except Exception:
                pass

        if creator_id and batch_id:
            for prefix in (
                f"creators/{creator_id}/batches/{batch_id}/",
                f"creator/{creator_id}/batch/{batch_id}/",
                f"batches/{batch_id}/",
            ):
                try:
                    deleted += delete_r2_prefix(prefix)
                except Exception as e:
                    try:
                        print("R2 prefix delete warning:", prefix, e)
                    except Exception:
                        pass

        if batch:
            if hasattr(batch, "status"):
                try:
                    _delete_batch_r2_objects(batch)
                except Exception:
                    pass
                batch.status = "deleted"
                db.session.add(batch)
            else:
                try:
                    _delete_batch_r2_objects(batch)
                except Exception:
                    pass
                db.session.delete(batch)
        return deleted
    except Exception as e:
        try:
            print("R2 batch cleanup warning:", e)
        except Exception:
            pass
        return 0

def _cleanup_upload_prefix(batch_id, creator_id):
    try:
        from app.services.r2 import delete_r2_prefix
        if batch_id and creator_id:
            return delete_r2_prefix(f"creators/{creator_id}/batches/{batch_id}/")
    except Exception:
        pass
    return 0


def _ny_dt(dt):
    if not dt:
        return None
    try:
        from zoneinfo import ZoneInfo
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        try:
            from datetime import timedelta
            return dt - timedelta(hours=5)
        except Exception:
            return dt


def _r2_collect_video_keys_for_batch(batch_id):
    keys = set()
    try:
        from app.models import Video
        videos = Video.query.filter_by(batch_id=batch_id).all()
        for v in videos:
            # known likely fields
            for attr in (
                "r2_video_key", "r2_thumbnail_key",
                "r2_key", "thumbnail_key",
                "video_key", "thumb_key",
                "file_path", "thumbnail_path",
                "storage_key", "storage_path",
                "object_key", "preview_key", "preview_path",
            ):
                val = getattr(v, attr, None)
                if val:
                    val = str(val)
                    if not val.startswith("http") and "/" in val:
                        keys.add(val)

            # scan every string field as fallback
            try:
                for val in vars(v).values():
                    if isinstance(val, str) and "/" in val and not val.startswith("http"):
                        low = val.lower()
                        if any(x in low for x in ("thumb", "preview", "upload", "creator", "batch", ".mp4", ".mov", ".m4v", ".jpg", ".jpeg", ".png")):
                            keys.add(val)
            except Exception:
                pass
    except Exception as e:
        try:
            print("R2 collect keys warning:", e)
        except Exception:
            pass
    return list(keys)


def _r2_prefixes_for_batch(batch_id, creator_id=None):
    prefixes = [
        f"batches/{batch_id}/",
        f"batch/{batch_id}/",
        f"uploads/batches/{batch_id}/",
        f"videos/batches/{batch_id}/",
        f"thumbs/batches/{batch_id}/",
        f"previews/batches/{batch_id}/",
    ]
    if creator_id:
        prefixes += [
            f"creators/{creator_id}/batches/{batch_id}/",
            f"creators/{creator_id}/batch/{batch_id}/",
            f"creator/{creator_id}/batches/{batch_id}/",
            f"creator/{creator_id}/batch/{batch_id}/",
            f"uploads/{creator_id}/{batch_id}/",
            f"videos/{creator_id}/{batch_id}/",
            f"thumbs/{creator_id}/{batch_id}/",
            f"previews/{creator_id}/{batch_id}/",
        ]
    return prefixes


def _delete_batch_r2_objects(batch):
    """Delete videos/thumbnails/previews for batch from R2 only."""
    try:
        from app.services.r2 import delete_r2_candidates
        batch_id = getattr(batch, "id", None)
        creator_id = getattr(batch, "creator_id", None) or getattr(batch, "creator_profile_id", None)
        keys = _r2_collect_video_keys_for_batch(batch_id)
        prefixes = _r2_prefixes_for_batch(batch_id, creator_id)
        deleted = len(keys or []) + len(prefixes or [])
        _schedule_batch_r2_delete({'keys': keys, 'prefixes': prefixes}, batch_id=getattr(batch, 'id', None))
        try:
            print("R2 delete batch cleanup:", {"batch_id": batch_id, "creator_id": creator_id, "keys": len(keys), "deleted": deleted})
        except Exception:
            pass
        return deleted
    except Exception as e:
        try:
            print("R2 delete batch cleanup warning:", e)
        except Exception:
            pass
        return 0




def _bsm_r2_keys_for_batch_v388(batch_id):
    keys = set()
    try:
        from app.models import Video
        for v in Video.query.filter_by(batch_id=batch_id).all():
            for attr in ("r2_video_key","r2_thumbnail_key","r2_key","thumbnail_key","video_key","thumb_key","file_path","thumbnail_path","storage_key","storage_path","object_key","preview_key","preview_path"):
                val = getattr(v, attr, None)
                if val:
                    val = str(val)
                    if "/" in val and not val.startswith("http"):
                        keys.add(val)
            try:
                for val in vars(v).values():
                    if isinstance(val, str) and "/" in val and not val.startswith("http"):
                        low = val.lower()
                        if any(x in low for x in ("thumb","preview","upload","creator","batch",".mp4",".mov",".m4v",".jpg",".jpeg",".png")):
                            keys.add(val)
            except Exception:
                pass
    except Exception as e:
        try: print("bsm r2 keys warning:", e)
        except Exception: pass
    return list(keys)


def _bsm_r2_prefixes_for_batch_v388(batch_id, creator_id=None):
    prefixes = [
        f"batches/{batch_id}/",
        f"batch/{batch_id}/",
        f"uploads/batches/{batch_id}/",
        f"videos/batches/{batch_id}/",
        f"thumbs/batches/{batch_id}/",
        f"previews/batches/{batch_id}/",
    ]
    if creator_id:
        prefixes += [
            f"creators/{creator_id}/batches/{batch_id}/",
            f"creators/{creator_id}/batch/{batch_id}/",
            f"creator/{creator_id}/batches/{batch_id}/",
            f"creator/{creator_id}/batch/{batch_id}/",
            f"uploads/{creator_id}/{batch_id}/",
            f"videos/{creator_id}/{batch_id}/",
            f"thumbs/{creator_id}/{batch_id}/",
            f"previews/{creator_id}/{batch_id}/",
        ]
    return prefixes


def _bsm_delete_batch_r2_and_db_v388(batch):
    """
    v40.4 async version:
    - collect R2 keys
    - mark DB rows deleted quickly
    - delete R2 files in background
    """
    batch_id = getattr(batch, "id", None)
    if not batch_id:
        return 0
    payload = _collect_batch_r2_delete_payload(batch_id)
    ok = _soft_delete_batch_db_only(batch_id)
    if ok:
        _schedule_batch_r2_delete(payload, batch_id=batch_id)
    return len(payload.get("keys") or []) + len(payload.get("prefixes") or [])


def _bsm_latest_active_batch_for_creator_v388(creator):
    try:
        from app.models import VideoBatch
        q = VideoBatch.query
        filters = []
        if hasattr(VideoBatch, "creator_id"):
            filters.append(VideoBatch.creator_id == creator.id)
        if hasattr(VideoBatch, "creator_profile_id"):
            filters.append(VideoBatch.creator_profile_id == creator.id)
        if filters:
            q = q.filter(db.or_(*filters))
        if hasattr(VideoBatch, "status"):
            q = q.filter(db.or_(VideoBatch.status == None, ~VideoBatch.status.in_(["deleted","cancelled","canceled"])))
        return q.order_by(VideoBatch.id.desc()).first()
    except Exception as e:
        try: print("BSM latest batch warning:", e)
        except Exception: pass
        return None




def _bsm_batch_is_incomplete_v388(batch):
    """
    Only allow manual ghost cleanup for truly incomplete/cancelled batches.
    Never delete a normal completed batch with active videos.
    """
    try:
        from app.models import Video
        status = str(getattr(batch, "status", "") or "").lower().strip()
        incomplete_statuses = {"uploading", "pending", "cancelled", "canceled", "incomplete", "failed", "error", "draft"}
        if status in incomplete_statuses:
            return True

        videos = Video.query.filter_by(batch_id=batch.id).all()

        # No videos at all = ghost/incomplete batch.
        if not videos:
            return True

        active_videos = []
        for v in videos:
            v_status = str(getattr(v, "status", "") or "").lower().strip()
            if v_status in {"deleted", "cancelled", "canceled", "failed", "error"}:
                continue
            active_videos.append(v)

        # Only deleted/cancelled/failed videos = incomplete.
        if not active_videos:
            return True

        # If all active videos are missing real R2/video key/path, treat as incomplete.
        valid_count = 0
        key_attrs = (
            "r2_video_key", "r2_key", "video_key", "file_path",
            "r2_video_path", "storage_key", "storage_path", "object_key"
        )
        for v in active_videos:
            for attr in key_attrs:
                val = getattr(v, attr, None)
                if val and isinstance(val, str) and "/" in val:
                    valid_count += 1
                    break

        if valid_count == 0:
            return True

        # Has active videos with real keys = good batch. Do NOT delete.
        return False
    except Exception as e:
        try:
            print("incomplete batch check warning:", e)
        except Exception:
            pass
        # Fail safe: do not delete if unsure.
        return False


def _bsm_latest_incomplete_batch_for_creator_v388(creator):
    try:
        from app.models import VideoBatch
        q = VideoBatch.query
        filters = []
        if hasattr(VideoBatch, "creator_id"):
            filters.append(VideoBatch.creator_id == creator.id)
        if hasattr(VideoBatch, "creator_profile_id"):
            filters.append(VideoBatch.creator_profile_id == creator.id)
        if filters:
            q = q.filter(db.or_(*filters))
        if hasattr(VideoBatch, "status"):
            q = q.filter(db.or_(VideoBatch.status == None, ~VideoBatch.status.in_(["deleted"])))

        # Check recent batches only, newest first. Delete only if incomplete.
        for batch in q.order_by(VideoBatch.id.desc()).limit(20).all():
            if _bsm_batch_is_incomplete_v388(batch):
                return batch
    except Exception as e:
        try:
            print("latest incomplete batch lookup warning:", e)
        except Exception:
            pass
    return None


def _creator_video_prices_from_pricing_page(creator):
    """
    Read the creator's own Pricing page options.
    No platform/default prices.
    Maps active pricing presets to Video fields only as configured by creator.
    """
    original_price = 0.0
    edited_price = 0.0
    bundle_price = 0.0
    try:
        presets = VideoPricingPreset.query.filter_by(creator_id=creator.id, active=True).order_by(
            VideoPricingPreset.is_default.desc(),
            VideoPricingPreset.id.asc()
        ).all()

        for p in presets:
            try:
                price = float(p.price or 0)
            except Exception:
                price = 0.0
            if price <= 0:
                continue

            dtype = (getattr(p, "delivery_type", "") or "").lower().strip()
            title = (getattr(p, "title", "") or "").lower().strip()
            label = dtype + " " + title

            if any(x in label for x in ["bundle", "combo", "original +", "original plus"]):
                if bundle_price <= 0:
                    bundle_price = price
            elif any(x in label for x in ["edit", "edited", "instagram", "short", "reel"]):
                if edited_price <= 0:
                    edited_price = price
            else:
                # instant/original/download/default = original purchase option
                if original_price <= 0:
                    original_price = price
    except Exception:
        pass

    return original_price, edited_price, bundle_price



def _apply_creator_pricing_to_existing_videos_by_id(creator_id):
    """
    Background job: update already-published videos after Pricing changes.
    Does not block the Save Price request.
    """
    try:
        from app.models import CreatorProfile, Video
        creator = CreatorProfile.query.get(creator_id)
        if not creator:
            return 0

        original_price, edited_price, bundle_price = _creator_video_prices_from_pricing_page(creator)

        q = Video.query
        filters = []
        if hasattr(Video, "creator_id"):
            filters.append(Video.creator_id == creator.id)
        if hasattr(Video, "creator_profile_id"):
            filters.append(Video.creator_profile_id == creator.id)
        if filters:
            q = q.filter(db.or_(*filters))

        if hasattr(Video, "status"):
            q = q.filter(db.or_(Video.status == None, ~Video.status.in_(["deleted", "cancelled", "canceled"])))

        updated = 0
        for v in q.yield_per(100):
            changed = False
            if hasattr(v, "original_price") and v.original_price != original_price:
                v.original_price = original_price
                changed = True
            if hasattr(v, "edited_price") and v.edited_price != edited_price:
                v.edited_price = edited_price
                changed = True
            if hasattr(v, "bundle_price") and v.bundle_price != bundle_price:
                v.bundle_price = bundle_price
                changed = True
            if changed:
                db.session.add(v)
                updated += 1

            # Commit in small chunks to avoid long locks.
            if updated and updated % 100 == 0:
                db.session.commit()
                try:
                    _schedule_creator_pricing_update(creator)
                except Exception:
                    pass

        db.session.commit()
        try:
            print("Creator pricing background update complete:", {"creator_id": creator_id, "updated": updated})
        except Exception:
            pass
        return updated
    except Exception as e:
        db.session.rollback()
        try:
            print("Creator pricing background update warning:", e)
        except Exception:
            pass
        return 0


def _schedule_creator_pricing_update(creator):
    """
    Schedule existing-video price updates in a background thread.
    Keeps Save Price fast and prevents the site from freezing.
    """
    try:
        import threading
        from flask import current_app
        app = current_app._get_current_object()
        creator_id = creator.id

        def worker():
            with app.app_context():
                _apply_creator_pricing_to_existing_videos_by_id(creator_id)

        t = threading.Thread(target=worker, daemon=True)
        t.start()
        return True
    except Exception as e:
        try:
            print("schedule pricing update warning:", e)
        except Exception:
            pass
        return False


def _apply_creator_pricing_to_existing_videos(creator):
    """
    Backward-compatible wrapper.
    It no longer updates synchronously; it schedules background update.
    """
    return _schedule_creator_pricing_update(creator)



def _creator_dashboard_overview_stats(creator):
    """
    Safe creator dashboard overview stats.
    Uses existing DB models if available. Defaults to zero when no sales exist.
    """
    stats = {
        "earned_today": 0.0,
        "earned_week": 0.0,
        "earned_month": 0.0,
        "earned_lifetime": 0.0,
        "videos_sold_month": 0,
        "videos_sold_total": 0,
        "pending_payouts": 0.0,
        "last_sale": None,
        "platform_commission_rate": 0.0,
        "gross_revenue": 0.0,
        "platform_commission": 0.0,
        "stripe_fees": 0.0,
        "net_earnings": 0.0,
        "preview_views": 0,
        "clicks": 0,
        "conversion_rate": 0.0,
    }

    try:
        from datetime import datetime, timedelta
        now = datetime.utcnow()
        start_today = datetime(now.year, now.month, now.day)
        start_week = start_today - timedelta(days=start_today.weekday())
        start_month = datetime(now.year, now.month, 1)

        # Try common sale/order model names.
        SaleModel = None
        for name in ("Sale", "Order", "Purchase", "VideoSale"):
            try:
                model = globals().get(name)
                if model is None:
                    import app.models as models
                    model = getattr(models, name, None)
                if model is not None:
                    SaleModel = model
                    break
            except Exception:
                pass

        sales = []
        if SaleModel is not None:
            q = SaleModel.query

            # Filter by creator if model has creator fields.
            filters = []
            for attr in ("creator_id", "creator_profile_id", "seller_id"):
                if hasattr(SaleModel, attr):
                    filters.append(getattr(SaleModel, attr) == creator.id)
            if filters:
                q = q.filter(db.or_(*filters))

            # Only paid/completed if status exists.
            if hasattr(SaleModel, "status"):
                q = q.filter(db.or_(SaleModel.status == None, SaleModel.status.in_(["paid", "completed", "succeeded", "success"])))

            sales = q.all()


        # Fallback sales table created by cart checkout.
        if not sales:
            try:
                rows = db.session.execute(db.text("""
                    SELECT unit_price, quantity, created_at
                    FROM bsm_cart_order_item
                    WHERE creator_id = :cid
                """), {"cid": creator.id}).mappings().all()
                class _SaleObj:
                    pass
                for row in rows:
                    s = _SaleObj()
                    s.net_amount = float(row["unit_price"] or 0) * int(row["quantity"] or 1)
                    s.gross_amount = s.net_amount
                    s.platform_commission = 0
                    s.stripe_fee = 0
                    s.created_at = row["created_at"]
                    sales.append(s)
            except Exception:
                db.session.rollback()

        def get_date(s):
            for attr in ("created_at", "paid_at", "completed_at", "created"):
                v = getattr(s, attr, None)
                if v:
                    return v
            return None

        def get_amount(s, names, default=0.0):
            for attr in names:
                try:
                    v = getattr(s, attr, None)
                    if v not in (None, "", 0, "0"):
                        return float(v)
                except Exception:
                    pass
            return default

        for s in sales:
            dt = get_date(s)
            gross = get_amount(s, ("gross_amount", "amount", "total", "price", "total_amount"), 0.0)
            commission = get_amount(s, ("platform_commission", "commission", "platform_fee"), 0.0)
            stripe_fee = get_amount(s, ("stripe_fee", "processing_fee"), 0.0)
            net = get_amount(s, ("net_amount", "creator_net", "net_payout", "seller_net"), gross - commission - stripe_fee)

            stats["gross_revenue"] += gross
            stats["platform_commission"] += commission
            stats["stripe_fees"] += stripe_fee
            stats["net_earnings"] += net
            stats["earned_lifetime"] += net
            stats["videos_sold_total"] += 1

            if dt:
                if dt >= start_month:
                    stats["earned_month"] += net
                    stats["videos_sold_month"] += 1
                if dt >= start_week:
                    stats["earned_week"] += net
                if dt >= start_today:
                    stats["earned_today"] += net

        if sales:
            try:
                stats["last_sale"] = sorted(sales, key=lambda s: get_date(s) or datetime.min, reverse=True)[0]
            except Exception:
                stats["last_sale"] = sales[-1]

        if stats["gross_revenue"] > 0 and stats["platform_commission"] > 0:
            stats["platform_commission_rate"] = round((stats["platform_commission"] / stats["gross_revenue"]) * 100, 2)

        # Pending payouts, best effort.
        PayoutModel = None
        for name in ("Payout", "CreatorPayout"):
            try:
                import app.models as models
                PayoutModel = getattr(models, name, None)
                if PayoutModel:
                    break
            except Exception:
                pass
        if PayoutModel is not None:
            q = PayoutModel.query
            filters = []
            for attr in ("creator_id", "creator_profile_id"):
                if hasattr(PayoutModel, attr):
                    filters.append(getattr(PayoutModel, attr) == creator.id)
            if filters:
                q = q.filter(db.or_(*filters))
            if hasattr(PayoutModel, "status"):
                q = q.filter(PayoutModel.status.in_(["pending", "processing"]))
            for p in q.all():
                stats["pending_payouts"] += get_amount(p, ("amount", "total", "net_amount"), 0.0)

        # Video performance best effort.
        try:
            from app.models import Video
            qv = Video.query
            filters = []
            for attr in ("creator_id", "creator_profile_id"):
                if hasattr(Video, attr):
                    filters.append(getattr(Video, attr) == creator.id)
            if filters:
                qv = qv.filter(db.or_(*filters))
            videos = qv.all()
            for v in videos:
                for attr in ("preview_views", "views", "view_count"):
                    val = getattr(v, attr, None)
                    if val:
                        stats["preview_views"] += int(val)
                        break
                for attr in ("clicks", "click_count", "preview_clicks"):
                    val = getattr(v, attr, None)
                    if val:
                        stats["clicks"] += int(val)
                        break
            if stats["clicks"] > 0 and stats["videos_sold_total"] > 0:
                stats["conversion_rate"] = round((stats["videos_sold_total"] / stats["clicks"]) * 100, 2)
        except Exception:
            pass

    except Exception as e:
        try:
            print("creator dashboard stats warning:", e)
        except Exception:
            pass

    raw_perf = _creator_raw_video_performance_stats(creator)
    stats["preview_views"] = raw_perf.get("preview_views", stats.get("preview_views", 0))
    stats["clicks"] = raw_perf.get("clicks", stats.get("clicks", 0))
    if stats.get("clicks", 0) and stats.get("videos_sold_total", 0):
        stats["conversion_rate"] = round((stats["videos_sold_total"] / stats["clicks"]) * 100, 2)
    return stats



def _creator_raw_video_performance_stats(creator):
    result = {"preview_views": 0, "clicks": 0}
    try:
        # Create columns if they do not exist; no Video model dependency.
        db.session.execute(db.text("ALTER TABLE video ADD COLUMN IF NOT EXISTS preview_views INTEGER DEFAULT 0"))
        db.session.execute(db.text("ALTER TABLE video ADD COLUMN IF NOT EXISTS preview_clicks INTEGER DEFAULT 0"))
        db.session.commit()

        where = []
        params = {"cid": creator.id}
        # Try creator_id first; if column does not exist, fallback inside except.
        try:
            row = db.session.execute(db.text("""
                SELECT COALESCE(SUM(preview_views),0) AS views,
                       COALESCE(SUM(preview_clicks),0) AS clicks
                FROM video
                WHERE creator_id = :cid
                  AND COALESCE(status, '') <> 'deleted'
            """), params).mappings().first()
            if row:
                result["preview_views"] = int(row["views"] or 0)
                result["clicks"] = int(row["clicks"] or 0)
                return result
        except Exception:
            db.session.rollback()

        try:
            row = db.session.execute(db.text("""
                SELECT COALESCE(SUM(preview_views),0) AS views,
                       COALESCE(SUM(preview_clicks),0) AS clicks
                FROM video
                WHERE creator_profile_id = :cid
                  AND COALESCE(status, '') <> 'deleted'
            """), params).mappings().first()
            if row:
                result["preview_views"] = int(row["views"] or 0)
                result["clicks"] = int(row["clicks"] or 0)
        except Exception:
            db.session.rollback()

    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
    return result



def _collect_batch_r2_delete_payload(batch_id):
    """
    Collect R2 keys before marking/deleting DB rows.
    This keeps the web request fast and lets the background worker delete R2 files.
    """
    payload = {"keys": [], "prefixes": []}
    try:
        from app.models import Video, VideoBatch
        batch = VideoBatch.query.get(batch_id)
        creator_id = getattr(batch, "creator_id", None) or getattr(batch, "creator_profile_id", None) if batch else None

        keys = set()
        for v in Video.query.filter_by(batch_id=batch_id).all():
            for attr in (
                "r2_video_key", "r2_thumbnail_key", "r2_key", "thumbnail_key",
                "video_key", "thumb_key", "file_path", "thumbnail_path",
                "storage_key", "storage_path", "object_key", "preview_key",
                "preview_path", "public_thumbnail_url"
            ):
                val = getattr(v, attr, None)
                if val:
                    val = str(val)
                    if "/" in val and not val.startswith("http"):
                        keys.add(val)
            try:
                for val in vars(v).values():
                    if isinstance(val, str) and "/" in val and not val.startswith("http"):
                        low = val.lower()
                        if any(x in low for x in ("thumb", "preview", "upload", "creator", "batch", ".mp4", ".mov", ".m4v", ".jpg", ".jpeg", ".png")):
                            keys.add(val)
            except Exception:
                pass

        prefixes = [
            f"batches/{batch_id}/",
            f"batch/{batch_id}/",
            f"uploads/batches/{batch_id}/",
            f"videos/batches/{batch_id}/",
            f"thumbs/batches/{batch_id}/",
            f"previews/batches/{batch_id}/",
        ]
        if creator_id:
            prefixes += [
                f"creators/{creator_id}/batches/{batch_id}/",
                f"creators/{creator_id}/batch/{batch_id}/",
                f"creator/{creator_id}/batches/{batch_id}/",
                f"creator/{creator_id}/batch/{batch_id}/",
                f"uploads/{creator_id}/{batch_id}/",
                f"videos/{creator_id}/{batch_id}/",
                f"thumbs/{creator_id}/{batch_id}/",
                f"previews/{creator_id}/{batch_id}/",
            ]

        payload["keys"] = list(keys)
        payload["prefixes"] = prefixes
    except Exception as e:
        try:
            print("collect async batch r2 payload warning:", e)
        except Exception:
            pass
    return payload


def _delete_r2_payload_worker(app, payload, batch_id=None):
    try:
        with app.app_context():
            try:
                from app.services.r2 import delete_r2_candidates
                deleted = delete_r2_candidates(keys=payload.get("keys") or [], prefixes=payload.get("prefixes") or [])
                try:
                    print("Async batch R2 delete complete:", {"batch_id": batch_id, "deleted_objects": deleted})
                except Exception:
                    pass
            except Exception as e:
                try:
                    print("Async batch R2 delete warning:", e)
                except Exception:
                    pass
    except Exception:
        pass


def _schedule_batch_r2_delete(payload, batch_id=None):
    """
    Delete R2 objects in background so the page does not freeze.
    """
    try:
        import threading
        from flask import current_app
        app = current_app._get_current_object()
        t = threading.Thread(target=_delete_r2_payload_worker, args=(app, payload, batch_id), daemon=True)
        t.start()
        return True
    except Exception as e:
        try:
            print("schedule async batch r2 delete warning:", e)
        except Exception:
            pass
        return False


def _soft_delete_batch_db_only(batch_id):
    """
    Fast DB-only deletion/marking. R2 cleanup runs separately in background.
    This removes the batch from the UI quickly.
    """
    try:
        from app.models import Video, VideoBatch
        batch = VideoBatch.query.get(batch_id)
        if not batch:
            return False

        for v in Video.query.filter_by(batch_id=batch_id).all():
            if hasattr(v, "status"):
                v.status = "deleted"
                db.session.add(v)
            else:
                db.session.delete(v)

        if hasattr(batch, "status"):
            batch.status = "deleted"
            db.session.add(batch)
        else:
            db.session.delete(batch)

        db.session.commit()
        return True
    except Exception as e:
        db.session.rollback()
        try:
            print("soft delete batch db warning:", e)
        except Exception:
            pass
        return False




def _creator_order_sales_v427(creator_id):
    try:
        return db.session.execute(db.text("""
            SELECT i.*, o.buyer_email, o.amount_total, o.created_at, v.location, v.filename, v.internal_filename, v.thumbnail_path, v.public_thumbnail_url, v.r2_thumbnail_key
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            LEFT JOIN video v ON v.id = i.video_id
            WHERE i.creator_id = :cid
            ORDER BY o.created_at DESC
            LIMIT 100
        """), {"cid": creator_id}).mappings().all()
    except Exception:
        db.session.rollback()
        return []


def _creator_order_sales_summary_v427(creator_id):
    rows = _creator_order_sales_v427(creator_id)
    total = 0.0
    count = 0
    for r in rows:
        try:
            total += float(r.get("unit_price") or 0) * int(r.get("quantity") or 1)
            count += int(r.get("quantity") or 1)
        except Exception:
            pass
    return {"recent_order_sales": rows, "order_sales_total": total, "order_sales_count": count}



def _creator_pending_edits_v440(creator_id):
    try:
        db.session.execute(db.text("ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_r2_key TEXT"))
        db.session.execute(db.text("ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_uploaded_at TIMESTAMP"))
        db.session.commit()
    except Exception:
        db.session.rollback()

    try:
        return db.session.execute(db.text("""
            SELECT i.*, o.buyer_email, o.created_at, v.location, v.filename, v.internal_filename,
                   v.thumbnail_path, v.public_thumbnail_url, v.r2_thumbnail_key
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            LEFT JOIN video v ON v.id = i.video_id
            WHERE i.creator_id = :cid
              AND lower(coalesce(i.package,'')) IN ('edited','edit','instagram_edit','tiktok_edit','short_edit','reel_edit')
              AND lower(coalesce(i.delivery_status,'')) NOT IN ('ready_to_download','ready','delivered')
            ORDER BY o.created_at ASC
        """), {"cid": creator_id}).mappings().all()
    except Exception:
        db.session.rollback()
        return []


def _creator_context_id_v440():
    try:
        return session.get("creator_id") or session.get("user_id")
    except Exception:
        return None


def _send_edited_ready_email_v440(to_email, item_id):
    if not to_email:
        return False
    try:
        # Reuse SendGrid env if configured.
        import os, requests
        api_key = os.environ.get("SENDGRID_API_KEY")
        from_email = os.environ.get("SENDGRID_FROM_EMAIL") or os.environ.get("FROM_EMAIL")
        if not api_key or not from_email:
            print("SendGrid not configured for edited ready email")
            return False
        subject = "Your edited video is ready"
        link = (os.environ.get("PUBLIC_BASE_URL") or os.environ.get("BASE_URL") or "https://boatspotmedia.com").rstrip("/") + "/buyer/dashboard"
        html = f"""
        <h2>Your edited video is ready</h2>
        <p>Your edited video has been uploaded and is now available in your BoatSpotMedia order.</p>
        <p><a href="{link}">Open My Orders</a></p>
        <p>For best results, download on a computer. On a phone, the video may open for playback only.</p>
        """
        payload = {
            "personalizations": [{"to": [{"email": to_email}]}],
            "from": {"email": from_email},
            "subject": subject,
            "content": [{"type": "text/html", "value": html}],
        }
        r = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": "Bearer " + api_key, "Content-Type": "application/json"},
            json=payload,
            timeout=12,
        )
        return r.status_code in (200, 202)
    except Exception as e:
        try: print("edited ready email warning v44.0:", e)
        except Exception: pass
        return False



def _bsm_video_has_active_purchase_v442(video_id):
    try:
        from datetime import datetime, timezone, timedelta
        db.session.execute(db.text("ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_uploaded_at TIMESTAMP"))
        db.session.commit()
    except Exception:
        db.session.rollback()

    try:
        rows = db.session.execute(db.text("""
            SELECT i.*, o.created_at AS order_created_at
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            WHERE i.video_id = :vid
        """), {"vid": video_id}).mappings().all()
    except Exception:
        db.session.rollback()
        rows = []

    now = datetime.now(timezone.utc)
    for row in rows:
        package = str(row.get("package") or "").lower()
        delivery = str(row.get("delivery_status") or "").lower()
        discount = str(row.get("discount_status") or "").lower()

        if discount in ["pending_review", "pending", "awaiting_creator", "needs_approval"]:
            return True

        if package in ["edited","edit","instagram_edit","tiktok_edit","reel_edit","short_edit"]:
            if delivery not in ["ready_to_download", "ready", "delivered"]:
                return True
            start = row.get("edited_uploaded_at") or row.get("order_created_at")
        else:
            start = row.get("order_created_at")

        try:
            if start:
                if getattr(start, "tzinfo", None) is None:
                    start = start.replace(tzinfo=timezone.utc)
                if now <= start + timedelta(hours=72):
                    return True
        except Exception:
            return True

    return False


def _bsm_safe_delete_batch_v442(batch_id):
    try:
        videos = db.session.execute(db.text("""
            SELECT id FROM video WHERE batch_id = :batch_id
        """), {"batch_id": batch_id}).mappings().all()
    except Exception:
        db.session.rollback()
        videos = []

    protected = []
    deletable = []

    for v in videos:
        vid = v.get("id")
        if _bsm_video_has_active_purchase_v442(vid):
            protected.append(vid)
        else:
            deletable.append(vid)

    deleted_count = 0
    for vid in deletable:
        try:
            db.session.execute(db.text("DELETE FROM video WHERE id=:vid"), {"vid": vid})
            deleted_count += 1
        except Exception:
            db.session.rollback()

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()

    if protected:
        return {
            "blocked": True,
            "message": f"{deleted_count} video(s) deleted. {len(protected)} purchased/active video(s) were kept in this batch and can be deleted after their 72-hour download window or edit/approval workflow ends."
        }

    try:
        db.session.execute(db.text("DELETE FROM upload_batch WHERE id=:batch_id"), {"batch_id": batch_id})
        db.session.commit()
    except Exception:
        db.session.rollback()

    return {"blocked": False, "message": f"Batch deleted. {deleted_count} video(s) removed."}



def _creator_sales_panel_v445(creator_id):
    """
    Creator Sales Panel v44.5
    Read-only sales panel. Does not touch upload batches.
    Shows recent order items sold by this creator.
    """
    try:
        rows = db.session.execute(db.text("""
            SELECT
                i.id AS item_id,
                i.video_id,
                i.package,
                i.delivery_status,
                i.discount_status,
                i.unit_price,
                i.quantity,
                i.edited_r2_key,
                i.edited_uploaded_at,
                o.id AS order_id,
                o.buyer_email,
                o.status AS order_status,
                o.created_at AS order_created_at,
                v.location,
                v.filename,
                v.internal_filename,
                v.thumbnail_path,
                v.public_thumbnail_url,
                v.r2_thumbnail_key
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            LEFT JOIN video v ON v.id = i.video_id
            WHERE (i.creator_id = :creator_id OR v.creator_id = :creator_id)
            ORDER BY o.created_at DESC, i.id DESC
            LIMIT 100
        """), {"creator_id": creator_id}).mappings().all()
    except Exception as e:
        try:
            db.session.rollback()
            print("creator sales panel warning v44.5:", e)
        except Exception:
            pass
        rows = []

    recent_sales = []
    gross_total = 0.0
    sold_count = 0
    pending_edits_count = 0
    pending_discount_count = 0

    for r in rows:
        item = dict(r)
        package = str(item.get("package") or "").lower()
        delivery_status = str(item.get("delivery_status") or "").lower()
        discount_status = str(item.get("discount_status") or "").lower()

        try:
            price = float(item.get("unit_price") or 0)
            qty = int(item.get("quantity") or 1)
        except Exception:
            price = 0.0
            qty = 1

        gross_total += price * qty
        sold_count += qty

        is_edited = package in ["edited", "edit", "instagram_edit", "tiktok_edit", "reel_edit", "short_edit"]
        is_bundle = package in ["bundle", "combo", "original_plus_edited", "original_edited", "original+edited", "original_edit"]

        item["is_edited"] = is_edited
        item["is_bundle"] = is_bundle
        item["is_pending_edit"] = (is_edited or is_bundle) and delivery_status not in ["ready_to_download", "ready", "delivered"]
        item["is_pending_discount"] = discount_status in ["pending_review", "pending", "awaiting_creator", "needs_approval"]

        if item["is_pending_edit"]:
            pending_edits_count += 1
        if item["is_pending_discount"]:
            pending_discount_count += 1

        # For thumbnails: prefer public URL, then local media route for stored key.
        thumb = item.get("public_thumbnail_url")
        if not thumb:
            thumb_key = item.get("thumbnail_path") or item.get("r2_thumbnail_key")
            if thumb_key:
                thumb = "/media/" + str(thumb_key).lstrip("/")
        item["thumbnail_url"] = thumb

        # Hide buyer email in UI by default; show order number instead.
        buyer_email = item.get("buyer_email") or ""
        item["buyer_display"] = "Buyer · Order #" + str(item.get("order_id") or "")
        item["buyer_email_masked"] = (buyer_email[:2] + "***@" + buyer_email.split("@")[-1]) if "@" in buyer_email else "Buyer"

        recent_sales.append(item)

    return {
        "creator_recent_sales_v445": recent_sales,
        "creator_sales_gross_total_v445": gross_total,
        "creator_sales_count_v445": sold_count,
        "creator_pending_edits_count_v445": pending_edits_count,
        "creator_pending_discount_count_v445": pending_discount_count,
    }



def _bsm_creator_id_v446():
    return session.get("creator_id") or session.get("user_id")



def _bsm_backfill_order_item_creator_ids_v460():
    """
    Ensures every order item is connected to the video creator.
    This fixes purchases that were created without creator_id in bsm_cart_order_item.
    Safe to run repeatedly.
    """
    try:
        db.session.execute(db.text("""
            UPDATE bsm_cart_order_item i
            SET creator_id = v.creator_id
            FROM video v
            WHERE i.video_id = v.id
              AND (i.creator_id IS NULL OR i.creator_id = 0)
              AND v.creator_id IS NOT NULL
        """))
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try:
            print("creator_id backfill warning v46.0:", e)
        except Exception:
            pass


def _bsm_creator_orders_v446(creator_id):
    """
    Creator Orders Panel v44.6.
    Read-only order list + actionable statuses.
    Does not touch upload/pricing/batches logic.
    """
    try:
        db.session.execute(db.text("ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_r2_key TEXT"))
        db.session.execute(db.text("ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_uploaded_at TIMESTAMP"))
        db.session.commit()
    except Exception:
        db.session.rollback()

    try:
        rows = db.session.execute(db.text("""
            SELECT
                i.*,
                i.id AS item_id,
                o.id AS order_id,
                o.buyer_email,
                o.status AS order_status,
                o.created_at AS order_created_at,
                v.location,
                v.filename,
                v.internal_filename,
                v.thumbnail_path,
                v.public_thumbnail_url,
                v.r2_thumbnail_key
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            LEFT JOIN video v ON v.id = i.video_id
            WHERE (i.creator_id = :creator_id OR v.creator_id = :creator_id)
            ORDER BY o.created_at DESC, i.id DESC
            LIMIT 150
        """), {"creator_id": creator_id}).mappings().all()
    except Exception as e:
        db.session.rollback()
        try: print("creator orders warning v44.6:", e)
        except Exception: pass
        rows = []

    orders = []
    pending_edits = []
    discount_requests = []
    gross_total = 0.0
    sold_count = 0

    for r in rows:
        item = dict(r)
        package = str(item.get("package") or "").lower()
        delivery = str(item.get("delivery_status") or "").lower()
        discount = str(item.get("discount_status") or "").lower()

        is_edited = package in ["edited", "edit", "instagram_edit", "tiktok_edit", "reel_edit", "short_edit"]
        is_bundle = package in ["bundle", "combo", "original_plus_edited", "original_edited", "original+edited", "original_edit"]
        needs_edit = (is_edited or is_bundle) and delivery not in ["ready_to_download", "ready", "delivered"]
        needs_discount = discount in ["pending_review", "pending", "awaiting_creator", "needs_approval"]

        item["is_edited"] = is_edited
        item["is_bundle"] = is_bundle
        item["needs_edit"] = needs_edit
        item["needs_discount"] = needs_discount

        try:
            gross_total += float(item.get("unit_price") or 0) * int(item.get("quantity") or 1)
            sold_count += int(item.get("quantity") or 1)
        except Exception:
            pass

        thumb = item.get("public_thumbnail_url")
        if not thumb:
            key = item.get("thumbnail_path") or item.get("r2_thumbnail_key")
            if key:
                thumb = "/media/" + str(key).lstrip("/")
        item["thumbnail_url"] = thumb

        if is_bundle:
            item["package_label"] = "Bundle: Original + Edited"
        elif is_edited:
            item["package_label"] = "Edited Video"
        else:
            item["package_label"] = "Original / Instant Download"

        if needs_edit:
            item["status_label"] = "Pending edit upload"
            pending_edits.append(item)
        elif needs_discount:
            item["status_label"] = "Discount approval pending"
            discount_requests.append(item)
        elif delivery in ["ready_to_download", "ready", "delivered"]:
            item["status_label"] = "Delivered / Ready"
        else:
            item["status_label"] = item.get("order_status") or "Paid"

        orders.append(item)

    return {
        "creator_orders_v446": orders,
        "creator_pending_edits_v446": pending_edits,
        "creator_discount_requests_v446": discount_requests,
        "creator_orders_gross_v446": gross_total,
        "creator_orders_sold_count_v446": sold_count,
        "creator_orders_pending_edits_count_v446": len(pending_edits),
        "creator_orders_pending_discount_count_v446": len(discount_requests),
    }


def _send_edited_ready_email_v446(to_email, order_id=None):
    """
    Sends buyer email when edited video is ready.
    Uses SendGrid if SENDGRID_API_KEY and SENDGRID_FROM_EMAIL/FROM_EMAIL are configured.
    """
    if not to_email:
        return False
    try:
        import os, requests
        api_key = os.environ.get("SENDGRID_API_KEY")
        from_email = os.environ.get("SENDGRID_FROM_EMAIL") or os.environ.get("FROM_EMAIL")
        if not api_key or not from_email:
            print("SendGrid missing for edited ready email v44.6")
            return False

        base_url = (os.environ.get("PUBLIC_BASE_URL") or os.environ.get("BASE_URL") or "https://boatspotmedia.com").rstrip("/")
        dashboard_url = base_url + "/buyer/dashboard"
        subject = "Your edited video is ready"
        html = f"""
        <h2>Your edited video is ready</h2>
        <p>Your edited video from BoatSpotMedia is ready to download.</p>
        <p><strong>Order:</strong> #{order_id or ""}</p>
        <p><a href="{dashboard_url}" style="background:#2563eb;color:#fff;padding:12px 16px;border-radius:8px;text-decoration:none;">Open My Orders</a></p>
        <p>For best results, download on a computer. On phones, the video may open for playback.</p>
        """

        payload = {
            "personalizations": [{"to": [{"email": to_email}]}],
            "from": {"email": from_email},
            "subject": subject,
            "content": [{"type": "text/html", "value": html}],
        }
        r = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": "Bearer " + api_key, "Content-Type": "application/json"},
            json=payload,
            timeout=12,
        )
        return r.status_code in (200, 202)
    except Exception as e:
        try: print("edited ready email warning v44.6:", e)
        except Exception: pass
        return False



def _bsm_creator_id_v447():
    return session.get("creator_id") or session.get("user_id")

def _bsm_creator_orders_v447(creator_id):
    try:
        db.session.execute(db.text("ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_r2_key TEXT"))
        db.session.execute(db.text("ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_uploaded_at TIMESTAMP"))
        db.session.commit()
    except Exception:
        db.session.rollback()

    try:
        rows = db.session.execute(db.text("""
            SELECT
                i.*,
                i.id AS item_id,
                o.id AS order_id,
                o.buyer_email,
                o.status AS order_status,
                o.created_at AS order_created_at,
                v.location,
                v.filename,
                v.internal_filename,
                v.thumbnail_path,
                v.public_thumbnail_url,
                v.r2_thumbnail_key
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            LEFT JOIN video v ON v.id = i.video_id
            WHERE (i.creator_id = :creator_id OR v.creator_id = :creator_id)
            ORDER BY o.created_at DESC, i.id DESC
            LIMIT 200
        """), {"creator_id": creator_id}).mappings().all()
    except Exception as e:
        db.session.rollback()
        try: print("creator orders page warning v44.7:", e)
        except Exception: pass
        rows = []

    orders = []
    pending_edits = []
    discount_requests = []
    gross_total = 0.0
    sold_count = 0

    for r in rows:
        item = dict(r)
        package = str(item.get("package") or "").lower()
        delivery = str(item.get("delivery_status") or "").lower()
        discount = str(item.get("discount_status") or "").lower()

        is_edited = package in ["edited", "edit", "instagram_edit", "tiktok_edit", "reel_edit", "short_edit"]
        is_bundle = package in ["bundle", "combo", "original_plus_edited", "original_edited", "original+edited", "original_edit"]
        needs_edit = (is_edited or is_bundle) and delivery not in ["ready_to_download", "ready", "delivered"]
        needs_discount = discount in ["pending_review", "pending", "awaiting_creator", "needs_approval"]

        item["is_edited"] = is_edited
        item["is_bundle"] = is_bundle
        item["needs_edit"] = needs_edit
        item["needs_discount"] = needs_discount

        try:
            gross_total += float(item.get("unit_price") or 0) * int(item.get("quantity") or 1)
            sold_count += int(item.get("quantity") or 1)
        except Exception:
            pass

        thumb = item.get("public_thumbnail_url")
        if not thumb:
            key = item.get("thumbnail_path") or item.get("r2_thumbnail_key")
            if key:
                thumb = "/media/" + str(key).lstrip("/")
        item["thumbnail_url"] = thumb

        if is_bundle:
            item["package_label"] = "Bundle: Original + Edited"
        elif is_edited:
            item["package_label"] = "Edited Video"
        else:
            item["package_label"] = "Original / Instant Download"

        if needs_edit:
            item["status_label"] = "Pending edit upload"
            pending_edits.append(item)
        elif needs_discount:
            item["status_label"] = "Discount approval pending"
            discount_requests.append(item)
        elif delivery in ["ready_to_download", "ready", "delivered"]:
            item["status_label"] = "Delivered / Ready"
        else:
            item["status_label"] = item.get("order_status") or "Paid"

        orders.append(item)

    return {
        "orders": orders,
        "pending_edits": pending_edits,
        "discount_requests": discount_requests,
        "gross_total": gross_total,
        "sold_count": sold_count,
        "pending_edits_count": len(pending_edits),
        "pending_discount_count": len(discount_requests),
    }

def _send_edited_ready_email_v447(to_email, order_id=None):
    if not to_email:
        return False
    try:
        import os, requests
        api_key = os.environ.get("SENDGRID_API_KEY")
        from_email = os.environ.get("SENDGRID_FROM_EMAIL") or os.environ.get("FROM_EMAIL")
        if not api_key or not from_email:
            print("SendGrid missing for edited ready email v44.7")
            return False
        base_url = (os.environ.get("PUBLIC_BASE_URL") or os.environ.get("BASE_URL") or "https://boatspotmedia.com").rstrip("/")
        dashboard_url = base_url + "/buyer/dashboard"
        payload = {
            "personalizations": [{"to": [{"email": to_email}]}],
            "from": {"email": from_email},
            "subject": "Your edited video is ready",
            "content": [{"type": "text/html", "value": f"""
                <h2>Your edited video is ready</h2>
                <p>Your edited video from BoatSpotMedia is ready to download.</p>
                <p><strong>Order:</strong> #{order_id or ""}</p>
                <p><a href="{dashboard_url}">Open My Orders</a></p>
                <p>For best results, download on a computer.</p>
            """}],
        }
        r = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": "Bearer " + api_key, "Content-Type": "application/json"},
            json=payload,
            timeout=12,
        )
        return r.status_code in (200, 202)
    except Exception as e:
        try: print("edited ready email warning v44.7:", e)
        except Exception: pass
        return False



def _bsm_creator_orders_v459(creator_id, page=1, q=""):
    _bsm_backfill_order_item_creator_ids_v460()
    """
    Creator Orders v45.9:
    - pagination so creators with thousands of sales do not load everything
    - search by buyer email and order id
    - optional best-effort search by buyer first/last name if buyer tables/columns exist later
    """
    try:
        page = max(1, int(page or 1))
    except Exception:
        page = 1

    per_page = 25
    offset = (page - 1) * per_page
    q = (q or "").strip()

    base_where = "WHERE (i.creator_id = :creator_id OR v.creator_id = :creator_id)"
    params = {"creator_id": creator_id, "limit": per_page, "offset": offset}

    search_sql = ""
    if q:
        params["q"] = f"%{q.lower()}%"
        params["q_exact"] = q
        # Email is the most reliable identifier. Order id is also supported.
        search_sql = """
          AND (
            LOWER(COALESCE(o.buyer_email, '')) LIKE :q
            OR CAST(o.id AS TEXT) = :q_exact
          )
        """

    try:
        count_row = db.session.execute(db.text(f"""
            SELECT COUNT(*) AS total
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            LEFT JOIN video v ON v.id = i.video_id
            {base_where}
            {search_sql}
        """), params).mappings().first()
        total = int(count_row.get("total") or 0)
    except Exception as e:
        db.session.rollback()
        try: print("creator orders count warning v45.9:", e)
        except Exception: pass
        total = 0

    try:
        rows = db.session.execute(db.text(f"""
            SELECT
                i.*,
                i.id AS item_id,
                o.id AS order_id,
                o.buyer_email,
                o.status AS order_status,
                o.created_at AS order_created_at,
                v.location,
                v.filename,
                v.internal_filename,
                v.thumbnail_path,
                v.public_thumbnail_url,
                v.r2_thumbnail_key
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            LEFT JOIN video v ON v.id = i.video_id
            {base_where}
            {search_sql}
            ORDER BY o.created_at DESC, i.id DESC
            LIMIT :limit OFFSET :offset
        """), params).mappings().all()
    except Exception as e:
        db.session.rollback()
        try: print("creator orders page warning v45.9:", e)
        except Exception: pass
        rows = []

    orders = []
    gross_page = 0.0
    pending_edits_page = 0
    pending_discount_page = 0

    for r in rows:
        item = dict(r)
        package = str(item.get("package") or "").lower()
        delivery = str(item.get("delivery_status") or "").lower()
        discount = str(item.get("discount_status") or "").lower()

        item["is_edited"] = package in ["edited", "edit", "instagram_edit", "tiktok_edit", "reel_edit", "short_edit"]
        item["is_bundle"] = package in ["bundle", "combo", "original_plus_edited", "original_edited", "original+edited", "original_edit"]
        item["needs_edit"] = (item["is_edited"] or item["is_bundle"]) and delivery not in ["ready_to_download", "ready", "delivered"]
        item["needs_discount"] = discount in ["pending_review", "pending", "awaiting_creator", "needs_approval"]

        if item["is_bundle"]:
            item["package_label"] = "Bundle: Original + Edited"
        elif item["is_edited"]:
            item["package_label"] = "Edited Video"
        else:
            item["package_label"] = "Original / Instant Download"

        if item["needs_edit"]:
            item["status_label"] = "Pending edit upload"
            pending_edits_page += 1
        elif item["needs_discount"]:
            item["status_label"] = "Discount approval pending"
            pending_discount_page += 1
        elif delivery in ["ready_to_download", "ready", "delivered"]:
            item["status_label"] = "Delivered / Ready"
        else:
            item["status_label"] = item.get("order_status") or "Paid"

        try:
            gross_page += float(item.get("unit_price") or 0) * int(item.get("quantity") or 1)
        except Exception:
            pass

        thumb = item.get("public_thumbnail_url")
        if not thumb:
            key = item.get("thumbnail_path") or item.get("r2_thumbnail_key")
            if key:
                thumb = "/media/" + str(key).lstrip("/")
        item["thumbnail_url"] = thumb

        orders.append(item)

    total_pages = max(1, (total + per_page - 1) // per_page)

    return {
        "orders": orders,
        "q": q,
        "page": page,
        "per_page": per_page,
        "total": total,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
        "prev_page": max(1, page - 1),
        "next_page": min(total_pages, page + 1),
        "gross_page": gross_page,
        "pending_edits_page": pending_edits_page,
        "pending_discount_page": pending_discount_page,
    }



def _bsm_fix_order_item_creator_id_v460(order_id=None):
    """
    After checkout, attach creator_id to order items from the purchased videos.
    This prevents creator orders dashboard from showing empty sales.
    """
    try:
        params = {}
        where_order = ""
        if order_id:
            params["order_id"] = order_id
            where_order = " AND i.cart_order_id = :order_id "
        db.session.execute(db.text(f"""
            UPDATE bsm_cart_order_item i
            SET creator_id = v.creator_id
            FROM video v
            WHERE i.video_id = v.id
              AND (i.creator_id IS NULL OR i.creator_id = 0)
              AND v.creator_id IS NOT NULL
              {where_order}
        """), params)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try:
            print("order item creator_id fix warning v46.0:", e)
        except Exception:
            pass



def _bsm_v463_get_creator_storage_status(creator_id):
    """
    Returns creator storage usage/limit in bytes. Uses flexible column detection.
    If no storage limit column exists yet, allows upload and reports limit as None.
    """
    try:
        # Try creator_profile first
        row = db.session.execute(db.text("""
            SELECT *
            FROM creator_profile
            WHERE id = :creator_id
            LIMIT 1
        """), {"creator_id": creator_id}).mappings().first()
    except Exception:
        db.session.rollback()
        row = None

    used = 0
    limit = None

    if row:
        for key in ["storage_used_bytes", "used_storage_bytes", "storage_bytes_used", "storage_used"]:
            if key in row and row.get(key) is not None:
                try:
                    used = int(row.get(key) or 0)
                    break
                except Exception:
                    pass

        for key in ["storage_limit_bytes", "storage_quota_bytes", "max_storage_bytes"]:
            if key in row and row.get(key) is not None:
                try:
                    limit = int(row.get(key) or 0)
                    break
                except Exception:
                    pass

        # Some plans may store GB as numeric.
        if limit is None:
            for key in ["storage_limit_gb", "storage_gb", "max_storage_gb"]:
                if key in row and row.get(key) is not None:
                    try:
                        limit = int(float(row.get(key)) * 1024 * 1024 * 1024)
                        break
                    except Exception:
                        pass

    # If there is no stored usage, calculate from videos + edited items if file_size columns exist.
    if used == 0:
        try:
            calc = db.session.execute(db.text("""
                SELECT COALESCE(SUM(COALESCE(file_size_bytes, size_bytes, 0)),0) AS total
                FROM video
                WHERE creator_id = :creator_id
            """), {"creator_id": creator_id}).mappings().first()
            used += int(calc.get("total") or 0)
        except Exception:
            db.session.rollback()

        try:
            calc2 = db.session.execute(db.text("""
                SELECT COALESCE(SUM(COALESCE(edited_file_size_bytes, 0)),0) AS total
                FROM bsm_cart_order_item
                WHERE creator_id = :creator_id
            """), {"creator_id": creator_id}).mappings().first()
            used += int(calc2.get("total") or 0)
        except Exception:
            db.session.rollback()

    return {"used": used, "limit": limit}


def _bsm_v463_add_creator_storage_usage(creator_id, bytes_to_add):
    """
    Adds edited delivery file size to creator storage usage when possible.
    Safe if storage column does not exist: keeps edited_file_size_bytes on order item.
    """
    if not creator_id or not bytes_to_add:
        return

    possible_columns = ["storage_used_bytes", "used_storage_bytes", "storage_bytes_used", "storage_used"]
    for col in possible_columns:
        try:
            db.session.execute(db.text(f"""
                UPDATE creator_profile
                SET {col} = COALESCE({col}, 0) + :bytes_to_add
                WHERE id = :creator_id
            """), {"bytes_to_add": int(bytes_to_add), "creator_id": creator_id})
            db.session.commit()
            return
        except Exception:
            db.session.rollback()


def _bsm_v463_send_edited_ready_email(to_email, order_id=None):
    if not to_email:
        return False
    try:
        import os, requests
        api_key = os.environ.get("SENDGRID_API_KEY")
        from_email = os.environ.get("SENDGRID_FROM_EMAIL") or os.environ.get("FROM_EMAIL")
        if not api_key or not from_email:
            print("SendGrid missing for edited ready email v46.3")
            return False

        base_url = (os.environ.get("PUBLIC_BASE_URL") or os.environ.get("BASE_URL") or "https://boatspotmedia.com").rstrip("/")
        dashboard_url = base_url + "/buyer/dashboard"

        payload = {
            "personalizations": [{"to": [{"email": to_email}]}],
            "from": {"email": from_email},
            "subject": "Your edited video is ready",
            "content": [{"type": "text/html", "value": f"""
              <h2>Your edited video is ready</h2>
              <p>Your edited video from BoatSpotMedia is ready to download.</p>
              <p><strong>Order:</strong> #{order_id or ""}</p>
              <p><a href="{dashboard_url}" style="background:#2563eb;color:#fff;padding:12px 16px;border-radius:8px;text-decoration:none;">Open My Orders</a></p>
              <p>For best results, download on a computer. On phones, the video may open for playback.</p>
            """}],
        }
        r = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": "Bearer " + api_key, "Content-Type": "application/json"},
            json=payload,
            timeout=12,
        )
        return r.status_code in (200, 202)
    except Exception as e:
        try: print("edited ready email warning v46.3:", e)
        except Exception: pass
        return False


@creator_bp.route("/creator/batches/<int:batch_id>/safe-delete", methods=["POST"])
def creator_safe_delete_batch_v442(batch_id):
    result = _bsm_safe_delete_batch_v442(batch_id)
    try:
        flash(result.get("message"))
    except Exception:
        pass
    return redirect(request.referrer or "/creator/dashboard")


@creator_bp.route("/creator/discount-review")
def creator_discount_review():
    return render_template("creator/discount_review.html", review_groups=[])

@creator_bp.route("/creator/batch/delete-latest-incomplete", methods=["POST"])
@creator_bp.route("/batch/delete-latest-incomplete", methods=["POST"])
def bsm_delete_latest_incomplete_batch_v388():
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401
    try:
        batch = _bsm_latest_incomplete_batch_for_creator_v388(creator)
        if not batch:
            return jsonify({
                "ok": False,
                "safe": True,
                "error": "No incomplete batch found. Nothing was deleted."
            }), 404
        deleted = _bsm_delete_batch_r2_and_db_v388(batch)
        return jsonify({"ok": True, "batch_id": batch.id, "deleted_objects": deleted})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@creator_bp.route("/creator/batch/<int:batch_id>/delete-incomplete", methods=["POST"])
@creator_bp.route("/batch/<int:batch_id>/delete-incomplete", methods=["POST"])
def bsm_delete_specific_incomplete_batch_v388(batch_id):
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401
    try:
        from app.models import VideoBatch
        batch = VideoBatch.query.get_or_404(batch_id)
        if not _bsm_batch_is_incomplete_v388(batch):
            return jsonify({
                "ok": False,
                "safe": True,
                "error": "This batch has active videos and was not deleted."
            }), 400
        deleted = _bsm_delete_batch_r2_and_db_v388(batch)
        return jsonify({"ok": True, "batch_id": batch.id, "deleted_objects": deleted})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@creator_bp.route("/r2-clean/batch/<int:batch_id>", methods=["POST"])
def r2_clean_batch_v388_delete_only(batch_id):
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401
    try:
        from app.models import VideoBatch, Video
        batch = VideoBatch.query.get_or_404(batch_id)
        payload = _collect_batch_r2_delete_payload(batch.id)
        deleted = len(payload.get('keys') or []) + len(payload.get('prefixes') or [])
        _soft_delete_batch_db_only(batch.id)
        _schedule_batch_r2_delete(payload, batch_id=batch.id)
        for v in Video.query.filter_by(batch_id=batch_id).all():
            if hasattr(v, "status"):
                v.status = "deleted"
                db.session.add(v)
            else:
                db.session.delete(v)
        if hasattr(batch, "status"):
            batch.status = "deleted"
            db.session.add(batch)
        else:
            db.session.delete(batch)
        db.session.commit()
        return jsonify({"ok": True, "deleted_objects": deleted})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500


@creator_bp.route("/upload/batch/<int:batch_id>/cancel-clean", methods=["POST"])
@creator_bp.route("/creator/upload/batch/<int:batch_id>/cancel-clean", methods=["POST"])
def cancel_upload_batch_clean(batch_id):
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401
    deleted = _cleanup_upload_prefix(batch_id, getattr(creator, "id", None))
    try:
        from app.models import VideoBatch, Video
        batch = VideoBatch.query.get(batch_id)
        if batch:
            for v in Video.query.filter_by(batch_id=batch_id).all():
                db.session.delete(v)
            try:
                _delete_batch_files_from_r2(batch)
            except Exception:
                pass
            try:
                _delete_batch_files_from_r2(batch)
            except Exception:
                pass
            try:
                _delete_batch_r2_objects(batch)
            except Exception:
                pass
            db.session.delete(batch)
            db.session.commit()
    except Exception as e:
        db.session.rollback()
        try: print("cancel upload DB cleanup warning:", e)
        except Exception: pass
    return jsonify({"ok": True, "deleted_objects": deleted})


@creator_bp.route("/batch/<int:batch_id>/delete-full", methods=["POST"])
@creator_bp.route("/creator/batch/<int:batch_id>/delete-full", methods=["POST"])
def delete_batch_full_cleanup(batch_id):
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401
    try:
        from app.models import VideoBatch, Video
        batch = VideoBatch.query.get_or_404(batch_id)
        deleted = _delete_batch_files_from_r2(batch)
        for v in Video.query.filter_by(batch_id=batch_id).all():
            db.session.delete(v)
        try:
            _delete_batch_files_from_r2(batch)
        except Exception:
            pass
        try:
            _delete_batch_files_from_r2(batch)
        except Exception:
            pass
        try:
            _delete_batch_r2_objects(batch)
        except Exception:
            pass
        db.session.delete(batch)
        db.session.commit()
        return jsonify({"ok": True, "deleted_objects": deleted})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500


@creator_bp.route("/health")
def health():
    c = current_creator()
    return {
        "ok": True,
        "creator_id": c.id,
        "display_name": creator_display_name(c),
        "instagram_safe": creator_instagram(c),
        "storage_limit_gb": c.storage_limit_gb
    }

@creator_bp.route("/logout")
def logout():
    return redirect("/")

@creator_bp.route("/login", methods=["GET", "POST"])
def login():
    _ensure_creator_profile_deleted_column()
    if request.method == "POST":
        email = (request.form.get("email") or request.form.get("username") or "").strip().lower()
        password = request.form.get("password") or ""

        user = User.query.filter(db.func.lower(User.email) == email).first()
        if not user:
            flash("Email or password is incorrect. Please check and try again.", "error")
            return render_template("creator/login.html")

        stored_hash = getattr(user, "password_hash", None) or getattr(user, "password", None)
        valid_password = False
        try:
            valid_password = bool(stored_hash and check_password_hash(stored_hash, password))
        except Exception:
            valid_password = False
        if not valid_password and stored_hash and stored_hash == password:
            valid_password = True

        if not valid_password:
            flash("Email or password is incorrect. Please check and try again.", "error")
            return render_template("creator/login.html")

        creator = CreatorProfile.query.filter(
            CreatorProfile.user_id == user.id,
            CreatorProfile.approved == True,
            CreatorProfile.suspended == False,
            db.or_(CreatorProfile.deleted == False, CreatorProfile.deleted.is_(None))
        ).first()

        if not creator:
            flash("Creator account is not approved or is no longer active.", "error")
            return render_template("creator/login.html")

        session.clear()
        session["user_id"] = user.id
        session["user_email"] = user.email
        session["creator_id"] = creator.id
        session["role"] = "creator"
        session["user_role"] = "creator"
        session["display_name"] = creator_display_name(creator)
        session["creator_name"] = creator_display_name(creator)
        return redirect(url_for("creator.dashboard"))

    return render_template("creator/login.html")




@creator_bp.route("/login/apple")
def apple_login_under_construction():
    flash("Apple login is under construction. You'll be able to log in with Apple soon.", "info")
    return redirect(url_for("creator.login"))


@creator_bp.route("/dashboard")
def dashboard():
    _ensure_creator_profile_deleted_column()
    creator = current_creator()
    if creator:
        _recalculate_creator_storage(creator.id)
    storage_limit_gb, max_batch_gb = _creator_plan_limits(creator)
    storage_used_gb = _creator_storage_used_gb(creator.id)
    if not creator:
        flash('Please log in with an approved creator account.', 'error')
        return redirect(url_for('creator.login'))
    try:
        db.session.rollback()
    except Exception:
        pass
    stats = CreatorClickStats.query.filter_by(creator_id=creator.id).first()
    if not stats:
        stats = CreatorClickStats(creator_id=creator.id)
        db.session.add(stats)
        db.session.commit()

    try:
        videos_count = Video.query.filter_by(creator_id=creator.id, status="active").count()
    except Exception:
        db.session.rollback()
        videos_count = 0

    return render_creator_template(
        "creator/dashboard.html",
        creator=creator,
        stats=stats,
        videos_count=videos_count
    )





@creator_bp.route("/videos/delete-selected", methods=["POST"])
def delete_selected_videos():
    creator = current_creator()
    for video_id in request.form.getlist("video_ids"):
        v = Video.query.filter_by(id=int(video_id), creator_id=creator.id).first()
        if v and v.status != "deleted":
            v.status = "deleted"
            creator.storage_used_bytes = max(0, creator.storage_used_bytes - (v.file_size_bytes or 0))
    db.session.commit()
    return redirect(request.referrer or url_for("creator.batches"))


def _bsm_creator_orders_data_v461(creator_id, page=1, q=""):
    try:
        page = max(1, int(page or 1))
    except Exception:
        page = 1

    per_page = 25
    offset = (page - 1) * per_page
    q = (q or "").strip()

    # Backfill cart order item creator_id from video.creator_id.
    try:
        db.session.execute(db.text("""
            UPDATE bsm_cart_order_item i
            SET creator_id = v.creator_id
            FROM video v
            WHERE i.video_id = v.id
              AND (i.creator_id IS NULL OR i.creator_id = 0)
              AND v.creator_id IS NOT NULL
        """))
        db.session.commit()
    except Exception:
        db.session.rollback()

    orders = []
    total = 0
    gross_page = 0.0
    pending_edits_page = 0
    pending_discount_page = 0

    search_sql = ""
    params = {"creator_id": creator_id, "limit": per_page, "offset": offset}
    if q:
        params["q"] = f"%{q.lower()}%"
        params["q_exact"] = q
        search_sql = """
          AND (
            LOWER(COALESCE(o.buyer_email, '')) LIKE :q
            OR CAST(o.id AS TEXT) = :q_exact
          )
        """

    # Primary modern cart order tables
    try:
        count_row = db.session.execute(db.text(f"""
            SELECT COUNT(*) AS total
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            LEFT JOIN video v ON v.id = i.video_id
            WHERE (i.creator_id = :creator_id OR v.creator_id = :creator_id)
            {search_sql}
        """), params).mappings().first()
        total = int(count_row.get("total") or 0)
    except Exception as e:
        db.session.rollback()
        try: print("creator orders count v46.1 warning:", e)
        except Exception: pass
        total = 0

    try:
        rows = db.session.execute(db.text(f"""
            SELECT
                i.*,
                i.id AS item_id,
                o.id AS order_id,
                o.buyer_email,
                o.status AS order_status,
                o.created_at AS order_created_at,
                v.location,
                v.filename,
                v.internal_filename,
                v.thumbnail_path,
                v.public_thumbnail_url,
                v.r2_thumbnail_key
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            LEFT JOIN video v ON v.id = i.video_id
            WHERE (i.creator_id = :creator_id OR v.creator_id = :creator_id)
            {search_sql}
            ORDER BY o.created_at DESC, i.id DESC
            LIMIT :limit OFFSET :offset
        """), params).mappings().all()
    except Exception as e:
        db.session.rollback()
        try: print("creator orders rows v46.1 warning:", e)
        except Exception: pass
        rows = []

    for r in rows:
        item = dict(r)
        package = str(item.get("package") or "").lower()
        delivery = str(item.get("delivery_status") or "").lower()
        discount = str(item.get("discount_status") or "").lower()

        is_edited = package in ["edited", "edit", "instagram_edit", "tiktok_edit", "reel_edit", "short_edit"]
        is_bundle = package in ["bundle", "combo", "original_plus_edited", "original_edited", "original+edited", "original_edit"]
        needs_edit = (is_edited or is_bundle) and delivery not in ["ready_to_download", "ready", "delivered"]
        needs_discount = discount in ["pending_review", "pending", "awaiting_creator", "needs_approval"]

        item["is_edited"] = is_edited
        item["is_bundle"] = is_bundle
        item["needs_edit"] = needs_edit
        item["needs_discount"] = needs_discount

        if is_bundle:
            item["package_label"] = "Bundle: Original + Edited"
        elif is_edited:
            item["package_label"] = "Edited Video"
        else:
            item["package_label"] = "Original / Instant Download"

        if needs_edit:
            item["status_label"] = "Pending edit upload"
            pending_edits_page += 1
        elif needs_discount:
            item["status_label"] = "Discount approval pending"
            pending_discount_page += 1
        elif delivery in ["ready_to_download", "ready", "delivered"]:
            item["status_label"] = "Delivered / Ready"
        else:
            item["status_label"] = item.get("order_status") or "Paid"

        try:
            gross_page += float(item.get("unit_price") or 0) * int(item.get("quantity") or 1)
        except Exception:
            pass

        thumb = item.get("public_thumbnail_url")
        if not thumb:
            key = item.get("thumbnail_path") or item.get("r2_thumbnail_key")
            if key:
                thumb = "/media/" + str(key).lstrip("/")
        item["thumbnail_url"] = thumb
        item["display_filename"] = item.get("filename") or item.get("internal_filename") or ("Video #" + str(item.get("video_id") or ""))
        orders.append(item)

    # Fallback old ORM order_item table, only when modern table has no rows for this query/page.
    if total == 0:
        try:
            old_q = db.session.query(OrderItem).filter(OrderItem.creator_id == creator_id)
            if q:
                if q.isdigit():
                    old_q = old_q.filter(OrderItem.order_id == int(q))
                else:
                    old_q = old_q.join(Order, OrderItem.order_id == Order.id).filter(db.func.lower(Order.buyer_email).like(f"%{q.lower()}%"))
            old_total = old_q.count()
            old_items = old_q.order_by(OrderItem.id.desc()).limit(per_page).offset(offset).all()
            total = old_total
            for oi in old_items:
                video = getattr(oi, "video", None)
                order = getattr(oi, "order", None)
                package = getattr(oi, "purchase_type", None) or "original"
                item = {
                    "item_id": oi.id,
                    "video_id": getattr(oi, "video_id", None),
                    "order_id": getattr(oi, "order_id", None),
                    "buyer_email": getattr(order, "buyer_email", "") if order else "",
                    "order_created_at": getattr(order, "created_at", "") if order else "",
                    "location": getattr(video, "location", None) if video else None,
                    "filename": getattr(video, "filename", None) if video else None,
                    "internal_filename": getattr(video, "internal_filename", None) if video else None,
                    "unit_price": float(getattr(oi, "price", 0) or 0),
                    "package_label": "Bundle: Original + Edited" if package == "bundle" else ("Edited Video" if package == "edited" else "Original / Instant Download"),
                    "status_label": getattr(oi, "edited_status", "") or "Paid",
                    "is_edited": package == "edited",
                    "is_bundle": package == "bundle",
                    "needs_edit": getattr(oi, "edited_status", "") == "pending",
                    "needs_discount": False,
                }
                thumb = getattr(video, "public_thumbnail_url", None) if video else None
                if not thumb and video:
                    key = getattr(video, "thumbnail_path", None) or getattr(video, "r2_thumbnail_key", None)
                    if key:
                        thumb = "/media/" + str(key).lstrip("/")
                item["thumbnail_url"] = thumb
                orders.append(item)
        except Exception as e:
            db.session.rollback()
            try: print("old order_item fallback v46.1 warning:", e)
            except Exception: pass

    total_pages = max(1, (int(total or 0) + per_page - 1) // per_page)

    return {
        "orders": orders,
        "q": q,
        "page": page,
        "per_page": per_page,
        "total": total,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
        "prev_page": max(1, page - 1),
        "next_page": min(total_pages, page + 1),
        "gross_page": gross_page,
        "pending_edits_page": pending_edits_page,
        "pending_discount_page": pending_discount_page,
        "creator_id_debug": creator_id,
    }



@creator_bp.route("/orders", endpoint="orders_page_v462")
def creator_orders_page_v462():
    creator = current_creator()
    creator_id = creator.id if creator else None
    page = request.args.get("page", 1)
    q = request.args.get("q", "")
    return render_template("creator/orders.html", **_bsm_creator_orders_data_v461(creator_id, page, q))


@creator_bp.route("/orders")
def orders():
    creator = current_creator()
    status = request.args.get("status", "pending")

    try:
        q = OrderItem.query.filter_by(creator_id=creator.id)
        if status == "pending":
            q = q.filter(OrderItem.edited_status == "pending")
        elif status == "completed":
            q = q.filter(OrderItem.edited_status.in_(["ready", "not_required"]))
        items = q.order_by(OrderItem.id.desc()).all()
    except Exception:
        db.session.rollback()
        items = []

    return render_creator_template("creator/orders.html", creator=creator, items=items, status=status)

@creator_bp.route("/products", methods=["GET", "POST"])
def products():
    creator = current_creator()
    edit_id = request.args.get("edit")
    edit_product = None

    if edit_id and edit_id.isdigit():
        edit_product = Product.query.filter_by(id=int(edit_id), creator_id=creator.id).first()

    if request.method == "POST":
        product_id = request.form.get("product_id")

        if product_id:
            p = Product.query.filter_by(id=int(product_id), creator_id=creator.id).first_or_404()
        else:
            p = Product(creator_id=creator.id)
            db.session.add(p)

        p.title = request.form.get("title")
        p.description = request.form.get("description")
        p.price = float(request.form.get("price") or 0)
        p.shipping_cost = float(request.form.get("shipping_cost") or 0)
        p.processing_time = request.form.get("processing_time")
        p.shipping_method = request.form.get("shipping_method")
        p.active = True
        db.session.commit()

        return redirect(url_for("creator.products"))

    products = Product.query.filter_by(creator_id=creator.id).all()
    return render_creator_template("creator/products.html", creator=creator, products=products, edit_product=edit_product)

@creator_bp.route("/products/<int:product_id>/delete", methods=["POST"])
def delete_product(product_id):
    creator = current_creator()
    p = Product.query.filter_by(id=product_id, creator_id=creator.id).first_or_404()
    p.active = False
    db.session.commit()
    return redirect(url_for("creator.products"))

@creator_bp.route("/pricing", methods=["GET", "POST"])
def pricing():
    creator = current_creator()

    if request.method == "POST":
        preset_id = request.form.get("preset_id")

        if request.form.get("is_default"):
            VideoPricingPreset.query.filter_by(creator_id=creator.id).update({"is_default": False})

        if preset_id:
            p = VideoPricingPreset.query.filter_by(id=int(preset_id), creator_id=creator.id).first_or_404()
        else:
            p = VideoPricingPreset(creator_id=creator.id)
            db.session.add(p)

        p.title = request.form.get("title") or "Default Video Price"
        p.description = request.form.get("description")
        p.price = float(request.form.get("price") or 0)
        p.delivery_type = request.form.get("delivery_type") or "instant"
        p.is_default = bool(request.form.get("is_default"))
        p.active = True

        creator.second_clip_discount_percent = int(request.form.get("second_clip_discount_percent") or creator.second_clip_discount_percent or 0)

        db.session.commit()
        try:
            _schedule_creator_pricing_update(creator)
        except Exception:
            pass
        return redirect(url_for("creator.pricing"))

    presets = VideoPricingPreset.query.filter_by(creator_id=creator.id, active=True).order_by(VideoPricingPreset.id.desc()).all()
    edit_id = request.args.get("edit")
    edit_preset = VideoPricingPreset.query.filter_by(id=int(edit_id), creator_id=creator.id).first() if edit_id and edit_id.isdigit() else None

    return render_creator_template("creator/pricing.html", creator=creator, presets=presets, edit_preset=edit_preset)

@creator_bp.route("/pricing/<int:preset_id>/delete", methods=["POST"])
def delete_pricing(preset_id):
    creator = current_creator()
    p = VideoPricingPreset.query.filter_by(id=preset_id, creator_id=creator.id).first_or_404()
    p.active = False
    db.session.commit()
    try:
        _schedule_creator_pricing_update(creator)
    except Exception:
        pass
    return redirect(url_for("creator.pricing"))

@creator_bp.route("/settings", methods=["GET", "POST"])
def settings():
    creator = current_creator()

    if request.method == "POST":
        action = request.form.get("action")

        if action == "change_plan":
            plan_id = request.form.get("plan_id")
            plan = StoragePlan.query.get(plan_id) if plan_id else None
            if plan and plan.active:
                creator.plan_id = plan.id
                creator.storage_limit_gb = plan.storage_limit_gb
                creator.commission_rate = plan.commission_rate
                db.session.commit()
                try:
                    _schedule_creator_pricing_update(creator)
                except Exception:
                    pass
            return redirect(url_for("creator.settings"))

        if creator.user:
            creator.user.display_name = request.form.get("display_name") or creator.user.display_name
            creator.user.email = request.form.get("email") or creator.user.email

        db.session.commit()
        return redirect(url_for("creator.settings"))

    plans = StoragePlan.query.filter_by(active=True).order_by(StoragePlan.storage_limit_gb.asc()).all()
    return render_creator_template("creator/settings.html", creator=creator, plans=plans)


@creator_bp.route("/products/<int:product_id>/variants", methods=["GET", "POST"])
def product_variants(product_id):
    creator = current_creator()
    product = Product.query.filter_by(id=product_id, creator_id=creator.id).first_or_404()
    if request.method == "POST":
        variant = ProductVariant(product_id=product.id)
        variant.color_name = request.form.get("color_name")
        variant.color_hex = request.form.get("color_hex")
        variant.price_adjustment = float(request.form.get("price_adjustment") or 0)
        variant.active = True
        db.session.add(variant)
        db.session.commit()
        return redirect(url_for("creator.product_variants", product_id=product.id))
    variants = ProductVariant.query.filter_by(product_id=product.id, active=True).all()
    return render_creator_template("creator/product_variants.html", creator=creator, product=product, variants=variants)

@creator_bp.route("/products/variants/<int:variant_id>/delete", methods=["POST"])
def delete_product_variant(variant_id):
    creator = current_creator()
    variant = ProductVariant.query.get_or_404(variant_id)
    product = Product.query.filter_by(id=variant.product_id, creator_id=creator.id).first_or_404()
    variant.active = False
    db.session.commit()
    return redirect(url_for("creator.product_variants", product_id=product.id))

# ===== Creator video upload v35 Cloudflare R2 Direct Upload =====

BATCH_LIMIT_BYTES = 128 * 1024 * 1024 * 1024  # 128 GB per batch


def _creator_storage_limit_bytes(creator):
    try:
        plan = getattr(creator, "plan", None) or getattr(creator, "storage_plan", None)
        if plan and getattr(plan, "storage_limit_gb", None):
            return int(plan.storage_limit_gb) * 1024 * 1024 * 1024
    except Exception:
        pass
    try:
        if getattr(creator, "storage_limit_gb", None):
            return int(creator.storage_limit_gb) * 1024 * 1024 * 1024
    except Exception:
        pass
    return 128 * 1024 * 1024 * 1024


def _creator_used_storage_bytes(creator_id):
    from app.models import Video
    total = db.session.query(db.func.coalesce(db.func.sum(Video.file_size_bytes), 0)).filter(
        Video.creator_id == creator_id,
        db.or_(Video.status == None, ~Video.status.in_(["deleted", "cancelled", "canceled"]))
    ).scalar()
    return int(total or 0)

@creator_bp.route("/upload", methods=["GET"])
def upload():
    _ensure_creator_profile_deleted_column()
    creator = current_creator()
    if not creator:
        return redirect("/creator/login")

    used = _creator_used_storage_bytes(creator.id)
    limit = _creator_storage_limit_bytes(creator)
    storage_limit_gb, max_batch_gb = _creator_plan_limits(creator)
    storage_used_gb = _creator_storage_used_gb(creator.id)
    return render_template("creator/upload.html",
        used_bytes=used,
        limit_bytes=limit,
        used_gb=round(used / 1024 / 1024 / 1024, 2),
        storage_limit_gb=storage_limit_gb,
        max_batch_gb=max_batch_gb,
        storage_used_gb=storage_used_gb,
        limit_gb=round(limit / 1024 / 1024 / 1024, 2),
        batch_limit_gb=128,
    )





def _creator_plan_limits(creator):
    """Return safe storage/batch limits for creator panel and uploads.
    Defaults: 500 GB storage, 128 GB per batch.
    Reads multiple legacy/new field names so Owner panel changes still show in Creator panel.
    """
    def first_number(*values, default=0):
        for v in values:
            try:
                if v is not None and str(v).strip() != "":
                    return float(v)
            except Exception:
                pass
        return float(default)

    # Try direct creator columns first
    storage_limit = first_number(
        getattr(creator, "storage_limit_gb", None),
        getattr(creator, "plan_storage_limit_gb", None),
        getattr(creator, "storage_gb", None),
        getattr(creator, "plan_storage_gb", None),
        default=500
    )
    batch_limit = first_number(
        getattr(creator, "max_batch_gb", None),
        getattr(creator, "max_batch_size_gb", None),
        getattr(creator, "batch_limit_gb", None),
        getattr(creator, "plan_batch_limit_gb", None),
        default=128
    )

    # Try related plan object if present
    plan = getattr(creator, "plan", None)
    if plan:
        storage_limit = first_number(
            getattr(plan, "storage_limit_gb", None),
            getattr(plan, "plan_storage_limit_gb", None),
            getattr(plan, "storage_gb", None),
            getattr(plan, "included_storage_gb", None),
            storage_limit,
            default=500
        )
        batch_limit = first_number(
            getattr(plan, "max_batch_gb", None),
            getattr(plan, "max_batch_size_gb", None),
            getattr(plan, "batch_limit_gb", None),
            batch_limit,
            default=128
        )

    if storage_limit <= 0:
        storage_limit = 500
    if batch_limit <= 0:
        batch_limit = 128
    return storage_limit, batch_limit


def _creator_storage_used_gb(creator_id):
    """Storage shown in dashboard/upload: active videos only."""
    try:
        used = _creator_used_storage_bytes(creator_id)
        return round(float(used) / (1024 ** 3), 2)
    except Exception as e:
        try:
            print("storage used gb warning:", e)
            db.session.rollback()
        except Exception:
            pass
        return 0

def _ensure_batch_exists_for_upload(batch_id, creator_id, batch_name="", location=""):
    """Guarantee video_batch row exists before inserting videos."""
    try:
        from app.models import VideoBatch
        existing = VideoBatch.query.get(batch_id)
        if existing:
            return existing
        b = VideoBatch(
            id=batch_id,
            creator_id=creator_id,
            batch_name=batch_name or f"Batch {batch_id}",
            location=location or "",
            status="uploaded"
        )
        db.session.add(b)
        db.session.commit()
        return b
    except Exception as e:
        db.session.rollback()
        print("VideoBatch ensure warning:", e)
        try:
            db.session.execute(db.text("""
                INSERT INTO video_batch (id, creator_id, batch_name, location, status, created_at)
                VALUES (:id, :creator_id, :name, :location, 'uploaded', NOW())
                ON CONFLICT (id) DO NOTHING
            """), {
                "id": batch_id,
                "creator_id": creator_id,
                "name": batch_name or f"Batch {batch_id}",
                "location": location or ""
            })
            db.session.commit()
        except Exception as e2:
            db.session.rollback()
            print("Raw video_batch ensure warning:", e2)
        return None


def _ensure_video_upload_columns():
    """Make sure old PostgreSQL video table has the columns required by the uploader before any SELECT."""
    statements = [
        "ALTER TABLE creator_profile ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE",
        "UPDATE creator_profile SET deleted = FALSE WHERE deleted IS NULL",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS price NUMERIC(10,2) DEFAULT 0",
        "UPDATE video SET price = 0 WHERE price IS NULL",
        "ALTER TABLE video ALTER COLUMN price SET DEFAULT 0",
        "ALTER TABLE video ALTER COLUMN price DROP NOT NULL",
        "DELETE FROM video WHERE batch_id IS NOT NULL AND batch_id NOT IN (SELECT id FROM video_batch)",
        "ALTER TABLE video DROP CONSTRAINT IF EXISTS video_batch_id_fkey",
        "ALTER TABLE video ADD CONSTRAINT video_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES video_batch(id) ON DELETE CASCADE",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS filename VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS file_path VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS thumbnail_path VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS internal_filename VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS r2_video_key VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS r2_thumbnail_key VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS public_thumbnail_url VARCHAR(500)",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS file_size_bytes BIGINT DEFAULT 0",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS original_price NUMERIC(10,2) DEFAULT 0",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS edited_price NUMERIC(10,2) DEFAULT 0",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS bundle_price NUMERIC(10,2) DEFAULT 0",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS recorded_at TIMESTAMP",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS recorded_date DATE",
        "ALTER TABLE video ADD COLUMN IF NOT EXISTS recorded_time TIME",
        "UPDATE video SET filename = COALESCE(NULLIF(filename, ''), internal_filename, split_part(r2_video_key, '/', array_length(string_to_array(r2_video_key, '/'), 1)), 'video.mp4') WHERE filename IS NULL OR filename = ''",
        "UPDATE video SET file_path = COALESCE(NULLIF(file_path, ''), r2_video_key, filename, internal_filename, 'video.mp4') WHERE file_path IS NULL OR file_path = ''",
        "UPDATE video SET thumbnail_path = COALESCE(NULLIF(thumbnail_path, ''), r2_thumbnail_key, public_thumbnail_url) WHERE thumbnail_path IS NULL OR thumbnail_path = ''",
        "ALTER TABLE video ALTER COLUMN filename SET DEFAULT ''",
        "ALTER TABLE video ALTER COLUMN file_path SET DEFAULT ''"
    ]
    try:
        for sql in statements:
            db.session.execute(db.text(sql))
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        print("Video uploader column repair warning:", e)


@creator_bp.route("/upload/r2/prepare", methods=["POST"])
def upload_r2_prepare():
    _ensure_video_upload_columns()
    creator = current_creator()
    storage_limit_gb, max_batch_gb = _creator_plan_limits(creator)
    storage_used_gb = _creator_storage_used_gb(creator.id)
    if not creator:
        return jsonify({"ok": False, "error": "Creator login required."}), 401

    from app.models import VideoBatch
    from app.services.r2 import r2_configured, create_presigned_put_url
    import uuid
    from werkzeug.utils import secure_filename

    if not r2_configured():
        return jsonify({"ok": False, "error": "Cloudflare R2 is not configured. Missing R2 variables in Railway."}), 400

    data = request.get_json(silent=True) or {}
    files = data.get("files", [])
    location = (data.get("location") or "").strip()
    batch_name = (data.get("batch_name") or "").strip() or "New video batch"

    original_price, edited_price, bundle_price = _creator_video_prices_from_pricing_page(creator)

    if not files:
        return jsonify({"ok": False, "error": "Choose at least one video file."}), 400

    # Safe plan validation. Never fail because plan fields are empty.
    total_upload_bytes = sum(int(f.get("size") or 0) for f in files)
    total_upload_gb = total_upload_bytes / (1024 ** 3)
    if total_upload_gb > float(max_batch_gb):
        return jsonify({"ok": False, "error": f"Batch exceeds your plan limit. Maximum per batch is {max_batch_gb:g} GB."}), 400
    if (float(storage_used_gb) + total_upload_gb) > float(storage_limit_gb):
        return jsonify({"ok": False, "error": f"Upload exceeds your storage plan. Used {storage_used_gb:g} GB of {storage_limit_gb:g} GB."}), 400

    # Prevent duplicate uploads by same creator using original filename.
    from app.models import Video
    duplicate_messages = []
    for f in files:
        original_name_for_check = secure_filename(f.get("name") or "video.mp4")
        existing = db.session.query(Video.id, Video.batch_id).filter(
            Video.creator_id == creator.id,
            Video.status != "deleted",
            db.or_(
                Video.filename == original_name_for_check,
                Video.internal_filename == original_name_for_check
            )
        ).order_by(Video.id.desc()).first()
        if existing:
            duplicate_messages.append(
                f"{original_name_for_check} already exists in batch #{existing.batch_id}. Please check that batch before uploading again."
            )
    if duplicate_messages:
        return jsonify({"ok": False, "error": "Duplicate file found: " + " | ".join(duplicate_messages)}), 409

    total_size = 0
    for f in files:
        total_size += int(f.get("size") or 0)

    if total_size <= 0:
        return jsonify({"ok": False, "error": "Invalid file size."}), 400

    if total_size > BATCH_LIMIT_BYTES:
        return jsonify({"ok": False, "error": "This batch is over 128 GB. Please split it into smaller batches."}), 400

    used = _creator_used_storage_bytes(creator.id)
    limit = _creator_storage_limit_bytes(creator)
    if used + total_size > limit:
        return jsonify({"ok": False, "error": "This upload exceeds your plan storage limit. Upgrade your plan or delete old videos."}), 400

    batch = VideoBatch(
        creator_id=creator.id,
        location=location,
        batch_name=batch_name,
        total_size_bytes=total_size,
        file_count=len(files),
        status="uploading"
    )
    db.session.add(batch)
    db.session.commit()
    try:
        db.session.refresh(batch)
    except Exception:
        pass
    _ensure_batch_exists_for_upload(batch.id, creator.id, getattr(batch, 'name', ''), location or getattr(batch, 'location', ''))

    uploads = []
    for f in files:
        original_name = secure_filename(f.get("name") or "video.mp4")
        content_type = f.get("type") or "application/octet-stream"
        key = f"creators/{creator.id}/batches/{batch.id}/{uuid.uuid4().hex}_{original_name}"
        thumb_key = f"creators/{creator.id}/batches/{batch.id}/thumbs/{uuid.uuid4().hex}_{original_name}.jpg"
        url = create_presigned_put_url(key, content_type=content_type, expires=60 * 60 * 6)
        thumb_url = create_presigned_put_url(thumb_key, content_type="image/jpeg", expires=60 * 60 * 6)
        uploads.append({
            "name": original_name,
            "size": int(f.get("size") or 0),
            "type": content_type,
            "key": key,
            "upload_url": url,
            "thumbnail_key": thumb_key,
            "thumbnail_upload_url": thumb_url,
            "last_modified": f.get("last_modified")
        })

    return jsonify({
        "ok": True,
        "batch_id": batch.id,
        "uploads": uploads,
        "message": "Upload prepared. Browser will upload directly to Cloudflare R2."
    })


@creator_bp.route("/upload/r2/complete", methods=["POST"])
def upload_r2_complete():
    _ensure_video_upload_columns()
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Creator login required."}), 401

    from app.models import Video, VideoBatch
    from app.services.r2 import public_url_for_key

    data = request.get_json(silent=True) or {}
    batch_id = data.get("batch_id")
    uploaded = data.get("uploaded", [])
    location = (data.get("location") or "").strip()

    original_price, edited_price, bundle_price = _creator_video_prices_from_pricing_page(creator)

    batch = VideoBatch.query.filter_by(id=batch_id, creator_id=creator.id).first()
    if not batch:
        return jsonify({"ok": False, "error": "Batch not found."}), 404

    if not uploaded:
        return jsonify({"ok": False, "error": "No uploaded videos received."}), 400

    _ensure_batch_exists_for_upload(batch.id, creator.id, getattr(batch, 'name', ''), location or getattr(batch, 'location', ''))

    default_original_price, default_edited_price, default_bundle_price = _creator_default_prices(creator)
    from datetime import datetime, timezone
    for item in uploaded:
        key = item.get("key")
        if not key:
            continue

        recorded_at = None
        recorded_date = None
        recorded_time = None
        try:
            lm = item.get("last_modified")
            if lm:
                recorded_at = datetime.fromtimestamp(int(lm) / 1000, tz=timezone.utc).replace(tzinfo=None)
                recorded_date = recorded_at.date()
                recorded_time = recorded_at.time()
        except Exception:
            recorded_at = None

        thumb_key = item.get("thumbnail_key")
        v = Video(
            creator_id=creator.id,
            batch_id=batch.id,
            location=location or batch.location,
            file_path=key,
            r2_video_key=key,
            thumbnail_path=thumb_key,
            r2_thumbnail_key=thumb_key,
            public_thumbnail_url=public_url_for_key(thumb_key) if thumb_key else "",
            recorded_at=recorded_at,
            recorded_date=recorded_date,
            recorded_time=recorded_time,
            file_size_bytes=int(item.get("size") or 0),
            original_price=original_price,
            edited_price=edited_price,
            bundle_price=bundle_price,
            status="active",
            filename=item.get("name") or key.split("/")[-1],
            internal_filename=item.get("name") or key.split("/")[-1]
        )
        db.session.add(v)

    batch.status = "uploaded"
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": "Videos uploaded, but database save failed: " + str(e)}), 500

    return jsonify({
        "ok": True,
        "message": "Batch uploaded successfully to BoatSpotMedia Storage. You can view it in Batches.",
        "batch_id": batch.id
    })


@creator_bp.route("/batches")
def batches():
    creator = current_creator()
    if not creator:
        return redirect("/creator/login")
    from app.models import VideoBatch
    batches = VideoBatch.query.filter(VideoBatch.creator_id == creator.id, db.or_(VideoBatch.status == None, ~VideoBatch.status.in_(["deleted", "cancelled", "canceled"]))).order_by(VideoBatch.id.desc()).all()
    return render_template("creator/batches.html", batches=batches)


@creator_bp.route("/batches/<int:batch_id>")
def batch_detail(batch_id):
    creator = current_creator()
    if not creator:
        return redirect("/creator/login")
    from app.models import Video, VideoBatch
    batch = VideoBatch.query.filter_by(id=batch_id, creator_id=creator.id).first_or_404()
    videos = Video.query.filter_by(batch_id=batch.id, creator_id=creator.id).order_by(Video.id.desc()).all()
    return render_template("creator/batch_detail.html", batch=batch, videos=videos)


@creator_bp.route("/batches/<int:batch_id>/delete", methods=["POST"])
def delete_batch(batch_id):
    creator = current_creator()
    if not creator:
        return redirect("/creator/login")
    from app.models import Video, VideoBatch
    batch = VideoBatch.query.filter_by(id=batch_id, creator_id=creator.id).first_or_404()
    videos = Video.query.filter_by(batch_id=batch.id, creator_id=creator.id).all()
    for v in videos:
        v.status = "deleted"
    try:
        _delete_batch_files_from_r2(batch)
    except Exception:
        pass
    try:
        _delete_batch_r2_objects(batch)
    except Exception:
        pass
    batch.status = "deleted"
    db.session.commit()
    try:
        _recalculate_creator_storage(creator.id)
    except Exception:
        pass
    return redirect(url_for("creator.batches"))


@creator_bp.route("/videos/<int:video_id>/delete", methods=["POST"])
def delete_video(video_id):
    creator = current_creator()
    if not creator:
        return redirect("/creator/login")
    from app.models import Video
    v = Video.query.filter_by(id=video_id, creator_id=creator.id).first_or_404()
    v.status = "deleted"
    db.session.commit()
    return redirect(request.referrer or url_for("creator.batches"))




@creator_bp.route("/apply", methods=["GET", "POST"])
def apply():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        social = (request.form.get("social") or "").strip()
        message = (request.form.get("message") or "").strip()

        try:
            db.session.execute(db.text("""
                CREATE TABLE IF NOT EXISTS creator_application (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    email VARCHAR(255),
                    social VARCHAR(500),
                    message TEXT,
                    status VARCHAR(50) DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            db.session.execute(db.text("""
                INSERT INTO creator_application (name, email, social, message, status, created_at)
                VALUES (:name, :email, :social, :message, 'pending', NOW())
            """), {"name": name, "email": email, "social": social, "message": message})
            db.session.commit()
            flash("Application submitted. BoatSpotMedia will review your request.", "success")
            return redirect(url_for("creator.login"))
        except Exception as e:
            db.session.rollback()
            print("creator apply warning:", e)
            flash("Application could not be submitted right now. Please try again.", "error")

    return render_template("creator/apply.html")


@creator_bp.route("/apply/google")
def apply_with_google():
    try:
        return redirect(url_for("public.auth_google_register", account_type="creator"))
    except Exception:
        flash("Google application/login is not fully configured yet. Please apply with email for now.", "info")
        return redirect(url_for("creator.apply"))



@creator_bp.route("/apply/apple")
def apply_with_apple_under_construction():
    flash("Apple login is under construction. You'll be able to apply or log in with Apple soon.", "info")
    return redirect(url_for("creator.apply"))



@creator_bp.route("/upload/batch/<int:batch_id>/cancel", methods=["POST"])
def cancel_upload_batch(batch_id):
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401
    try:
        from app.models import Video, VideoBatch
        videos = Video.query.filter_by(creator_id=creator.id, batch_id=batch_id).all()
        try:
            from app.services import r2
            for v in videos:
                for key in [getattr(v, "r2_video_key", None), getattr(v, "file_path", None), getattr(v, "r2_thumbnail_key", None), getattr(v, "thumbnail_path", None)]:
                    if key and hasattr(r2, "delete"):
                        r2.delete(key)
        except Exception as e:
            print("R2 cancel delete warning:", e)
        for v in videos:
            db.session.delete(v)
        batch = VideoBatch.query.filter_by(id=batch_id, creator_id=creator.id).first()
        if batch:
            try:
                _delete_batch_files_from_r2(batch)
            except Exception:
                pass
            try:
                _delete_batch_files_from_r2(batch)
            except Exception:
                pass
            try:
                _delete_batch_r2_objects(batch)
            except Exception:
                pass
            db.session.delete(batch)
        db.session.commit()
        _recalculate_creator_storage(creator.id)
        return jsonify({"ok": True, "message": "Upload cancelled and uploaded files were removed."})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500



@creator_bp.route("/videos/<int:video_id>/regenerate-thumbnail", methods=["POST"])
def regenerate_video_thumbnail(video_id):
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401

    try:
        from app.models import Video
        import tempfile, subprocess, os, json, urllib.request, uuid
        from app.services.r2 import public_url_for_key
        try:
            from app.services.r2 import upload_file as r2_upload_file
        except Exception:
            r2_upload_file = None

        v = Video.query.filter_by(id=video_id, creator_id=creator.id).first()
        if not v:
            return jsonify({"ok": False, "error": "Video not found."}), 404

        public_video_url = public_url_for_key(v.r2_video_key or v.file_path)
        if not public_video_url:
            return jsonify({"ok": False, "error": "Video public URL unavailable for thumbnail generation."}), 400

        tmpdir = tempfile.mkdtemp()
        video_path = os.path.join(tmpdir, "video_original")
        thumb_path = os.path.join(tmpdir, "thumb.jpg")

        urllib.request.urlretrieve(public_video_url, video_path)

        def duration(path):
            try:
                r = subprocess.run(["ffprobe","-v","error","-show_entries","format=duration","-of","json",path], capture_output=True, text=True, timeout=60)
                return float(json.loads(r.stdout or "{}").get("format",{}).get("duration") or 0)
            except Exception:
                return 0

        d = duration(video_path)
        points = [0.50,0.45,0.55,0.60,0.40,0.65,0.35,0.70,0.25,0.75]
        made = False
        for pct in points:
            t = max(1.0, d * pct if d else 2.0)
            cmd = [
                "ffmpeg","-y","-ss",str(t),"-i",video_path,
                "-frames:v","1",
                "-vf","crop=iw*0.70:ih*0.70:iw*0.15:ih*0.15,scale=1280:-2,eq=brightness=0.02:saturation=1.1",
                "-q:v","2",thumb_path
            ]
            subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 4000:
                # basic black check using ffmpeg signalstats would be heavy; size threshold + multiple points is enough here.
                made = True
                break

        if not made:
            return jsonify({"ok": False, "error": "Could not generate a usable thumbnail."}), 500

        thumb_key = f"creators/{creator.id}/batches/{v.batch_id}/thumbs/backend_{uuid.uuid4().hex}_{v.filename or 'thumb'}.jpg"
        if r2_upload_file:
            r2_upload_file(thumb_path, thumb_key, content_type="image/jpeg")
        else:
            try:
                from app.services import r2
                if hasattr(r2, "upload"):
                    r2.upload(thumb_path, thumb_key, content_type="image/jpeg")
                else:
                    return jsonify({"ok": False, "error": "R2 upload helper not available."}), 500
            except Exception as e:
                return jsonify({"ok": False, "error": "R2 upload helper not available: " + str(e)}), 500

        v.r2_thumbnail_key = thumb_key
        v.thumbnail_path = thumb_key
        v.public_thumbnail_url = public_url_for_key(thumb_key)
        db.session.commit()
        return jsonify({"ok": True, "thumbnail_url": v.public_thumbnail_url})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500


@creator_bp.route("/r2-clean/batch/<int:batch_id>", methods=["POST"])
def r2_clean_batch_safe(batch_id):
    creator = current_creator()
    if not creator:
        return jsonify({"ok": False, "error": "Please log in again."}), 401

    deleted = 0
    try:
        from app.models import VideoBatch, Video
        from app.services.r2 import delete_r2_object, delete_r2_prefix

        batch = VideoBatch.query.get(batch_id)
        creator_id = getattr(batch, "creator_id", None) or getattr(batch, "creator_profile_id", None) or getattr(creator, "id", None)

        for v in Video.query.filter_by(batch_id=batch_id).all():
            for attr in ("r2_video_key", "r2_thumbnail_key", "file_path", "thumbnail_path"):
                key = getattr(v, attr, None)
                if key and not str(key).startswith("http"):
                    try:
                        delete_r2_object(key)
                        deleted += 1
                    except Exception as e:
                        try:
                            print("R2 object delete warning:", key, e)
                        except Exception:
                            pass
            try:
                if hasattr(v, "status"):
                    v.status = "deleted"
                    db.session.add(v)
                else:
                    db.session.delete(v)
            except Exception:
                pass

        if creator_id:
            for prefix in (
                f"creators/{creator_id}/batches/{batch_id}/",
                f"creator/{creator_id}/batch/{batch_id}/",
                f"batches/{batch_id}/",
            ):
                try:
                    deleted += delete_r2_prefix(prefix)
                except Exception as e:
                    try:
                        print("R2 prefix delete warning:", prefix, e)
                    except Exception:
                        pass

        if batch:
            if hasattr(batch, "status"):
                try:
                    _delete_batch_r2_objects(batch)
                except Exception:
                    pass
                batch.status = "deleted"
                db.session.add(batch)
            else:
                try:
                    _delete_batch_r2_objects(batch)
                except Exception:
                    pass
                db.session.delete(batch)

        db.session.commit()
        return jsonify({"ok": True, "deleted_objects": deleted})
    except Exception as e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500



@creator_bp.route("/creator/order-item/<int:item_id>/approve-discount", methods=["POST"])
def approve_discount_item_v439(item_id):
    try:
        db.session.execute(db.text("""
            UPDATE bsm_cart_order_item
            SET discount_status='approved',
                delivery_status=CASE WHEN delivery_status IN ('pending_discount_review','pending','not_ready') THEN 'ready_to_download' ELSE delivery_status END
            WHERE id=:item_id
        """), {"item_id": item_id})
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try: print("approve discount warning v43.9:", e)
        except Exception: pass
    return redirect(request.referrer or "/creator/dashboard")


@creator_bp.route("/creator/order-item/<int:item_id>/reject-discount", methods=["POST"])
def reject_discount_item_v439(item_id):
    try:
        db.session.execute(db.text("""
            UPDATE bsm_cart_order_item
            SET discount_status='rejected',
                delivery_status=CASE WHEN delivery_status IN ('pending_discount_review','pending','not_ready') THEN 'ready_to_download' ELSE delivery_status END
            WHERE id=:item_id
        """), {"item_id": item_id})
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try: print("reject discount warning v43.9:", e)
        except Exception: pass
    return redirect(request.referrer or "/creator/dashboard")



@creator_bp.route("/creator/pending-edits")
def creator_pending_edits_v440():
    creator_id = _creator_context_id_v440()
    rows = _creator_pending_edits_v440(creator_id)
    return render_template("creator/pending_edits.html", pending_edits=rows)


@creator_bp.route("/creator/order-item/<int:item_id>/upload-edited", methods=["POST"])
def creator_upload_edited_v440(item_id):
    file = request.files.get("edited_video")
    if not file or not file.filename:
        return redirect(request.referrer or "/creator/pending-edits")

    try:
        row = db.session.execute(db.text("""
            SELECT i.*, o.buyer_email, o.id AS order_id
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            WHERE i.id=:item_id
            LIMIT 1
        """), {"item_id": item_id}).mappings().first()
    except Exception:
        db.session.rollback()
        row = None

    if not row:
        return "Order item not found", 404

    creator_id = row.get("creator_id") or _creator_context_id_v440()
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", file.filename)
    key = f"edited/creators/{creator_id}/orders/{row.get('order_id')}/items/{item_id}/{safe_name}"

    try:
        from app.services.r2 import r2_client, _bucket_name
        client = r2_client()
        bucket = _bucket_name()
        file.stream.seek(0)
        client.upload_fileobj(file.stream, bucket, key, ExtraArgs={"ContentType": file.mimetype or "video/mp4"})
    except Exception as e:
        try: print("edited upload r2 warning v44.0:", e)
        except Exception: pass
        return "Could not upload edited video to R2", 500

    try:
        db.session.execute(db.text("""
            ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_r2_key TEXT
        """))
        db.session.execute(db.text("""
            ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_uploaded_at TIMESTAMP
        """))
        db.session.execute(db.text("""
            UPDATE bsm_cart_order_item
            SET edited_r2_key=:key,
                delivery_status='ready_to_download',
                edited_uploaded_at=CURRENT_TIMESTAMP
            WHERE id=:item_id
        """), {"key": key, "item_id": item_id})
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try: print("edited item update warning v44.0:", e)
        except Exception: pass
        return "Edited video uploaded but order item could not be updated", 500

    _send_edited_ready_email_v440(row.get("buyer_email"), item_id)
    return redirect(request.referrer or "/creator/pending-edits")



@creator_bp.route("/creator/order-item/<int:item_id>/upload-edited", methods=["POST"])
def creator_upload_edited_video_v446(item_id):
    file = request.files.get("edited_video")
    if not file or not file.filename:
        try: flash("Please select an edited video file.")
        except Exception: pass
        return redirect(request.referrer or "/creator/dashboard")

    try:
        row = db.session.execute(db.text("""
            SELECT i.*, o.buyer_email, o.id AS order_id
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            WHERE i.id=:item_id
            LIMIT 1
        """), {"item_id": item_id}).mappings().first()
    except Exception:
        db.session.rollback()
        row = None

    if not row:
        return "Order item not found", 404

    creator_id = row.get("creator_id") or _bsm_creator_id_v446()
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", file.filename)
    key = f"edited/creators/{creator_id}/orders/{row.get('order_id')}/items/{item_id}/{safe_name}"

    try:
        from app.services.r2 import r2_client, _bucket_name
        client = r2_client()
        bucket = _bucket_name()
        file.stream.seek(0)
        client.upload_fileobj(file.stream, bucket, key, ExtraArgs={"ContentType": file.mimetype or "video/mp4"})
    except Exception as e:
        try: print("edited upload r2 warning v44.6:", e)
        except Exception: pass
        return "Could not upload edited video to R2", 500

    try:
        db.session.execute(db.text("ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_r2_key TEXT"))
        db.session.execute(db.text("ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_uploaded_at TIMESTAMP"))
        db.session.execute(db.text("""
            UPDATE bsm_cart_order_item
            SET edited_r2_key=:key,
                delivery_status='ready_to_download',
                edited_uploaded_at=CURRENT_TIMESTAMP
            WHERE id=:item_id
        """), {"key": key, "item_id": item_id})
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try: print("edited order item update warning v44.6:", e)
        except Exception: pass
        return "Edited video uploaded but order status could not be updated", 500

    sent = _send_edited_ready_email_v446(row.get("buyer_email"), row.get("order_id"))
    try:
        flash("Edited video uploaded. Buyer download is now active." + (" Email sent." if sent else " Email was not sent; check SendGrid settings."))
    except Exception:
        pass
    return redirect(request.referrer or "/creator/dashboard")


@creator_bp.route("/creator/order-item/<int:item_id>/approve-discount", methods=["POST"])
def creator_approve_discount_v446(item_id):
    try:
        db.session.execute(db.text("""
            UPDATE bsm_cart_order_item
            SET discount_status='approved',
                delivery_status=CASE WHEN delivery_status IN ('pending_discount_review','pending','not_ready') THEN 'ready_to_download' ELSE delivery_status END
            WHERE id=:item_id
        """), {"item_id": item_id})
        db.session.commit()
        try: flash("Discount approved. Buyer download is now active.")
        except Exception: pass
    except Exception as e:
        db.session.rollback()
        try: print("approve discount warning v44.6:", e)
        except Exception: pass
    return redirect(request.referrer or "/creator/dashboard")


@creator_bp.route("/creator/order-item/<int:item_id>/reject-discount", methods=["POST"])
def creator_reject_discount_v446(item_id):
    try:
        db.session.execute(db.text("""
            UPDATE bsm_cart_order_item
            SET discount_status='rejected',
                delivery_status=CASE WHEN delivery_status IN ('pending_discount_review','pending','not_ready') THEN 'ready_to_download' ELSE delivery_status END
            WHERE id=:item_id
        """), {"item_id": item_id})
        db.session.commit()
        try: flash("Discount rejected. Buyer download is now active at normal price.")
        except Exception: pass
    except Exception as e:
        db.session.rollback()
        try: print("reject discount warning v44.6:", e)
        except Exception: pass
    return redirect(request.referrer or "/creator/dashboard")




@creator_bp.route("/creator/order-item/<int:item_id>/upload-edited-v447", methods=["POST"])
def creator_upload_edited_video_v447(item_id):
    file = request.files.get("edited_video")
    if not file or not file.filename:
        try: flash("Please select an edited video file.")
        except Exception: pass
        return redirect(request.referrer or "/creator/orders")

    try:
        row = db.session.execute(db.text("""
            SELECT i.*, o.buyer_email, o.id AS order_id
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            WHERE i.id=:item_id
            LIMIT 1
        """), {"item_id": item_id}).mappings().first()
    except Exception:
        db.session.rollback()
        row = None

    if not row:
        return "Order item not found", 404

    creator_id = row.get("creator_id") or _bsm_creator_id_v447()
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", file.filename)
    key = f"edited/creators/{creator_id}/orders/{row.get('order_id')}/items/{item_id}/{safe_name}"

    try:
        from app.services.r2 import r2_client, _bucket_name
        client = r2_client()
        bucket = _bucket_name()
        file.stream.seek(0)
        client.upload_fileobj(file.stream, bucket, key, ExtraArgs={"ContentType": file.mimetype or "video/mp4"})
    except Exception as e:
        try: print("edited upload r2 warning v44.7:", e)
        except Exception: pass
        return "Could not upload edited video to R2", 500

    try:
        db.session.execute(db.text("ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_r2_key TEXT"))
        db.session.execute(db.text("ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_uploaded_at TIMESTAMP"))
        db.session.execute(db.text("""
            UPDATE bsm_cart_order_item
            SET edited_r2_key=:key,
                delivery_status='ready_to_download',
                edited_uploaded_at=CURRENT_TIMESTAMP
            WHERE id=:item_id
        """), {"key": key, "item_id": item_id})
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try: print("edited order item update warning v44.7:", e)
        except Exception: pass
        return "Edited video uploaded but order status could not be updated", 500

    sent = _send_edited_ready_email_v447(row.get("buyer_email"), row.get("order_id"))
    try:
        flash("Edited video uploaded. Buyer download is now active." + (" Email sent." if sent else " Email not sent; check SendGrid."))
    except Exception:
        pass
    return redirect(request.referrer or "/creator/orders")

@creator_bp.route("/creator/order-item/<int:item_id>/approve-discount-v447", methods=["POST"])
def creator_approve_discount_v447(item_id):
    try:
        db.session.execute(db.text("""
            UPDATE bsm_cart_order_item
            SET discount_status='approved',
                delivery_status=CASE WHEN delivery_status IN ('pending_discount_review','pending','not_ready') THEN 'ready_to_download' ELSE delivery_status END
            WHERE id=:item_id
        """), {"item_id": item_id})
        db.session.commit()
        try: flash("Discount approved. Buyer download is now active.")
        except Exception: pass
    except Exception as e:
        db.session.rollback()
        try: print("approve discount warning v44.7:", e)
        except Exception: pass
    return redirect(request.referrer or "/creator/orders")

@creator_bp.route("/creator/order-item/<int:item_id>/reject-discount-v447", methods=["POST"])
def creator_reject_discount_v447(item_id):
    try:
        db.session.execute(db.text("""
            UPDATE bsm_cart_order_item
            SET discount_status='rejected',
                delivery_status=CASE WHEN delivery_status IN ('pending_discount_review','pending','not_ready') THEN 'ready_to_download' ELSE delivery_status END
            WHERE id=:item_id
        """), {"item_id": item_id})
        db.session.commit()
        try: flash("Discount rejected. Buyer download is now active at normal price.")
        except Exception: pass
    except Exception as e:
        db.session.rollback()
        try: print("reject discount warning v44.7:", e)
        except Exception: pass
    return redirect(request.referrer or "/creator/orders")



@creator_bp.route("/creator/pricing")
def creator_pricing_page_v452():
    return render_template("creator/pricing.html")





@creator_bp.route("/pending-edits", endpoint="pending_edits_v463")
def creator_pending_edits_v463():
    creator = current_creator()
    creator_id = creator.id if creator else None
    data = _bsm_creator_orders_data_v461(creator_id, request.args.get("page", 1), request.args.get("q", ""))
    data["orders"] = [x for x in data.get("orders", []) if x.get("needs_edit")]
    data["total"] = len(data["orders"])
    data["pending_only"] = True
    return render_template("creator/orders.html", **data)


@creator_bp.route("/order-item/<int:item_id>/upload-edited-v463", methods=["POST"], endpoint="upload_edited_v463")
def creator_upload_edited_video_v463(item_id):
    creator = current_creator()
    creator_id = creator.id if creator else None

    file = request.files.get("edited_video")
    if not file or not file.filename:
        try: flash("Please select an edited video file.")
        except Exception: pass
        return redirect(request.referrer or "/creator/orders")

    try:
        file.stream.seek(0, 2)
        file_size = file.stream.tell()
        file.stream.seek(0)
    except Exception:
        file_size = 0

    storage = _bsm_v463_get_creator_storage_status(creator_id)
    if storage.get("limit") is not None and storage.get("used", 0) + int(file_size or 0) > storage.get("limit"):
        try:
            flash("Storage limit reached. Upgrade your plan or delete expired/unneeded videos before uploading this edited delivery.")
        except Exception:
            pass
        return redirect(request.referrer or "/creator/orders")

    try:
        row = db.session.execute(db.text("""
            SELECT i.*, o.buyer_email, o.id AS order_id
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            WHERE i.id=:item_id
            LIMIT 1
        """), {"item_id": item_id}).mappings().first()
    except Exception:
        db.session.rollback()
        row = None

    if not row:
        return "Order item not found", 404

    # Security: order item must belong to this creator or video creator.
    try:
        check = db.session.execute(db.text("""
            SELECT v.creator_id
            FROM video v
            WHERE v.id=:video_id
            LIMIT 1
        """), {"video_id": row.get("video_id")}).mappings().first()
        video_creator_id = check.get("creator_id") if check else None
    except Exception:
        db.session.rollback()
        video_creator_id = None

    if creator_id and row.get("creator_id") and int(row.get("creator_id")) != int(creator_id) and str(video_creator_id) != str(creator_id):
        return "Not authorized for this order item", 403

    import re as _re
    safe_name = _re.sub(r"[^A-Za-z0-9_.-]+", "_", file.filename)
    key = f"edited/creators/{creator_id}/orders/{row.get('order_id')}/items/{item_id}/{safe_name}"

    try:
        from app.services.r2 import r2_client, _bucket_name
        client = r2_client()
        bucket = _bucket_name()
        file.stream.seek(0)
        client.upload_fileobj(file.stream, bucket, key, ExtraArgs={"ContentType": file.mimetype or "video/mp4"})
    except Exception as e:
        try: print("edited upload R2 warning v46.3:", e)
        except Exception: pass
        return "Could not upload edited video to R2", 500

    try:
        db.session.execute(db.text("ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_r2_key TEXT"))
        db.session.execute(db.text("ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_uploaded_at TIMESTAMP"))
        db.session.execute(db.text("ALTER TABLE bsm_cart_order_item ADD COLUMN IF NOT EXISTS edited_file_size_bytes BIGINT DEFAULT 0"))
        db.session.execute(db.text("""
            UPDATE bsm_cart_order_item
            SET edited_r2_key=:key,
                edited_file_size_bytes=:file_size,
                delivery_status='ready_to_download',
                edited_uploaded_at=CURRENT_TIMESTAMP,
                creator_id=COALESCE(creator_id, :creator_id)
            WHERE id=:item_id
        """), {"key": key, "file_size": int(file_size or 0), "creator_id": creator_id, "item_id": item_id})
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try: print("edited order update warning v46.3:", e)
        except Exception: pass
        return "Edited video uploaded but order status could not be updated", 500

    _bsm_v463_add_creator_storage_usage(creator_id, file_size)
    sent = _bsm_v463_send_edited_ready_email(row.get("buyer_email"), row.get("order_id"))

    try:
        flash("Edited video uploaded. Buyer download is now active." + (" Email sent." if sent else " Email not sent; check SendGrid."))
    except Exception:
        pass
    return redirect(request.referrer or "/creator/orders")
