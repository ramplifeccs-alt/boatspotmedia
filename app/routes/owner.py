from sqlalchemy import text
from datetime import datetime, timedelta
from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from werkzeug.security import generate_password_hash
from sqlalchemy import text
from app.models import User, CreatorProfile, StoragePlan, CommissionOverrideLog, Video, Product
from app.services.db import db
from app.services.emailer import send_email
from app.services.db_repair import repair_creator_application_table, repair_all_known_tables

owner_bp = Blueprint("owner", __name__)


def _ensure_creator_profile_deleted_column():
    """Create creator_profile.deleted before Owner ORM queries reference it."""
    try:
        db.session.execute(text("ALTER TABLE creator_profile ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE"))
        db.session.execute(text("UPDATE creator_profile SET deleted = FALSE WHERE deleted IS NULL"))
        db.session.commit()
        _bsm_fix_order_item_creator_id_v460()
    except Exception as e:
        db.session.rollback()
        print("owner creator_profile.deleted repair warning:", e)



def _owner_order_sales_v427():
    try:
        rows = db.session.execute(db.text("""
            SELECT i.*, o.buyer_email, o.amount_total, o.created_at, v.location, v.filename, u.email AS creator_email
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            LEFT JOIN video v ON v.id = i.video_id
            LEFT JOIN "user" u ON u.id = i.creator_id
            ORDER BY o.created_at DESC
            LIMIT 100
        """)).mappings().all()
    except Exception:
        db.session.rollback()
        rows = []
    total = 0.0
    count = 0
    for r in rows:
        try:
            total += float(r.get("unit_price") or 0) * int(r.get("quantity") or 1)
            count += int(r.get("quantity") or 1)
        except Exception:
            pass
    return {"owner_recent_order_sales": rows, "owner_order_sales_total": total, "owner_order_sales_count": count}



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




def _owner_table_exists_v478(table_name):
    try:
        row = db.session.execute(db.text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema='public' AND table_name=:table_name
            ) AS exists
        """), {"table_name": table_name}).mappings().first()
        return bool(row and row.get("exists"))
    except Exception:
        db.session.rollback()
        return False

def _owner_col_exists_v478(table_name, col_name):
    try:
        row = db.session.execute(db.text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name=:table_name AND column_name=:col_name
            ) AS exists
        """), {"table_name": table_name, "col_name": col_name}).mappings().first()
        return bool(row and row.get("exists"))
    except Exception:
        db.session.rollback()
        return False

def _owner_scalar_v478(sql, params=None, default=0):
    try:
        row = db.session.execute(db.text(sql), params or {}).mappings().first()
        if not row:
            return default
        return list(row.values())[0]
    except Exception as e:
        db.session.rollback()
        try: print("owner metric SQL warning v47.8:", e, sql)
        except Exception: pass
        return default

def _owner_pick_table_v478(*names):
    for n in names:
        if _owner_table_exists_v478(n):
            return n
    return None

def _owner_dashboard_metrics_v477():
    metrics = {}

    apps_table = _owner_pick_table_v478("creator_application", "creator_applications")
    creators_table = _owner_pick_table_v478("creator_profile", "creators", "creator")
    buyers_table = _owner_pick_table_v478("buyer_user", "buyers", "buyer")
    orders_table = _owner_pick_table_v478("orders", "bsm_cart_order", "cart_order")
    items_table = _owner_pick_table_v478("order_items", "bsm_cart_order_item", "cart_order_item")
    video_table = _owner_pick_table_v478("video", "videos")

    # Applications
    metrics["applications_total"] = _owner_scalar_v478(f"SELECT COUNT(*) FROM {apps_table}") if apps_table else 0
    metrics["applications_pending"] = _owner_scalar_v478(f"SELECT COUNT(*) FROM {apps_table} WHERE COALESCE(status,'pending')='pending'") if apps_table and _owner_col_exists_v478(apps_table,"status") else 0
    metrics["applications_approved"] = _owner_scalar_v478(f"SELECT COUNT(*) FROM {apps_table} WHERE COALESCE(status,'')='approved'") if apps_table and _owner_col_exists_v478(apps_table,"status") else 0
    metrics["applications_rejected"] = _owner_scalar_v478(f"SELECT COUNT(*) FROM {apps_table} WHERE COALESCE(status,'')='rejected'") if apps_table and _owner_col_exists_v478(apps_table,"status") else 0

    # Creators
    metrics["creators_total"] = _owner_scalar_v478(f"SELECT COUNT(*) FROM {creators_table}") if creators_table else 0
    if creators_table and _owner_col_exists_v478(creators_table, "status"):
        metrics["creators_active"] = _owner_scalar_v478(f"SELECT COUNT(*) FROM {creators_table} WHERE COALESCE(status,'active')='active'")
    else:
        metrics["creators_active"] = metrics["creators_total"]

    if _owner_table_exists_v478("creator_subscription"):
        metrics["subscriptions_past_due"] = _owner_scalar_v478("SELECT COUNT(*) FROM creator_subscription WHERE status IN ('past_due','unpaid','canceled','inactive')")
    else:
        metrics["subscriptions_past_due"] = 0

    # Orders and sales
    metrics["orders_total"] = _owner_scalar_v478(f"SELECT COUNT(*) FROM {orders_table}") if orders_table else 0
    total_col = "total_amount" if orders_table and _owner_col_exists_v478(orders_table,"total_amount") else ("total" if orders_table and _owner_col_exists_v478(orders_table,"total") else None)
    status_col = "status" if orders_table and _owner_col_exists_v478(orders_table,"status") else None
    date_col = "created_at" if orders_table and _owner_col_exists_v478(orders_table,"created_at") else ("created" if orders_table and _owner_col_exists_v478(orders_table,"created") else None)
    if orders_table and total_col:
        paid_filter = f"WHERE COALESCE({status_col},'paid') IN ('paid','complete','completed')" if status_col else ""
        metrics["sales_total"] = _owner_scalar_v478(f"SELECT COALESCE(SUM(COALESCE({total_col},0)),0) FROM {orders_table} {paid_filter}")
        if date_col:
            today_filter = f"{paid_filter} AND DATE({date_col})=CURRENT_DATE" if paid_filter else f"WHERE DATE({date_col})=CURRENT_DATE"
            metrics["sales_today"] = _owner_scalar_v478(f"SELECT COALESCE(SUM(COALESCE({total_col},0)),0) FROM {orders_table} {today_filter}")
        else:
            metrics["sales_today"] = 0
    else:
        metrics["sales_total"] = 0
        metrics["sales_today"] = 0

    # Pending edits / discounts
    if items_table:
        package_col = "package" if _owner_col_exists_v478(items_table,"package") else ("purchase_type" if _owner_col_exists_v478(items_table,"purchase_type") else None)
        edited_key_col = "edited_r2_key" if _owner_col_exists_v478(items_table,"edited_r2_key") else None
        if package_col and edited_key_col:
            metrics["pending_edits"] = _owner_scalar_v478(f"""
                SELECT COUNT(*) FROM {items_table}
                WHERE {package_col} IN ('edited','edit','bundle','combo','original_plus_edited','original_edited','original+edited','original_edit')
                  AND ({edited_key_col} IS NULL OR {edited_key_col}='')
            """)
        elif edited_key_col:
            metrics["pending_edits"] = _owner_scalar_v478(f"SELECT COUNT(*) FROM {items_table} WHERE {edited_key_col} IS NULL")
        else:
            metrics["pending_edits"] = 0

        if _owner_col_exists_v478(items_table,"discount_status"):
            metrics["discount_approvals"] = _owner_scalar_v478(f"""
                SELECT COUNT(*) FROM {items_table}
                WHERE discount_status IN ('pending','pending_review','awaiting_creator','needs_approval')
            """)
        else:
            metrics["discount_approvals"] = 0
    else:
        metrics["pending_edits"] = 0
        metrics["discount_approvals"] = 0

    # Storage
    metrics["original_storage_bytes"] = 0
    if video_table:
        if _owner_col_exists_v478(video_table,"size_bytes"):
            metrics["original_storage_bytes"] = _owner_scalar_v478(f"SELECT COALESCE(SUM(COALESCE(size_bytes,0)),0) FROM {video_table} {_owner_storage_original_filter_v479(video_table)}")
        elif _owner_col_exists_v478(video_table,"file_size_bytes"):
            metrics["original_storage_bytes"] = _owner_scalar_v478(f"SELECT COALESCE(SUM(COALESCE(file_size_bytes,0)),0) FROM {video_table} {_owner_storage_original_filter_v479(video_table)}")

    metrics["edited_storage_bytes"] = 0
    if items_table and _owner_col_exists_v478(items_table,"edited_file_size_bytes"):
        metrics["edited_storage_bytes"] = _owner_scalar_v478(f"SELECT COALESCE(SUM(COALESCE(edited_file_size_bytes,0)),0) FROM {items_table}")

    metrics["storage_total_bytes"] = int(metrics["original_storage_bytes"] or 0) + int(metrics["edited_storage_bytes"] or 0)

    def gb(x):
        try: return round(float(x or 0)/1024/1024/1024,2)
        except Exception: return 0
    metrics["original_storage_gb"] = gb(metrics["original_storage_bytes"])
    metrics["edited_storage_gb"] = gb(metrics["edited_storage_bytes"])
    metrics["storage_total_gb"] = gb(metrics["storage_total_bytes"])

    metrics["creators_near_limit"] = 0
    if creators_table:
        try:
            cols = []
            for c in ["storage_used_bytes","used_storage_bytes","storage_bytes_used","storage_used"]:
                if _owner_col_exists_v478(creators_table,c): cols.append(c)
            lims = []
            for c in ["storage_limit_bytes","storage_quota_bytes","max_storage_bytes"]:
                if _owner_col_exists_v478(creators_table,c): lims.append(c)
            if cols and lims:
                used_expr = "COALESCE(" + ",".join(cols) + ",0)"
                lim_expr = "COALESCE(" + ",".join(lims) + ",0)"
                metrics["creators_near_limit"] = _owner_scalar_v478(f"SELECT COUNT(*) FROM {creators_table} WHERE {lim_expr}>0 AND {used_expr} >= ({lim_expr} * 0.85)")
        except Exception:
            db.session.rollback()
    return metrics



# v47.9 owner schema helpers
def _owner_columns_v479(table_name):
    try:
        rows = db.session.execute(db.text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=:t
        """), {"t": table_name}).mappings().all()
        return {r.get("column_name") for r in rows}
    except Exception:
        db.session.rollback()
        return set()

def _owner_select_expr_v479(cols, choices, default_sql="''"):
    for c in choices:
        if c in cols:
            return c
    return default_sql

def _owner_name_expr_v479(cols):
    if "name" in cols: return "COALESCE(name,'')"
    if "full_name" in cols: return "COALESCE(full_name,'')"
    if "brand" in cols: return "COALESCE(brand,'')"
    if "brand_name" in cols: return "COALESCE(brand_name,'')"
    if "company_name" in cols: return "COALESCE(company_name,'')"
    if "first_name" in cols and "last_name" in cols: return "TRIM(COALESCE(first_name,'') || ' ' || COALESCE(last_name,''))"
    if "first_name" in cols: return "COALESCE(first_name,'')"
    return "''"

def _owner_email_expr_v479(cols):
    return "email" if "email" in cols else "''"

def _owner_phone_expr_v479(cols):
    if "phone" in cols: return "COALESCE(phone,'')"
    if "phone_number" in cols: return "COALESCE(phone_number,'')"
    if "mobile" in cols: return "COALESCE(mobile,'')"
    return "''"

def _owner_status_expr_v479(cols):
    return "COALESCE(status,'active')" if "status" in cols else "'active'"

def _owner_social_expr_v479(cols):
    for c in ["instagram","instagram_handle","social_handle","tiktok","youtube"]:
        if c in cols:
            return f"COALESCE({c},'')"
    return "''"

def _owner_storage_original_filter_v479(video_table):
    cols = _owner_columns_v479(video_table)
    filters = []
    if "deleted_at" in cols:
        filters.append("deleted_at IS NULL")
    if "is_deleted" in cols:
        filters.append("COALESCE(is_deleted,false)=false")
    if "status" in cols:
        filters.append("COALESCE(status,'active') NOT IN ('deleted','cancelled','canceled','removed')")
    if "r2_key" in cols:
        filters.append("COALESCE(r2_key,'') <> ''")
    elif "r2_object_key" in cols:
        filters.append("COALESCE(r2_object_key,'') <> ''")
    elif "file_path" in cols:
        filters.append("COALESCE(file_path,'') <> ''")
    return ("WHERE " + " AND ".join(filters)) if filters else ""



# v48.1 robust owner display helpers + plan commission
def _owner_norm_value_v481(row, keys):
    for k in keys:
        try:
            v = row.get(k)
        except Exception:
            v = None
        if v not in [None, ""]:
            return v
    return ""

def _owner_normalize_rows_v481(rows, kind=""):
    out = []
    seen = set()
    for r in rows or []:
        d = dict(r)
        email = str(_owner_norm_value_v481(d, ["email", "display_email", "buyer_email", "user_email"]) or "").strip().lower()
        # skip duplicate emails but don't skip blank emails
        if email:
            if email in seen:
                continue
            seen.add(email)
        display_name = _owner_norm_value_v481(d, [
            "display_name", "name", "full_name", "brand", "brand_name", "company_name", "username",
            "first_name"
        ])
        if not display_name:
            first = d.get("first_name") or ""
            last = d.get("last_name") or ""
            display_name = (first + " " + last).strip()
        if not display_name:
            display_name = f"{kind.title()} #{d.get('id','')}".strip()

        d["display_name"] = display_name
        d["display_email"] = _owner_norm_value_v481(d, ["display_email", "email", "buyer_email", "user_email"])
        d["display_phone"] = _owner_norm_value_v481(d, ["display_phone", "phone", "phone_number", "mobile"])
        d["display_social"] = _owner_norm_value_v481(d, ["display_social", "instagram", "instagram_handle", "social_handle", "tiktok", "youtube"])
        d["display_brand"] = _owner_norm_value_v481(d, ["display_brand", "brand", "brand_name", "company_name"])
        d["display_status"] = _owner_norm_value_v481(d, ["display_status", "status", "account_status"]) or "active"
        out.append(d)
    return out

def _owner_ensure_plan_commission_v481():
    try:
        db.session.execute(db.text("ALTER TABLE creator_plan ADD COLUMN IF NOT EXISTS commission_percent NUMERIC DEFAULT 20"))
        db.session.commit()
    except Exception:
        db.session.rollback()



# v48.2 owner dynamic table helpers
def _owner_all_tables_v482():
    try:
        rows = db.session.execute(db.text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
            ORDER BY table_name
        """)).mappings().all()
        return [r.get("table_name") for r in rows]
    except Exception:
        db.session.rollback()
        return []

def _owner_table_columns_v482(table):
    try:
        rows = db.session.execute(db.text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=:t
            ORDER BY ordinal_position
        """), {"t": table}).mappings().all()
        return [r.get("column_name") for r in rows]
    except Exception:
        db.session.rollback()
        return []

def _owner_pick_existing_table_v482(candidates, contains=None):
    tables = set(_owner_all_tables_v482())
    for c in candidates:
        if c in tables:
            return c
    if contains:
        for t in tables:
            tl = t.lower()
            if all(x in tl for x in contains):
                return t
    return None

def _owner_dynamic_rows_v482(table, where="", params=None, limit=300):
    if not table:
        return [], []
    cols = _owner_table_columns_v482(table)
    try:
        sql = f"SELECT * FROM {table} {where} ORDER BY id DESC LIMIT {int(limit)}" if "id" in cols else f"SELECT * FROM {table} {where} LIMIT {int(limit)}"
        rows = db.session.execute(db.text(sql), params or {}).mappings().all()
        return [dict(r) for r in rows], cols
    except Exception as e:
        db.session.rollback()
        try: print("dynamic rows v48.2 warning:", e, table)
        except Exception: pass
        return [], cols

def _owner_display_from_row_v482(row):
    def first(keys):
        for k in keys:
            v = row.get(k)
            if v not in [None, ""]:
                return v
        return ""

    name = first([
        "display_name", "public_name", "name", "full_name", "brand", "brand_name",
        "company_name", "username"
    ])
    if not name:
        name = ((row.get("first_name") or "") + " " + (row.get("last_name") or "")).strip()
    if not name:
        name = "Record #" + str(row.get("id",""))

    status = first(["display_status", "status", "account_status"])
    if not status and "approved" in row:
        status = "approved" if row.get("approved") else "pending"
    if not status and "is_active" in row:
        status = "active" if row.get("is_active") else "inactive"
    if not status:
        status = "active"

    return {
        "id": row.get("id",""),
        "name": name,
        "email": first(["display_email", "email", "user_email", "buyer_email"]),
        "phone": first(["display_phone", "phone", "phone_number", "mobile"]),
        "brand": first(["display_brand", "company_name", "brand_name", "brand", "primary_location"]),
        "social": first(["display_social", "instagram", "instagram_handle", "social_handle", "social_link", "social_link_2", "tiktok", "youtube", "facebook"]),
        "status": status,
    }


def _owner_to_display_rows_v482(rows):
    out=[]
    seen=set()
    for row in rows or []:
        d=dict(row)
        disp=_owner_display_from_row_v482(d)
        email=str(disp.get("email") or "").strip().lower()
        if email and email in seen:
            continue
        if email:
            seen.add(email)
        d.update({
            "display_name": disp["name"],
            "display_email": disp["email"],
            "display_phone": disp["phone"],
            "display_brand": disp["brand"],
            "display_social": disp["social"],
            "display_status": disp["status"],
            "_display": disp,
        })
        out.append(d)
    return out

def _owner_buyer_table_and_where_v482():
    # Real DB table from Railway DB Debug: "user" with role column.
    if _owner_table_exists_v478("user"):
        cols = _owner_table_columns_v482("user")
        if "role" in cols:
            return "user", "WHERE LOWER(COALESCE(role,'')) IN ('buyer','customer')", {}
        return "user", "", {}
    if _owner_table_exists_v478("users"):
        cols = _owner_table_columns_v482("users")
        if "role" in cols:
            return "users", "WHERE LOWER(COALESCE(role,'')) IN ('buyer','customer')", {}
        return "users", "", {}
    table = _owner_pick_existing_table_v482(["buyer_user","buyer_users","buyers","buyer"])
    if table:
        return table, "", {}
    return None, "", {}


def _owner_creator_table_v482():
    # Real DB table from Railway DB Debug: "creators".
    if _owner_table_exists_v478("creators"):
        return "creators"
    return _owner_pick_existing_table_v482(["creator","creator_profile","creator_profiles"], contains=["creator"])


def _owner_application_table_v482():
    # Real DB table from Railway DB Debug: "creator_application".
    if _owner_table_exists_v478("creator_application"):
        return "creator_application"
    if _owner_table_exists_v478("creator_applications"):
        return "creator_applications"
    return _owner_pick_existing_table_v482(["applications"], contains=["application"])


def _owner_ensure_plan_commission_v482():
    try:
        db.session.execute(db.text("ALTER TABLE creator_plan ADD COLUMN IF NOT EXISTS commission_percent NUMERIC DEFAULT 20"))
        db.session.commit()
    except Exception:
        db.session.rollback()



def _owner_set_status_any_v483(table, row_id, status):
    cols = _owner_table_columns_v482(table)
    try:
        if "status" in cols:
            db.session.execute(db.text(f"UPDATE {table} SET status=:status WHERE id=:id"), {"status": status, "id": row_id})
        elif "approved" in cols:
            approved = status in ["active", "approved"]
            db.session.execute(db.text(f"UPDATE {table} SET approved=:approved WHERE id=:id"), {"approved": approved, "id": row_id})
        elif "is_active" in cols:
            is_active = status in ["active", "approved"]
            db.session.execute(db.text(f"UPDATE {table} SET is_active=:is_active WHERE id=:id"), {"is_active": is_active, "id": row_id})
        else:
            return False
        db.session.commit()
        return True
    except Exception:
        db.session.rollback()
        return False


@owner_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        return redirect("/owner/applications")
    return render_template("owner/login.html")







# v49.1H corrected creator approval logic.
# Correct architecture:
# - "user".id is the account id.
# - creator_profile.id is its own creator id/autoincrement sequence.
# - creator_profile.user_id points to "user".id.
# - free plan must come from storage_plan.id=5 when available.
def _v491h_default_free_storage_plan():
    plan = None
    try:
        plan = StoragePlan.query.get(5)
    except Exception:
        plan = None
    if not plan:
        try:
            plan = StoragePlan.query.filter(
                db.func.lower(StoragePlan.name).in_(["free", "demo", "starter"])
            ).order_by(StoragePlan.id.asc()).first()
        except Exception:
            plan = None
    if not plan:
        try:
            plan = StoragePlan.query.order_by(StoragePlan.id.asc()).first()
        except Exception:
            plan = None
    return plan

def _v491h_plan_storage_limit(plan):
    if not plan:
        return 5
    for attr in ["storage_limit_gb", "storage_gb", "limit_gb"]:
        if hasattr(plan, attr):
            val = getattr(plan, attr)
            if val is not None:
                try:
                    return float(val)
                except Exception:
                    pass
    return 5

def _v491h_plan_commission(plan):
    if not plan:
        return 15
    for attr in ["commission_rate", "commission_percent"]:
        if hasattr(plan, attr):
            val = getattr(plan, attr)
            if val is not None:
                try:
                    return float(val)
                except Exception:
                    pass
    return 15

def _v491h_create_or_update_creator_profile(user, app_row, selected_plan=None):
    selected_plan = selected_plan or _v491h_default_free_storage_plan()
    storage_limit = _v491h_plan_storage_limit(selected_plan)
    commission_rate = _v491h_plan_commission(selected_plan)

    profile = CreatorProfile.query.filter_by(user_id=user.id).first()
    if not profile:
        # IMPORTANT: do NOT set id manually. Let creator_profile sequence create the next id.
        profile = CreatorProfile(
            user_id=user.id,
            plan_id=selected_plan.id if selected_plan else None,
            storage_limit_gb=storage_limit,
            storage_used_bytes=0,
            commission_rate=commission_rate,
            product_commission_rate=20,
            approved=True,
            suspended=False
        )
        if hasattr(profile, "deleted"):
            profile.deleted = False
        if hasattr(profile, "instagram"):
            profile.instagram = (app_row.get("instagram") or "")
        db.session.add(profile)
        db.session.flush()
    else:
        profile.approved = True
        profile.suspended = False
        if hasattr(profile, "deleted"):
            profile.deleted = False
        if selected_plan:
            profile.plan_id = selected_plan.id
        profile.storage_limit_gb = storage_limit
        profile.commission_rate = commission_rate
        if hasattr(profile, "instagram") and app_row.get("instagram"):
            profile.instagram = app_row.get("instagram")
        db.session.flush()

    try:
        db.session.execute(text("""
            INSERT INTO creator_subscription (creator_id, plan_key, status, storage_limit_gb, created_at, updated_at)
            VALUES (:creator_id, 'free', 'active', :storage_limit_gb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT DO NOTHING
        """), {
            "creator_id": profile.id,
            "storage_limit_gb": storage_limit
        })
        db.session.execute(text("""
            UPDATE creator_subscription
            SET plan_key=COALESCE(NULLIF(plan_key,''), 'free'),
                status=COALESCE(NULLIF(status,''), 'active'),
                storage_limit_gb=:storage_limit_gb,
                updated_at=CURRENT_TIMESTAMP
            WHERE creator_id=:creator_id
        """), {
            "creator_id": profile.id,
            "storage_limit_gb": storage_limit
        })
    except Exception as e:
        db.session.rollback()
        try:
            print("creator_subscription v49.1H warning:", e)
        except Exception:
            pass

    return profile


@owner_bp.route("/applications/<int:app_id>/approve", methods=["POST"])
def approve_application(app_id):
    repair_all_known_tables()
    repair_creator_application_table()

    row = db.session.execute(text("SELECT * FROM creator_application WHERE id=:id LIMIT 1"), {"id": app_id}).mappings().first()
    if not row:
        return redirect("/owner/applications")

    selected_plan = _v491h_default_free_storage_plan()

    email = (row.get("email") or "").lower().strip()
    brand_name = row.get("brand_name") or row.get("company_name") or row.get("instagram") or "Boat Creator"

    if not email:
        flash("Application email is required.")
        return redirect("/owner/applications")

    user = User.query.filter_by(email=email).first()
    if not user:
        user = User(
            email=email,
            password_hash=generate_password_hash("TempCreator123!"),
            role="creator",
            display_name=brand_name,
            is_active=True
        )
        db.session.add(user)
        db.session.flush()
    else:
        user.role = "creator"
        user.is_active = True
        if not getattr(user, "display_name", None):
            user.display_name = brand_name

    if hasattr(user, "first_name") and row.get("first_name"):
        user.first_name = row.get("first_name")
    if hasattr(user, "last_name") and row.get("last_name"):
        user.last_name = row.get("last_name")
    if hasattr(user, "phone") and row.get("phone"):
        user.phone = row.get("phone")
    if hasattr(user, "primary_location") and brand_name:
        user.primary_location = brand_name
    if hasattr(user, "public_name") and brand_name:
        user.public_name = brand_name

    db.session.flush()
    creator = _v491h_create_or_update_creator_profile(user, row, selected_plan)

    db.session.execute(text("UPDATE creator_application SET status='approved', reviewed_at=CURRENT_TIMESTAMP WHERE id=:id"), {"id": app_id})
    db.session.commit()

    send_email(email, "BoatSpotMedia Creator Approved", "Your creator application was approved. Login at /creator/login. Temporary password: TempCreator123!")
    return redirect("/owner/applications")

@owner_bp.route("/applications/<int:app_id>/reject", methods=["POST"])
def reject_application(app_id):
    repair_creator_application_table()
    db.session.execute(text("UPDATE creator_application SET status='rejected', reviewed_at=CURRENT_TIMESTAMP WHERE id=:id"), {"id": app_id})
    db.session.commit()
    return redirect("/owner/applications")

@owner_bp.route("/plans/create", methods=["POST"])
def create_plan():
    db.session.add(StoragePlan(name=request.form.get("name"), storage_limit_gb=int(request.form.get("storage_limit_gb") or 512), monthly_price=request.form.get("monthly_price") or 0, commission_rate=int(request.form.get("commission_rate") or 20), active=True))
    db.session.commit(); return redirect("/owner/applications")

@owner_bp.route("/plans/<int:plan_id>/edit", methods=["POST"])
def edit_plan(plan_id):
    p=StoragePlan.query.get_or_404(plan_id)
    p.name=request.form.get("name") or p.name; p.storage_limit_gb=int(request.form.get("storage_limit_gb") or p.storage_limit_gb); p.monthly_price=request.form.get("monthly_price") or p.monthly_price; p.commission_rate=int(request.form.get("commission_rate") or p.commission_rate)
    db.session.commit(); return redirect("/owner/applications")

@owner_bp.route("/plans/<int:plan_id>/delete", methods=["POST"])
def delete_plan(plan_id):
    p=StoragePlan.query.get_or_404(plan_id); p.active=False; db.session.commit(); return redirect("/owner/applications")

@owner_bp.route("/creator/<int:creator_id>/override", methods=["POST"])
def override_commission(creator_id):
    c=CreatorProfile.query.get_or_404(creator_id)
    rate=int(request.form.get("rate") or 0); days=int(request.form.get("days") or 30); reason=request.form.get("reason") or ""; typ=request.form.get("commission_type") or "video"; expires=datetime.utcnow()+timedelta(days=days)
    if typ=="product":
        old=c.active_product_commission_rate(); c.product_commission_override_rate=rate; c.product_commission_override_until=expires; c.product_commission_override_reason=reason
    else:
        old=c.active_commission_rate(); c.commission_override_rate=rate; c.commission_override_until=expires; c.commission_override_reason=reason
    db.session.add(CommissionOverrideLog(creator_id=c.id, commission_type=typ, old_rate=old, new_rate=rate, days=days, reason=reason, expires_at=expires))
    db.session.commit(); return redirect("/owner/applications")

@owner_bp.route("/creator/<int:creator_id>/override/reset", methods=["POST"])
def reset_override(creator_id):
    c=CreatorProfile.query.get_or_404(creator_id); typ=request.form.get("commission_type") or "video"
    if typ=="product":
        c.product_commission_override_rate=None; c.product_commission_override_until=None; c.product_commission_override_reason=None
    else:
        c.commission_override_rate=None; c.commission_override_until=None; c.commission_override_reason=None
    db.session.commit(); return redirect("/owner/applications")

@owner_bp.route("/creators/<int:creator_id>/edit", methods=["POST"])
def edit_creator(creator_id):
    c=CreatorProfile.query.get_or_404(creator_id)
    if c.user:
        c.user.display_name=request.form.get("display_name") or c.user.display_name; c.user.email=request.form.get("email") or c.user.email
    c.storage_limit_gb=int(request.form.get("storage_limit_gb") or c.storage_limit_gb); c.commission_rate=int(request.form.get("commission_rate") or c.commission_rate); c.product_commission_rate=int(request.form.get("product_commission_rate") or c.product_commission_rate or 20)
    db.session.commit(); return redirect("/owner/applications")

@owner_bp.route("/creators/<int:creator_id>/suspend", methods=["POST"])
def suspend_creator(creator_id):
    c=CreatorProfile.query.get_or_404(creator_id)
    c.suspended=True; c.approved=False
    if c.user: c.user.is_active=False
    for v in Video.query.filter_by(creator_id=c.id).all():
        if v.status != "deleted": v.status="suspended"
    for p in Product.query.filter_by(creator_id=c.id).all():
        p.active=False
    db.session.commit(); return redirect("/owner/applications")

@owner_bp.route("/creators/<int:creator_id>/activate", methods=["POST"])
def activate_creator(creator_id):
    c=CreatorProfile.query.get_or_404(creator_id)
    c.suspended=False; c.approved=True
    if c.user: c.user.is_active=True; c.user.role="creator"
    for v in Video.query.filter_by(creator_id=c.id, status="suspended").all():
        v.status="active"
    db.session.commit(); return redirect("/owner/applications")




@owner_bp.route("/creator-passwords")
def creator_passwords():
    _ensure_creator_profile_deleted_column()
    creators = CreatorProfile.query.filter(
        (CreatorProfile.deleted == False) | (CreatorProfile.deleted.is_(None))
    ).order_by(CreatorProfile.id.desc()).all()
    return render_template("owner/creator_passwords.html", creators=creators)


@owner_bp.route("/creators/<int:creator_id>/reset-password", methods=["GET", "POST"])
def reset_creator_password_page(creator_id):
    _ensure_creator_profile_deleted_column()
    c = CreatorProfile.query.get_or_404(creator_id)

    if request.method == "POST":
        new_password = (request.form.get("new_password") or "").strip()
        confirm_password = (request.form.get("confirm_password") or "").strip()

        if len(new_password) < 6:
            flash("Password must be at least 6 characters.", "error")
            return render_template("owner/reset_creator_password.html", creator=c)

        if new_password != confirm_password:
            flash("Passwords do not match.", "error")
            return render_template("owner/reset_creator_password.html", creator=c)

        if not c.user:
            flash("Creator user account not found.", "error")
            return redirect("/owner/applications")

        if hasattr(c.user, "password_hash"):
            c.user.password_hash = generate_password_hash(new_password)
        elif hasattr(c.user, "password"):
            c.user.password = generate_password_hash(new_password)

        c.approved = True
        c.suspended = False
        try:
            c.deleted = False
        except Exception:
            pass

        if hasattr(c.user, "is_active"):
            c.user.is_active = True

        db.session.commit()
        flash("Creator password updated successfully.", "success")
        return redirect("/owner/applications")

    return render_template("owner/reset_creator_password.html", creator=c)


@owner_bp.route("/creators/<int:creator_id>/password", methods=["POST"])
def reset_creator_password(creator_id):
    _ensure_creator_profile_deleted_column()
    c = CreatorProfile.query.get_or_404(creator_id)
    new_password = (request.form.get("new_password") or request.form.get("password") or "").strip()

    if len(new_password) < 6:
        flash("Password must be at least 6 characters.", "error")
        return redirect("/owner/applications")

    if not c.user:
        flash("Creator user account not found.", "error")
        return redirect("/owner/applications")

    if hasattr(c.user, "password_hash"):
        c.user.password_hash = generate_password_hash(new_password)
    elif hasattr(c.user, "password"):
        c.user.password = generate_password_hash(new_password)

    try:
        c.deleted = False
    except Exception:
        pass
    c.suspended = False
    c.approved = True
    if hasattr(c.user, "is_active"):
        c.user.is_active = True

    db.session.commit()
    flash("Creator password updated successfully.", "success")
    return redirect("/owner/applications")


@owner_bp.route("/creators/<int:creator_id>/delete", methods=["POST"])
def delete_creator(creator_id):
    _ensure_creator_profile_deleted_column()
    repair_all_known_tables()
    c = CreatorProfile.query.get_or_404(creator_id)
    user_email = c.user.email if c.user else None
    user_id = c.user_id

    # Remove public/business records first so the creator disappears everywhere.
    try:
        Video.query.filter_by(creator_id=c.id).delete(synchronize_session=False)
    except Exception:
        db.session.rollback()
    try:
        Product.query.filter_by(creator_id=c.id).delete(synchronize_session=False)
    except Exception:
        db.session.rollback()
    try:
        db.session.execute(text("DELETE FROM video_batch WHERE creator_id=:cid"), {"cid": c.id})
        db.session.execute(text("DELETE FROM commission_override_log WHERE creator_id=:cid"), {"cid": c.id})
        if user_email:
            db.session.execute(text("UPDATE creator_application SET status='deleted', reviewed_at=CURRENT_TIMESTAMP WHERE lower(email)=lower(:email)"), {"email": user_email})
        c.approved = False
        c.suspended = True
        c.deleted = True
        if c.user:
            c.user.is_active = False
            c.user.role = "deleted_creator"
            if hasattr(c.user, "password_hash"):
                c.user.password_hash = "DELETED_CREATOR_LOGIN_DISABLED"
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        print("Delete creator warning:", e)

    return redirect("/owner/applications")

@owner_bp.route("/repair-db-now")
def repair_db_now():
    repair_all_known_tables(); repair_creator_application_table(); return "DB repair completed."

@owner_bp.route("/applications-raw")
def applications_raw():
    repair_creator_application_table()
    rows=db.session.execute(text("SELECT * FROM creator_application ORDER BY id DESC LIMIT 100")).mappings().all()
    return {"applications":[dict(r) for r in rows]}



# v47.3 Owner editable creator subscription plans
def _owner_ensure_creator_plan_table_v473():
    try:
        db.session.execute(db.text("""
            CREATE TABLE IF NOT EXISTS creator_plan (
                id SERIAL PRIMARY KEY,
                plan_key TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                price_label TEXT NOT NULL DEFAULT '$0/mo',
                description TEXT,
                storage_gb INTEGER NOT NULL DEFAULT 5,
                stripe_price_id TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                sort_order INTEGER DEFAULT 0,
                commission_percent NUMERIC DEFAULT 20,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.commit()
    except Exception:
        db.session.rollback()






# ============================================================
# v48.8 Owner Dashboard: REAL creator flow
# Confirmed production schema:
# - "user" stores login/account.
# - creator_profile.id is real creator_id for video/R2/storage.
# - creator_profile.user_id joins to "user".id.
# - creator_subscription.creator_id joins to creator_profile.id.
# - creator_application stores applications.
# - DO NOT use empty/old creators table.
# ============================================================

def _owner_scalar_v488(sql, params=None, default=0):
    try:
        row = db.session.execute(db.text(sql), params or {}).mappings().first()
        if not row:
            return default
        return list(row.values())[0]
    except Exception as e:
        db.session.rollback()
        try:
            print("owner scalar v48.8 warning:", e, sql)
        except Exception:
            pass
        return default

def _owner_rows_v488(sql, params=None):
    try:
        return [dict(r) for r in db.session.execute(db.text(sql), params or {}).mappings().all()]
    except Exception as e:
        db.session.rollback()
        try:
            print("owner rows v48.8 warning:", e, sql)
        except Exception:
            pass
        return []

def _owner_exec_v488(sql, params=None):
    try:
        db.session.execute(db.text(sql), params or {})
        db.session.commit()
        return True
    except Exception as e:
        db.session.rollback()
        try:
            print("owner exec v48.8 warning:", e, sql)
        except Exception:
            pass
        return False

def _owner_table_exists_v488(table_name):
    return bool(_owner_scalar_v488("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='public' AND table_name=:t
        )
    """, {"t": table_name}, False))

def _owner_col_exists_v488(table_name, col_name):
    return bool(_owner_scalar_v488("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name=:t AND column_name=:c
        )
    """, {"t": table_name, "c": col_name}, False))

def _owner_bootstrap_v488():
    # Safe columns only. No new duplicate tables.
    _owner_exec_v488('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS phone TEXT')
    _owner_exec_v488("ALTER TABLE creator_application ADD COLUMN IF NOT EXISTS phone TEXT")
    _owner_exec_v488("ALTER TABLE creator_profile ADD COLUMN IF NOT EXISTS instagram TEXT")
    _owner_exec_v488("ALTER TABLE creator_profile ADD COLUMN IF NOT EXISTS commission_rate NUMERIC DEFAULT 15")
    _owner_exec_v488("ALTER TABLE creator_subscription ADD COLUMN IF NOT EXISTS storage_limit_gb NUMERIC DEFAULT 5")
    _owner_exec_v488("ALTER TABLE creator_subscription ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP")

def _owner_default_creator_plan_v488():
    # Use existing creator_plan/storage_plan if available, but do not require it.
    plan_id = None
    plan_key = "free"
    storage_limit_gb = 5
    commission_rate = 15

    if _owner_table_exists_v488("creator_plan"):
        row = db.session.execute(db.text("""
            SELECT id, plan_key, storage_gb, commission_percent
            FROM creator_plan
            WHERE COALESCE(is_active,true)=true
            ORDER BY CASE WHEN plan_key='free' THEN 0 ELSE 1 END, sort_order ASC, id ASC
            LIMIT 1
        """)).mappings().first()
        if row:
            plan_id = row.get("id")
            plan_key = row.get("plan_key") or "free"
            storage_limit_gb = row.get("storage_gb") or storage_limit_gb
            commission_rate = row.get("commission_percent") or commission_rate
    elif _owner_table_exists_v488("storage_plan"):
        row = db.session.execute(db.text("""
            SELECT id, name, storage_limit_gb, commission_rate
            FROM storage_plan
            WHERE COALESCE(active,true)=true
            ORDER BY CASE WHEN LOWER(name)='free' THEN 0 ELSE 1 END, id ASC
            LIMIT 1
        """)).mappings().first()
        if row:
            plan_id = row.get("id")
            plan_key = (row.get("name") or "free").lower()
            storage_limit_gb = row.get("storage_limit_gb") or storage_limit_gb
            commission_rate = row.get("commission_rate") or commission_rate

    return {
        "plan_id": plan_id,
        "plan_key": plan_key,
        "storage_limit_gb": float(storage_limit_gb or 5),
        "commission_rate": float(commission_rate or 15),
    }

def _owner_dashboard_metrics_v477():
    _owner_bootstrap_v488()
    metrics = {}
    metrics["applications_total"] = _owner_scalar_v488("SELECT COUNT(*) FROM creator_application")
    metrics["applications_pending"] = _owner_scalar_v488("SELECT COUNT(*) FROM creator_application WHERE COALESCE(status,'pending')='pending'")
    metrics["applications_approved"] = _owner_scalar_v488("SELECT COUNT(*) FROM creator_application WHERE COALESCE(status,'')='approved'")
    metrics["applications_rejected"] = _owner_scalar_v488("SELECT COUNT(*) FROM creator_application WHERE COALESCE(status,'') IN ('rejected','denied')")

    metrics["creators_total"] = _owner_scalar_v488("SELECT COUNT(*) FROM creator_profile")
    metrics["creators_active"] = _owner_scalar_v488("""
        SELECT COUNT(*)
        FROM creator_profile cp
        LEFT JOIN "user" u ON u.id = cp.user_id
        WHERE COALESCE(u.is_active,true)=true
    """)
    metrics["subscriptions_past_due"] = _owner_scalar_v488("""
        SELECT COUNT(*)
        FROM creator_subscription
        WHERE COALESCE(status,'active') NOT IN ('active','trialing')
    """)
    metrics["orders_total"] = _owner_scalar_v488("SELECT COUNT(*) FROM bsm_cart_order")
    metrics["sales_total"] = _owner_scalar_v488("""
        SELECT COALESCE(SUM(COALESCE(amount_total,0)),0)
        FROM bsm_cart_order
        WHERE COALESCE(status,'') IN ('paid','complete','completed')
    """)
    metrics["sales_today"] = _owner_scalar_v488("""
        SELECT COALESCE(SUM(COALESCE(amount_total,0)),0)
        FROM bsm_cart_order
        WHERE COALESCE(status,'') IN ('paid','complete','completed')
          AND DATE(created_at)=CURRENT_DATE
    """)
    metrics["pending_edits"] = _owner_scalar_v488("""
        SELECT COUNT(*)
        FROM bsm_cart_order_item
        WHERE package IN ('edited','edit','bundle','combo','original_plus_edited','original_edited','original+edited','original_edit')
          AND (edited_r2_key IS NULL OR edited_r2_key='')
    """)
    metrics["discount_approvals"] = _owner_scalar_v488("""
        SELECT COUNT(*)
        FROM bsm_cart_order_item
        WHERE COALESCE(discount_status,'') IN ('pending','pending_review','awaiting_creator','needs_approval')
    """)

    metrics["original_storage_bytes"] = _owner_scalar_v488("""
        SELECT COALESCE(SUM(COALESCE(file_size_bytes,0)),0)
        FROM video
        WHERE COALESCE(status,'active') NOT IN ('deleted','cancelled','canceled','removed')
          AND COALESCE(r2_video_key,'') <> ''
    """)
    metrics["edited_storage_bytes"] = _owner_scalar_v488("""
        SELECT COALESCE(SUM(COALESCE(edited_file_size_bytes,0)),0)
        FROM bsm_cart_order_item
        WHERE COALESCE(edited_r2_key,'') <> ''
    """)
    metrics["storage_total_bytes"] = int(metrics["original_storage_bytes"] or 0) + int(metrics["edited_storage_bytes"] or 0)

    def gb(x):
        try:
            return round(float(x or 0) / 1024 / 1024 / 1024, 2)
        except Exception:
            return 0

    metrics["original_storage_gb"] = gb(metrics["original_storage_bytes"])
    metrics["edited_storage_gb"] = gb(metrics["edited_storage_bytes"])
    metrics["storage_total_gb"] = gb(metrics["storage_total_bytes"])
    metrics["creators_near_limit"] = _owner_scalar_v488("""
        SELECT COUNT(*)
        FROM creator_profile cp
        LEFT JOIN creator_subscription cs ON cs.creator_id=cp.id
        WHERE COALESCE(cs.storage_limit_gb, cp.storage_limit_gb, 0) > 0
          AND (COALESCE(cp.storage_used_bytes,0)::numeric / 1024 / 1024 / 1024) >= (COALESCE(cs.storage_limit_gb, cp.storage_limit_gb, 0)::numeric * 0.85)
    """)
    return metrics

def _owner_current_applications_v488():
    _owner_bootstrap_v488()
    return _owner_rows_v488("""
        SELECT id,
               TRIM(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')) AS display_name,
               COALESCE(brand_name,'') AS display_brand,
               COALESCE(email,'') AS display_email,
               COALESCE(phone,'') AS display_phone,
               COALESCE(instagram,'') AS display_social,
               COALESCE(status,'pending') AS display_status,
               submitted_at
        FROM creator_application
        ORDER BY id DESC
        LIMIT 300
    """)

def _owner_current_creators_v488():
    _owner_bootstrap_v488()
    return _owner_rows_v488("""
        SELECT cp.id AS id,
               cp.id AS creator_id,
               cp.user_id AS user_id,
               COALESCE(u.display_name, u.public_name, TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')), u.email, 'Creator #' || cp.id::text) AS display_name,
               COALESCE(u.primary_location,'') AS display_brand,
               COALESCE(u.email,'') AS display_email,
               COALESCE(u.phone,'') AS display_phone,
               COALESCE(cp.instagram, u.social_link, u.social_link_2, '') AS display_social,
               CASE WHEN COALESCE(u.is_active,true)=true THEN 'active' ELSE 'inactive' END AS display_status,
               COALESCE(cs.plan_key, cp.plan_id::text, '') AS plan_key,
               COALESCE(cs.status,'') AS subscription_status,
               COALESCE(cs.storage_limit_gb, cp.storage_limit_gb, 0) AS storage_limit_gb,
               COALESCE(cp.storage_used_bytes,0) AS storage_used_bytes,
               COALESCE(cp.commission_rate, 0) AS commission_rate,
               cp.created_at
        FROM creator_profile cp
        LEFT JOIN "user" u ON u.id = cp.user_id
        LEFT JOIN creator_subscription cs ON cs.creator_id = cp.id
        ORDER BY cp.id DESC
        LIMIT 300
    """)

def _owner_current_buyers_v488():
    _owner_bootstrap_v488()
    return _owner_rows_v488("""
        SELECT id,
               COALESCE(display_name, public_name, TRIM(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')), email, 'Buyer #' || id::text) AS display_name,
               COALESCE(email,'') AS display_email,
               COALESCE(phone,'') AS display_phone,
               CASE WHEN COALESCE(is_active,true)=true THEN 'active' ELSE 'inactive' END AS display_status,
               created_at
        FROM "user"
        WHERE LOWER(COALESCE(role,''))='buyer'
        ORDER BY id DESC
        LIMIT 300
    """)

def _owner_get_application_v488(row_id):
    rows = _owner_rows_v488("""
        SELECT id, first_name, last_name, brand_name, email, phone, instagram, status
        FROM creator_application
        WHERE id=:id
        LIMIT 1
    """, {"id": row_id})
    return rows[0] if rows else None

def _owner_get_creator_v488(creator_id):
    rows = _owner_rows_v488("""
        SELECT cp.id AS id,
               cp.user_id,
               u.email,
               u.first_name,
               u.last_name,
               u.public_name,
               u.display_name,
               u.primary_location,
               u.phone,
               u.is_active,
               cp.instagram,
               COALESCE(cs.storage_limit_gb, cp.storage_limit_gb, 0) AS storage_limit_gb,
               cp.storage_used_bytes,
               cp.commission_rate,
               COALESCE(cs.plan_key,'') AS plan_key
        FROM creator_profile cp
        LEFT JOIN "user" u ON u.id = cp.user_id
        LEFT JOIN creator_subscription cs ON cs.creator_id=cp.id
        WHERE cp.id=:id
        LIMIT 1
    """, {"id": creator_id})
    return rows[0] if rows else None

def _owner_get_buyer_v488(row_id):
    rows = _owner_rows_v488("""
        SELECT id, email, first_name, last_name, display_name, phone, is_active
        FROM "user"
        WHERE id=:id AND LOWER(COALESCE(role,''))='buyer'
        LIMIT 1
    """, {"id": row_id})
    return rows[0] if rows else None

def _owner_find_or_create_creator_from_application_v488(app):
    from werkzeug.security import generate_password_hash

    plan = _owner_default_creator_plan_v488()
    email = (app.get("email") or "").strip().lower()
    first_name = app.get("first_name") or ""
    last_name = app.get("last_name") or ""
    brand_name = app.get("brand_name") or ""
    phone = app.get("phone") or ""
    instagram = app.get("instagram") or ""
    display_name = (brand_name or (first_name + " " + last_name).strip() or email).strip()
    temp_password = "BoatSpot123!"

    user_row = db.session.execute(db.text("""
        SELECT id, role
        FROM "user"
        WHERE LOWER(email)=:email
        LIMIT 1
    """), {"email": email}).mappings().first()

    if user_row:
        user_id = user_row["id"]
        current_role = (user_row.get("role") or "").lower()
        # Support buyer -> creator upgrade without duplicating account.
        new_role = "creator" if current_role in ["", "buyer", "customer", "creator"] else current_role
        db.session.execute(db.text("""
            UPDATE "user"
            SET role=:role,
                first_name=:first_name,
                last_name=:last_name,
                display_name=:display_name,
                public_name=:display_name,
                phone=:phone,
                primary_location=:primary_location,
                is_active=true
            WHERE id=:id
        """), {
            "id": user_id,
            "role": new_role,
            "first_name": first_name,
            "last_name": last_name,
            "display_name": display_name,
            "phone": phone,
            "primary_location": brand_name,
        })
    else:
        user_id = db.session.execute(db.text("""
            INSERT INTO "user"
            (role, email, password_hash, first_name, last_name, display_name, public_name, phone, primary_location, is_active, created_at)
            VALUES ('creator', :email, :password_hash, :first_name, :last_name, :display_name, :display_name, :phone, :primary_location, true, CURRENT_TIMESTAMP)
            RETURNING id
        """), {
            "email": email,
            "password_hash": generate_password_hash(temp_password),
            "first_name": first_name,
            "last_name": last_name,
            "display_name": display_name,
            "phone": phone,
            "primary_location": brand_name,
        }).scalar()

    profile = db.session.execute(db.text("""
        SELECT id
        FROM creator_profile
        WHERE user_id=:user_id
        LIMIT 1
    """), {"user_id": user_id}).mappings().first()

    if profile:
        creator_id = profile["id"]
        db.session.execute(db.text("""
            UPDATE creator_profile
            SET instagram=:instagram,
                commission_rate=COALESCE(commission_rate, :commission_rate)
            WHERE id=:creator_id
        """), {
            "creator_id": creator_id,
            "instagram": instagram,
            "commission_rate": plan["commission_rate"],
        })
    else:
        creator_id = db.session.execute(db.text("""
            INSERT INTO creator_profile
            (user_id, plan_id, storage_limit_gb, storage_used_bytes, commission_rate, instagram, created_at)
            VALUES (:user_id, :plan_id, :storage_limit_gb, 0, :commission_rate, :instagram, CURRENT_TIMESTAMP)
            RETURNING id
        """), {
            "user_id": user_id,
            "plan_id": plan["plan_id"],
            "storage_limit_gb": plan["storage_limit_gb"],
            "commission_rate": plan["commission_rate"],
            "instagram": instagram,
        }).scalar()

    sub = db.session.execute(db.text("""
        SELECT id
        FROM creator_subscription
        WHERE creator_id=:creator_id
        LIMIT 1
    """), {"creator_id": creator_id}).mappings().first()

    if sub:
        db.session.execute(db.text("""
            UPDATE creator_subscription
            SET plan_key=COALESCE(NULLIF(plan_key,''), :plan_key),
                status=COALESCE(NULLIF(status,''), 'active'),
                storage_limit_gb=COALESCE(storage_limit_gb, :storage_limit_gb),
                updated_at=CURRENT_TIMESTAMP
            WHERE creator_id=:creator_id
        """), {
            "creator_id": creator_id,
            "plan_key": plan["plan_key"],
            "storage_limit_gb": plan["storage_limit_gb"],
        })
    else:
        db.session.execute(db.text("""
            INSERT INTO creator_subscription
            (creator_id, plan_key, status, storage_limit_gb, created_at, updated_at)
            VALUES (:creator_id, :plan_key, 'active', :storage_limit_gb, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """), {
            "creator_id": creator_id,
            "plan_key": plan["plan_key"],
            "storage_limit_gb": plan["storage_limit_gb"],
        })

    return creator_id, temp_password

@owner_bp.route("/panel")
def owner_panel_v477():
    return render_template("owner/panel.html", metrics=_owner_dashboard_metrics_v477(), q=(request.args.get("q") or "").strip())

@owner_bp.route("/applications")
def owner_applications_v479():
    return render_template("owner/applications.html", applications=_owner_current_applications_v488(), table_name="creator_application", columns=[])

@owner_bp.route("/creators", endpoint="owner_creators_v478")
def owner_creators_v478():
    return render_template("owner/manage_people.html", rows=_owner_current_creators_v488(), columns=[], table_name='creator_profile + user + creator_subscription', kind="creator", title="Creators")

@owner_bp.route("/buyers", endpoint="owner_buyers_v478")
def owner_buyers_v478():
    return render_template("owner/manage_people.html", rows=_owner_current_buyers_v488(), columns=[], table_name='user(role=buyer)', kind="buyer", title="Buyers")

@owner_bp.route("/applications/<int:row_id>/approve", methods=["POST"])
def owner_approve_application_v488(row_id):
    _owner_bootstrap_v488()
    app = _owner_get_application_v488(row_id)
    if not app:
        flash("Application not found.")
        return redirect("/owner/applications")

    if not (app.get("email") or "").strip():
        flash("Application needs an email before approval.")
        return redirect("/owner/applications")

    try:
        creator_id, temp_password = _owner_find_or_create_creator_from_application_v488(app)
        db.session.execute(db.text("""
            UPDATE creator_application
            SET status='approved'
            WHERE id=:id
        """), {"id": row_id})
        if _owner_col_exists_v488("creator_application", "reviewed_at"):
            db.session.execute(db.text("""
                UPDATE creator_application
                SET reviewed_at=CURRENT_TIMESTAMP
                WHERE id=:id
            """), {"id": row_id})
        db.session.commit()
        flash(f"Application approved. Creator ID #{creator_id} is ready. Temporary password: {temp_password}")
    except Exception as e:
        db.session.rollback()
        try:
            print("approve application v48.8 warning:", e)
        except Exception:
            pass
        flash("Could not approve application.")
    return redirect("/owner/applications")

@owner_bp.route("/application/<int:row_id>/status/<status>", methods=["POST"])
def owner_application_status_v488(row_id, status):
    if status not in ["pending", "approved", "suspended", "rejected"]:
        status = "pending"
    ok = _owner_exec_v488("UPDATE creator_application SET status=:status WHERE id=:id", {"id": row_id, "status": status})
    flash("Application status updated." if ok else "Could not update application.")
    return redirect("/owner/applications")

@owner_bp.route("/application/<int:row_id>/delete", methods=["POST"])
def owner_application_delete_v488(row_id):
    ok = _owner_exec_v488("DELETE FROM creator_application WHERE id=:id", {"id": row_id})
    flash("Application deleted." if ok else "Could not delete application.")
    return redirect("/owner/applications")

@owner_bp.route("/application/<int:row_id>/edit", methods=["GET","POST"])
def owner_edit_application_v488(row_id):
    row = _owner_get_application_v488(row_id)
    if not row:
        flash("Application not found.")
        return redirect("/owner/applications")
    if request.method == "POST":
        ok = _owner_exec_v488("""
            UPDATE creator_application
            SET first_name=:first_name,
                last_name=:last_name,
                brand_name=:brand_name,
                email=:email,
                phone=:phone,
                instagram=:instagram,
                status=:status
            WHERE id=:id
        """, {
            "id": row_id,
            "first_name": request.form.get("first_name") or "",
            "last_name": request.form.get("last_name") or "",
            "brand_name": request.form.get("brand_name") or "",
            "email": request.form.get("email") or "",
            "phone": request.form.get("phone") or "",
            "instagram": request.form.get("instagram") or "",
            "status": request.form.get("status") or "pending",
        })
        flash("Application saved." if ok else "Could not save application.")
        return redirect("/owner/applications")
    return render_template("owner/edit_person.html", row=row, kind="application", title="Edit Application")

@owner_bp.route("/creator/<int:creator_id>/edit", methods=["GET","POST"])
def owner_edit_creator_v488(creator_id):
    row = _owner_get_creator_v488(creator_id)
    if not row:
        flash("Creator not found.")
        return redirect("/owner/creators")
    if request.method == "POST":
        try:
            db.session.execute(db.text("""
                UPDATE "user"
                SET email=:email,
                    first_name=:first_name,
                    last_name=:last_name,
                    display_name=:display_name,
                    public_name=:display_name,
                    phone=:phone,
                    primary_location=:primary_location,
                    is_active=:is_active
                WHERE id=:user_id
            """), {
                "user_id": row.get("user_id"),
                "email": request.form.get("email") or "",
                "first_name": request.form.get("first_name") or "",
                "last_name": request.form.get("last_name") or "",
                "display_name": request.form.get("display_name") or "",
                "phone": request.form.get("phone") or "",
                "primary_location": request.form.get("primary_location") or "",
                "is_active": True if request.form.get("is_active") else False,
            })
            db.session.execute(db.text("""
                UPDATE creator_profile
                SET instagram=:instagram,
                    commission_rate=:commission_rate
                WHERE id=:creator_id
            """), {
                "creator_id": creator_id,
                "instagram": request.form.get("instagram") or "",
                "commission_rate": float(request.form.get("commission_rate") or 0),
            })
            db.session.execute(db.text("""
                UPDATE creator_subscription
                SET storage_limit_gb=:storage_limit_gb,
                    updated_at=CURRENT_TIMESTAMP
                WHERE creator_id=:creator_id
            """), {
                "creator_id": creator_id,
                "storage_limit_gb": float(request.form.get("storage_limit_gb") or 0),
            })
            db.session.commit()
            flash("Creator saved.")
        except Exception as e:
            db.session.rollback()
            try:
                print("owner edit creator v48.8 warning:", e)
            except Exception:
                pass
            flash("Could not save creator.")
        return redirect("/owner/creators")
    return render_template("owner/edit_person.html", row=row, kind="creator_profile", title="Edit Creator")

@owner_bp.route("/creator/<int:creator_id>/status/<status>", methods=["POST"])
def owner_creator_status_v488(creator_id, status):
    row = _owner_get_creator_v488(creator_id)
    if not row:
        flash("Creator not found.")
        return redirect("/owner/creators")
    ok = _owner_exec_v488('UPDATE "user" SET is_active=:active WHERE id=:user_id', {
        "active": status in ["active", "approved"],
        "user_id": row.get("user_id"),
    })
    flash("Creator status updated." if ok else "Could not update creator.")
    return redirect("/owner/creators")

@owner_bp.route("/creator/<int:creator_id>/reset-password", methods=["POST"])
def owner_creator_reset_password_v488(creator_id):
    row = _owner_get_creator_v488(creator_id)
    if not row:
        flash("Creator not found.")
        return redirect("/owner/creators")
    new_password = (request.form.get("new_password") or "").strip()
    if not new_password:
        flash("Enter a new password.")
        return redirect("/owner/creators")
    from werkzeug.security import generate_password_hash
    ok = _owner_exec_v488('UPDATE "user" SET password_hash=:p WHERE id=:user_id', {
        "p": generate_password_hash(new_password),
        "user_id": row.get("user_id"),
    })
    flash("Creator password reset successfully." if ok else "Could not reset creator password.")
    return redirect("/owner/creators")

@owner_bp.route("/buyer/<int:row_id>/edit", methods=["GET","POST"])
def owner_edit_buyer_v488(row_id):
    row = _owner_get_buyer_v488(row_id)
    if not row:
        flash("Buyer not found.")
        return redirect("/owner/buyers")
    if request.method == "POST":
        ok = _owner_exec_v488("""
            UPDATE "user"
            SET email=:email,
                first_name=:first_name,
                last_name=:last_name,
                display_name=:display_name,
                phone=:phone,
                is_active=:is_active
            WHERE id=:id
        """, {
            "id": row_id,
            "email": request.form.get("email") or "",
            "first_name": request.form.get("first_name") or "",
            "last_name": request.form.get("last_name") or "",
            "display_name": request.form.get("display_name") or "",
            "phone": request.form.get("phone") or "",
            "is_active": True if request.form.get("is_active") else False,
        })
        flash("Buyer saved." if ok else "Could not save buyer.")
        return redirect("/owner/buyers")
    return render_template("owner/edit_person.html", row=row, kind="buyer", title="Edit Buyer")

@owner_bp.route("/buyer/<int:row_id>/status/<status>", methods=["POST"])
def owner_buyer_status_v488(row_id, status):
    ok = _owner_exec_v488('UPDATE "user" SET is_active=:active WHERE id=:id', {
        "active": status in ["active", "approved"],
        "id": row_id,
    })
    flash("Buyer status updated." if ok else "Could not update buyer.")
    return redirect("/owner/buyers")

@owner_bp.route("/buyer/<int:row_id>/reset-password", methods=["POST"])
def owner_buyer_reset_password_v488(row_id):
    new_password = (request.form.get("new_password") or "").strip()
    if not new_password:
        flash("Enter a new password.")
        return redirect("/owner/buyers")
    from werkzeug.security import generate_password_hash
    ok = _owner_exec_v488('UPDATE "user" SET password_hash=:p WHERE id=:id', {
        "p": generate_password_hash(new_password),
        "id": row_id,
    })
    flash("Buyer password reset successfully." if ok else "Could not reset buyer password.")
    return redirect("/owner/buyers")

@owner_bp.route("/db-debug")
def owner_db_debug_v488():
    try:
        tables = []
        rows = db.session.execute(db.text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
            ORDER BY table_name
        """)).mappings().all()
        for r in rows:
            t = r.get("table_name")
            if any(x in t.lower() for x in ["creator","buyer","user","order","video","plan","application"]):
                cols = db.session.execute(db.text("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=:t
                    ORDER BY ordinal_position
                """), {"t": t}).mappings().all()
                tables.append({"name": t, "columns": [c.get("column_name") for c in cols]})
        return render_template("owner/db_debug.html", tables=tables)
    except Exception:
        db.session.rollback()
        return render_template("owner/db_debug.html", tables=[])



@owner_bp.route("/applications-endpoint-alias", endpoint="applications")
def applications_endpoint_alias_v489():
    return redirect("/owner/applications")

@owner_bp.route("/creator-plans", methods=["GET", "POST"])
def owner_creator_plans_v473():
    _owner_ensure_creator_plan_table_v473()
    _owner_ensure_plan_commission_v481()

    def seed_defaults():
        defaults = [
            ("free","Free","$0/mo","Basic creator testing plan.",5,"",True,0,20),
            ("starter","Starter","$19/mo","Good for small creators starting to sell clips.",100,"",True,10,18),
            ("pro","Pro","$49/mo","Recommended plan for active boat videographers.",512,"",True,20,15),
            ("studio","Studio","$149/mo","High-volume plan for studios and multi-location creators.",2048,"",True,30,12),
        ]
        for p in defaults:
            db.session.execute(db.text("""
                INSERT INTO creator_plan
                (plan_key, name, price_label, description, storage_gb, stripe_price_id, is_active, sort_order, commission_percent)
                VALUES (:plan_key,:name,:price_label,:description,:storage_gb,:stripe_price_id,:is_active,:sort_order,:commission_percent)
                ON CONFLICT (plan_key) DO NOTHING
            """), {
                "plan_key":p[0],"name":p[1],"price_label":p[2],"description":p[3],
                "storage_gb":p[4],"stripe_price_id":p[5],"is_active":p[6],"sort_order":p[7], "commission_percent":p[8] if len(p)>8 else 20
            })

    # Make sure first visit has default plans.
    try:
        count = db.session.execute(db.text("SELECT COUNT(*) AS total FROM creator_plan")).mappings().first()
        if int(count.get("total") or 0) == 0:
            seed_defaults()
            db.session.commit()
    except Exception:
        db.session.rollback()

    if request.method == "POST":
        plan_key = (request.form.get("plan_key") or "").strip().lower().replace(" ","_")
        name = (request.form.get("name") or "").strip()
        price_label = (request.form.get("price_label") or "").strip()
        description = (request.form.get("description") or "").strip()
        stripe_price_id = (request.form.get("stripe_price_id") or "").strip()
        storage_gb = int(float(request.form.get("storage_gb") or 0))
        sort_order = int(float(request.form.get("sort_order") or 0))
        commission_percent = float(request.form.get("commission_percent") or 0)
        is_active = bool(request.form.get("is_active"))

        if not plan_key or not name:
            flash("Plan key and name are required.")
            return redirect("/owner/creator-plans")

        try:
            db.session.execute(db.text("""
                INSERT INTO creator_plan
                (plan_key, name, price_label, description, storage_gb, stripe_price_id, is_active, sort_order, updated_at)
                VALUES (:plan_key,:name,:price_label,:description,:storage_gb,:stripe_price_id,:is_active,:sort_order,CURRENT_TIMESTAMP)
                ON CONFLICT (plan_key) DO UPDATE SET
                    name=EXCLUDED.name,
                    price_label=EXCLUDED.price_label,
                    description=EXCLUDED.description,
                    storage_gb=EXCLUDED.storage_gb,
                    stripe_price_id=EXCLUDED.stripe_price_id,
                    commission_percent=EXCLUDED.commission_percent,
                    is_active=EXCLUDED.is_active,
                    sort_order=EXCLUDED.sort_order,
                    updated_at=CURRENT_TIMESTAMP
            """), {
                "plan_key":plan_key,
                "name":name,
                "price_label":price_label,
                "description":description,
                "storage_gb":storage_gb,
                "stripe_price_id":stripe_price_id,
                "commission_percent":commission_percent,
                "is_active":is_active,
                "sort_order":sort_order,
            })
            db.session.commit()
            # commission_percent_v482 direct fix
            try:
                _owner_ensure_plan_commission_v482()
                db.session.execute(db.text("UPDATE creator_plan SET commission_percent=:commission_percent WHERE plan_key=:plan_key"), {
                    "commission_percent": commission_percent,
                    "plan_key": plan_key,
                })
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                try: print("commission save v48.2 warning:", e)
                except Exception: pass
            flash("Creator plan saved successfully.")
        except Exception as e:
            db.session.rollback()
            flash("Could not save creator plan.")
        return redirect("/owner/creator-plans")

    edit_key = (request.args.get("edit") or "").strip()
    edit_plan = None
    try:
        plans = db.session.execute(db.text("""
            SELECT *
            FROM creator_plan
            ORDER BY sort_order ASC, id ASC
        """)).mappings().all()
    except Exception:
        db.session.rollback()
        plans = []

    if edit_key:
        try:
            edit_plan = db.session.execute(db.text("""
                SELECT *
                FROM creator_plan
                WHERE plan_key=:plan_key
                LIMIT 1
            """), {"plan_key": edit_key}).mappings().first()
        except Exception:
            db.session.rollback()
            edit_plan = None

    return render_template("owner/creator_plans.html", plans=plans, edit_plan=edit_plan)



def _owner_table_for_v478(kind):
    if kind == "creator":
        return _owner_creator_table_v482()
    if kind == "buyer":
        return _owner_buyer_table_and_where_v482()[0]
    if kind == "application":
        return _owner_application_table_v482()
    return None


def _owner_read_row_v478(table, row_id):
    try:
        return db.session.execute(db.text(f"SELECT * FROM {table} WHERE id=:id LIMIT 1"), {"id": row_id}).mappings().first()
    except Exception:
        db.session.rollback()
        return None



@owner_bp.route("/<kind>/<int:row_id>/edit", methods=["GET","POST"], endpoint="owner_edit_person_v478")
def owner_edit_person_v478(kind, row_id):
    table = _owner_table_for_v478(kind)
    if not table:
        flash("Table not found.")
        return redirect("/owner/panel")
    row = _owner_read_row_v478(table, row_id)
    if not row:
        flash("Record not found.")
        return redirect("/owner/panel")

    if request.method == "POST":
        fields = {}
        for col in ["name","full_name","first_name","last_name","email","phone","phone_number","brand","brand_name","company_name","instagram","instagram_handle","status"]:
            if _owner_col_exists_v478(table, col) and col in request.form:
                fields[col] = request.form.get(col)
        if fields:
            set_sql = ", ".join([f"{k}=:{k}" for k in fields.keys()])
            fields["id"] = row_id
            try:
                db.session.execute(db.text(f"UPDATE {table} SET {set_sql} WHERE id=:id"), fields)
                db.session.commit()
                flash(f"{kind.title()} saved successfully.")
            except Exception as e:
                db.session.rollback()
                flash(f"Could not save {kind}.")
        return redirect(f"/owner/{kind}s")
    return render_template("owner/edit_person.html", row=row, kind=kind, title=f"Edit {kind.title()}")







# v50.0 Owner Earnings + Analytics
def _bsm_owner_period_sql_v500(period,col="o.created_at"):
    p=(period or "30d").lower()
    if p=="today": return f" AND {col} >= CURRENT_DATE "
    if p=="7d": return f" AND {col} >= NOW() - INTERVAL '7 days' "
    if p=="30d": return f" AND {col} >= NOW() - INTERVAL '30 days' "
    return ""

def _bsm_owner_analytics_data_v500(period="30d"):
    where_date=_bsm_owner_period_sql_v500(period,"o.created_at")
    data={"period":period or "30d","gross_sales":0.0,"platform_fees":0.0,"creator_estimated_payouts":0.0,"orders_count":0,"items_sold":0,"subscriptions_active":0,"creator_rows":[],"daily_sales":[],"recent_orders":[],"views":0,"clicks":0,"conversion_rate":0.0}
    try:
        row=db.session.execute(db.text(f"""
            SELECT COALESCE(SUM(COALESCE(i.unit_price,0)*COALESCE(i.quantity,1)),0) gross_sales,
                   COUNT(DISTINCT o.id) orders_count, COALESCE(SUM(COALESCE(i.quantity,1)),0) items_sold
            FROM bsm_cart_order_item i JOIN bsm_cart_order o ON o.id=i.cart_order_id
            WHERE COALESCE(o.status,'') IN ('paid','complete','completed','succeeded') {where_date}
        """)).mappings().first()
        if row:
            data["gross_sales"]=float(row.get("gross_sales") or 0); data["orders_count"]=int(row.get("orders_count") or 0); data["items_sold"]=int(row.get("items_sold") or 0)
        try:
            fee=db.session.execute(db.text(f"""
                SELECT COALESCE(SUM(COALESCE(platform_fee_amount,0)),0) platform_fees,
                       COALESCE(SUM(COALESCE(creator_gross_amount,0)),0) creator_payouts
                FROM bsm_cart_order o WHERE COALESCE(o.status,'') IN ('paid','complete','completed','succeeded') {where_date}
            """)).mappings().first()
            data["platform_fees"]=float((fee or {}).get("platform_fees") or 0); data["creator_estimated_payouts"]=float((fee or {}).get("creator_payouts") or 0)
        except Exception: db.session.rollback()
        
        try:
            trow=db.session.execute(db.text(f"""
                SELECT
                  COUNT(*) FILTER (WHERE event_type='view') AS views,
                  COUNT(*) FILTER (WHERE event_type IN ('click','purchase_click')) AS clicks
                FROM analytics_event
                WHERE 1=1
                {where_date.replace('o.created_at','created_at')}
            """)).mappings().first()
            data["views"]=int((trow or {}).get("views") or 0)
            data["clicks"]=int((trow or {}).get("clicks") or 0)
            data["conversion_rate"]=round((data["items_sold"]/data["views"]*100),2) if data["views"] else 0.0
        except Exception:
            db.session.rollback()

        if data["creator_estimated_payouts"]<=0 and data["gross_sales"]>0:
            data["platform_fees"]=round(data["gross_sales"]*.25,2); data["creator_estimated_payouts"]=round(data["gross_sales"]-data["platform_fees"],2)
        try:
            s=db.session.execute(db.text("SELECT COUNT(*) c FROM creator_subscription WHERE COALESCE(status,'')='active' AND COALESCE(plan_key,'free') <> 'free'")).mappings().first()
            data["subscriptions_active"]=int((s or {}).get("c") or 0)
        except Exception: db.session.rollback()
        data["creator_rows"]=[dict(r) for r in db.session.execute(db.text(f"""
            SELECT cp.id creator_id, COALESCE(u.display_name,u.public_name,u.primary_location,u.email,'Creator') creator_name,
                   COALESCE(SUM(COALESCE(i.unit_price,0)*COALESCE(i.quantity,1)),0) gross_sales,
                   COUNT(DISTINCT o.id) orders_count, COALESCE(SUM(COALESCE(i.quantity,1)),0) items_sold,
                   COALESCE(MAX(cp.commission_rate),25) commission_rate
            FROM creator_profile cp LEFT JOIN "user" u ON u.id=cp.user_id LEFT JOIN video v ON v.creator_id=cp.id
            LEFT JOIN bsm_cart_order_item i ON i.video_id=v.id OR i.creator_id=cp.id
            LEFT JOIN bsm_cart_order o ON o.id=i.cart_order_id AND COALESCE(o.status,'') IN ('paid','complete','completed','succeeded') {where_date}
            GROUP BY cp.id,u.display_name,u.public_name,u.primary_location,u.email
            HAVING COALESCE(SUM(COALESCE(i.unit_price,0)*COALESCE(i.quantity,1)),0)>0
            ORDER BY gross_sales DESC LIMIT 20
        """)).mappings().all()]
        data["daily_sales"]=[dict(r) for r in db.session.execute(db.text(f"""
            SELECT DATE(o.created_at) day, COALESCE(SUM(COALESCE(i.unit_price,0)*COALESCE(i.quantity,1)),0) gross_sales, COUNT(DISTINCT o.id) orders_count
            FROM bsm_cart_order o JOIN bsm_cart_order_item i ON i.cart_order_id=o.id
            WHERE COALESCE(o.status,'') IN ('paid','complete','completed','succeeded') {where_date}
            GROUP BY DATE(o.created_at) ORDER BY day DESC LIMIT 30
        """)).mappings().all()]
        data["recent_orders"]=[dict(r) for r in db.session.execute(db.text(f"""
            SELECT o.id,o.created_at,COALESCE(o.buyer_email,'') buyer_email,COALESCE(o.amount_total,0) amount_total,
                   COALESCE(o.platform_fee_amount,0) platform_fee_amount,COALESCE(o.creator_gross_amount,0) creator_gross_amount
            FROM bsm_cart_order o WHERE COALESCE(o.status,'') IN ('paid','complete','completed','succeeded') {where_date}
            ORDER BY o.created_at DESC LIMIT 15
        """)).mappings().all()]
    except Exception as e:
        db.session.rollback(); print("owner analytics v50 warning:", e)
    return data

@owner_bp.route("/analytics")
def owner_analytics_v500():
    period=request.args.get("period") or "30d"
    return render_template("owner/analytics.html", analytics=_bsm_owner_analytics_data_v500(period))

