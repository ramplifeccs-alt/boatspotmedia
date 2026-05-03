from sqlalchemy import text
from datetime import datetime, timedelta
from flask import Blueprint, render_template, request, redirect, url_for, flash
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
        return redirect(url_for("owner.applications"))
    return render_template("owner/login.html")



@owner_bp.route("/applications/<int:app_id>/approve", methods=["POST"])
def approve_application(app_id):
    repair_all_known_tables(); repair_creator_application_table()
    selected_plan = StoragePlan.query.get(request.form.get("plan_id")) if request.form.get("plan_id") else StoragePlan.query.first()
    row = db.session.execute(text("SELECT * FROM creator_application WHERE id=:id LIMIT 1"), {"id": app_id}).mappings().first()
    if not row: return redirect(url_for("owner.applications"))
    email = (row.get("email") or "").lower().strip()
    brand_name = row.get("brand_name") or row.get("instagram") or "Boat Creator"
    user = User.query.filter_by(email=email).first()
    if not user:
        user = User(email=email, password_hash=generate_password_hash("TempCreator123!"), role="creator", display_name=brand_name, is_active=True)
        db.session.add(user); db.session.flush()
    else:
        user.display_name = user.display_name or brand_name; user.is_active=True; user.role="creator"
    creator = CreatorProfile.query.filter_by(user_id=user.id).first()
    if not creator:
        creator = CreatorProfile(user_id=user.id, plan_id=selected_plan.id if selected_plan else None, storage_limit_gb=selected_plan.storage_limit_gb if selected_plan else 512, commission_rate=selected_plan.commission_rate if selected_plan else 20, product_commission_rate=20, approved=True, suspended=False)
        db.session.add(creator)
    else:
        creator.approved=True; creator.suspended=False; creator.deleted=False
        if selected_plan:
            creator.plan_id=selected_plan.id; creator.storage_limit_gb=selected_plan.storage_limit_gb; creator.commission_rate=selected_plan.commission_rate
    db.session.execute(text("UPDATE creator_application SET status='approved', reviewed_at=CURRENT_TIMESTAMP WHERE id=:id"), {"id": app_id})
    db.session.commit()
    send_email(email, "BoatSpotMedia Creator Approved", "Your creator application was approved. Login at /creator/login. Temporary password: TempCreator123!")
    return redirect(url_for("owner.applications"))

@owner_bp.route("/applications/<int:app_id>/reject", methods=["POST"])
def reject_application(app_id):
    repair_creator_application_table()
    db.session.execute(text("UPDATE creator_application SET status='rejected', reviewed_at=CURRENT_TIMESTAMP WHERE id=:id"), {"id": app_id})
    db.session.commit()
    return redirect(url_for("owner.applications"))

@owner_bp.route("/plans/create", methods=["POST"])
def create_plan():
    db.session.add(StoragePlan(name=request.form.get("name"), storage_limit_gb=int(request.form.get("storage_limit_gb") or 512), monthly_price=request.form.get("monthly_price") or 0, commission_rate=int(request.form.get("commission_rate") or 20), active=True))
    db.session.commit(); return redirect(url_for("owner.applications"))

@owner_bp.route("/plans/<int:plan_id>/edit", methods=["POST"])
def edit_plan(plan_id):
    p=StoragePlan.query.get_or_404(plan_id)
    p.name=request.form.get("name") or p.name; p.storage_limit_gb=int(request.form.get("storage_limit_gb") or p.storage_limit_gb); p.monthly_price=request.form.get("monthly_price") or p.monthly_price; p.commission_rate=int(request.form.get("commission_rate") or p.commission_rate)
    db.session.commit(); return redirect(url_for("owner.applications"))

@owner_bp.route("/plans/<int:plan_id>/delete", methods=["POST"])
def delete_plan(plan_id):
    p=StoragePlan.query.get_or_404(plan_id); p.active=False; db.session.commit(); return redirect(url_for("owner.applications"))

@owner_bp.route("/creator/<int:creator_id>/override", methods=["POST"])
def override_commission(creator_id):
    c=CreatorProfile.query.get_or_404(creator_id)
    rate=int(request.form.get("rate") or 0); days=int(request.form.get("days") or 30); reason=request.form.get("reason") or ""; typ=request.form.get("commission_type") or "video"; expires=datetime.utcnow()+timedelta(days=days)
    if typ=="product":
        old=c.active_product_commission_rate(); c.product_commission_override_rate=rate; c.product_commission_override_until=expires; c.product_commission_override_reason=reason
    else:
        old=c.active_commission_rate(); c.commission_override_rate=rate; c.commission_override_until=expires; c.commission_override_reason=reason
    db.session.add(CommissionOverrideLog(creator_id=c.id, commission_type=typ, old_rate=old, new_rate=rate, days=days, reason=reason, expires_at=expires))
    db.session.commit(); return redirect(url_for("owner.applications"))

@owner_bp.route("/creator/<int:creator_id>/override/reset", methods=["POST"])
def reset_override(creator_id):
    c=CreatorProfile.query.get_or_404(creator_id); typ=request.form.get("commission_type") or "video"
    if typ=="product":
        c.product_commission_override_rate=None; c.product_commission_override_until=None; c.product_commission_override_reason=None
    else:
        c.commission_override_rate=None; c.commission_override_until=None; c.commission_override_reason=None
    db.session.commit(); return redirect(url_for("owner.applications"))

@owner_bp.route("/creators/<int:creator_id>/edit", methods=["POST"])
def edit_creator(creator_id):
    c=CreatorProfile.query.get_or_404(creator_id)
    if c.user:
        c.user.display_name=request.form.get("display_name") or c.user.display_name; c.user.email=request.form.get("email") or c.user.email
    c.storage_limit_gb=int(request.form.get("storage_limit_gb") or c.storage_limit_gb); c.commission_rate=int(request.form.get("commission_rate") or c.commission_rate); c.product_commission_rate=int(request.form.get("product_commission_rate") or c.product_commission_rate or 20)
    db.session.commit(); return redirect(url_for("owner.applications"))

@owner_bp.route("/creators/<int:creator_id>/suspend", methods=["POST"])
def suspend_creator(creator_id):
    c=CreatorProfile.query.get_or_404(creator_id)
    c.suspended=True; c.approved=False
    if c.user: c.user.is_active=False
    for v in Video.query.filter_by(creator_id=c.id).all():
        if v.status != "deleted": v.status="suspended"
    for p in Product.query.filter_by(creator_id=c.id).all():
        p.active=False
    db.session.commit(); return redirect(url_for("owner.applications"))

@owner_bp.route("/creators/<int:creator_id>/activate", methods=["POST"])
def activate_creator(creator_id):
    c=CreatorProfile.query.get_or_404(creator_id)
    c.suspended=False; c.approved=True
    if c.user: c.user.is_active=True; c.user.role="creator"
    for v in Video.query.filter_by(creator_id=c.id, status="suspended").all():
        v.status="active"
    db.session.commit(); return redirect(url_for("owner.applications"))




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
            return redirect(url_for("owner.applications"))

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
        return redirect(url_for("owner.applications"))

    return render_template("owner/reset_creator_password.html", creator=c)


@owner_bp.route("/creators/<int:creator_id>/password", methods=["POST"])
def reset_creator_password(creator_id):
    _ensure_creator_profile_deleted_column()
    c = CreatorProfile.query.get_or_404(creator_id)
    new_password = (request.form.get("new_password") or request.form.get("password") or "").strip()

    if len(new_password) < 6:
        flash("Password must be at least 6 characters.", "error")
        return redirect(url_for("owner.applications"))

    if not c.user:
        flash("Creator user account not found.", "error")
        return redirect(url_for("owner.applications"))

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
    return redirect(url_for("owner.applications"))


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

    return redirect(url_for("owner.applications"))

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
# v48.4 Owner Dashboard fixed to REAL BoatSpotMedia schema
# ============================================================

def _owner_scalar_v484(sql, params=None, default=0):
    try:
        row = db.session.execute(db.text(sql), params or {}).mappings().first()
        if not row:
            return default
        return list(row.values())[0]
    except Exception as e:
        db.session.rollback()
        try:
            print("owner scalar v48.4 warning:", e, sql)
        except Exception:
            pass
        return default

def _owner_rows_v484(sql, params=None):
    try:
        return [dict(r) for r in db.session.execute(db.text(sql), params or {}).mappings().all()]
    except Exception as e:
        db.session.rollback()
        try:
            print("owner rows v48.4 warning:", e, sql)
        except Exception:
            pass
        return []

def _owner_exec_v484(sql, params=None):
    try:
        db.session.execute(db.text(sql), params or {})
        db.session.commit()
        return True
    except Exception as e:
        db.session.rollback()
        try:
            print("owner exec v48.4 warning:", e, sql)
        except Exception:
            pass
        return False

def _owner_dashboard_metrics_v477():
    metrics = {}
    metrics["applications_total"] = _owner_scalar_v484("SELECT COUNT(*) FROM creator_application")
    metrics["applications_pending"] = _owner_scalar_v484("SELECT COUNT(*) FROM creator_application WHERE COALESCE(status,'pending')='pending'")
    metrics["applications_approved"] = _owner_scalar_v484("SELECT COUNT(*) FROM creator_application WHERE COALESCE(status,'')='approved'")
    metrics["applications_rejected"] = _owner_scalar_v484("SELECT COUNT(*) FROM creator_application WHERE COALESCE(status,'') IN ('rejected','denied')")

    metrics["creators_total"] = _owner_scalar_v484("SELECT COUNT(*) FROM creators")
    metrics["creators_active"] = _owner_scalar_v484("SELECT COUNT(*) FROM creators WHERE COALESCE(approved,false)=true")
    metrics["subscriptions_past_due"] = _owner_scalar_v484("SELECT COUNT(*) FROM creator_subscription WHERE COALESCE(status,'active') NOT IN ('active','trialing')")

    metrics["orders_total"] = _owner_scalar_v484("SELECT COUNT(*) FROM bsm_cart_order")
    metrics["sales_total"] = _owner_scalar_v484("""
        SELECT COALESCE(SUM(COALESCE(amount_total,0)),0)
        FROM bsm_cart_order
        WHERE COALESCE(status,'') IN ('paid','complete','completed')
    """)
    metrics["sales_today"] = _owner_scalar_v484("""
        SELECT COALESCE(SUM(COALESCE(amount_total,0)),0)
        FROM bsm_cart_order
        WHERE COALESCE(status,'') IN ('paid','complete','completed')
          AND DATE(created_at)=CURRENT_DATE
    """)
    metrics["pending_edits"] = _owner_scalar_v484("""
        SELECT COUNT(*)
        FROM bsm_cart_order_item
        WHERE package IN ('edited','edit','bundle','combo','original_plus_edited','original_edited','original+edited','original_edit')
          AND (edited_r2_key IS NULL OR edited_r2_key='')
    """)
    metrics["discount_approvals"] = _owner_scalar_v484("""
        SELECT COUNT(*)
        FROM bsm_cart_order_item
        WHERE COALESCE(discount_status,'') IN ('pending','pending_review','awaiting_creator','needs_approval')
    """)
    metrics["original_storage_bytes"] = _owner_scalar_v484("""
        SELECT COALESCE(SUM(COALESCE(file_size_bytes,0)),0)
        FROM video
        WHERE COALESCE(status,'active') NOT IN ('deleted','cancelled','canceled','removed')
          AND COALESCE(r2_video_key,'') <> ''
    """)
    metrics["edited_storage_bytes"] = _owner_scalar_v484("""
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
    metrics["creators_near_limit"] = _owner_scalar_v484("""
        SELECT COUNT(*)
        FROM creator_profile
        WHERE COALESCE(storage_limit_gb,0) > 0
          AND (COALESCE(storage_used_bytes,0)::numeric / 1024 / 1024 / 1024) >= (COALESCE(storage_limit_gb,0)::numeric * 0.85)
    """)
    return metrics

def _owner_current_creator_applications_v484():
    return _owner_rows_v484("""
        SELECT id,
               TRIM(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')) AS display_name,
               COALESCE(brand_name,'') AS display_brand,
               COALESCE(email,'') AS display_email,
               '' AS display_phone,
               COALESCE(instagram,'') AS display_social,
               COALESCE(status,'pending') AS display_status,
               submitted_at,
               reviewed_at
        FROM creator_application
        ORDER BY id DESC
        LIMIT 300
    """)

def _owner_current_creators_v484():
    return _owner_rows_v484("""
        SELECT id,
               COALESCE(public_name, company_name, username, email, 'Creator #' || id::text) AS display_name,
               COALESCE(company_name,'') AS display_brand,
               COALESCE(email,'') AS display_email,
               '' AS display_phone,
               COALESCE(instagram,'') AS display_social,
               CASE WHEN COALESCE(approved,false)=true THEN 'approved' ELSE 'suspended/pending' END AS display_status,
               COALESCE(username,'') AS username,
               created_at
        FROM creators
        ORDER BY id DESC
        LIMIT 300
    """)

def _owner_current_buyers_v484():
    return _owner_rows_v484("""
        SELECT id,
               COALESCE(display_name, public_name, TRIM(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')), email, 'Buyer #' || id::text) AS display_name,
               COALESCE(primary_location,'') AS display_brand,
               COALESCE(email,'') AS display_email,
               '' AS display_phone,
               '' AS display_social,
               CASE WHEN COALESCE(is_active,true)=true THEN 'active' ELSE 'inactive' END AS display_status,
               role,
               created_at
        FROM "user"
        WHERE LOWER(COALESCE(role,''))='buyer'
        ORDER BY id DESC
        LIMIT 300
    """)

def _owner_get_application_v484(row_id):
    rows = _owner_rows_v484("SELECT id, first_name, last_name, brand_name, email, instagram, status FROM creator_application WHERE id=:id LIMIT 1", {"id": row_id})
    return rows[0] if rows else None

def _owner_get_creator_v484(row_id):
    rows = _owner_rows_v484("SELECT id, public_name, company_name, email, username, instagram, approved FROM creators WHERE id=:id LIMIT 1", {"id": row_id})
    return rows[0] if rows else None

def _owner_get_buyer_v484(row_id):
    rows = _owner_rows_v484('SELECT id, email, first_name, last_name, public_name, display_name, primary_location, is_active FROM "user" WHERE id=:id AND LOWER(COALESCE(role,\'\'))=\'buyer\' LIMIT 1', {"id": row_id})
    return rows[0] if rows else None

@owner_bp.route("/panel")
def owner_panel_v477():
    metrics = _owner_dashboard_metrics_v477()
    q = (request.args.get("q") or "").strip()
    return render_template("owner/panel.html", metrics=metrics, q=q)

@owner_bp.route("/applications")
def owner_applications_v479():
    rows = _owner_current_creator_applications_v484()
    return render_template("owner/applications.html", applications=rows, table_name="creator_application", columns=["id","first_name","last_name","brand_name","email","instagram","status","submitted_at","reviewed_at"])

@owner_bp.route("/creators", endpoint="owner_creators_v478")
def owner_creators_v478():
    rows = _owner_current_creators_v484()
    return render_template("owner/manage_people.html", rows=rows, columns=["id","public_name","company_name","email","username","instagram","approved","created_at"], table_name="creators", kind="creator", title="Creators")

@owner_bp.route("/buyers", endpoint="owner_buyers_v478")
def owner_buyers_v478():
    rows = _owner_current_buyers_v484()
    return render_template("owner/manage_people.html", rows=rows, columns=["id","role","email","first_name","last_name","public_name","display_name","primary_location","is_active","created_at"], table_name="user", kind="buyer", title="Buyers")

@owner_bp.route("/applications/<int:row_id>/approve", methods=["POST"])
def owner_approve_application_v484(row_id):
    ok = _owner_exec_v484("UPDATE creator_application SET status='approved', reviewed_at=CURRENT_TIMESTAMP WHERE id=:id", {"id": row_id})
    flash("Application approved." if ok else "Could not approve application.")
    return redirect("/owner/applications")

@owner_bp.route("/application/<int:row_id>/status/<status>", methods=["POST"], endpoint="owner_application_status_v478")
def owner_application_status_v478(row_id, status):
    if status not in ["pending","approved","suspended","rejected"]:
        status = "pending"
    ok = _owner_exec_v484("UPDATE creator_application SET status=:status, reviewed_at=CURRENT_TIMESTAMP WHERE id=:id", {"id": row_id, "status": status})
    flash("Application status updated." if ok else "Could not update application.")
    return redirect("/owner/applications")

@owner_bp.route("/application/<int:row_id>/delete", methods=["POST"], endpoint="owner_application_delete_v478")
def owner_application_delete_v478(row_id):
    ok = _owner_exec_v484("DELETE FROM creator_application WHERE id=:id", {"id": row_id})
    flash("Application deleted." if ok else "Could not delete application.")
    return redirect("/owner/applications")

@owner_bp.route("/application/<int:row_id>/edit", methods=["GET","POST"], endpoint="owner_edit_application_v479")
def owner_edit_application_v479(row_id):
    row = _owner_get_application_v484(row_id)
    if not row:
        flash("Application not found.")
        return redirect("/owner/applications")
    if request.method == "POST":
        ok = _owner_exec_v484("""
            UPDATE creator_application
            SET first_name=:first_name, last_name=:last_name, brand_name=:brand_name, email=:email, instagram=:instagram, status=:status
            WHERE id=:id
        """, {"id": row_id, "first_name": request.form.get("first_name") or "", "last_name": request.form.get("last_name") or "", "brand_name": request.form.get("brand_name") or "", "email": request.form.get("email") or "", "instagram": request.form.get("instagram") or "", "status": request.form.get("status") or "pending"})
        flash("Application saved." if ok else "Could not save application.")
        return redirect("/owner/applications")
    return render_template("owner/edit_person.html", row=row, kind="application", title="Edit Application")

@owner_bp.route("/creator/<int:row_id>/edit", methods=["GET","POST"], endpoint="owner_edit_creator_v484")
def owner_edit_creator_v484(row_id):
    row = _owner_get_creator_v484(row_id)
    if not row:
        flash("Creator not found.")
        return redirect("/owner/creators")
    if request.method == "POST":
        ok = _owner_exec_v484("""
            UPDATE creators
            SET public_name=:public_name, company_name=:company_name, email=:email, username=:username, instagram=:instagram, approved=:approved
            WHERE id=:id
        """, {"id": row_id, "public_name": request.form.get("public_name") or "", "company_name": request.form.get("company_name") or "", "email": request.form.get("email") or "", "username": request.form.get("username") or "", "instagram": request.form.get("instagram") or "", "approved": True if request.form.get("approved") else False})
        flash("Creator saved." if ok else "Could not save creator.")
        return redirect("/owner/creators")
    return render_template("owner/edit_person.html", row=row, kind="creator", title="Edit Creator")

@owner_bp.route("/buyer/<int:row_id>/edit", methods=["GET","POST"], endpoint="owner_edit_buyer_v484")
def owner_edit_buyer_v484(row_id):
    row = _owner_get_buyer_v484(row_id)
    if not row:
        flash("Buyer not found.")
        return redirect("/owner/buyers")
    if request.method == "POST":
        ok = _owner_exec_v484("""
            UPDATE "user"
            SET email=:email, first_name=:first_name, last_name=:last_name, public_name=:public_name, display_name=:display_name, primary_location=:primary_location, is_active=:is_active
            WHERE id=:id
        """, {"id": row_id, "email": request.form.get("email") or "", "first_name": request.form.get("first_name") or "", "last_name": request.form.get("last_name") or "", "public_name": request.form.get("public_name") or "", "display_name": request.form.get("display_name") or "", "primary_location": request.form.get("primary_location") or "", "is_active": True if request.form.get("is_active") else False})
        flash("Buyer saved." if ok else "Could not save buyer.")
        return redirect("/owner/buyers")
    return render_template("owner/edit_person.html", row=row, kind="buyer", title="Edit Buyer")

@owner_bp.route("/<kind>/<int:row_id>/status/<status>", methods=["POST"], endpoint="owner_set_status_v478")
def owner_set_status_v478(kind, row_id, status):
    if kind == "creator":
        ok = _owner_exec_v484("UPDATE creators SET approved=:approved WHERE id=:id", {"approved": status in ["active","approved"], "id": row_id})
        dest = "/owner/creators"
    elif kind == "buyer":
        ok = _owner_exec_v484('UPDATE "user" SET is_active=:is_active WHERE id=:id', {"is_active": status in ["active","approved"], "id": row_id})
        dest = "/owner/buyers"
    else:
        ok = False
        dest = "/owner/panel"
    flash("Status updated." if ok else "Could not update status.")
    return redirect(dest)

@owner_bp.route("/<kind>/<int:row_id>/delete", methods=["POST"], endpoint="owner_delete_person_v478")
def owner_delete_person_v478(kind, row_id):
    if kind == "creator":
        ok = _owner_exec_v484("DELETE FROM creators WHERE id=:id", {"id": row_id})
        dest = "/owner/creators"
    elif kind == "buyer":
        ok = _owner_exec_v484('DELETE FROM "user" WHERE id=:id AND LOWER(COALESCE(role,\'\'))=\'buyer\'', {"id": row_id})
        dest = "/owner/buyers"
    else:
        ok = False
        dest = "/owner/panel"
    flash("Deleted." if ok else "Could not delete. It may have linked orders/videos.")
    return redirect(dest)

@owner_bp.route("/<kind>/<int:row_id>/reset-password", methods=["POST"], endpoint="owner_reset_password_v478")
def owner_reset_password_v478(kind, row_id):
    new_password = (request.form.get("new_password") or "").strip()
    if not new_password:
        flash("Enter a new password.")
        return redirect(request.referrer or "/owner/panel")
    try:
        from werkzeug.security import generate_password_hash
        hashed = generate_password_hash(new_password)
        if kind == "buyer":
            ok = _owner_exec_v484('UPDATE "user" SET password_hash=:p WHERE id=:id', {"p": hashed, "id": row_id})
            dest = "/owner/buyers"
        elif kind == "creator":
            ok = _owner_exec_v484("UPDATE creators SET password=:p WHERE id=:id", {"p": hashed, "id": row_id})
            dest = "/owner/creators"
        else:
            ok = False
            dest = "/owner/panel"
        flash("Password reset successfully." if ok else "Could not reset password.")
        return redirect(dest)
    except Exception:
        flash("Could not reset password.")
        return redirect(request.referrer or "/owner/panel")

@owner_bp.route("/db-debug", endpoint="owner_db_debug_v482")
def owner_db_debug_v482():
    try:
        tables = []
        rows = db.session.execute(db.text("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")).mappings().all()
        for r in rows:
            t = r.get("table_name")
            if any(x in t.lower() for x in ["creator","buyer","user","order","video","plan","application"]):
                cols = db.session.execute(db.text("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=:t ORDER BY ordinal_position"), {"t": t}).mappings().all()
                tables.append({"name": t, "columns": [c.get("column_name") for c in cols]})
        return render_template("owner/db_debug.html", tables=tables)
    except Exception:
        db.session.rollback()
        return render_template("owner/db_debug.html", tables=[])


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




