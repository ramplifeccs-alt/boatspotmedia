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
            metrics["original_storage_bytes"] = _owner_scalar_v478(f"SELECT COALESCE(SUM(COALESCE(size_bytes,0)),0) FROM {video_table}")
        elif _owner_col_exists_v478(video_table,"file_size_bytes"):
            metrics["original_storage_bytes"] = _owner_scalar_v478(f"SELECT COALESCE(SUM(COALESCE(file_size_bytes,0)),0) FROM {video_table}")

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


@owner_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        return redirect(url_for("owner.applications"))
    return render_template("owner/login.html")

@owner_bp.route("/panel")
def owner_panel_v477():
    metrics = _owner_dashboard_metrics_v477()
    q = (request.args.get("q") or "").strip()
    return render_template("owner/panel.html", metrics=metrics, q=q)

@owner_bp.route("/applications")
def applications():
    _ensure_creator_profile_deleted_column()
    repair_all_known_tables(); repair_creator_application_table()
    rows = db.session.execute(text("SELECT * FROM creator_application WHERE COALESCE(status,'') <> 'deleted' ORDER BY CASE WHEN status='pending' THEN 0 ELSE 1 END, id DESC")).mappings().all()
    plans = StoragePlan.query.filter_by(active=True).order_by(StoragePlan.storage_limit_gb.asc()).all()
    show_all = request.args.get("show_all") == "1"
    creator_query = CreatorProfile.query.filter((CreatorProfile.deleted == False) | (CreatorProfile.deleted.is_(None)))
    if not show_all:
        creator_query = creator_query.filter(CreatorProfile.approved == True, CreatorProfile.suspended == False)
    creators = creator_query.order_by(CreatorProfile.id.desc()).all()
    logs = CommissionOverrideLog.query.order_by(CommissionOverrideLog.created_at.desc()).limit(100).all()
    return render_template("owner/applications.html", applications=rows, plans=plans, creators=creators, logs=logs)

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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.commit()
    except Exception:
        db.session.rollback()

@owner_bp.route("/creator-plans", methods=["GET", "POST"])
def owner_creator_plans_v473():
    _owner_ensure_creator_plan_table_v473()

    def seed_defaults():
        defaults = [
            ("free","Free","$0/mo","Basic creator testing plan.",5,"",True,0),
            ("starter","Starter","$19/mo","Good for small creators starting to sell clips.",100,"",True,10),
            ("pro","Pro","$49/mo","Recommended plan for active boat videographers.",512,"",True,20),
            ("studio","Studio","$149/mo","High-volume plan for studios and multi-location creators.",2048,"",True,30),
        ]
        for p in defaults:
            db.session.execute(db.text("""
                INSERT INTO creator_plan
                (plan_key, name, price_label, description, storage_gb, stripe_price_id, is_active, sort_order)
                VALUES (:plan_key,:name,:price_label,:description,:storage_gb,:stripe_price_id,:is_active,:sort_order)
                ON CONFLICT (plan_key) DO NOTHING
            """), {
                "plan_key":p[0],"name":p[1],"price_label":p[2],"description":p[3],
                "storage_gb":p[4],"stripe_price_id":p[5],"is_active":p[6],"sort_order":p[7]
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
                "is_active":is_active,
                "sort_order":sort_order,
            })
            db.session.commit()
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
        return _owner_pick_table_v478("creator_profile","creators","creator")
    if kind == "buyer":
        return _owner_pick_table_v478("buyer_user","buyers","buyer")
    if kind == "application":
        return _owner_pick_table_v478("creator_application","creator_applications")
    return None

def _owner_read_row_v478(table, row_id):
    try:
        return db.session.execute(db.text(f"SELECT * FROM {table} WHERE id=:id LIMIT 1"), {"id": row_id}).mappings().first()
    except Exception:
        db.session.rollback()
        return None

@owner_bp.route("/creators", endpoint="owner_creators_v478")
def owner_creators_v478():
    table = _owner_table_for_v478("creator")
    rows = []
    if table:
        try:
            rows = db.session.execute(db.text(f"SELECT * FROM {table} ORDER BY id DESC LIMIT 200")).mappings().all()
        except Exception:
            db.session.rollback()
    return render_template("owner/manage_people.html", rows=rows, kind="creator", title="Creators")

@owner_bp.route("/buyers", endpoint="owner_buyers_v478")
def owner_buyers_v478():
    table = _owner_table_for_v478("buyer")
    rows = []
    if table:
        try:
            rows = db.session.execute(db.text(f"SELECT * FROM {table} ORDER BY id DESC LIMIT 200")).mappings().all()
        except Exception:
            db.session.rollback()
    return render_template("owner/manage_people.html", rows=rows, kind="buyer", title="Buyers")

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

@owner_bp.route("/<kind>/<int:row_id>/status/<status>", methods=["POST"], endpoint="owner_set_status_v478")
def owner_set_status_v478(kind, row_id, status):
    table = _owner_table_for_v478(kind)
    if table and _owner_col_exists_v478(table, "status"):
        try:
            db.session.execute(db.text(f"UPDATE {table} SET status=:status WHERE id=:id"), {"status": status, "id": row_id})
            db.session.commit()
            flash(f"{kind.title()} status updated.")
        except Exception:
            db.session.rollback()
            flash("Could not update status.")
    return redirect(request.referrer or f"/owner/{kind}s")

@owner_bp.route("/<kind>/<int:row_id>/delete", methods=["POST"], endpoint="owner_delete_person_v478")
def owner_delete_person_v478(kind, row_id):
    table = _owner_table_for_v478(kind)
    if table:
        try:
            db.session.execute(db.text(f"DELETE FROM {table} WHERE id=:id"), {"id": row_id})
            db.session.commit()
            flash(f"{kind.title()} deleted.")
        except Exception:
            db.session.rollback()
            flash(f"Could not delete {kind}. It may have linked orders/videos.")
    return redirect(request.referrer or f"/owner/{kind}s")

@owner_bp.route("/<kind>/<int:row_id>/reset-password", methods=["POST"], endpoint="owner_reset_password_v478")
def owner_reset_password_v478(kind, row_id):
    table = _owner_table_for_v478(kind)
    new_password = (request.form.get("new_password") or "").strip()
    if not new_password:
        flash("Enter a new password.")
        return redirect(request.referrer or f"/owner/{kind}s")
    if not table:
        return redirect("/owner/panel")

    try:
        from werkzeug.security import generate_password_hash
        hashed = generate_password_hash(new_password)
        if _owner_col_exists_v478(table, "password_hash"):
            db.session.execute(db.text(f"UPDATE {table} SET password_hash=:p WHERE id=:id"), {"p": hashed, "id": row_id})
        elif _owner_col_exists_v478(table, "password"):
            db.session.execute(db.text(f"UPDATE {table} SET password=:p WHERE id=:id"), {"p": hashed, "id": row_id})
        else:
            flash("This table has no password column.")
            return redirect(request.referrer or f"/owner/{kind}s")
        db.session.commit()
        flash("Password reset successfully.")
    except Exception:
        db.session.rollback()
        flash("Could not reset password.")
    return redirect(request.referrer or f"/owner/{kind}s")



@owner_bp.route("/application/<int:row_id>/status/<status>", methods=["POST"], endpoint="owner_application_status_v478")
def owner_application_status_v478(row_id, status):
    table = _owner_table_for_v478("application")
    if table and _owner_col_exists_v478(table, "status"):
        try:
            db.session.execute(db.text(f"UPDATE {table} SET status=:status WHERE id=:id"), {"status": status, "id": row_id})
            db.session.commit()
            flash("Application status updated.")
        except Exception:
            db.session.rollback()
            flash("Could not update application.")
    return redirect("/owner/applications")

@owner_bp.route("/application/<int:row_id>/delete", methods=["POST"], endpoint="owner_application_delete_v478")
def owner_application_delete_v478(row_id):
    table = _owner_table_for_v478("application")
    if table:
        try:
            db.session.execute(db.text(f"DELETE FROM {table} WHERE id=:id"), {"id": row_id})
            db.session.commit()
            flash("Application deleted.")
        except Exception:
            db.session.rollback()
            flash("Could not delete application.")
    return redirect("/owner/applications")
