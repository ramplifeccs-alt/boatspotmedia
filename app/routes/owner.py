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


@owner_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        return redirect(url_for("owner.applications"))
    return render_template("owner/login.html")

@owner_bp.route("/panel")
def panel():
    return redirect(url_for("owner.applications"))

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

    if request.method == "POST":
        action = request.form.get("action") or "save"
        if action == "seed":
            defaults = [
                ("free","Free","$0/mo","Basic creator testing plan.",5,"",True,0),
                ("starter","Starter","$19/mo","Good for small creators starting to sell clips.",100,"",True,10),
                ("pro","Pro","$49/mo","Recommended plan for active boat videographers.",512,"",True,20),
                ("studio","Studio","$149/mo","High-volume plan for studios and multi-location creators.",2048,"",True,30),
            ]
            try:
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
                db.session.commit()
                flash("Default creator plans created.")
            except Exception as e:
                db.session.rollback()
                flash("Could not seed plans.")
            return redirect("/owner/creator-plans")

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
            flash("Creator plan saved.")
        except Exception as e:
            db.session.rollback()
            flash("Could not save creator plan.")
        return redirect("/owner/creator-plans")

    try:
        plans = db.session.execute(db.text("""
            SELECT *
            FROM creator_plan
            ORDER BY sort_order ASC, id ASC
        """)).mappings().all()
    except Exception:
        db.session.rollback()
        plans = []
    return render_template("owner/creator_plans.html", plans=plans)
