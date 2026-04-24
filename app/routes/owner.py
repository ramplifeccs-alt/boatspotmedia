from datetime import datetime, timedelta
from flask import Blueprint, render_template, request, redirect, url_for
from werkzeug.security import generate_password_hash
from sqlalchemy import text
from app.models import User, CreatorProfile, StoragePlan
from app.services.db import db
from app.services.emailer import send_email
from app.services.db_repair import repair_creator_application_table, repair_all_known_tables

owner_bp = Blueprint("owner", __name__)

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
    repair_all_known_tables()
    repair_creator_application_table()

    rows = db.session.execute(text("""
        SELECT id, first_name, last_name, brand_name, email, instagram, status, submitted_at, reviewed_at
        FROM creator_application
        ORDER BY
            CASE WHEN status = 'pending' THEN 0 ELSE 1 END,
            id DESC
    """)).mappings().all()

    plans = StoragePlan.query.filter_by(active=True).order_by(StoragePlan.storage_limit_gb.asc()).all()
    creators = CreatorProfile.query.order_by(CreatorProfile.id.desc()).all()
    return render_template("owner/applications.html", applications=rows, plans=plans, creators=creators)

@owner_bp.route("/applications/<int:app_id>/approve", methods=["POST"])
def approve_application(app_id):
    repair_all_known_tables()
    repair_creator_application_table()

    plan_id = request.form.get("plan_id")
    selected_plan = StoragePlan.query.get(plan_id) if plan_id else StoragePlan.query.first()

    row = db.session.execute(text("""
        SELECT *
        FROM creator_application
        WHERE id = :id
        LIMIT 1
    """), {"id": app_id}).mappings().first()

    if not row:
        return redirect(url_for("owner.applications"))

    email = row.get("email")
    brand_name = row.get("brand_name") or row.get("instagram") or f"{row.get('first_name','')} {row.get('last_name','')}".strip() or "Boat Creator"

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
        user.display_name = user.display_name or brand_name
        user.is_active = True
        user.role = "creator"

    creator = CreatorProfile.query.filter_by(user_id=user.id).first()
    if not creator:
        creator = CreatorProfile(
            user_id=user.id,
            plan_id=selected_plan.id if selected_plan else None,
            storage_limit_gb=selected_plan.storage_limit_gb if selected_plan else 512,
            commission_rate=selected_plan.commission_rate if selected_plan else 20,
            product_commission_rate=20,
            approved=True,
            suspended=False
        )
        db.session.add(creator)
    else:
        creator.approved = True
        creator.suspended = False
        if selected_plan:
            creator.plan_id = selected_plan.id
            creator.storage_limit_gb = selected_plan.storage_limit_gb
            creator.commission_rate = selected_plan.commission_rate

    db.session.execute(text("""
        UPDATE creator_application
        SET status = 'approved', reviewed_at = CURRENT_TIMESTAMP
        WHERE id = :id
    """), {"id": app_id})

    db.session.commit()

    send_email(
        email,
        "BoatSpotMedia Creator Approved",
        "Your creator application was approved. You can now access the creator login at /creator/login. Temporary password: TempCreator123!"
    )

    return redirect(url_for("owner.applications"))

@owner_bp.route("/applications/<int:app_id>/reject", methods=["POST"])
def reject_application(app_id):
    repair_creator_application_table()

    row = db.session.execute(text("""
        SELECT email
        FROM creator_application
        WHERE id = :id
    """), {"id": app_id}).mappings().first()

    db.session.execute(text("""
        UPDATE creator_application
        SET status = 'rejected', reviewed_at = CURRENT_TIMESTAMP
        WHERE id = :id
    """), {"id": app_id})

    db.session.commit()

    if row and row.get("email"):
        send_email(row.get("email"), "BoatSpotMedia Creator Application", "Your creator application was not approved at this time.")

    return redirect(url_for("owner.applications"))

@owner_bp.route("/plans/create", methods=["POST"])
def create_plan():
    plan = StoragePlan(
        name=request.form.get("name"),
        storage_limit_gb=int(request.form.get("storage_limit_gb") or 512),
        monthly_price=request.form.get("monthly_price") or 0,
        commission_rate=int(request.form.get("commission_rate") or 20),
        active=True
    )
    db.session.add(plan)
    db.session.commit()
    return redirect(url_for("owner.applications"))

@owner_bp.route("/plans/<int:plan_id>/edit", methods=["POST"])
def edit_plan(plan_id):
    plan = StoragePlan.query.get_or_404(plan_id)
    plan.name = request.form.get("name") or plan.name
    plan.storage_limit_gb = int(request.form.get("storage_limit_gb") or plan.storage_limit_gb)
    plan.monthly_price = request.form.get("monthly_price") or plan.monthly_price
    plan.commission_rate = int(request.form.get("commission_rate") or plan.commission_rate)
    db.session.commit()
    return redirect(url_for("owner.applications"))

@owner_bp.route("/plans/<int:plan_id>/delete", methods=["POST"])
def delete_plan(plan_id):
    plan = StoragePlan.query.get_or_404(plan_id)
    plan.active = False
    db.session.commit()
    return redirect(url_for("owner.applications"))

@owner_bp.route("/creator/<int:creator_id>/override", methods=["POST"])
def override_commission(creator_id):
    c = CreatorProfile.query.get_or_404(creator_id)
    rate = int(request.form.get("rate") or 0)
    days = int(request.form.get("days") or 30)
    reason = request.form.get("reason") or ""
    commission_type = request.form.get("commission_type") or "video"

    if commission_type == "product":
        c.product_commission_override_rate = rate
        c.product_commission_override_until = datetime.utcnow() + timedelta(days=days)
        c.product_commission_override_reason = reason
    else:
        c.commission_override_rate = rate
        c.commission_override_until = datetime.utcnow() + timedelta(days=days)
        c.commission_override_reason = reason

    db.session.commit()
    return redirect(url_for("owner.applications"))

@owner_bp.route("/creators/<int:creator_id>/edit", methods=["POST"])
def edit_creator(creator_id):
    c = CreatorProfile.query.get_or_404(creator_id)

    if c.user:
        c.user.display_name = request.form.get("display_name") or c.user.display_name
        c.user.email = request.form.get("email") or c.user.email

    c.storage_limit_gb = int(request.form.get("storage_limit_gb") or c.storage_limit_gb)
    c.commission_rate = int(request.form.get("commission_rate") or c.commission_rate)
    c.product_commission_rate = int(request.form.get("product_commission_rate") or c.product_commission_rate or 20)

    db.session.commit()
    return redirect(url_for("owner.applications"))

@owner_bp.route("/creators/<int:creator_id>/delete", methods=["POST"])
def delete_creator(creator_id):
    c = CreatorProfile.query.get_or_404(creator_id)
    c.suspended = True
    c.approved = False

    if c.user:
        c.user.is_active = False

    db.session.commit()
    return redirect(url_for("owner.applications"))

@owner_bp.route("/repair-db-now")
def repair_db_now():
    repair_all_known_tables()
    repair_creator_application_table()
    return "DB repair completed."

@owner_bp.route("/applications-raw")
def applications_raw():
    repair_creator_application_table()
    rows = db.session.execute(text("""
        SELECT *
        FROM creator_application
        ORDER BY id DESC
        LIMIT 100
    """)).mappings().all()
    return {"applications": [dict(r) for r in rows]}
