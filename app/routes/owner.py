from datetime import datetime, timedelta
from flask import Blueprint, render_template, request, redirect, url_for
from werkzeug.security import generate_password_hash
from app.models import CreatorApplication, User, CreatorProfile, StoragePlan, ServiceAd, CharterListing
from app.services.db import db
from app.services.emailer import send_email

owner_bp = Blueprint("owner", __name__)

@owner_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        return redirect(url_for("owner.panel"))
    return render_template("owner/login.html")

@owner_bp.route("/panel")
def panel():
    return render_template("owner/panel.html",
        apps=CreatorApplication.query.order_by(CreatorApplication.submitted_at.desc()).all(),
        plans=StoragePlan.query.all(),
        creators=CreatorProfile.query.all()
    )

@owner_bp.route("/applications/<int:app_id>/approve", methods=["POST"])
def approve(app_id):
    app = CreatorApplication.query.get_or_404(app_id)
    app.status = "approved"; app.reviewed_at = datetime.utcnow()
    user = User(email=app.email, role="creator", display_name=f"{app.first_name} {app.last_name}", is_active=True)
    user.password_hash = generate_password_hash("TempCreator123!")
    db.session.add(user); db.session.flush()
    plan = StoragePlan.query.first()
    creator = CreatorProfile(user_id=user.id, plan_id=plan.id if plan else None, approved=True,
                             storage_limit_gb=plan.storage_limit_gb if plan else 512,
                             commission_rate=plan.commission_rate if plan else 20)
    db.session.add(creator)
    db.session.commit()
    send_email(app.email, "BoatSpotMedia creator approved", "Your creator account was approved. Login at /creator/login")
    return redirect(url_for("owner.panel"))

@owner_bp.route("/applications/<int:app_id>/reject", methods=["POST"])
def reject(app_id):
    app = CreatorApplication.query.get_or_404(app_id)
    app.status = "rejected"; app.reviewed_at = datetime.utcnow()
    db.session.commit()
    send_email(app.email, "BoatSpotMedia creator application", "Your creator application was not approved.")
    return redirect(url_for("owner.panel"))

@owner_bp.route("/plans/create", methods=["POST"])
def create_plan():
    plan = StoragePlan(
        name=request.form.get("name"),
        storage_limit_gb=int(request.form.get("storage_limit_gb")),
        monthly_price=request.form.get("monthly_price"),
        commission_rate=int(request.form.get("commission_rate"))
    )
    db.session.add(plan); db.session.commit()
    return redirect(url_for("owner.panel"))

@owner_bp.route("/creator/<int:creator_id>/override", methods=["POST"])
def override_commission(creator_id):
    c = CreatorProfile.query.get_or_404(creator_id)
    c.commission_override_rate = int(request.form.get("rate"))
    c.commission_override_until = datetime.utcnow() + timedelta(days=int(request.form.get("days") or 30))
    db.session.commit()
    return redirect(url_for("owner.panel"))


@owner_bp.route("/reset-db-danger", methods=["POST"])
def reset_db():
    # TESTING ONLY: drops and recreates all tables.
    from app.services.db import db
    db.drop_all()
    db.create_all()
    return "Database reset. Redeploy/reload app to seed defaults."
@owner_bp.route("/repair-db-now")
def repair_db_now():
    from app.services.db_repair import repair_all_known_tables
    repair_all_known_tables()
    return "DB repair completed."

@owner_bp.route("/applications-raw")
def applications_raw():
    from sqlalchemy import text
    from app.services.db_repair import repair_creator_application_table
    repair_creator_application_table()
    rows = db.session.execute(text("""
        SELECT *
        FROM creator_application
        ORDER BY id DESC
        LIMIT 100
    """)).mappings().all()
    return {"applications": [dict(r) for r in rows]}
