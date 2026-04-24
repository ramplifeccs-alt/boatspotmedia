from flask import Blueprint, render_template, request, redirect, url_for
from werkzeug.security import generate_password_hash
from app.models import ServiceAccount, ServiceAd
from app.services.db import db
from app.services.db_repair import repair_all_known_tables

services_bp = Blueprint("services_panel", __name__, url_prefix="/service-account")

def current_service_account():
    repair_all_known_tables()
    acct = ServiceAccount.query.first()
    if not acct:
        acct = ServiceAccount(
            business_name="Demo Marine Service",
            contact_name="Service Owner",
            email="service@example.com",
            password_hash=generate_password_hash("demo"),
            balance=25,
            is_active=True
        )
        db.session.add(acct)
        db.session.commit()
    return acct

@services_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        return redirect(url_for("services_panel.dashboard"))
    return render_template("services_panel/login.html")

@services_bp.route("/dashboard")
def dashboard():
    acct = current_service_account()
    ads = ServiceAd.query.filter_by(service_account_id=acct.id).order_by(ServiceAd.id.desc()).all()
    total_clicks = sum([(a.clicks or 0) for a in ads])
    return render_template("services_panel/dashboard.html", acct=acct, ads=ads, total_clicks=total_clicks)

@services_bp.route("/ads", methods=["GET", "POST"])
def ads():
    acct = current_service_account()
    if request.method == "POST":
        ad = ServiceAd(
            service_account_id=acct.id,
            title=request.form.get("title"),
            description=request.form.get("description"),
            website_url=request.form.get("website_url"),
            phone=request.form.get("phone"),
            category=request.form.get("category"),
            location=request.form.get("location"),
            cost_per_click=float(request.form.get("cost_per_click") or 0.15),
            active=True
        )
        db.session.add(ad)
        db.session.commit()
        return redirect(url_for("services_panel.ads"))
    ads = ServiceAd.query.filter_by(service_account_id=acct.id).order_by(ServiceAd.id.desc()).all()
    return render_template("services_panel/ads.html", acct=acct, ads=ads)

@services_bp.route("/ads/<int:ad_id>/pause", methods=["POST"])
def pause_ad(ad_id):
    acct = current_service_account()
    ad = ServiceAd.query.filter_by(id=ad_id, service_account_id=acct.id).first_or_404()
    ad.active = False
    db.session.commit()
    return redirect(url_for("services_panel.ads"))

@services_bp.route("/ads/<int:ad_id>/activate", methods=["POST"])
def activate_ad(ad_id):
    acct = current_service_account()
    ad = ServiceAd.query.filter_by(id=ad_id, service_account_id=acct.id).first_or_404()
    ad.active = True
    db.session.commit()
    return redirect(url_for("services_panel.ads"))

@services_bp.route("/billing", methods=["GET", "POST"])
def billing():
    acct = current_service_account()
    if request.method == "POST":
        acct.balance = float(acct.balance or 0) + float(request.form.get("amount") or 0)
        db.session.commit()
        return redirect(url_for("services_panel.billing"))
    return render_template("services_panel/billing.html", acct=acct)
