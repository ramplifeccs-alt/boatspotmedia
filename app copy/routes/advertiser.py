from flask import Blueprint, render_template, request, redirect, url_for
from app.models import AdvertiserProfile, ServiceAd, User, AdClick
from app.services.db import db

advertiser_bp = Blueprint("advertiser", __name__)

@advertiser_bp.route("/dashboard")
def dashboard():
    advertiser = AdvertiserProfile.query.first()
    ads = ServiceAd.query.all()
    return render_template("advertiser/dashboard.html", advertiser=advertiser, ads=ads)

@advertiser_bp.route("/create-ad", methods=["POST"])
def create_ad():
    advertiser = AdvertiserProfile.query.first()
    if not advertiser:
        user = User(email="advertiser@test.com", role="advertiser", display_name="Test Advertiser")
        db.session.add(user); db.session.flush()
        advertiser = AdvertiserProfile(user_id=user.id, balance=25)
        db.session.add(advertiser); db.session.flush()
    ad = ServiceAd(
        advertiser_id=advertiser.id,
        title=request.form.get("title"),
        description=request.form.get("description"),
        website_url=request.form.get("website_url"),
        target_location=request.form.get("target_location"),
        cost_per_click=float(request.form.get("cost_per_click") or 0.15),
        status="active" if float(advertiser.balance or 0) > 0 else "paused"
    )
    db.session.add(ad); db.session.commit()
    return redirect(url_for("advertiser.dashboard"))

@advertiser_bp.route("/click/<int:ad_id>")
def ad_click(ad_id):
    ad = ServiceAd.query.get_or_404(ad_id)
    adv = AdvertiserProfile.query.get(ad.advertiser_id)
    if ad.status != "active" or float(adv.balance or 0) <= 0:
        return "Ad inactive", 403
    adv.balance = float(adv.balance) - float(ad.cost_per_click)
    if adv.balance <= 0:
        adv.balance = 0
        ad.status = "paused"
    db.session.add(AdClick(ad_id=ad.id, ip_area="approx-location-placeholder"))
    db.session.commit()
    return redirect(ad.website_url or "/services")
