from flask import Blueprint, render_template, request, redirect, url_for
from app.models import CharterListing, User
from app.services.db import db

charters_bp = Blueprint("charters", __name__)

@charters_bp.route("/dashboard")
def dashboard():
    listings = CharterListing.query.all()
    return render_template("charters/dashboard.html", listings=listings)

@charters_bp.route("/create", methods=["POST"])
def create():
    provider = User.query.filter_by(role="charter_provider").first()
    if not provider:
        provider = User(email="charter@test.com", role="charter_provider", display_name="Test Charter Provider")
        db.session.add(provider); db.session.flush()
    listing = CharterListing(
        provider_id=provider.id,
        title=request.form.get("title"),
        boat_name=request.form.get("boat_name"),
        location=request.form.get("location"),
        capacity=int(request.form.get("capacity") or 6),
        price_hour=float(request.form.get("price_hour") or 0),
        price_trip=float(request.form.get("price_trip") or 0),
        description=request.form.get("description"),
        status="active"
    )
    db.session.add(listing); db.session.commit()
    return redirect(url_for("charters.dashboard"))
