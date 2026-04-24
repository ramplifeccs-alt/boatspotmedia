from flask import Blueprint, render_template, request
from datetime import datetime
from sqlalchemy import text
from app.models import Video, Location, ServiceAd, CharterListing
from app.services.db import db

public_bp = Blueprint("public", __name__)

@public_bp.route("/")
def home():
    try:
        latest = Video.query.filter_by(status="active").order_by(Video.created_at.desc()).limit(20).all()
    except Exception:
        db.session.rollback()
        latest = []
    selected, used = [], set()
    for v in latest:
        cid = getattr(v, "creator_id", None)
        if cid not in used:
            selected.append(v); used.add(cid)
        if len(selected) == 3:
            break
    for v in latest:
        if len(selected) == 3:
            break
        if v not in selected:
            selected.append(v)
    return render_template("public/home.html", videos=selected)

@public_bp.route("/search")
def search_page():
    try:
        locations = Location.query.order_by(Location.name.asc()).all()
    except Exception:
        db.session.rollback()
        locations = []
    return render_template("public/search.html", locations=locations, results=None)

@public_bp.route("/search/results")
def search_results():
    location = request.args.get("location")
    date_s = request.args.get("date")
    start_s = request.args.get("start_time")
    end_s = request.args.get("end_time")
    results = []
    try:
        q = Video.query.filter_by(status="active")
        if location:
            q = q.filter(Video.location == location)
        if date_s and start_s and end_s:
            d = datetime.strptime(date_s, "%Y-%m-%d").date()
            start_dt = datetime.combine(d, datetime.strptime(start_s, "%H:%M").time())
            end_dt = datetime.combine(d, datetime.strptime(end_s, "%H:%M").time())
            q = q.filter(Video.recorded_at >= start_dt, Video.recorded_at <= end_dt)
        results = q.order_by(Video.recorded_at.asc()).limit(200).all()
    except Exception:
        db.session.rollback()
    try:
        locations = Location.query.order_by(Location.name.asc()).all()
    except Exception:
        db.session.rollback()
        locations = []
    return render_template("public/search.html", locations=locations, results=results)

@public_bp.route("/preview/<int:video_id>")
def preview_video(video_id):
    from app.models import CreatorClickStats
    v = Video.query.get_or_404(video_id)
    stats = CreatorClickStats.query.filter_by(creator_id=v.creator_id).first()
    if not stats:
        stats = CreatorClickStats(creator_id=v.creator_id)
        db.session.add(stats)
    stats.clicks_today += 1
    stats.clicks_week += 1
    stats.clicks_month += 1
    stats.clicks_lifetime += 1
    db.session.commit()
    return render_template("public/preview.html", video=v)

@public_bp.route("/apply-creator", methods=["GET", "POST"])
def apply_creator():
    if request.method == "POST":
        social_fields = [request.form.get(k) for k in ["instagram", "facebook", "youtube", "tiktok"]]
        if not any(social_fields):
            return render_template("public/apply_creator.html", error="At least one social media link is required.")

        try:
            from app.services.db_repair import repair_creator_application_table
            repair_creator_application_table()

            with db.engine.begin() as conn:
                result = conn.execute(text("""
                    INSERT INTO creator_application
                    (first_name, last_name, email, instagram, facebook, youtube, tiktok, status, submitted_at)
                    VALUES
                    (:first_name, :last_name, :email, :instagram, :facebook, :youtube, :tiktok, 'pending', CURRENT_TIMESTAMP)
                    RETURNING id
                """), {
                    "first_name": request.form.get("first_name", ""),
                    "last_name": request.form.get("last_name", ""),
                    "email": request.form.get("email", ""),
                    "instagram": request.form.get("instagram", ""),
                    "facebook": request.form.get("facebook", ""),
                    "youtube": request.form.get("youtube", ""),
                    "tiktok": request.form.get("tiktok", "")
                })
                app_id = result.scalar()
            return render_template("public/apply_creator.html", success=True, application_id=app_id)

        except Exception as e:
            db.session.rollback()
            return render_template(
                "public/apply_creator.html",
                error=f"Application could not be saved yet. Error: {str(e)[:300]}"
            )

    return render_template("public/apply_creator.html")

@public_bp.route("/services")
def services():
    try:
        ads = ServiceAd.query.filter_by(status="active").all()
    except Exception:
        db.session.rollback()
        ads = []
    return render_template("public/services.html", ads=ads)

@public_bp.route("/charters")
def charters_public():
    try:
        listings = CharterListing.query.filter_by(status="active").all()
    except Exception:
        db.session.rollback()
        listings = []
    return render_template("public/charters.html", listings=listings)
