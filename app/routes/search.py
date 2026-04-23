
from flask import Blueprint, request, jsonify
from app.db.connection import get_db

search_bp = Blueprint("search", __name__)

@search_bp.route("/videos")
def search_videos():
    location=request.args.get("location")

    db=get_db()
    cur=db.cursor()

    cur.execute(
        "SELECT id,location,recorded_at FROM videos WHERE location=%s LIMIT 10",
        (location,)
    )

    rows=cur.fetchall()

    return jsonify({"results":rows})
