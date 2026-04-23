
from flask import Blueprint, request, jsonify

search_bp = Blueprint("search", __name__)

@search_bp.route("/videos")
def search_videos():
    location = request.args.get("location")
    date = request.args.get("date")
    time_range = request.args.get("time_range")

    return jsonify({
        "status": "ok",
        "filters": {
            "location": location,
            "date": date,
            "time_range": time_range
        }
    })
