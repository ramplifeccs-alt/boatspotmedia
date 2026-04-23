
from flask import Blueprint
from app.db.connection import get_db

analytics_bp = Blueprint("analytics", __name__)

@analytics_bp.route("/click/<creator_id>")
def register_click(creator_id):

    db=get_db()
    cur=db.cursor()

    cur.execute(
        "UPDATE creator_click_stats SET clicks_lifetime = clicks_lifetime + 1 WHERE creator_id=%s",
        (creator_id,)
    )

    db.commit()

    return "click recorded"
