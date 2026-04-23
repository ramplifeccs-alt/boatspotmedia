
from flask import Blueprint

charters_bp = Blueprint("charters", __name__)

@charters_bp.route("/listings")
def charter_listings():
    return "Charter marketplace active"
