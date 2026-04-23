
from flask import Blueprint

charters_bp = Blueprint("charters", __name__)

@charters_bp.route("/listings")
def listings():
    return "Charter marketplace ready"
