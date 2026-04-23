
from flask import Blueprint

public_bp = Blueprint("public", __name__)

@public_bp.route("/")
def homepage():
    return "BoatSpotMedia v12.6 runtime engine active"
