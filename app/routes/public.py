
from flask import Blueprint

public_bp = Blueprint("public", __name__)

@public_bp.route("/")
def homepage():
    return "BoatSpotMedia marketplace homepage ready 🚤"
