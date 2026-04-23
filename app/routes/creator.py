
from flask import Blueprint

creator_bp = Blueprint("creator", __name__)

@creator_bp.route("/login")
def login():
    return "Creator hidden login ready"

@creator_bp.route("/dashboard")
def dashboard():
    return "Creator dashboard ready"
