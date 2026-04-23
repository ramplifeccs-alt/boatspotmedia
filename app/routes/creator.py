
from flask import Blueprint

creator_bp = Blueprint("creator", __name__)

@creator_bp.route("/login")
def creator_login():
    return "Creator login hidden route"

@creator_bp.route("/dashboard")
def creator_dashboard():
    return "Creator dashboard active"
