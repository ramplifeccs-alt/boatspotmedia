
from flask import Blueprint

creator_bp = Blueprint("creator", __name__)

@creator_bp.route("/dashboard")
def dashboard():
    return "Creator storage tracking + analytics ready"
