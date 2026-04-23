
from flask import Blueprint

ads_bp = Blueprint("ads", __name__)

@ads_bp.route("/dashboard")
def ads_dashboard():
    return "Service ads PPC dashboard ready"
