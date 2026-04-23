
from flask import Blueprint

owner_bp = Blueprint("owner", __name__)

@owner_bp.route("/login")
def owner_login():
    return "Owner login hidden route"

@owner_bp.route("/panel")
def owner_panel():
    return "Owner control panel active"
