
from flask import Blueprint

owner_bp = Blueprint("owner", __name__)

@owner_bp.route("/login")
def login():
    return "Owner hidden login ready"

@owner_bp.route("/panel")
def panel():
    return "Owner control panel ready"
