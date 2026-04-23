
from flask import Blueprint

buyer_bp = Blueprint("buyer", __name__)

@buyer_bp.route("/orders")
def orders():
    return "Download token expiration system ready"
