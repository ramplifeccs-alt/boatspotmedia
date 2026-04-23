
from flask import Blueprint

buyer_bp = Blueprint("buyer", __name__)

@buyer_bp.route("/orders")
def buyer_orders():
    return "Buyer orders dashboard"
