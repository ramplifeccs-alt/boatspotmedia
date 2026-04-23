
from flask import Blueprint

owner_bp = Blueprint("owner", __name__)

@owner_bp.route("/panel")
def panel():
    return "Owner plan limits + commissions ready"
