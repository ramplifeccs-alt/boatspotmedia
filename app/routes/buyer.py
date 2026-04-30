from flask import Blueprint, render_template, session, redirect

buyer_bp = Blueprint("buyer", __name__)


@buyer_bp.route("/dashboard")
def dashboard():
    if not session.get("user_id"):
        return redirect("/login")
    return render_template(
        "buyer/dashboard.html",
        display_name=session.get("display_name") or session.get("user_email") or "Buyer",
        email=session.get("user_email"),
    )


@buyer_bp.route("/orders")
def orders():
    if not session.get("user_id"):
        return redirect("/login")
    return render_template("buyer/orders.html")


@buyer_bp.route("/downloads")
def downloads():
    if not session.get("user_id"):
        return redirect("/login")
    return render_template("buyer/downloads.html")



@buyer_bp.route("/buyer/dashboard")
def buyer_order_dashboard_v416():
    """
    Buyer order dashboard placeholder.
    Full buyer account registration/login will connect this to order history.
    """
    return render_template("buyer/dashboard.html")


@buyer_bp.route("/buyer/register", methods=["GET", "POST"])
def buyer_register_v416():
    return render_template("buyer/register.html")


@buyer_bp.route("/buyer/login", methods=["GET", "POST"])
def buyer_login_v416():
    return render_template("buyer/login.html")
