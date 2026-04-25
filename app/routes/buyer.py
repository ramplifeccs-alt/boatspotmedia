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
