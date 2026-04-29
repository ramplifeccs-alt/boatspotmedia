
from flask import Blueprint, request, redirect, render_template, jsonify
from app.services.cart import add_video_to_cart, remove_item, clear_cart, cart_summary, build_cart_display_items

cart_bp = Blueprint("cart", __name__)

@cart_bp.route("/cart")
def view_cart():
    return render_template("cart/view.html", cart=cart_summary(), items=build_cart_display_items())

@cart_bp.route("/cart/add/video/<int:video_id>", methods=["POST","GET"])
def add_video(video_id):
    from app.models import Video
    video = Video.query.get_or_404(video_id)
    add_video_to_cart(video, package=request.values.get("package","original"), price_id=request.values.get("price_id"))
    return redirect("/cart")

@cart_bp.route("/cart/remove/<int:index>", methods=["POST"])
def remove_cart_item(index):
    remove_item(index)
    return redirect("/cart")

@cart_bp.route("/cart/clear", methods=["POST"])
def clear_cart_route():
    clear_cart()
    return redirect("/cart")

@cart_bp.route("/cart/status")
def cart_status():
    return jsonify(cart_summary())
