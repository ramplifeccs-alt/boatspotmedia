from app import app

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080)

# BoatSpotMedia payments routes registration v41.5
try:
    from app.routes.payments import payments_bp
    app.register_blueprint(payments_bp)
except Exception as e:
    try:
        print("payments blueprint registration warning:", e)
    except Exception:
        pass



# BoatSpotMedia v43.6 direct app-level R2 download routes
def _bsm_r2_download_response_v436(video_id):
    from flask import session, redirect
    try:
        from app import db
    except Exception:
        from __main__ import db

    if not session.get("user_id") or session.get("user_role") != "buyer":
        session["after_login_redirect"] = "/buyer/dashboard"
        session.modified = True
        return redirect("/buyer/login?next=/buyer/dashboard")

    uid = session.get("user_id")
    email = (session.get("user_email") or "").lower()

    try:
        db.session.execute(db.text("ALTER TABLE bsm_cart_order ADD COLUMN IF NOT EXISTS buyer_user_id INTEGER"))
        db.session.commit()
    except Exception:
        db.session.rollback()

    purchased = None

    # First treat id as video_id.
    try:
        purchased = db.session.execute(db.text("""
            SELECT i.id AS order_item_id, i.video_id, i.delivery_status, i.package,
                   o.id AS order_id, o.buyer_user_id, o.buyer_email, o.status
            FROM bsm_cart_order_item i
            JOIN bsm_cart_order o ON o.id = i.cart_order_id
            WHERE i.video_id = :vid
              AND (
                    o.buyer_user_id = :uid
                    OR lower(coalesce(o.buyer_email,'')) = lower(:email)
                  )
            ORDER BY o.created_at DESC
            LIMIT 1
        """), {"vid": video_id, "uid": uid or 0, "email": email}).mappings().first()
    except Exception:
        db.session.rollback()
        purchased = None

    # Fallback treat id as order_item_id.
    if not purchased:
        try:
            purchased = db.session.execute(db.text("""
                SELECT i.id AS order_item_id, i.video_id, i.delivery_status, i.package,
                       o.id AS order_id, o.buyer_user_id, o.buyer_email, o.status
                FROM bsm_cart_order_item i
                JOIN bsm_cart_order o ON o.id = i.cart_order_id
                WHERE i.id = :item_id
                  AND (
                        o.buyer_user_id = :uid
                        OR lower(coalesce(o.buyer_email,'')) = lower(:email)
                      )
                ORDER BY o.created_at DESC
                LIMIT 1
            """), {"item_id": video_id, "uid": uid or 0, "email": email}).mappings().first()
        except Exception:
            db.session.rollback()
            purchased = None

    if not purchased:
        return "Download not found: this video is not linked to your paid orders.", 404

    if str(purchased.get("delivery_status") or "").lower() in ["pending_edit","editing","not_ready","pending"]:
        return "This edited video is not ready for download yet.", 400

    real_video_id = purchased.get("video_id") or video_id

    try:
        video = db.session.execute(db.text("SELECT * FROM video WHERE id=:vid LIMIT 1"), {"vid": real_video_id}).mappings().first()
    except Exception:
        db.session.rollback()
        video = None

    if not video:
        return "Video record not found.", 404

    # IMPORTANT: use R2 key/path columns first.
    r2_keys = [
        "r2_video_key",
        "r2_key",
        "video_key",
        "storage_key",
        "file_path",
        "original_file_path",
        "original_path",
        "internal_filename",
        "filename",
    ]
    url_keys = [
        "public_url",
        "download_url",
        "file_url",
        "original_url",
        "video_url",
        "r2_public_url",
    ]

    # If a full public/signed URL exists, use it.
    for key in url_keys:
        val = video.get(key) if key in video else None
        if val:
            val = str(val).strip()
            if val.startswith("http://") or val.startswith("https://") or val.startswith("/"):
                return redirect(val)

    # For R2 object keys, reuse app's /media/<key> route.
    # This app already serves thumbnails/previews/videos from R2 through /media.
    for key in r2_keys:
        val = video.get(key) if key in video else None
        if val:
            val = str(val).strip()
            if not val:
                continue
            if val.startswith("http://") or val.startswith("https://") or val.startswith("/"):
                return redirect(val)
            return redirect("/media/" + val.lstrip("/"))

    return "R2 video key was not found in the video record. Contact support with Order #" + str(purchased.get("order_id")), 404


try:
    @app.route("/download-video/<int:video_id>")
    @app.route("/download-item/<int:video_id>")
    @app.route("/buyer/download-item/<int:video_id>")
    def bsm_app_download_video_v436(video_id):
        return _bsm_r2_download_response_v436(video_id)
except Exception as e:
    try:
        print("v43.6 app-level download route warning:", e)
    except Exception:
        pass

