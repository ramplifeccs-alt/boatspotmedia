
from flask import Blueprint, request, jsonify
from app.services.metadata_service import extract_creation_time
from app.services.storage_service import update_storage

upload_bp = Blueprint("upload", __name__)

@upload_bp.route("/upload", methods=["POST"])
def upload_video():
    creator_id=request.form.get("creator_id")
    file_size=int(request.form.get("file_size",0))

    recorded_at="metadata_pending"

    update_storage(creator_id,file_size)

    return jsonify({
        "status":"upload-registered",
        "recorded_at":recorded_at
    })
