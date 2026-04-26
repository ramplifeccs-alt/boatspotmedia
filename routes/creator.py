
from flask import Blueprint, jsonify, request, current_app
from app import db

creator_bp = Blueprint("creator", __name__)

def _ffmpeg_bin():
    try:
        import imageio_ffmpeg
        path = imageio_ffmpeg.get_ffmpeg_exe()
        if path:
            return path
    except Exception:
        pass
    return "ffmpeg"

def _thumb_image_is_dark(path):
    try:
        from PIL import Image, ImageStat
        img = Image.open(path).convert("L").resize((80, 45))
        stat = ImageStat.Stat(img)
        mean = stat.mean[0] if stat.mean else 0
        lo, hi = img.getextrema()
        return mean < 18 and (hi - lo) < 45
    except Exception:
        return False

def _ffmpeg_duration_seconds(local_video):
    try:
        import subprocess, re
        ffmpeg = _ffmpeg_bin()
        result = subprocess.run(
            [ffmpeg, "-hide_banner", "-i", local_video],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=20,
        )
        text = (result.stderr or "") + "\n" + (result.stdout or "")
        m = re.search(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)", text)
        if not m:
            return 0
        return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))
    except Exception:
        return 0

def _run_ffmpeg_thumbnail(input_path, output_path):
    import subprocess
    import os

    ffmpeg = _ffmpeg_bin()

    commands = [

        # FAST KEYFRAME SEEK (best for HEVC cameras)
        [
            ffmpeg,
            "-y",
            "-skip_frame", "nokey",
            "-i", input_path,
            "-frames:v", "1",
            "-vf", "scale=1664:-1,crop=1280:720",
            "-q:v", "2",
            output_path,
        ],

        # fallback: 50% midpoint approx
        [
            ffmpeg,
            "-y",
            "-ss", "15",
            "-i", input_path,
            "-frames:v", "1",
            "-vf", "scale=1664:-1,crop=1280:720",
            "-q:v", "2",
            output_path,
        ],

        # fallback: thumbnail filter
        [
            ffmpeg,
            "-y",
            "-i", input_path,
            "-vf", "thumbnail,scale=1664:-1,crop=1280:720",
            "-frames:v", "1",
            "-q:v", "2",
            output_path,
        ],
    ]

    for cmd in commands:
        try:
            if os.path.exists(output_path):
                os.remove(output_path)

            subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=25,
            )

            if os.path.exists(output_path) and os.path.getsize(output_path) > 1000:
                return True

        except Exception:
            pass

    return False
def _generate_and_attach_thumbnail_for_video(video):
    try:
        import os, tempfile, uuid, shutil
        from app.services.r2 import get_r2_client, _bucket_name

        client = get_r2_client()
        bucket = _bucket_name()

        tmp_dir = tempfile.mkdtemp()
        local_video = os.path.join(tmp_dir, "in.mp4")
        local_thumb = os.path.join(tmp_dir, "out.jpg")

        client.download_file(bucket, video.r2_video_key, local_video)

        ok = _run_ffmpeg_thumbnail(local_video, local_thumb)
        if not ok:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return False

        thumb_key = f"thumbs/{uuid.uuid4().hex}.jpg"
        client.upload_file(local_thumb, bucket, thumb_key, ExtraArgs={"ContentType": "image/jpeg"})

        video.thumbnail_path = thumb_key
        db.session.add(video)
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return True
    except Exception:
        return False

def _thumbnail_background_worker(app, video_ids):
    try:
        with app.app_context():
            from app.models import Video
            for vid in video_ids:
                v = Video.query.get(vid)
                if not v:
                    continue
                try:
                    _generate_and_attach_thumbnail_for_video(v)
                    db.session.commit()
                except Exception:
                    db.session.rollback()
    except Exception:
        pass

def _schedule_thumbnail_generation(video_ids):
    try:
        import threading
        app = current_app._get_current_object()
        t = threading.Thread(target=_thumbnail_background_worker, args=(app, video_ids), daemon=True)
        t.start()
    except Exception:
        pass

@creator_bp.route("/creator/upload/r2/complete", methods=["POST"])
def upload_r2_complete():
    # Simulated success response and background processing
    created_ids = request.json.get("video_ids", [])
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({"ok": False}), 500

    _schedule_thumbnail_generation(created_ids)
    return jsonify({"ok": True})
