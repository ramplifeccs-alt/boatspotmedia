import os, subprocess, json, tempfile
from datetime import datetime

def extract_creation_time(path):
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_entries", "format_tags=creation_time", path],
            capture_output=True, text=True, timeout=30
        )
        data = json.loads(result.stdout or "{}")
        raw = data.get("format", {}).get("tags", {}).get("creation_time")
        if raw:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        pass
    return None

def generate_center_thumbnail(video_path, output_path):
    # Best-effort center frame: seek to 50% using ffprobe duration. Fallback to 2 seconds.
    seek = "00:00:02"
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=nw=1:nk=1", video_path],
            capture_output=True, text=True, timeout=30
        )
        duration = float(result.stdout.strip())
        seek_seconds = max(1, duration / 2)
        seek = str(seek_seconds)
    except Exception:
        pass

    subprocess.run(
        ["ffmpeg", "-y", "-ss", seek, "-i", video_path, "-frames:v", "1", "-q:v", "3", output_path],
        capture_output=True, text=True, timeout=60
    )
    return output_path
