
import subprocess

def extract_creation_datetime(video_path):
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_entries", "format_tags=creation_time", video_path],
            capture_output=True,
            text=True
        )
        return result.stdout
    except Exception:
        return None
