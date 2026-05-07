import os, subprocess, json

def _probe_duration(video_path):
    try:
        result = subprocess.run(["ffprobe","-v","error","-show_entries","format=duration","-of","json",video_path], capture_output=True, text=True, timeout=45)
        return float(json.loads(result.stdout or "{}").get("format",{}).get("duration") or 0)
    except Exception:
        return 0.0

def generate_video_thumbnail(video_path, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    middle = max(_probe_duration(video_path) / 2, 1.0)
    vf = "crop=iw*0.70:ih*0.70:iw*0.15:ih*0.15,scale=1280:-2"
    for filters in [vf, None]:
        cmd = ["ffmpeg","-y","-ss",str(middle),"-i",video_path,"-frames:v","1"]
        if filters:
            cmd += ["-vf", filters]
        cmd += ["-q:v","2",output_path]
        try:
            subprocess.run(cmd, capture_output=True, text=True, timeout=90, check=True)
            if os.path.exists(output_path) and os.path.getsize(output_path)>0:
                return output_path
        except Exception as e:
            print("thumbnail warning:", e)
    return None
