
import subprocess, json

def extract_creation_time(video_path):
    cmd=[
        "ffprobe","-v","quiet",
        "-print_format","json",
        "-show_entries","format_tags=creation_time",
        video_path
    ]
    result=subprocess.run(cmd,capture_output=True,text=True)
    try:
        data=json.loads(result.stdout)
        return data["format"]["tags"]["creation_time"]
    except:
        return None
