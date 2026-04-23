
import subprocess

def generate_thumbnail(video_path, output_path):
    subprocess.run([
        "ffmpeg",
        "-i", video_path,
        "-vf", "select=eq(n\,1)",
        "-q:v", "3",
        output_path
    ])
    return output_path
