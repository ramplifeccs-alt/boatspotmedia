
import os
import requests


def send_download_email(to_email, download_url, video_title="Your BoatSpotMedia video", order_id=None):
    """
    Send buyer download email using SendGrid.
    Requires:
      SENDGRID_API_KEY
      SENDGRID_FROM_EMAIL
    Optional:
      SENDGRID_FROM_NAME
    """
    api_key = os.environ.get("SENDGRID_API_KEY")
    from_email = os.environ.get("SENDGRID_FROM_EMAIL")
    from_name = os.environ.get("SENDGRID_FROM_NAME", "BoatSpotMedia")

    if not api_key or not from_email or not to_email:
        try:
            print("SendGrid email skipped: missing api key/from/to")
        except Exception:
            pass
        return False

    subject = "Your BoatSpotMedia video is ready"
    html = f"""
    <div style="font-family:Arial,sans-serif;line-height:1.5;color:#0f172a;">
      <h2>Your BoatSpotMedia video is ready</h2>
      <p>Thank you for your purchase.</p>
      <p><strong>{video_title}</strong></p>
      {'<p>Order: ' + str(order_id) + '</p>' if order_id else ''}
      <p>
        <a href="{download_url}" style="display:inline-block;background:#2563eb;color:#fff;padding:12px 18px;border-radius:8px;text-decoration:none;font-weight:bold;">
          Download your video
        </a>
      </p>
      <p>If the button does not work, copy and paste this link into your browser:</p>
      <p style="word-break:break-all;">{download_url}</p>
      <hr>
      <p style="font-size:12px;color:#64748b;">BoatSpotMedia.com</p>
    </div>
    """

    payload = {
        "personalizations": [{"to": [{"email": to_email}]}],
        "from": {"email": from_email, "name": from_name},
        "subject": subject,
        "content": [{"type": "text/html", "value": html}],
    }

    try:
        resp = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=15,
        )
        if resp.status_code in (200, 202):
            return True
        try:
            print("SendGrid email failed:", resp.status_code, resp.text[:500])
        except Exception:
            pass
        return False
    except Exception as e:
        try:
            print("SendGrid email exception:", e)
        except Exception:
            pass
        return False
