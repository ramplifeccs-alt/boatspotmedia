import os

def send_sms(to_phone: str, body: str) -> bool:
    """
    Optional SMS helper for BoatSpotMedia.

    Configure Twilio variables:
    TWILIO_ACCOUNT_SID
    TWILIO_AUTH_TOKEN
    TWILIO_FROM_PHONE

    Safe fallback: returns False if Twilio is not configured.
    """
    to_phone = (to_phone or "").strip()
    if not to_phone:
        return False

    sid = os.environ.get("TWILIO_ACCOUNT_SID")
    token = os.environ.get("TWILIO_AUTH_TOKEN")
    from_phone = os.environ.get("TWILIO_FROM_PHONE")

    if not sid or not token or not from_phone:
        print("SMS not sent: Twilio not configured.")
        return False

    try:
        from twilio.rest import Client
        client = Client(sid, token)
        client.messages.create(to=to_phone, from_=from_phone, body=body)
        return True
    except Exception as e:
        print("SMS send failed:", e)
        return False
