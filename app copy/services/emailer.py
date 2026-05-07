from flask import current_app

def send_email(to, subject, body):
    api_key = current_app.config.get("SENDGRID_API_KEY")
    sender = current_app.config.get("SENDGRID_SENDER")
    if not api_key:
        print(f"[EMAIL DEV MODE] To:{to} Subject:{subject}\n{body}")
        return True
    try:
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Mail
        msg = Mail(from_email=sender, to_emails=to, subject=subject, plain_text_content=body)
        SendGridAPIClient(api_key).send(msg)
        return True
    except Exception as e:
        print("SendGrid error:", e)
        return False
