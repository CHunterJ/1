"""
Automated email sender for Gmail – hard‑coded credentials version.
⚠️  SECURITY WARNING
Embedding your real Gmail password in code is risky. Google recommends using an
App Password (https://myaccount.google.com/apppasswords) or an environment
variable instead. Proceed only if you understand the implications.
"""

import smtplib
from email.message import EmailMessage

# SMTP server configuration
HOST = "smtp.gmail.com"
PORT = 465  # SSL port

# *** User credentials (hard‑coded as requested) ***
USERNAME = "chujorgensen@gmail.com"
PASSWORD = "Alamo4967"  # Consider using an app password!

def send_email(subject: str, body: str, to_address: str) -> None:
    """Send a plain‑text email via Gmail."""
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = USERNAME
    msg["To"] = to_address
    msg.set_content(body)

    with smtplib.SMTP_SSL(HOST, PORT) as server:
        server.login(USERNAME, PASSWORD)
        server.send_message(msg)

    print(f"✅ Email sent to {to_address}")

if __name__ == "__main__":
    # Example usage
    send_email(
        subject="Automated Test",
        body="This is an automated email sent from Python.",
        to_address="recipient@example.com",
    )