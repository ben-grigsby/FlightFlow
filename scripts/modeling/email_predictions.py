# scripts/modeling/email_predictions.py

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os


def send_flight_predictions_email(ti):
    """
    Pulls the Excel file path from XCom (predict_and_return_excel_path)
    and sends it as an email attachment via Gmail SMTP.
    """

    # Get file path from XCom
    excel_path = ti.xcom_pull(task_ids='predict_and_return_excel_path')

    if not excel_path or not os.path.exists(excel_path):
        raise FileNotFoundError(f"❌ Excel file not found at path: {excel_path}")

    # --- Email setup ---
    sender = "northern0207@gmail.com"              # <--- replace with your Gmail
    password = "482266070207"         # <--- use an app password if 2FA is enabled
    recipient = "northern0207@gmail.com"

    subject = "Future Flight Delay Predictions"
    body = "Attached is the latest future flight delay prediction report."

    # --- Build MIME message ---
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    # --- Attach Excel file ---
    with open(excel_path, "rb") as file:
        part = MIMEApplication(file.read(), Name=os.path.basename(excel_path))
    part["Content-Disposition"] = f'attachment; filename="{os.path.basename(excel_path)}"'
    msg.attach(part)

    # --- Send email via Gmail SMTP ---
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender, password)
        server.send_message(msg)

    print(f"✅ Email sent successfully to {recipient} with attachment: {excel_path}")