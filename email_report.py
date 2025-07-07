import os
import smtplib
import pandas as pd
from email.message import EmailMessage
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")

def send_email_report(report_path, zip_path):
    if not os.path.exists(report_path):
        print("❌ No report found to email.")
        return

    # Load report to generate summary
    try:
        df = pd.read_excel(report_path)
    except Exception as e:
        print(f"❌ Failed to read report: {e}")
        return

    # Identify validation column
    col_candidates = [col for col in df.columns if "validation" in col.lower() or "correct" in col.lower()]
    if not col_candidates:
        print("❌ No validation column found.")
        return

    validation_col = col_candidates[0]
    total = len(df)
    correct = df[df[validation_col].astype(str).str.startswith("✅", na=False)].shape[0]
    flagged = df[df[validation_col].astype(str).str.startswith("🚩", na=False)].shape[0]
    modified = df[df[validation_col].astype(str).str.startswith("✏️", na=False)].shape[0]
    late = df[df[validation_col].astype(str).str.contains("Late Upload", na=False)].shape[0]

    # Prepare email
    today_str = datetime.now().strftime("%Y-%m-%d")
    msg = EmailMessage()
    msg["Subject"] = f"Vendor Invoice Validation Report - {today_str}"
    msg["From"] = SMTP_USER
    msg["To"] = "tax@koenig-solutions.com"
    # Optional test copy
    # msg["Cc"] = "your.email@koenig-solutions.com"

    body = f"""Dear Team,

Please find attached the Vendor Invoice Validation Report for {today_str}.

🔢 Total Invoices Checked: {total}
✅ Correct: {correct}
🚩 Flagged: {flagged}
✏️ Modified Since Last Check: {modified}
📌 Late Uploads: {late}

Attachments:
1. 📊 Excel Report (validation_result.xlsx)
2. 📦 Zipped Invoices (invoices.koenigzip)

Regards,  
Invoice Management Team  
Koenig Solutions
"""
    msg.set_content(body)

    # Attach Excel report
    with open(report_path, "rb") as f:
        msg.add_attachment(f.read(),
                           maintype="application",
                           subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           filename="validation_result.xlsx")

    # Attach renamed ZIP file
    if os.path.exists(zip_path):
        with open(zip_path, "rb") as f:
            msg.add_attachment(f.read(),
                               maintype="application",
                               subtype="octet-stream",
                               filename="invoices.koenigzip")
        print("📎 ZIP file attached as invoices.koenigzip")
    else:
        print("⚠️ ZIP file not found, only report will be sent.")

    # Send email
    try:
        print("📨 Connecting to Office365 SMTP...")
        with smtplib.SMTP("smtp.office365.com", 587) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        print(f"📧 Email sent successfully to {msg['To']}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
