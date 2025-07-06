# email_report.py

import os
import smtplib
import pandas as pd
from email.message import EmailMessage
from email.utils import formataddr
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

    # Load report
    try:
        df = pd.read_excel(report_path)
    except Exception as e:
        print(f"❌ Failed to read report: {e}")
        return

    # Summary counts
    col_candidates = [col for col in df.columns if "validation" in col.lower()]
    if not col_candidates:
        print("❌ No 'Validation' column found.")
        return

    validation_col = col_candidates[0]
    total = len(df)
    correct = df[df[validation_col].astype(str).str.startswith("✅", na=False)].shape[0]
    flagged = df[df[validation_col].astype(str).str.startswith("🚩", na=False)].shape[0]
    modified = df[df[validation_col].astype(str).str.startswith("✏️", na=False)].shape[0]
    late = df[df[validation_col].astype(str).str.contains("Late Upload", na=False)].shape[0]

    # Compose email
    today_str = datetime.now().strftime("%Y-%m-%d")
    msg = EmailMessage()
    msg["Subject"] = f"Vendor Invoice Validation Report - {today_str}"
    msg["From"] = formataddr(("Invoice Management Team", SMTP_USER))
    msg["To"] = "tax@koenig-solutions.com"

    body = f"""Dear Team,

Please find attached the Vendor Invoice Validation Report for {today_str}.

🔢 Total Invoices Checked: {total}
✅ Correct: {correct}
🚩 Flagged: {flagged}
✏️ Modified Since Last Check: {modified}
📌 Late Uploads: {late}
2. 📦 Zip file of invoice PDFs

Regards,  
Invoice Management Team  
Koenig Solutions
"""
    msg.set_content(body)

    # Attach Excel
    with open(report_path, "rb") as f:
        msg.add_attachment(f.read(), maintype="application",
                           subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           filename="validation_result.xlsx")

    # Attach ZIP
    if os.path.exists(zip_path):
        with open(zip_path, "rb") as f:
            msg.add_attachment(f.read(), maintype="application",
                               subtype="zip", filename="invoices.zip")
    else:
        print("⚠️ Zip file not found, sending only report.")

    # Send email
    try:
        with smtplib.SMTP("smtp.office365.com", 587) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        print("📧 Email sent successfully to tax@koenig-solutions.com")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
