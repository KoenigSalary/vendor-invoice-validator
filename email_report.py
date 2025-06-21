from dotenv import load_dotenv
import smtplib
from email.message import EmailMessage
from email.utils import formataddr
import os
from datetime import datetime
import pandas as pd

# Load environment variables
load_dotenv()
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")

# Email recipients
to_list = ["ap@koenig-solutions.com"]
cc_list = [
    "aditya.singh@koenig-solutions.com",
    "tax@koenig-solutions.com",
    "sunil.kushwaha@koenig-solutions.com"
]

# File path and report name
today_str = datetime.now().strftime('%Y-%m-%d')
file_path = f"data/delta_report_{today_str}.xlsx"
filename = f"delta_report_{today_str}.xlsx"

# Check if file exists
if not os.path.exists(file_path):
    print(f"❌ Report file not found: {file_path}")
    exit()

# Load report and compute metrics
df = pd.read_excel(file_path)
total = len(df)

# Auto-detect validation status column
status_col = next((col for col in df.columns if "validation" in col.lower() and "status" in col.lower()), None)

if status_col:
    flagged = df[df[status_col].str.upper() == "FLAGGED"].shape[0]
    changed = df[df[status_col].str.upper() == "CHANGED"].shape[0]
else:
    flagged = changed = 0
    print("⚠️ 'Validation Status' column not found. Skipping flagged/changed counts.")

# Prepare email
msg = EmailMessage()
msg["Subject"] = f"Vendor Invoice Validation Report – {today_str}"
msg["From"] = formataddr(("Invoice Management Team", SMTP_USER))
msg["To"] = ", ".join(to_list)
msg["Cc"] = ", ".join(cc_list)

body = f"""
Dear Team,

Please find attached the Vendor Invoice Validation Report for {today_str}.

🔢 Total Invoices Checked: {total}
🚩 Flagged: {flagged}
✏️ Modified Since Last Check: {changed}

Regards,  
Invoice Management Team  
Koenig Solutions
"""

msg.set_content(body)

# Attach the Excel file
with open(file_path, "rb") as f:
    msg.add_attachment(f.read(), maintype="application",
                       subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       filename=filename)

# Send email
try:
    with smtplib.SMTP("smtp.office365.com", 587) as smtp:
        smtp.starttls()
        smtp.login(SMTP_USER, SMTP_PASS)
        smtp.send_message(msg)
    print("✅ Email sent successfully.")
except Exception as e:
    print(f"❌ Failed to send email: {e}")
