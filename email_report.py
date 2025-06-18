from dotenv import load_dotenv
import smtplib
from email.message import EmailMessage
from email.utils import formataddr
import os
from datetime import datetime

load_dotenv()

# === Email setup ===
user = os.getenv("SMTP_USER")
password = os.getenv("SMTP_PASS")  # From .env
to_list = ["ap@koenig-solutions.com"]
cc_list = [
    "aditya.singh@koenig-solutions.com",
    "tax@koenig-solutions.com",
    "sunil.kushwaha@koenig-solutions.com"
]

# === File path ===
today_str = datetime.now().strftime('%Y-%m-%d')
file_path = f"data/delta_report_{today_str}.xlsx"
filename = f"delta_report_{today_str}.xlsx"

if not os.path.exists(file_path):
    print(f"❌ Report file not found: {file_path}")
    exit()

# === Email Content ===
msg = EmailMessage()
msg["Subject"] = f"Vendor Invoice Validation Report – {today_str}"
msg["From"] = formataddr(("Invoice Management Team", user))  # ✅ Corrected
msg["To"] = ", ".join(to_list)
msg["Cc"] = ", ".join(cc_list)

msg.set_content(f"""
Dear Team,

Please find attached the Vendor Invoice Validation Report for {today_str}.

Regards,  
Invoice Management Team  
Koenig Solutions
""")

# Attach report
with open(file_path, "rb") as f:
    msg.add_attachment(f.read(), maintype="application",
                       subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       filename=filename)

# === Send email ===
with smtplib.SMTP("smtp.office365.com", 587) as smtp:
    smtp.starttls()
    smtp.login(user, password)  # ✅ Fixed variable name
    smtp.send_message(msg)

print("✅ Email sent successfully.")
