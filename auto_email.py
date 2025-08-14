#!/usr/bin/env python3

import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587
SENDER_EMAIL = "Anurag.chauhan@koenig-solutions.com"
SENDER_NAME = "Tax Team"
SENDER_PASSWORD = os.getenv("OUTLOOK_APP_PASSWORD")  # App password stored in environment
RECIPIENT_EMAIL = "tax@koenig-solutions.com"

def send_reconciliation_email(output_file_path, salary_month_name):
    """
    Send reconciliation report via email
    
    Args:
        output_file_path: Full path to the reconciliation report file
        salary_month_name: Name of the salary month (e.g., "June")
    """
    
    if not SENDER_PASSWORD:
        print("❌ OUTLOOK_APP_PASSWORD not set in .env file")
        print("   Please set your Outlook app password in the .env file")
        return False
    
    # Extract year from current date or filename
    current_year = datetime.now().year
    
    # Create subject and body
    subject = f"Salary, TDS, and EPF Reconciliation Report - {salary_month_name} {current_year}"
    body = f"""Dear Team,

Please find attached the salary reconciliation report for {salary_month_name} {current_year}.

This report includes:
• Salary reconciliation with bank payments
• TDS reconciliation
• EPF reconciliation (if available)

Best regards,
Tax Team
Sent via: {SENDER_EMAIL}
"""

    # Check if file exists
    if not os.path.exists(output_file_path):
        print(f"❌ Reconciliation file not found: {output_file_path}")
        return False
    
    filename = os.path.basename(output_file_path)
    
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = f"{SENDER_NAME} <{SENDER_EMAIL}>"
        msg['To'] = RECIPIENT_EMAIL
        msg['Subject'] = subject

        # Attach body
        msg.attach(MIMEText(body, 'plain'))

        # Attach file
        with open(output_file_path, "rb") as f:
            part = MIMEApplication(f.read(), Name=filename)
            part['Content-Disposition'] = f'attachment; filename="{filename}"'
            msg.attach(part)

        # Send email
        print(f"📧 Sending email to {RECIPIENT_EMAIL}...")
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        
        print("✅ Email sent successfully")
        return True
        
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        return False

def send_email(output_file_path=None, salary_month_name=None):
    """Compatibility function for main.py"""
    
    # If no parameters provided, try to find the latest report
    if not output_file_path:
        output_dir = "output"
        if os.path.exists(output_dir):
            # Find the most recent reconciliation report
            files = [f for f in os.listdir(output_dir) if f.startswith("Salary_Reconciliation_Report") and f.endswith(".xlsx")]
            if files:
                latest_file = max(files, key=lambda f: os.path.getctime(os.path.join(output_dir, f)))
                output_file_path = os.path.join(output_dir, latest_file)
                print(f"🔍 Found latest report: {latest_file}")
    
    # If still no file, use default naming
    if not output_file_path:
        current_month = datetime.now().strftime("%B")
        current_year = datetime.now().year
        filename = f"Salary_Reconciliation_Report_{current_month}_{current_year}.xlsx"
        output_file_path = os.path.join("output", filename)
    
    # Extract month name from filename if not provided
    if not salary_month_name:
        try:
            filename = os.path.basename(output_file_path)
            # Extract month from filename like "Salary_Reconciliation_Report_June_2025.xlsx"
            parts = filename.replace(".xlsx", "").split("_")
            if len(parts) >= 4:
                salary_month_name = parts[3]  # Should be the month name
            else:
                salary_month_name = datetime.now().strftime("%B")
        except:
            salary_month_name = datetime.now().strftime("%B")
    
    return send_reconciliation_email(output_file_path, salary_month_name)

def main():
    """Main function for standalone execution"""
    return send_email()

def run():
    """Alternative function name for main.py"""
    return send_email()

# Export functions for main.py
__all__ = ['send_reconciliation_email', 'send_email', 'main', 'run']

if __name__ == "__main__":
    send_email()