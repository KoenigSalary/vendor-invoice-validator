# email_notifier.py

import smtplib
import os
import zipfile
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class EmailNotifier:
    def __init__(self):
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.office365.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.username = os.getenv('EMAIL_USERNAME')
        self.password = os.getenv('EMAIL_PASSWORD')
        self.from_email = os.getenv('EMAIL_FROM', self.username)
        
    def create_validation_zip(self, report_date):
        """Create zip file with validation report and invoices"""
        try:
            data_dir = f"data/{report_date}"
            zip_filename = f"data/invoice_validation_{report_date}.zip"
            
            with zipfile.ZipFile(zip_filename, 'w') as zipf:
                # Add validation report
                report_file = f"data/delta_report_{report_date}.xlsx"
                if os.path.exists(report_file):
                    zipf.write(report_file, f"validation_report_{report_date}.xlsx")
                
                # Add original invoice ZIP if it exists
                original_zip = f"{data_dir}/invoices.zip"
                if os.path.exists(original_zip):
                    zipf.write(original_zip, "original_invoices.zip")
            
            print(f"✅ Created validation zip: {zip_filename}")
            return zip_filename
            
        except Exception as e:
            print(f"❌ Failed to create zip file: {str(e)}")
            return None
    
    def send_validation_report(self, report_date, recipients, issues_found=0):
        """Send validation report with attachments"""
        try:
            # Create ZIP file
            zip_file = self.create_validation_zip(report_date)
            
            # Prepare email
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = ", ".join(recipients)
            msg['Subject'] = f"Invoice Validation Report - {report_date}"
            
            # Email body
            body = f"""
Dear Team,

Please find the automated invoice validation report for {report_date}.

Summary:
- Validation Date: {report_date}
- Issues Found: {issues_found}
- Report Period: Last 4 days
- Past Data Check: Last 3 months

The attached ZIP file contains:
- Detailed validation report (Excel format)
- Original invoice files from RMS system

Please review the findings and take necessary actions for any identified issues.

Best regards,
Invoice Validation System
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Attach ZIP file
            if zip_file and os.path.exists(zip_file):
                with open(zip_file, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename= invoice_validation_{report_date}.zip'
                )
                msg.attach(part)
            
            # Send email
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.username, self.password)
            server.send_message(msg)
            server.quit()
            
            print(f"✅ Validation report sent to: {', '.join(recipients)}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to send validation report: {str(e)}")
            return False

# Test function
if __name__ == "__main__":
    # Test email configuration
    notifier = EmailNotifier()
    print("Email configuration loaded:")
    print(f"SMTP Server: {notifier.smtp_server}:{notifier.smtp_port}")
    print(f"Username: {notifier.username}")
    print(f"From: {notifier.from_email}")
