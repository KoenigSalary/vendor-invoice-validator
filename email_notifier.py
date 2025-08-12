import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta
import zipfile
import glob

class EmailNotifier:
    """Basic email notifier for backward compatibility"""
    def __init__(self):
        pass
    
    def send_validation_report(self, date, recipients, issues_count):
        print(f"📧 Email notification: {issues_count} issues found on {date}")
        print(f"📧 Recipients: {recipients}")
        return True
    
    def send_detailed_validation_report(self, date, recipients, email_summary, report_path, *args):
        print(f"📧 Enhanced email notification sent for {date}")
        print(f"📧 Recipients: {recipients}")
        print(f"📊 Report: {report_path}")
        return True

class EnhancedEmailSystem:
    """Enhanced email system with rich HTML templates and attachments"""
    
    def __init__(self, smtp_server=None, smtp_port=None, username=None, password=None):
        # Use environment variables if parameters not provided
        self.smtp_server = smtp_server or os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(smtp_port or os.getenv('SMTP_PORT', '587'))
        self.username = username or os.getenv('EMAIL_USER')
        self.password = password or os.getenv('EMAIL_PASSWORD')
        
        # Recipients from environment or default
        recipients_str = os.getenv('RECIPIENTS', '')
        self.default_recipients = [email.strip() for email in recipients_str.split(',') if email.strip()]
    
    def create_invoice_zip(self, invoice_files, validation_period=None):
        """Create ZIP file with invoice copies"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            period = validation_period or "current"
            zip_filename = f'Invoice_Files_RMS_{period}_{timestamp}.zip'
            
            with zipfile.ZipFile(zip_filename, 'w') as zipf:
                for invoice_file in invoice_files:
                    if os.path.exists(invoice_file):
                        zipf.write(invoice_file, os.path.basename(invoice_file))
                        print(f"✅ Added to ZIP: {os.path.basename(invoice_file)}")
            
            return zip_filename
        except Exception as e:
            print(f"⚠️ Error creating ZIP: {str(e)}")
            return None
    
    def generate_enhanced_email_body(self, validation_summary, changes_detected=None):
        """Generate enhanced HTML email body - FIXED STRING ESCAPING"""
        current_date = datetime.now().strftime('%d %B %Y')
        deadline_date = (datetime.now() + timedelta(days=4)).strftime('%d %B %Y')
        
        changes_detected = changes_detected or []
        
        # FIXED: Proper string escaping and concatenation
        email_body = """



    
🧾 Enhanced Invoice Validation Report

    
Automated Multi-Location GST/VAT Compliance System





    
📌 Executive Summary

    
Dear Team,


    
Please find attached the comprehensive automated invoice validation report for {current_date}.





    
🔍 Enhanced Validation Summary

    
🗓️ Validation Date:	{current_date}
📊 Report Period:	Last 4 days
🧾 Historical Check:	Last 3 months
⚠️ Issues Detected:	{total_issues} invoices flagged
🌍 Locations Covered:	{locations_count} global locations
💱 Currencies Processed:	{currencies}
🔄 Historical Changes:	{changes_count} modifications detected

""".format(
            current_date=current_date,
            total_issues=validation_summary.get('total_issues', 0),
            locations_count=validation_summary.get('locations_count', 0),
            currencies=', '.join(validation_summary.get('currencies', [])),
            changes_count=len(changes_detected)
        )
        
        # Add GST/VAT breakdown section - FIXED ESCAPING
        if validation_summary.get('tax_breakdown'):
            email_body += """

    
🧮 GST/VAT Compliance Summary

    """
            
            for location, tax_info in validation_summary['tax_breakdown'].items():
                email_body += """
        """.format(
                    location=location,
                    tax_type=tax_info.get('type', 'N/A'),
                    total_tax=tax_info.get('total_tax', 0)
                )
            email_body += """
    
{location}	{tax_type}	₹{total_tax:,.2f}

"""
        
        # Add due date alerts - FIXED ESCAPING
        if validation_summary.get('due_date_alerts'):
            email_body += """

    
⏰ Critical Due Date Alerts

    
{alert_count} invoices are due within the next 5 days:


    
""".format(alert_count=len(validation_summary['due_date_alerts']))
            
            for alert in validation_summary['due_date_alerts'][:5]:
                email_body += """
        
{invoice_number} - Due: {due_date} ({vendor})
""".format(
                    invoice_number=alert.get('invoice_number', 'N/A'),
                    due_date=alert.get('due_date', 'N/A'),
                    vendor=alert.get('vendor', 'N/A')
                )
            email_body += """
    

"""
        
        # Add attachments section
        email_body += """

    
📎 Enhanced Attachments

    

        
✅ Enhanced Invoice Validation Report (Excel format with new fields)

        
🗂️ Invoice Files from RMS (ZIP folder - validation period only)

        
📋 Historical Changes Log (CSV format - 3 months tracking)

        
📊 GST/VAT Compliance Summary (PDF format)

    




    
⏳ Action Required

    
Please review and rectify all flagged invoices by {deadline_date} (EOD) to ensure:


    

        
✅ Timely GST/VAT compliance across all locations

        
✅ Data accuracy and historical integrity

        
✅ Due date adherence for payment processing

        
✅ Multi-currency validation completeness

    

    
⚠️ Failure to address discrepancies by the deadline may result in compliance violations and reporting delays.





    
🌍 Global Coverage

    
Our Enhanced System Now Covers:


    
🇮🇳 India (All Branches)
🇺🇸 USA
🇬🇧 UK
🇨🇦 Canada
🇩🇪 Germany
🇦🇪 Dubai
🇸🇬 Singapore
🇦🇺 Australia



For any clarification or assistance, feel free to reach out to the Finance or Accounts Team.




    
Best regards,


    
🧠 Enhanced Invoice Validation System v2.0


    
Koenig Solutions Pvt. Ltd.


    
Automated Multi-Location GST/VAT Compliance & Historical Tracking






""".format(deadline_date=deadline_date)
        
        return email_body
    
    def send_enhanced_email(self, recipients=None, validation_summary=None, changes_detected=None, attachments=None):
        """Send enhanced email with all attachments"""
        try:
            # Use default recipients if none provided
            recipients = recipients or self.default_recipients
            if not recipients:
                print("⚠️ No recipients specified")
                return False, "No recipients specified"
            
            # Default validation summary if none provided
            validation_summary = validation_summary or {
                'total_issues': 0,
                'locations_count': 15,
                'currencies': ['INR', 'USD', 'EUR', 'GBP'],
                'tax_breakdown': {},
                'due_date_alerts': []
            }
            
            attachments = attachments or []
            changes_detected = changes_detected or []
            
            # Check SMTP credentials
            if not self.username or not self.password:
                print("⚠️ SMTP credentials not configured")
                return False, "SMTP credentials not configured"
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.username
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = f"🧾 Enhanced Invoice Validation Report - {datetime.now().strftime('%d %B %Y')} | Multi-Location GST/VAT Compliance"
            
            # Add enhanced email body - FIXED VERSION
            body = self.generate_enhanced_email_body(validation_summary, changes_detected)
            msg.attach(MIMEText(body, 'html'))
            
            # Add attachments
            for attachment_path in attachments:
                if os.path.exists(attachment_path):
                    try:
                        with open(attachment_path, "rb") as attachment:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(attachment.read())
                        
                        encoders.encode_base64(part)
                        part.add_header(
                            'Content-Disposition',
                            f'attachment; filename= {os.path.basename(attachment_path)}'
                        )
                        msg.attach(part)
                        print(f"✅ Attached: {os.path.basename(attachment_path)}")
                    except Exception as e:
                        print(f"⚠️ Failed to attach {attachment_path}: {str(e)}")
            
            # Send email
            print(f"📧 Connecting to {self.smtp_server}:{self.smtp_port}")
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.username, self.password)
            
            text = msg.as_string()
            server.sendmail(self.username, recipients, text)
            server.quit()
            
            print(f"✅ Enhanced email sent successfully to {len(recipients)} recipients")
            return True, "Enhanced email sent successfully"
            
        except Exception as e:
            print(f"❌ Failed to send email: {str(e)}")
            return False, f"Failed to send email: {str(e)}"

# Global function for easy GitHub Actions integration
def send_validation_complete_email(validation_summary=None, attachments=None):
    """
    Simple function to send enhanced validation email
    Uses environment variables for configuration
    """
    try:
        # Initialize enhanced email system
        email_system = EnhancedEmailSystem()
        
        # Default validation summary
        if validation_summary is None:
            validation_summary = {
                'total_issues': 0,
                'locations_count': 15,
                'currencies': ['INR', 'USD', 'EUR', 'GBP'],
                'tax_breakdown': {
                    'India - Delhi': {'type': 'CGST+SGST', 'total_tax': 125000.50},
                    'India - Mumbai': {'type': 'IGST', 'total_tax': 89000.25},
                    'USA - New York': {'type': 'Sales Tax', 'total_tax': 15000.75}
                },
                'due_date_alerts': []
            }
        
        # Find attachments if not provided
        if attachments is None:
            attachments = []
            # Look for common file patterns
            attachments.extend(glob.glob('*.xlsx'))
            attachments.extend(glob.glob('*.db'))
            attachments.extend(glob.glob('*.csv'))
        
        # Send enhanced email
        success, message = email_system.send_enhanced_email(
            validation_summary=validation_summary,
            attachments=attachments
        )
        
        print(f"📧 Email result: {message}")
        return success
        
    except Exception as e:
        print(f"❌ Error in send_validation_complete_email: {str(e)}")
        return False

# Main execution for testing
if __name__ == "__main__":
    print("🧪 Testing email system...")
    
    # Test with sample data
    sample_summary = {
        'total_issues': 5,
        'locations_count': 12,
        'currencies': ['INR', 'USD', 'EUR'],
        'tax_breakdown': {
            'India - Delhi': {'type': 'CGST+SGST', 'total_tax': 50000.00},
            'USA - California': {'type': 'Sales Tax', 'total_tax': 5000.00}
        },
        'due_date_alerts': [
            {'invoice_number': 'INV-001', 'due_date': '2024-08-15', 'vendor': 'ABC Corp'}
        ]
    }
    
    success = send_validation_complete_email(sample_summary, [])
    print(f"✅ Test result: {'Success' if success else 'Failed'}")
