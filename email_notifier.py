import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import zipfile
import os
import glob
from datetime import datetime, timedelta
import json

class EmailNotifier:
    """Enhanced Email Notification System for Koenig Invoice Validation"""
    
    def __init__(self):
        # Load email configuration from environment variables or GitHub Secrets
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.username = os.getenv('EMAIL_USER', '')
        self.password = os.getenv('EMAIL_PASSWORD', '')
        self.default_recipients = os.getenv('RECIPIENTS', '').split(',')
        
        # Clean up recipients list
        self.default_recipients = [r.strip() for r in self.default_recipients if r.strip()]
    
    def send_validation_report(self, date, recipients=None, issues_count=0):
        """Send basic validation report (backward compatibility)"""
        if not recipients:
            recipients = self.default_recipients
            
        print(f"📧 Email notification: {issues_count} issues found on {date}")
        print(f"📧 Recipients: {recipients}")
        
        # Use enhanced email system for actual sending
        validation_summary = {
            'total_issues': issues_count,
            'validation_date': date,
            'locations_count': 1,
            'currencies': ['INR']
        }
        
        return self.send_enhanced_validation_email(recipients, validation_summary)
    
    def send_detailed_validation_report(self, date, recipients=None, email_summary=None, report_path=None, *args):
        """Send detailed validation report (backward compatibility)"""
        if not recipients:
            recipients = self.default_recipients
            
        print(f"📧 Enhanced email notification sent for {date}")
        print(f"📧 Recipients: {recipients}")
        print(f"📊 Report: {report_path}")
        
        # Prepare enhanced email data
        validation_summary = email_summary or {}
        attachments = [report_path] if report_path and os.path.exists(report_path) else []
        
        return self.send_enhanced_validation_email(recipients, validation_summary, attachments)
    
    def create_invoice_zip(self, validation_period="4days"):
        """Create ZIP file with invoice copies for the validation period"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        zip_filename = f'Invoice_Files_RMS_{validation_period}_{timestamp}.zip'
        
        # Find invoice files to include
        invoice_patterns = ['*.pdf', '*.xlsx', '*.xls', 'invoices/*.pdf']
        invoice_files = []
        
        for pattern in invoice_patterns:
            invoice_files.extend(glob.glob(pattern))
        
        if invoice_files:
            with zipfile.ZipFile(zip_filename, 'w') as zipf:
                for invoice_file in invoice_files:
                    if os.path.exists(invoice_file):
                        zipf.write(invoice_file, os.path.basename(invoice_file))
            
            print(f"📁 Created ZIP file: {zip_filename} with {len(invoice_files)} files")
            return zip_filename
        
        return None
    
    def generate_enhanced_email_body(self, validation_summary, changes_detected=None):
        """Generate rich HTML email body with all enhanced features"""
        current_date = datetime.now().strftime('%d %B %Y')
        deadline_date = (datetime.now() + timedelta(days=4)).strftime('%d %B %Y')
        
        # Extract summary data with defaults
        total_issues = validation_summary.get('total_issues', 0)
        locations_count = validation_summary.get('locations_count', 0)
        currencies = validation_summary.get('currencies', ['INR'])
        changes_count = len(changes_detected) if changes_detected else 0
        
        email_body = f"""
        
        
        
        
            🧾 Enhanced Invoice Validation Report
            Automated Multi-Location GST/VAT Compliance System
            {current_date}
        
        
        
            
            
                📌 Executive Summary
                Dear Finance & Accounts Team,
                Please find attached the comprehensive automated invoice validation report for {current_date}.
                ⚠️ {total_issues} invoices require immediate attention.
            
            
            
                🔍 Enhanced Validation Summary
                
                    🗓️ Validation Date:{current_date}
                    📊 Report Period:Last 4 days (Automated Schedule)
                    🧾 Historical Check:Last 3 months tracking
                    ⚠️ Issues Detected:{total_issues} invoices flagged
                    🌍 Locations Covered:{locations_count} global locations
                    💱 Currencies Processed:{', '.join(currencies)}
                    🔄 Historical Changes:{changes_count} modifications detected
                
            
        """
        
        # Add GST/VAT breakdown if available
        if validation_summary.get('tax_breakdown'):
            email_body += """
            
                🧮 GST/VAT Compliance Summary
                
            """
            for location, tax_info in validation_summary['tax_breakdown'].items():
                email_body += f"""
                
                """
            email_body += "
                    
                        Location
                        Tax Type
                        Total Tax
                    
                    {location}
                    {tax_info.get('type', 'GST/VAT')}
                    ₹{tax_info.get('total_tax', 0):,.2f}
                "
        
        # Add due date alerts if available
        if validation_summary.get('due_date_alerts'):
            email_body += f"""
            
                ⏰ Critical Due Date Alerts
                {len(validation_summary['due_date_alerts'])} invoices are due within the next 5 days:
                
            """
            for alert in validation_summary['due_date_alerts'][:5]:
                email_body += f"{alert['invoice_number']} - Due: {alert['due_date']} ({alert['vendor']})"
            email_body += ""
        
        # Add historical changes if available
        if changes_detected and len(changes_detected) > 0:
            email_body += f"""
            
                📊 Historical Data Changes (Last 3 Months)
                {len(changes_detected)} modifications/deletions detected in previously validated invoices:
                
            """
            for change in changes_detected[:10]:
                email_body += f"""
                {change.get('invoice_id', 'Unknown')} - {change.get('field_name', 'Field')}: 
                    {change.get('old_value', 'Old')} 
                    → {change.get('new_value', 'New')}
                
                """
            email_body += ""
        
        # Enhanced attachments section
        email_body += """
        
            📎 Enhanced Attachments
            
                ✅Enhanced Invoice Validation Report - Excel format with 21 enhanced fields
                🗂️Invoice Files from RMS - ZIP folder containing validation period invoices
                📋Historical Changes Log - CSV format with 3-month tracking data
                📊GST/VAT Compliance Summary - Detailed tax calculation report
            
        
        
        
            ⏳ Action Required
            Please review and rectify all flagged invoices by {deadline_date} (EOD) to ensure:
            
                ✅ Timely GST/VAT compliance across all global locations
                ✅ Data accuracy and historical integrity maintenance
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
            
        
        
        
            Best regards,
            🧠 Enhanced Invoice Validation System v2.0
            Koenig Solutions Pvt. Ltd.
            Automated Multi-Location GST/VAT Compliance & Historical Tracking
        
        
        
        

        
        """
        
        return email_body
    
    def send_enhanced_validation_email(self, recipients=None, validation_summary=None, attachments=None, changes_detected=None):
        """Send enhanced validation email with rich formatting and attachments"""
        if not recipients:
            recipients = self.default_recipients
            
        if not recipients:
            print("❌ No recipients configured. Please set RECIPIENTS environment variable.")
            return False
            
        if not self.username or not self.password:
            print("❌ Email credentials not configured. Please set EMAIL_USER and EMAIL_PASSWORD.")
            return False
        
        # Default validation summary
        if not validation_summary:
            validation_summary = {
                'total_issues': 0,
                'locations_count': 1,
                'currencies': ['INR'],
                'validation_date': datetime.now().strftime('%Y-%m-%d')
            }
        
        try:
            # Create email message
            msg = MIMEMultipart()
            msg['From'] = self.username
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = f"🧾 Enhanced Invoice Validation Report - {datetime.now().strftime('%d %B %Y')} | Multi-Location GST/VAT Compliance"
            
            # Generate and attach HTML body
            body = self.generate_enhanced_email_body(validation_summary, changes_detected)
            msg.attach(MIMEText(body, 'html'))
            
            # Add attachments
            if attachments:
                for attachment_path in attachments:
                    if os.path.exists(attachment_path):
                        with open(attachment_path, "rb") as attachment:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(attachment.read())
                        
                        encoders.encode_base64(part)
                        part.add_header(
                            'Content-Disposition',
                            f'attachment; filename= {os.path.basename(attachment_path)}'
                        )
                        msg.attach(part)
                        print(f"📎 Attached: {os.path.basename(attachment_path)}")
            
            # Create and attach invoice ZIP file
            zip_file = self.create_invoice_zip()
            if zip_file:
                with open(zip_file, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename= {os.path.basename(zip_file)}'
                )
                msg.attach(part)
                print(f"📁 Attached ZIP: {os.path.basename(zip_file)}")
            
            # Send email via SMTP
            print(f"📧 Connecting to SMTP server: {self.smtp_server}:{self.smtp_port}")
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.username, self.password)
            
            text = msg.as_string()
            server.sendmail(self.username, recipients, text)
            server.quit()
            
            print(f"✅ Enhanced email sent successfully to: {', '.join(recipients)}")
            print(f"📊 Summary: {validation_summary.get('total_issues', 0)} issues, {len(attachments) if attachments else 0} attachments")
            
            # Clean up temporary files
            if zip_file and os.path.exists(zip_file):
                os.remove(zip_file)
                print(f"🗑️ Cleaned up temporary file: {zip_file}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to send enhanced email: {str(e)}")
            return False

# Global function for backward compatibility and easy usage
def send_validation_complete_email(attachments=None):
    """Convenience function to send validation complete email"""
    notifier = EmailNotifier()
    
    # Create default validation summary
    validation_summary = {
        'total_issues': 0,
        'locations_count': 8,  # Koenig global locations
        'currencies': ['INR', 'USD', 'EUR', 'GBP'],
        'validation_date': datetime.now().strftime('%Y-%m-%d')
    }
    
    # Look for Excel files to attach
    if not attachments:
        attachments = []
        excel_patterns = ['enhanced_invoices_*.xlsx', 'validation_detailed_*.xlsx', '*.xlsx']
        for pattern in excel_patterns:
            attachments.extend(glob.glob(pattern))
    
    return notifier.send_enhanced_validation_email(
        validation_summary=validation_summary,
        attachments=attachments
    )

# For direct script execution
if __name__ == "__main__":
    print("🚀 Testing Enhanced Email System...")
    
    # Test email configuration
    notifier = EmailNotifier()
    
    if not notifier.username:
        print("❌ EMAIL_USER not set. Please configure email credentials.")
        print("Required environment variables:")
        print("- EMAIL_USER: your-email@gmail.com")
        print("- EMAIL_PASSWORD: your-app-password")
        print("- RECIPIENTS: recipient1@example.com,recipient2@example.com")
    else:
        print(f"📧 Email configured: {notifier.username}")
        print(f"📬 Recipients: {notifier.default_recipients}")
        
        # Send test email
        test_summary = {
            'total_issues': 5,
            'locations_count': 8,
            'currencies': ['INR', 'USD', 'EUR'],
            'validation_date': datetime.now().strftime('%Y-%m-%d')
        }
        
        success = notifier.send_enhanced_validation_email(validation_summary=test_summary)
        if success:
            print("✅ Test email sent successfully!")
        else:
            print("❌ Test email failed. Check configuration and credentials.")
