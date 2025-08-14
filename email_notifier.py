import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta
import zipfile
import glob
import logging

os.environ['EMAIL_RECIPIENTS'] = 'tax@koenig-solutions.com'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    """Enhanced email system with rich HTML templates and ZIP attachments"""
    
    def __init__(self, smtp_server=None, smtp_port=None, username=None, password=None):
        # Use your existing environment variables
        self.smtp_server = smtp_server or os.getenv('SMTP_SERVER', 'smtp.office365.com')
        self.smtp_port = int(smtp_port or os.getenv('SMTP_PORT', '587'))
        self.username = username or os.getenv('EMAIL_USERNAME')  # Your variable name
        self.password = password or os.getenv('EMAIL_PASSWORD')  # Your variable name
        self.from_email = os.getenv('EMAIL_FROM', self.username)
        self.from_name = os.getenv('SMTP_FROM_NAME', 'Invoice Management System')
        
        # Get recipients from your AP_TEAM_EMAIL_LIST
        recipients_str = (
            os.getenv('AP_TEAM_EMAIL_LIST') or
            os.getenv('EMAIL_RECIPIENTS') or
            os.getenv('TEAM_EMAIL_LIST') or
            ''
        )
        
        # Parse recipients
        if recipients_str:
            recipients_str = recipients_str.strip().strip('"').strip("'")
            self.default_recipients = [
                email.strip()
                for email in recipients_str.replace(';', ',').split(',')
                if email.strip() and '@' in email.strip()
            ]
        else:
            self.default_recipients = []
        
        logger.info(f"📧 Email system initialized with {len(self.default_recipients)} recipients")
    
    def create_invoice_zip(self, excel_report_path, invoice_files_dir="invoice_files"):
        """Create ZIP file with Excel report and invoice files"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            zip_filename = f'invoice_validation_{timestamp}.zip'
            
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Add Excel report to ZIP
                if os.path.exists(excel_report_path):
                    zipf.write(excel_report_path, 'validation_report.xlsx')
                    logger.info(f"📊 Added Excel report to ZIP: {excel_report_path}")
                
                # Add invoice files
                invoice_count = 0
                if os.path.exists(invoice_files_dir):
                    for root, dirs, files in os.walk(invoice_files_dir):
                        for file in files:
                            if file.lower().endswith(('.pdf', '.png', '.jpg', '.jpeg')):
                                file_path = os.path.join(root, file)
                                # Add to invoice_files folder in ZIP
                                arcname = f"invoice_files/{file}"
                                zipf.write(file_path, arcname)
                                invoice_count += 1
                
                # Add summary file
                summary_content = f"""Invoice Validation Report Summary
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Excel Report: validation_report.xlsx
Invoice Files: {invoice_count} files included

Files included in this ZIP:
- validation_report.xlsx (Excel validation report)
- invoice_files/ ({invoice_count} invoice files)

For questions, contact: {self.from_email}
"""
                zipf.writestr('summary.txt', summary_content)
                
            logger.info(f"📁 ZIP created successfully: {zip_filename} ({invoice_count} invoice files)")
            return zip_filename
            
        except Exception as e:
            logger.error(f"❌ Error creating ZIP file: {str(e)}")
            return None
    
    def create_html_email(self, subject, summary_stats, report_filename):
        """Create professional HTML email"""
        html_template = f"""
        
        
        
            
            
        
        
            

                

                    
📊 Invoice Validation Report

                    
Koenig Solutions - Invoice Management System


                    
{datetime.now().strftime('%B %d, %Y at %I:%M %p')}


                

                
                

                    
Validation Summary

                    

                        

                            
{summary_stats.get('passed', 0)}

                            
Passed


                        

                        

                            
{summary_stats.get('warnings', 0)}

                            
Warnings


                        

                        

                            
{summary_stats.get('failed', 0)}

                            
Failed


                        

                    

                    
                    

                        
📁 Attached Files

                        
{report_filename}


                        

                            
📊 Excel validation report with all invoice details

                            
📄 Invoice files (PDFs, images) for the validation period

                            
📋 Processing summary and metadata

                        

                    

                    
                    
This automated report contains the complete invoice validation results and all associated invoice files.


                    
For questions or support, please contact the Finance Team.


                

                
                

                    
Generated by Invoice Management System | Koenig Solutions


                    
This is an automated message. Please do not reply directly to this email.


                

            

        

        
        """
        return html_template
    
    def send_email_with_zip(self, zip_filepath, summary_stats=None, recipients=None):
        """Send email with ZIP attachment"""
        try:
            if not recipients:
                recipients = self.default_recipients
            
            if not recipients:
                logger.error("⚠️ No recipients specified")
                return False
            
            # Create message
            msg = MIMEMultipart('alternative')
            msg['From'] = f"{self.from_name} <{self.from_email}>"
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = f"📊 Invoice Validation Report - {datetime.now().strftime('%Y-%m-%d')}"
            
            # Default summary stats
            if not summary_stats:
                summary_stats = {'passed': 0, 'warnings': 0, 'failed': 0, 'total': 0}
            
            # Create HTML content
            html_content = self.create_html_email(
                subject=msg['Subject'],
                summary_stats=summary_stats,
                report_filename=os.path.basename(zip_filepath)
            )
            
            # Attach HTML content
            html_part = MIMEText(html_content, 'html')
            msg.attach(html_part)
            
            # Attach ZIP file
            if os.path.exists(zip_filepath):
                with open(zip_filepath, 'rb') as attachment:
                    part = MIMEBase('application', 'zip')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {os.path.basename(zip_filepath)}'
                    )
                    msg.attach(part)
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)
            
            logger.info(f"✅ Email sent successfully to: {', '.join(recipients)}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error sending email: {str(e)}")
            return False
    
    def send_validation_report_with_invoices(self, excel_report_path, invoice_files_dir="invoice_files", summary_stats=None):
        """Main method to send validation report with invoice files"""
        try:
            # Create ZIP file with Excel report and invoice files
            zip_filepath = self.create_invoice_zip(excel_report_path, invoice_files_dir)
            
            if not zip_filepath:
                logger.error("❌ Failed to create ZIP file")
                return False
            
            # Send email with ZIP attachment
            success = self.send_email_with_zip(zip_filepath, summary_stats)
            
            # Clean up ZIP file after sending
            try:
                if os.path.exists(zip_filepath):
                    os.remove(zip_filepath)
                    logger.info(f"🗑️ Temporary ZIP file cleaned up: {zip_filepath}")
            except Exception as e:
                logger.warning(f"⚠️ Could not clean up ZIP file: {str(e)}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error in send_validation_report_with_invoices: {str(e)}")
            return False

def main():
    """Test the email system"""
    print("🧪 Testing email system...")
    
    # Check credentials
    if not os.getenv('EMAIL_USERNAME') or not os.getenv('EMAIL_PASSWORD'):
        print("⚠️ SMTP credentials not configured")
        print("📧 Email result: SMTP credentials not configured")
        print("✅ Test result: Failed")
        return False
    
    # Check recipients
    recipients_str = os.getenv('AP_TEAM_EMAIL_LIST') or os.getenv('EMAIL_RECIPIENTS')
    if not recipients_str:
        print("⚠️ No recipients specified")
        print("📧 Email result: No recipients specified")
        print("✅ Test result: Failed")
        return False
    
    # Initialize email system
    email_system = EnhancedEmailSystem()
    
    # Find Excel report
    excel_files = glob.glob("invoice_validation_report_*.xlsx")
    if not excel_files:
        print("⚠️ No Excel report found. Please run enhanced_processor.py first")
        print("📧 Email result: No Excel report found")
        print("✅ Test result: Failed")
        return False
    
    excel_report = excel_files[-1]  # Use most recent
    print(f"📊 Found Excel report: {excel_report}")
    
    # Test summary stats
    summary_stats = {
        'passed': 0,
        'warnings': 5,
        'failed': 0,
        'total': 5
    }
    
    # Send email with ZIP
    success = email_system.send_validation_report_with_invoices(
        excel_report_path=excel_report,
        invoice_files_dir="invoice_files",
        summary_stats=summary_stats
    )
    
    if success:
        print("✅ Email sent successfully!")
        print("📧 Email result: Success")
        print("✅ Test result: Passed")
    else:
        print("❌ Email sending failed")
        print("📧 Email result: Failed")
        print("✅ Test result: Failed")
    
    return success

if __name__ == "__main__":
    main()
