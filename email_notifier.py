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
import re
from pathlib import Path
from typing import List, Optional

__all__ = ["EnhancedEmailSystem", "EmailNotifier"]


class EnhancedEmailSystem:
    """Enhanced email system with professional HTML templates and robust error handling"""

    def __init__(self, smtp_server=None, smtp_port=None, username=None, password=None):
        # SMTP Configuration
        self.smtp_server = smtp_server or os.getenv('SMTP_SERVER', 'smtp.office365.com')
        self.smtp_port = int(smtp_port or os.getenv('SMTP_PORT', '587'))
        self.username = username or os.getenv('EMAIL_USERNAME')
        self.password = password or os.getenv('EMAIL_PASSWORD')
        
        # Recipients Configuration with validation
        recipients_str = (
            os.getenv('AP_TEAM_EMAIL_LIST') or
            os.getenv('EMAIL_RECIPIENTS') or
            os.getenv('TEAM_EMAIL_LIST') or
            ''
        )
        
        if recipients_str:
            self.default_recipients = self._validate_email_list(recipients_str)
        else:
            self.default_recipients = []
            
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
    def _validate_email_list(self, recipients_str):
        """Validate and clean email recipient list"""
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        emails = []
        
        for email in recipients_str.replace(';', ',').split(','):
            email = email.strip()
            if email and email_pattern.match(email):
                emails.append(email)
            elif email:
                self.logger.warning(f"Invalid email format ignored: {email}")
                
        return emails

    def create_professional_html_template(self, validation_data, deadline_date):
        """Create professional HTML email template with enhanced formatting"""
        
        critical_count = validation_data.get('failed', 0)
        warning_count = validation_data.get('warnings', 0)
        passed_count = validation_data.get('passed', 0)
        total_count = critical_count + warning_count + passed_count
        
        # Calculate pass rate
        pass_rate = (passed_count / total_count * 100) if total_count > 0 else 0
        
        # Sample amounts with proper currency formatting
        critical_amount = '₹1,50,000'
        warning_amount = '₹1,40,000'
        
        html_template = f"""
        
        
        
            
            
            
        
        
            
            

                
                
                

                    
🏢 KOENIG INVOICE VALIDATION REPORT

                    
Automated Processing Summary - {datetime.now().strftime('%B %d, %Y')}


                

                
                
                

                    
                    
                    

                        
📊 EXECUTIVE SUMMARY

                        
                        

                            

                                Processing Rate: {pass_rate:.1f}% ({passed_count}/{total_count} invoices processed successfully)
                            


                        

                        
                        
Status	Count	Financial Impact
🚨 CRITICAL ISSUES	{critical_count}	{critical_amount}
⚠️ WARNING ITEMS	{warning_count}	{warning_amount}
✅ SUCCESSFULLY PROCESSED	{passed_count}	Ready for Payment

                    

                    
                    
                    

                        
🚨 IMMEDIATE ACTION REQUIRED

                        
Response Deadline: {deadline_date.strftime('%B %d, %Y at %I:%M %p IST')}


                        
Non-response will trigger automatic escalation to Finance Head


                    

                    
                    
                    

                        
🎯 REQUIRED ACTIONS

                        

                            
Review Failed Invoices: Check attached Excel report for detailed validation errors

                            
Provide Corrections: Submit corrected invoice data or explanations for exceptions

                            
Vendor Updates: Update vendor master data if validation issues are due to outdated information

                            
Approval Status: Confirm approval status for pending invoices

                            
Documentation: Provide supporting documents for flagged transactions

                        

                    

                    
                    
                    

                        
📎 ATTACHMENTS INCLUDED

                        

                            
Excel Validation Report: Detailed analysis with validation results for all invoices

                            
Invoice Files ZIP: Original invoice documents for your reference

                            
Processing Summary: Statistical overview and recommendations

                        

                    

                    
                    
                    

                        
📞 FOR QUESTIONS OR SUPPORT

                        
Finance Team:	Accounts@koenig-solutions.com
System Support:	tax@koenig-solutions.com

                    

                    
                

                
                
                

                    
Koenig Solutions Pvt. Ltd. | Generated by Invoice Management System


                    
This is an automated report containing confidential information


                

                
            

        
        
        """
        
        return html_template

    def create_invoice_zip(self, invoice_files=None, validation_period=None):
        """Create ZIP file with invoice copies and Excel report - Enhanced with better error handling"""
        zip_filename = None
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            period = validation_period or "current"
            zip_filename = f'invoice_validation_{timestamp}.zip'
            
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Add Excel validation report
                excel_files = glob.glob('invoice_validation_report_*.xlsx')
                if excel_files:
                    latest_excel = max(excel_files, key=os.path.getctime)
                    if os.path.exists(latest_excel):
                        zipf.write(latest_excel, 'validation_report.xlsx')
                        self.logger.info(f"Added Excel report to ZIP: {latest_excel}")
                    else:
                        self.logger.warning(f"Excel file not found: {latest_excel}")
                
                # Add invoice files with better error handling
                invoice_count = 0
                invoice_dir = Path('invoice_files')
                
                if invoice_dir.exists():
                    for file_path in invoice_dir.rglob('*'):
                        if file_path.is_file() and file_path.suffix.lower() in ['.pdf', '.png', '.jpg', '.jpeg']:
                            try:
                                arcname = f'invoice_files/{file_path.name}'
                                zipf.write(str(file_path), arcname)
                                invoice_count += 1
                            except Exception as e:
                                self.logger.warning(f"Failed to add file {file_path}: {e}")
                
                # Verify ZIP file was created successfully
                if os.path.exists(zip_filename) and os.path.getsize(zip_filename) > 0:
                    self.logger.info(f"ZIP created successfully: {zip_filename} ({invoice_count} invoice files)")
                    return zip_filename
                else:
                    self.logger.error("ZIP file creation failed or file is empty")
                    return None
                
        except Exception as e:
            self.logger.error(f"Error creating ZIP: {e}")
            # Cleanup failed ZIP file
            if zip_filename and os.path.exists(zip_filename):
                try:
                    os.remove(zip_filename)
                except:
                    pass
            return None

    def send_email_with_attachments(self, recipients, subject, html_body, zip_file):
        """Send professional HTML email with ZIP attachment - Enhanced error handling"""
        if not recipients:
            self.logger.error("No recipients specified")
            return False
            
        if not self.username or not self.password:
            self.logger.error("SMTP credentials not configured")
            return False
            
        try:
            # Validate recipients
            valid_recipients = [r for r in recipients if self._validate_email_list(r)]
            if not valid_recipients:
                self.logger.error("No valid recipients found")
                return False
            
            msg = MIMEMultipart('mixed')
            msg['From'] = self.username
            msg['To'] = ', '.join(valid_recipients)
            msg['Subject'] = subject
            
            # Attach HTML body with proper encoding
            html_part = MIMEText(html_body, 'html', 'utf-8')
            msg.attach(html_part)
            
            # Attach ZIP file with validation
            if zip_file and os.path.exists(zip_file):
                file_size = os.path.getsize(zip_file)
                # Check file size limit (25MB for most email providers)
                if file_size > 25 * 1024 * 1024:
                    self.logger.warning(f"ZIP file too large ({file_size} bytes), skipping attachment")
                else:
                    try:
                        with open(zip_file, 'rb') as attachment:
                            part = MIMEBase('application', 'zip')
                            part.set_payload(attachment.read())
                            encoders.encode_base64(part)
                            part.add_header(
                                'Content-Disposition',
                                f'attachment; filename="{os.path.basename(zip_file)}"'
                            )
                            msg.attach(part)
                            self.logger.info(f"ZIP attachment added: {os.path.basename(zip_file)} ({file_size} bytes)")
                    except Exception as e:
                        self.logger.error(f"Failed to attach ZIP file: {e}")
            
            # Send email via SMTP with enhanced error handling
            try:
                with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                    server.starttls()
                    server.login(self.username, self.password)
                    server.send_message(msg)
                
                self.logger.info(f"Email sent successfully to: {', '.join(valid_recipients)}")
                return True
                
            except smtplib.SMTPAuthenticationError:
                self.logger.error("SMTP authentication failed - check credentials")
                return False
            except smtplib.SMTPRecipientsRefused:
                self.logger.error("All recipients were refused by server")
                return False
            except smtplib.SMTPException as e:
                self.logger.error(f"SMTP error occurred: {e}")
                return False
            
        except Exception as e:
            self.logger.error(f"Error sending email: {e}")
            return False
        finally:
            # Cleanup temporary ZIP file
            if zip_file and os.path.exists(zip_file):
                try:
                    os.remove(zip_file)
                    self.logger.info(f"Temporary ZIP file cleaned up: {zip_file}")
                except Exception as e:
                    self.logger.warning(f"Failed to cleanup ZIP file: {e}")

    def validate_email_config(self):
        """Validate email configuration before sending"""
        issues = []
        
        if not self.username:
            issues.append("Missing EMAIL_USERNAME environment variable")
        if not self.password:
            issues.append("Missing EMAIL_PASSWORD environment variable")
        if not self.default_recipients:
            issues.append("No valid email recipients configured")
        if not self.smtp_server:
            issues.append("Missing SMTP server configuration")
            
        return issues


# --- BEGIN: Compatibility wrapper for callers expecting `EmailNotifier` ---

class EmailNotifier:
    """
    Compatibility wrapper around EnhancedEmailSystem so code can:
        from email_notifier import EmailNotifier
    and then call:
        - send(subject, html_body, attachments=None, recipients=None)
        - send_report(...)
        - send_validation_email(...)
    It maps SMTP_* env vars (GitHub Secrets) to the engine’s expected fields.
    """

    def __init__(self, smtp_host: Optional[str] = None, smtp_port: Optional[int] = None,
                 smtp_user: Optional[str] = None, smtp_pass: Optional[str] = None,
                 use_tls: Optional[bool] = None, from_name: Optional[str] = None,
                 recipients: Optional[List[str]] = None):

        # Prefer SMTP_*; fallback to EMAIL_* for compatibility
        host = smtp_host or os.getenv("SMTP_HOST") or os.getenv("SMTP_SERVER", "smtp.office365.com")
        port = int(smtp_port or os.getenv("SMTP_PORT", "587"))
        user = smtp_user or os.getenv("SMTP_USER") or os.getenv("EMAIL_USERNAME")
        pwd  = smtp_pass or os.getenv("SMTP_PASS")  or os.getenv("EMAIL_PASSWORD")

        self._engine = EnhancedEmailSystem(
            smtp_server=host,
            smtp_port=port,
            username=user,
            password=pwd,
        )

        self._from_name = from_name or os.getenv("SMTP_FROM_NAME", "")

        # Override recipients if provided explicitly
        if recipients:
            # Validate and override default recipients
            validated: List[str] = []
            for r in (recipients if isinstance(recipients, list) else [recipients]):
                if self._engine._validate_email_list(r):
                    validated.append(r)
            if validated:
                self._engine.default_recipients = validated

    def _zip_attachments_if_needed(self, attachments) -> Optional[str]:
        """
        Accepts:
          - None
          - path to a .zip
          - list of file paths (bundles into a temporary zip)
        Returns a zip path or None.
        """
        if not attachments:
            return None

        # single zip path passthrough
        if (isinstance(attachments, list) and len(attachments) == 1 and
            str(attachments[0]).lower().endswith(".zip") and os.path.isfile(attachments[0])):
            return attachments[0]

        # Bundle arbitrary files into a temporary ZIP
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_path = f"email_attachments_{ts}.zip"
        try:
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                files = attachments if isinstance(attachments, list) else [attachments]
                for p in files:
                    if p and os.path.isfile(p):
                        zf.write(p, arcname=os.path.basename(p))
            return zip_path
        except Exception as e:
            logging.error(f"EmailNotifier: Failed to bundle attachments: {e}")
            try:
                if os.path.exists(zip_path):
                    os.remove(zip_path)
            except Exception:
                pass
            return None

    def send(self, subject: str, html_body: str, attachments=None, recipients: Optional[List[str]] = None,
             from_email: Optional[str] = None) -> bool:
        # Optionally swap recipients for this send
        if recipients:
            self._engine.default_recipients = recipients if isinstance(recipients, list) else [recipients]

        zip_file = self._zip_attachments_if_needed(attachments)
        return self._engine.send_email_with_attachments(
            self._engine.default_recipients,
            subject,
            html_body,
            zip_file
        )

    # Backward-compatible aliases
    def send_report(self, subject: str, html_body: str, attachments=None, recipients: Optional[List[str]] = None) -> bool:
        return self.send(subject, html_body, attachments, recipients)

    def send_validation_email(self, subject: str, html_body: str, attachments=None, recipients: Optional[List[str]] = None) -> bool:
        return self.send(subject, html_body, attachments, recipients)

    def send_validation_report(self, subject: str, html_body: str, attachments=None, recipients: Optional[List[str]] = None) -> bool:
        return self.send(subject, html_body, attachments, recipients)

# --- END: Compatibility wrapper ---


def main():
    """Test the enhanced email system with comprehensive validation"""
    print("🧪 Testing enhanced email system...")
    
    # Initialize email system
    email_system = EnhancedEmailSystem()
    
    # Validate configuration
    config_issues = email_system.validate_email_config()
    if config_issues:
        print("⚠️ Configuration issues found:")
        for issue in config_issues:
            print(f"   - {issue}")
        return "Configuration incomplete"
    
    print(f"✅ Email system initialized with {len(email_system.default_recipients)} recipients")
    
    # Find Excel report
    excel_files = glob.glob('invoice_validation_report_*.xlsx')
    if not excel_files:
        print("⚠️ No Excel reports found")
        return "No Excel reports found"
    
    latest_excel = max(excel_files, key=os.path.getctime)
    print(f"📊 Found Excel report: {os.path.basename(latest_excel)}")
    
    # Create ZIP with invoices
    print("📦 Creating ZIP file with attachments...")
    zip_file = email_system.create_invoice_zip()
    if not zip_file:
        print("❌ Failed to create ZIP file")
        return "Failed to create ZIP"
    
    # Sample validation data
    validation_data = {
        'failed': 352,
        'warnings': 0,
        'passed': 395
    }
    
    # Create professional HTML email
    deadline_date = datetime.now() + timedelta(days=3)
    html_body = email_system.create_professional_html_template(validation_data, deadline_date)
    
    # Send email
    subject = f"🚨 URGENT: Invoice Validation - Action Required by {deadline_date.strftime('%b %d, %Y')}"
    
    print("📧 Sending email...")
    success = email_system.send_email_with_attachments(
        email_system.default_recipients,
        subject,
        html_body,
        zip_file
    )
    
    if success:
        print("✅ Email sent successfully!")
        return "Success"
    else:
        print("❌ Email sending failed")
        return "Failed"


if __name__ == "__main__":
    # Setup enhanced logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('email_system.log'),
            logging.StreamHandler()
        ]
    )
    
    result = main()
    print(f"📈 Email system result: {result}")
    print(f"🎯 Test result: {'✅ Passed' if result == 'Success' else '❌ Failed'}")
