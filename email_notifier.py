# email_notifier.py
import os
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime
import traceback

class EmailNotifier:
    def __init__(self):
        # Get SMTP settings from environment variables
        self.smtp_user = os.getenv('SMTP_USER')
        self.smtp_pass = os.getenv('SMTP_PASS')
        self.smtp_server = 'smtp.office365.com'  # Outlook SMTP server
        self.smtp_port = 587
        
        # Validate settings
        if not self.smtp_user or not self.smtp_pass:
            print("⚠️ SMTP credentials not found in environment variables")
        
    def send_email(self, subject, recipients, html_content, text_content, attachments=None):
        """Enhanced email sending with better error handling and diagnostics"""
        try:
            # Create message container
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.smtp_user
            msg['To'] = ', '.join(recipients) if isinstance(recipients, list) else recipients
            
            # Attach parts
            part1 = MIMEText(text_content, 'plain')
            part2 = MIMEText(html_content, 'html')
            msg.attach(part1)
            msg.attach(part2)
            
            # Attach files if provided
            if attachments:
                if not isinstance(attachments, list):
                    attachments = [attachments]
                
                for file_path in attachments:
                    try:
                        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                            with open(file_path, 'rb') as file:
                                attachment = MIMEApplication(file.read())
                                attachment.add_header(
                                    'Content-Disposition', 
                                    'attachment', 
                                    filename=os.path.basename(file_path)
                                )
                                msg.attach(attachment)
                                print(f"✅ Attached file: {os.path.basename(file_path)}")
                        else:
                            print(f"⚠️ Attachment not found or empty: {file_path}")
                    except Exception as e:
                        print(f"⚠️ Error attaching file {file_path}: {str(e)}")
            
            # Create secure connection and send email
            print(f"🔄 Connecting to SMTP server: {self.smtp_server}:{self.smtp_port}")
            
            # Use this improved SMTP connection method
            context = ssl.create_default_context()
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                # Connection diagnostics
                print("🔄 SMTP: Starting connection")
                server.set_debuglevel(1)  # Enable debug messages
                
                # Identify ourselves to the server
                server.ehlo()
                print("🔄 SMTP: EHLO completed")
                
                # Secure the connection
                server.starttls(context=context)
                print("🔄 SMTP: STARTTLS completed")
                
                # Re-identify after TLS
                server.ehlo()
                print("🔄 SMTP: Second EHLO completed")
                
                # Login
                print(f"🔄 SMTP: Attempting login as {self.smtp_user}")
                server.login(self.smtp_user, self.smtp_pass)
                print("✅ SMTP: Login successful")
                
                # Send email
                server.sendmail(self.smtp_user, recipients, msg.as_string())
                print(f"✅ Email sent to {len(recipients) if isinstance(recipients, list) else 1} recipient(s)")
            
            return True
            
        except smtplib.SMTPAuthenticationError:
            print("❌ SMTP Authentication failed! Please check username and password.")
            print("⚠️ Note: For Microsoft accounts, you may need to use an App Password.")
            return False
            
        except smtplib.SMTPException as e:
            print(f"❌ SMTP Error: {str(e)}")
            return False
            
        except Exception as e:
            print(f"❌ Failed to send email: {str(e)}")
            traceback.print_exc()
            return False
    
    def send_validation_report(self, today_str, recipients, issues_count):
        """Basic validation report email"""
        subject = f"Invoice Validation Report - {today_str}"
        
        text_content = f"""
        Invoice Validation Report - {today_str}
        
        A total of {issues_count} issues were found during validation.
        Please see the attached report for details.
        
        This is an automated email from the Invoice Validation System.
        """
        
        html_content = f"""
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px;">
                Invoice Validation Report - {today_str}
            </h2>
            
            <p>A total of <strong>{issues_count}</strong> issues were found during validation.</p>
            <p>Please see the attached report for details.</p>
            
            <p style="color: #7f8c8d; margin-top: 30px; font-size: 0.9em;">
                This is an automated email from the Invoice Validation System.
            </p>
        </div>
        """
        
        result = self.send_email(
            subject, 
            recipients, 
            html_content, 
            text_content
        )
        
        return result
    
    def send_detailed_validation_report(self, today_str, recipients, email_summary, report_path=None,
                                      current_batch_start=None, current_batch_end=None, 
                                      cumulative_start=None, cumulative_end=None):
        """Enhanced validation report with detailed statistics"""
        try:
            subject = f"Invoice Validation Report - {today_str}"
            
            # Safely get HTML and text content
            html_content = email_summary.get('html_summary', "<p>No summary available</p>")
            text_content = email_summary.get('text_summary', "No summary available")
            
            # Add extra information if provided
            if current_batch_start and current_batch_end:
                batch_info = f"""
                <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-top: 20px;">
                    <h3 style="color: #2c3e50; margin-top: 0;">Additional Information</h3>
                    <p><strong>Current Batch:</strong> {current_batch_start} to {current_batch_end}</p>
                    <p><strong>Cumulative Range:</strong> {cumulative_start or 'N/A'} to {cumulative_end or 'N/A'}</p>
                </div>
                """
                html_content += batch_info
            
            # Set up attachments
            attachments = []
            if report_path and os.path.exists(report_path):
                attachments.append(report_path)
                
                html_content += f"""
                <div style="background-color: #e8f4fd; padding: 15px; border-radius: 5px; margin-top: 20px;">
                    <h3 style="color: #2980b9; margin-top: 0;">📎 Attachments</h3>
                    <p>The detailed validation report is attached to this email: <strong>{os.path.basename(report_path)}</strong></p>
                </div>
                """
            
            result = self.send_email(
                subject, 
                recipients, 
                html_content, 
                text_content, 
                attachments=attachments
            )
            
            return result
            
        except Exception as e:
            print(f"❌ Enhanced email failed: {str(e)}")
            traceback.print_exc()
            
            # Get statistics safely for fallback email
            statistics = email_summary.get('statistics', {})
            failed = statistics.get('failed_invoices', 0)
            warnings = statistics.get('warning_invoices', 0)
            
            # Calculate total issues safely (avoiding the len() error)
            total_issues = 0
            if isinstance(failed, int):
                total_issues += failed
            elif failed is not None:
                total_issues += len(failed)
                
            if isinstance(warnings, int):
                total_issues += warnings
            elif warnings is not None:
                total_issues += len(warnings)
            
            # Send fallback email
            self.send_validation_report(today_str, recipients, total_issues)
            return False
