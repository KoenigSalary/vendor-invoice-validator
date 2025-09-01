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

class EnhancedEmailSystem:
    """Enhanced email system with professional HTML templates and black text"""

    def __init__(self, smtp_server=None, smtp_port=None, username=None, password=None):
        # SMTP Configuration
        SMTP_USER = os.getenv("SMTP_USER")
        SMTP_PASS = os.getenv("SMTP_PASS")
        SMTP_HOST = os.getenv("SMTP_HOST", "smtp.office365.com")
        SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
        SMTP_USE_TLS = str(os.getenv("SMTP_USE_TLS", "true")).lower() in ("1", "true", "yes")
        SMTP_FROM_NAME = os.getenv("SMTP_FROM_NAME", "Invoice Management System")

        FROM_ADDR = SMTP_USER
        
        # Recipients Configuration
        recipients_str = (
            os.getenv('AP_TEAM_EMAIL_LIST') or
            os.getenv('EMAIL_RECIPIENTS') or
            os.getenv('TEAM_EMAIL_LIST') or
            ''
        )
        
        if recipients_str:
            self.default_recipients = [
                email.strip()
                for email in recipients_str.replace(';', ',').split(',')
                if email.strip() and '@' in email.strip()
            ]
        else:
            self.default_recipients = []

    def create_professional_html_template(self, validation_data, deadline_date):
        """Create professional HTML email template with BLACK text"""
        
        critical_count = validation_data.get('failed', 0)
        warning_count = validation_data.get('warnings', 0)
        passed_count = validation_data.get('passed', 0)
        total_count = critical_count + warning_count + passed_count
        
        critical_amount = '₹1,50,000'  # Sample amounts
        warning_amount = '₹1,40,000'
        
        html_template = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Invoice Validation Report</title>
        </head>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #000000; background-color: #ffffff; margin: 0; padding: 20px;">
            
            <div style="max-width: 800px; margin: 0 auto; background-color: #ffffff; border: 2px solid #e9ecef; border-radius: 10px; overflow: hidden;">
                
                <!-- Main Content -->
                <div style="padding: 30px; background-color: #ffffff;">
                    
                    <!-- Executive Summary -->
                    <div style="background-color: #f8f9fa; border: 2px solid #dee2e6; border-radius: 8px; padding: 25px; margin-bottom: 25px;">
                        <h2 style="margin: 0 0 20px 0; color: #000000; font-size: 22px; font-weight: bold;">      📑 ✔️ KOENIG INVOICE VALIDATION REPORT</h2>
                        
                        <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
                            <tr>
                                <td style="padding: 12px; border: 1px solid #dee2e6; background-color: #ffffff; color: #000000; font-weight: bold;">Status</td>
                                <td style="padding: 12px; border: 1px solid #dee2e6; background-color: #ffffff; color: #000000; font-weight: bold;">Count</td>
                                <td style="padding: 12px; border: 1px solid #dee2e6; background-color: #ffffff; color: #000000; font-weight: bold;">Financial Impact</td>
                            </tr>
                            <tr>
                                <td style="padding: 12px; border: 1px solid #dee2e6; background-color: #ffebee; color: #000000;">🚨 CRITICAL ISSUES</td>
                                <td style="padding: 12px; border: 1px solid #dee2e6; background-color: #ffebee; color: #000000; font-weight: bold;">{critical_count}</td>
                                <td style="padding: 12px; border: 1px solid #dee2e6; background-color: #ffebee; color: #000000; font-weight: bold;">{critical_amount}</td>
                            </tr>
                            <tr>
                                <td style="padding: 12px; border: 1px solid #dee2e6; background-color: #fff3e0; color: #000000;">⚠️ WARNING ITEMS</td>
                                <td style="padding: 12px; border: 1px solid #dee2e6; background-color: #fff3e0; color: #000000; font-weight: bold;">{warning_count}</td>
                                <td style="padding: 12px; border: 1px solid #dee2e6; background-color: #fff3e0; color: #000000; font-weight: bold;">{warning_amount}</td>
                            </tr>
                            <tr>
                                <td style="padding: 12px; border: 1px solid #dee2e6; background-color: #e8f5e8; color: #000000;">✅ SUCCESSFULLY PROCESSED</td>
                                <td style="padding: 12px; border: 1px solid #dee2e6; background-color: #e8f5e8; color: #000000; font-weight: bold;">{passed_count}</td>
                                <td style="padding: 12px; border: 1px solid #dee2e6; background-color: #e8f5e8; color: #000000; font-weight: bold;">Ready for Payment</td>
                            </tr>
                        </table>
                    </div>
                    
                    <!-- Urgent Action Required -->
                    <div style="background-color: #ffebee; border-left: 5px solid #d32f2f; padding: 20px; margin-bottom: 25px;">
                        <h3 style="margin: 0 0 15px 0; color: #000000; font-size: 18px;">🚨 IMMEDIATE ACTION REQUIRED</h3>
                        <p style="margin: 0; color: #000000; font-size: 16px; font-weight: bold;">Response Deadline: {deadline_date.strftime('%B %d, %Y at %I:%M %p IST')}</p>
                        <p style="margin: 10px 0 0 0; color: #000000; font-size: 14px;">Non-response will trigger automatic escalation to Finance Head</p>
                    </div>
                    
                    <!-- Required Actions -->
                    <div style="background-color: #ffffff; border: 2px solid #dee2e6; border-radius: 8px; padding: 25px; margin-bottom: 25px;">
                        <h3 style="margin: 0 0 20px 0; color: #000000; font-size: 18px;">🎯 REQUIRED ACTIONS</h3>
                        <ol style="color: #000000; font-size: 15px; line-height: 1.8; margin: 0; padding-left: 20px;">
                            <li style="color: #000000; margin-bottom: 8px;"><strong style="color: #000000;">Review Failed Invoices:</strong> Check attached Excel report for detailed validation errors</li>
                            <li style="color: #000000; margin-bottom: 8px;"><strong style="color: #000000;">Provide Corrections:</strong> Submit corrected invoice data or explanations for exceptions</li>
                            <li style="color: #000000; margin-bottom: 8px;"><strong style="color: #000000;">Vendor Updates:</strong> Update vendor master data if validation issues are due to outdated information</li>
                            <li style="color: #000000; margin-bottom: 8px;"><strong style="color: #000000;">Approval Status:</strong> Confirm approval status for pending invoices</li>
                            <li style="color: #000000; margin-bottom: 8px;"><strong style="color: #000000;">Documentation:</strong> Provide supporting documents for flagged transactions</li>
                        </ol>
                    </div>
                    
                    <!-- Attachments -->
                    <div style="background-color: #e3f2fd; border: 2px solid #2196f3; border-radius: 8px; padding: 20px; margin-bottom: 25px;">
                        <h3 style="margin: 0 0 15px 0; color: #000000; font-size: 18px;">📎 ATTACHMENTS INCLUDED</h3>
                        <ul style="color: #000000; font-size: 15px; line-height: 1.6; margin: 0; padding-left: 20px;">
                            <li style="color: #000000; margin-bottom: 5px;"><strong style="color: #000000;">Excel Validation Report:</strong> Detailed analysis with validation results for all invoices</li>
                            <li style="color: #000000; margin-bottom: 5px;"><strong style="color: #000000;">Invoice Files ZIP:</strong> Original invoice documents for your reference</li>
                            <li style="color: #000000; margin-bottom: 5px;"><strong style="color: #000000;">Processing Summary:</strong> Statistical overview and recommendations</li>
                        </ul>
                    </div>
                    
                    <!-- Contact Information -->
                    <div style="background-color: #f5f5f5; border: 2px solid #9e9e9e; border-radius: 8px; padding: 20px;">
                        <h3 style="margin: 0 0 15px 0; color: #000000; font-size: 18px;">📞 FOR QUESTIONS OR SUPPORT</h3>
                        <table style="width: 100%; border-collapse: collapse;">
                            <tr>
                                <td style="padding: 8px 0; color: #000000; font-weight: bold; width: 30%;">Finance Team:</td>
                                <td style="padding: 8px 0; color: #000000;">Accounts@koenig-solutions.com</td>
                            </tr>
                           
                            <tr>
                                <td style="padding: 8px 0; color: #000000; font-weight: bold;">System Support:</td>
                                <td style="padding: 8px 0; color: #000000;">tax@koenig-solutions.com</td>
                            </tr>
                        </table>
                    </div>
                    
                </div>
                
                <!-- Footer -->
                <div style="background-color: #f8f9fa; padding: 20px; text-align: center; border-top: 2px solid #dee2e6;">
                    <p style="margin: 0; color: #000000; font-size: 12px;">Koenig Solutions Pvt. Ltd. | Generated by Invoice Management System</p>
                    <p style="margin: 5px 0 0 0; color: #000000; font-size: 11px;">This is an automated report containing confidential information</p>
                </div>
                
            </div>
        </body>
        </html>
        """
        
        return html_template

    def create_invoice_zip(self, invoice_files=None, validation_period=None):
        """Create ZIP file with invoice copies and Excel report"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            period = validation_period or "current"
            zip_filename = f'invoice_validation_{timestamp}.zip'
            
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Add Excel validation report
                excel_files = glob.glob('invoice_validation_report_*.xlsx')
                if excel_files:
                    latest_excel = max(excel_files, key=os.path.getctime)
                    zipf.write(latest_excel, 'validation_report.xlsx')
                    logging.info(f"📊 Added Excel report to ZIP: {latest_excel}")
                
                # Add invoice files
                invoice_count = 0
                if os.path.exists('invoice_files'):
                    for root, dirs, files in os.walk('invoice_files'):
                        for file in files:
                            if file.lower().endswith(('.pdf', '.png', '.jpg', '.jpeg')):
                                file_path = os.path.join(root, file)
                                arcname = os.path.join('invoice_files', file)
                                zipf.write(file_path, arcname)
                                invoice_count += 1
                
                logging.info(f"📁 ZIP created successfully: {zip_filename} ({invoice_count} invoice files)")
                return zip_filename
                
        except Exception as e:
            logging.error(f"❌ Error creating ZIP: {e}")
            return None

    def send_email_with_attachments(self, recipients, subject, html_body, zip_file):
        """Send professional HTML email with ZIP attachment"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.username
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = subject
            
            # Attach HTML body
            msg.attach(MIMEText(html_body, 'html'))
            
            # Attach ZIP file
            if zip_file and os.path.exists(zip_file):
                with open(zip_file, 'rb') as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {os.path.basename(zip_file)}'
                    )
                    msg.attach(part)
            
            # Send email via SMTP
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)
            
            logging.info(f"✅ Email sent successfully to: {', '.join(recipients)}")
            return True
            
        except Exception as e:
            logging.error(f"❌ Error sending email: {e}")
            return False
        finally:
            # Cleanup temporary ZIP file
            if zip_file and os.path.exists(zip_file):
                os.remove(zip_file)
                logging.info(f"🗑️ Temporary ZIP file cleaned up: {zip_file}")

class EmailNotifier:
    """
    Adapter class for EnhancedEmailSystem to maintain compatibility with main.py
    """
    
    def __init__(self):
        self.email_system = EnhancedEmailSystem()
        self.smtp_server = self.email_system.smtp_server
        self.smtp_port = self.email_system.smtp_port
        self.email_username = self.email_system.username
        self.email_password = self.email_system.password
        self.email_from = self.email_system.username
        self.smtp_use_tls = True
    
    def send_detailed_validation_report(self, date_str, recipients, email_summary, 
                                      report_path=None, batch_start=None, batch_end=None,
                                      cumulative_start=None, cumulative_end=None):
        """
        Send detailed validation report with enhanced statistics and attachments
        """
        try:
            # Extract statistics from email_summary
            statistics = email_summary.get('statistics', {})
            total_invoices = statistics.get('total_invoices', 0)
            failed_invoices = statistics.get('failed_invoices', 0)
            warning_invoices = statistics.get('warning_invoices', 0)
            passed_invoices = statistics.get('passed_invoices', 0)
            
            # Prepare validation data for the enhanced email template
            validation_data = {
                'failed': failed_invoices,
                'warnings': warning_invoices,
                'passed': passed_invoices
            }
            
            # Create deadline (3 days from now for urgent items)
            deadline_date = datetime.now() + timedelta(days=3)
            
            # Generate professional HTML email using existing template
            html_body = self.email_system.create_professional_html_template(
                validation_data, 
                deadline_date
            )
            
            # Add enhanced validation summary to the email
            enhanced_summary = f"""
            

                

                    📊 VALIDATION PERIOD SUMMARY
                

                

                            Validation Date:
                        	
                            {date_str}
                        

                            Current Batch:
                        	
                            {batch_start or 'N/A'} to {batch_end or 'N/A'}
                        

                            Cumulative Range:
                        	
                            {cumulative_start or 'N/A'} to {cumulative_end or 'N/A'}
                        

                            Total Invoices:
                        	
                            {total_invoices}
                        

            

            """
            
            # Insert enhanced summary into the HTML body
            html_body = html_body.replace(
                '',
                f'\n{enhanced_summary}'
            )
            
            # Create subject line with validation statistics
            pass_rate = (passed_invoices / total_invoices * 100) if total_invoices > 0 else 0
            subject = f"📊 Invoice Validation Report - {date_str} | {total_invoices} Invoices | {pass_rate:.1f}% Pass Rate"
            
            # Create ZIP file with validation reports
            zip_file = None
            if report_path and os.path.exists(report_path):
                try:
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    zip_filename = f'validation_report_{timestamp}.zip'
                    
                    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                        # Add main validation report
                        zipf.write(report_path, f'validation_report_{date_str}.xlsx')
                        
                        # Add enhanced report if it exists
                        enhanced_report_path = report_path.replace('invoice_validation_detailed_', 'enhanced_invoice_validation_detailed_')
                        if os.path.exists(enhanced_report_path):
                            zipf.write(enhanced_report_path, f'enhanced_validation_report_{date_str}.xlsx')
                        
                        # Add email summary HTML if it exists
                        email_summary_path = f'data/email_summary_{date_str}.html'
                        if os.path.exists(email_summary_path):
                            zipf.write(email_summary_path, f'email_summary_{date_str}.html')
                    
                    zip_file = zip_filename
                    print(f"📦 Created validation ZIP: {zip_filename}")
                    
                except Exception as e:
                    print(f"⚠️ Could not create ZIP file: {e}")
            
            # Send email with attachments
            success = self.email_system.send_email_with_attachments(
                recipients, 
                subject, 
                html_body, 
                zip_file
            )
            
            if success:
                print(f"✅ Detailed validation report sent successfully to: {', '.join(recipients)}")
                print(f"   📊 Statistics: {total_invoices} total, {passed_invoices} passed, {failed_invoices} failed")
                return True
            else:
                print(f"❌ Failed to send detailed validation report")
                return False
                
        except Exception as e:
            print(f"❌ Error in send_detailed_validation_report: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def send_validation_report(self, date_str, recipients, issues_count):
        """
        Fallback method for basic validation report (maintains compatibility)
        """
        try:
            # Create basic validation data
            validation_data = {
                'failed': issues_count,
                'warnings': max(0, issues_count // 2),
                'passed': max(0, 10 - issues_count)  # Assume some baseline
            }
            
            # Create deadline
            deadline_date = datetime.now() + timedelta(days=2)
            
            # Generate HTML email
            html_body = self.email_system.create_professional_html_template(
                validation_data, 
                deadline_date
            )
            
            # Create subject
            subject = f"📋 Invoice Validation Report - {date_str} | {issues_count} Issues Found"
            
            # Send basic email (no ZIP attachment for fallback)
            success = self.email_system.send_email_with_attachments(
                recipients, 
                subject, 
                html_body, 
                None  # No ZIP file for basic report
            )
            
            if success:
                print(f"✅ Basic validation report sent to: {', '.join(recipients)}")
                return True
            else:
                print(f"❌ Failed to send basic validation report")
                return False
                
        except Exception as e:
            print(f"❌ Error in send_validation_report: {str(e)}")
            return False

# Add this to your EmailNotifier class in email_notifier.py

def test_email_connection(self):
    """Test email connection and authentication"""
    try:
        print("🔍 Testing email connection...")
        print(f"📧 SMTP Server: {self.email_system.smtp_server}")
        print(f"📧 SMTP Port: {self.email_system.smtp_port}")
        print(f"📧 Username: {self.email_system.username}")
        print(f"📧 Recipients: {len(self.email_system.default_recipients)} configured")
        
        # Test SMTP connection
        import smtplib
        with smtplib.SMTP(self.email_system.smtp_server, self.email_system.smtp_port) as server:
            server.starttls()
            server.login(self.email_system.username, self.email_system.password)
            print("✅ SMTP connection successful")
            return True
            
    except Exception as e:
        print(f"❌ Email connection failed: {e}")
        return False
        
def send_test_email(self):
    """Send a simple test email"""
    try:
        if not self.test_email_connection():
            return False
            
        # Send simple test
        test_subject = "🧪 Invoice System Test Email"
        test_body = """
        <html><body>
        <h2>Test Email from Invoice Validation System</h2>
        <p>If you receive this email, the system is working correctly.</p>
        <p>Time: {}</p>
        </body></html>
        """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        success = self.email_system.send_email_with_attachments(
            self.email_system.default_recipients,
            test_subject,
            test_body,
            None  # No attachment
        )
        
        return success
        
    except Exception as e:
        print(f"❌ Test email failed: {e}")
        return False

def main():
    """Test the enhanced email system"""
    print("🧪 Testing email system...")
    
    # Initialize email system
    email_system = EnhancedEmailSystem()
    
    if not email_system.username or not email_system.password:
        print("⚠️ SMTP credentials not configured")
        return "SMTP credentials not configured"
    
    if not email_system.default_recipients:
        print("⚠️ No recipients specified")
        return "No recipients specified"
    
    logging.info(f"📧 Email system initialized with {len(email_system.default_recipients)} recipients")
    
    # Find Excel report
    excel_files = glob.glob('invoice_validation_report_*.xlsx')
    if not excel_files:
        print("📊 No Excel reports found")
        return "No Excel reports found"
    
    latest_excel = max(excel_files, key=os.path.getctime)
    print(f"📊 Found Excel report: {os.path.basename(latest_excel)}")
    
    # Create ZIP with invoices
    zip_file = email_system.create_invoice_zip()
    if not zip_file:
        print("❌ Failed to create ZIP file")
        return "Failed to create ZIP"
    
    # Sample validation data
    validation_data = {
        'failed': 3,
        'warnings': 4,
        'passed': 8
    }
    
    # Create professional HTML email
    deadline_date = datetime.now() + timedelta(days=3)
    html_body = email_system.create_professional_html_template(validation_data, deadline_date)
    
    # Send email
    subject = f"🚨 URGENT: Invoice Validation - Action Required by {deadline_date.strftime('%b %d, %Y')}"
    
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
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    result = main()
    print(f"📧 Email result: {result}")
    print(f"✅ Test result: {'Passed' if result == 'Success' else 'Failed'}")
