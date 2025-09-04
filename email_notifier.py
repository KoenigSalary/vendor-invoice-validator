import smtplib
import os
import logging
import zipfile
import glob
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta

class EnhancedEmailSystem:
    """Enhanced email system with professional templates and comprehensive error handling"""

    def __init__(self):
        """Initialize the email system with environment configuration"""
        self.smtp_server = os.getenv('EMAIL_SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('EMAIL_SMTP_PORT', '587'))
        self.username = os.getenv('EMAIL_USERNAME', '')
        self.password = os.getenv('EMAIL_PASSWORD', '')
        default_recipients_str = os.getenv('EMAIL_RECIPIENTS', '')
        self.default_recipients = [email.strip() for email in default_recipients_str.split(',') if email.strip()]
        if not all([self.username, self.password]):
            logging.warning('Email credentials not configured - email functionality may be limited')

    def create_professional_html_template(self, validation_data, deadline_date):
        """Create professional HTML email template with comprehensive validation statistics"""
        stats = validation_data.get('statistics', {})
        date_ranges = validation_data.get('date_ranges', {})
        total_invoices = stats.get('total_invoices', 0)
        passed_invoices = stats.get('passed_invoices', 0)
        warning_invoices = stats.get('warning_invoices', 0)
        failed_invoices = stats.get('failed_invoices', 0)
        pass_rate = stats.get('pass_rate', 0)
        health_status = stats.get('health_status', 'Unknown')
        if health_status == 'Excellent':
            status_color = '#28a745'
            status_icon = 'âœ…'
        elif health_status == 'Good':
            status_color = '#17a2b8'
            status_icon = 'âœ…'
        elif health_status == 'Fair':
            status_color = '#ffc107'
            status_icon = 'âš ï¸'
        else:
            status_color = '#dc3545'
            status_icon = 'âŒ'
        html_template = f"""\n        <!DOCTYPE html>\n        <html>\n        <head>\n            <meta charset="UTF-8">\n            <meta name="viewport" content="width=device-width, initial-scale=1.0">\n            <title>Invoice Validation Report</title>\n            <style>\n                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}\n                .container {{ max-width: 800px; margin: 0 auto; background-color: white; border-radius: 10px; box-shadow: 0 0 20px rgba(0,0,0,0.1); overflow: hidden; }}\n                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; text-align: center; }}\n                .header h1 {{ margin: 0; font-size: 28px; font-weight: 300; }}\n                .header p {{ margin: 10px 0 0 0; opacity: 0.9; }}\n                .content {{ padding: 30px; }}\n                .alert {{ background-color: #fff3cd; border: 1px solid #ffeaa7; border-radius: 5px; padding: 15px; margin: 20px 0; }}\n                .alert-critical {{ background-color: #f8d7da; border-color: #f5c6cb; }}\n                .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 25px 0; }}\n                .stat-card {{ background: #f8f9fa; border-radius: 8px; padding: 20px; text-align: center; border-left: 4px solid #007bff; }}\n                .stat-card.passed {{ border-left-color: #28a745; }}\n                .stat-card.warning {{ border-left-color: #ffc107; }}\n                .stat-card.failed {{ border-left-color: #dc3545; }}\n                .stat-card.health {{ border-left-color: {status_color}; }}\n                .stat-number {{ font-size: 32px; font-weight: bold; margin-bottom: 5px; }}\n                .stat-label {{ color: #6c757d; font-size: 14px; }}\n                .section {{ margin: 25px 0; padding: 20px; background: #f8f9fa; border-radius: 8px; }}\n                .section h3 {{ margin-top: 0; color: #495057; }}\n                .action-button {{ display: inline-block; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 12px 25px; text-decoration: none; border-radius: 5px; margin: 10px 5px; font-weight: 500; }}\n                .footer {{ background: #343a40; color: white; padding: 20px; text-align: center; font-size: 12px; }}\n                .progress-bar {{ width: 100%; height: 20px; background: #e9ecef; border-radius: 10px; overflow: hidden; margin: 10px 0; }}\n                .progress-fill {{ height: 100%; background: linear-gradient(90deg, #28a745, #20c997); transition: width 0.3s ease; }}\n                .deadline-notice {{ background: linear-gradient(135deg, #ff7b7b 0%, #ff416c 100%); color: white; padding: 15px; border-radius: 8px; margin: 20px 0; text-align: center; }}\n            </style>\n        </head>\n        <body>\n            <div class="container">\n                <div class="header">\n                    <h1>ðŸš¨ Invoice Validation Report</h1>\n                    <p>Comprehensive Analysis & Action Required</p>\n                </div>\n\n                <div class="content">\n                    <div class="deadline-notice">\n                        <h3 style="margin: 0 0 10px 0;">â° URGENT ACTION REQUIRED</h3>\n                        <p style="margin: 0; font-size: 16px;">Please review and address all validation issues by <strong>{deadline_date.strftime('%B %d, %Y')}</strong></p>\n                    </div>\n\n                    <div class="alert {('alert-critical' if failed_invoices > 0 else '')}">\n                        <strong>{status_icon} System Health Status: {health_status}</strong>\n                        <br>Validation completed for {total_invoices} invoices with {pass_rate:.1f}% success rate.\n                    </div>\n\n                    <div class="stats-grid">\n                        <div class="stat-card passed">\n                            <div class="stat-number" style="color: #28a745;">{passed_invoices}</div>\n                            <div class="stat-label">Passed Invoices</div>\n                        </div>\n                        <div class="stat-card warning">\n                            <div class="stat-number" style="color: #ffc107;">{warning_invoices}</div>\n                            <div class="stat-label">Warnings</div>\n                        </div>\n                        <div class="stat-card failed">\n                            <div class="stat-number" style="color: #dc3545;">{failed_invoices}</div>\n                            <div class="stat-label">Failed Invoices</div>\n                        </div>\n                        <div class="stat-card health">\n                            <div class="stat-number" style="color: {status_color};">{pass_rate:.1f}%</div>\n                            <div class="stat-label">Success Rate</div>\n                        </div>\n                    </div>\n\n                    <div class="section">\n                        <h3>ðŸ“… Validation Period</h3>\n                        <p><strong>Current Batch:</strong> {date_ranges.get('current_batch', 'N/A')}</p>\n                        <p><strong>Cumulative Range:</strong> {date_ranges.get('cumulative', 'N/A')}</p>\n                        <p><strong>Report Generated:</strong> {date_ranges.get('validation_date', 'N/A')}</p>\n                    </div>\n\n                    <div class="section">\n                        <h3>ðŸŽ¯ Success Rate Analysis</h3>\n                        <div class="progress-bar">\n                            <div class="progress-fill" style="width: {min(pass_rate, 100)}%;"></div>\n                        </div>\n                        <p>Current pass rate: <strong>{pass_rate:.1f}%</strong> ({passed_invoices} of {total_invoices} invoices)</p>\n                    </div>\n\n                    <div class="section">\n                        <h3>ðŸ“‹ Action Items</h3>\n                        <ul>\n                            {('<li><strong>HIGH PRIORITY:</strong> Review and resolve ' + str(failed_invoices) + ' failed invoices</li>' if failed_invoices > 0 else '')}\n                            {('<li><strong>MEDIUM PRIORITY:</strong> Address ' + str(warning_invoices) + ' warning cases</li>' if warning_invoices > 0 else '')}\n                            <li>Review detailed validation report (attached)</li>\n                            <li>Update invoice processing procedures if needed</li>\n                            <li>Confirm resolution by deadline: <strong>{deadline_date.strftime('%B %d, %Y')}</strong></li>\n                        </ul>\n                    </div>\n\n                    <div style="text-align: center; margin: 30px 0;">\n                        <a href="mailto:support@company.com?subject=Invoice Validation Support" class="action-button">\n                            ðŸ“ž Contact Support\n                        </a>\n                        <a href="#" class="action-button">\n                            ðŸ“Š View Dashboard\n                        </a>\n                    </div>\n                </div>\n\n                <div class="footer">\n                    <p>This is an automated report from the Invoice Validation System</p>\n                    <p>Generated on {datetime.now().strftime('%Y-%m-%d at %H:%M:%S UTC')}</p>\n                    <p>For technical support, please contact the IT team</p>\n                </div>\n            </div>\n        </body>\n        </html>\n        """
        return html_template

    def create_invoice_zip(self, invoice_files=None, validation_period=None):
        """Create a ZIP file containing invoice reports and validation data"""
        try:
            if not validation_period:
                validation_period = datetime.now().strftime('%Y-%m-%d')
            zip_filename = f"invoice_validation_report_{validation_period.replace(' ', '_').replace('to', '-')}.zip"
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                if invoice_files:
                    for file_path in invoice_files:
                        if os.path.exists(file_path):
                            archive_name = os.path.basename(file_path)
                            zipf.write(file_path, archive_name)
                            logging.info(f'Added {file_path} to ZIP as {archive_name}')
                        else:
                            logging.warning(f'File not found: {file_path}')
                report_patterns = ['reports/*.csv', 'reports/*.xlsx', '*validation*report*.csv', '*validation*report*.xlsx']
                for pattern in report_patterns:
                    matching_files = glob.glob(pattern)
                    for file_path in matching_files:
                        if file_path not in (invoice_files or []):
                            archive_name = os.path.basename(file_path)
                            zipf.write(file_path, archive_name)
                            logging.info(f'Added additional file {file_path} to ZIP')
                summary_content = f"Invoice Validation Report Summary\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\nValidation Period: {validation_period}\nFiles Included: {len(zipf.namelist())} files\n\nFile List:\n{chr(10).join((f'- {name}' for name in zipf.namelist()))}\n\nThis archive contains validation reports and supporting data.\nPlease review all files and take appropriate action on failed validations.\n"
                zipf.writestr('README.txt', summary_content)
            if os.path.exists(zip_filename):
                logging.info(f'Successfully created ZIP file: {zip_filename}')
                return zip_filename
            else:
                logging.error('Failed to create ZIP file')
                return None
        except Exception as e:
            logging.error(f'Error creating ZIP file: {str(e)}')
            return None

    def send_email_with_attachments(self, recipients, subject, html_body, zip_file=None):
        """Send professional email with optional ZIP attachment"""
        try:
            if not all([self.username, self.password]):
                logging.error('Email credentials not configured')
                return False
            if not recipients:
                logging.error('No recipients specified')
                return False
            msg = MIMEMultipart('alternative')
            msg['From'] = self.username
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = subject
            html_part = MIMEText(html_body, 'html')
            msg.attach(html_part)
            if zip_file and os.path.exists(zip_file):
                try:
                    with open(zip_file, 'rb') as attachment:
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', f'attachment; filename= {os.path.basename(zip_file)}')
                    msg.attach(part)
                    logging.info(f'Attached ZIP file: {zip_file}')
                except Exception as attach_error:
                    logging.error(f'Failed to attach ZIP file: {attach_error}')
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)
            logging.info(f'Email sent successfully to {len(recipients)} recipients')
            return True
        except Exception as e:
            logging.error(f'Failed to send email: {str(e)}')
            return False

    def send_detailed_validation_report(self, validation_date, recipients, email_summary, detailed_report_path=None, current_batch_start=None, current_batch_end=None, cumulative_start=None, cumulative_end=None):
        """
        Send detailed validation report with comprehensive statistics and attachments

        Args:
            validation_date (str): Date of validation
            recipients (list): List of email addresses
            email_summary (dict): Summary statistics and data
            detailed_report_path (str, optional): Path to detailed report file
            current_batch_start (str, optional): Start date of current batch
            current_batch_end (str, optional): End date of current batch
            cumulative_start (str, optional): Start date of cumulative range
            cumulative_end (str, optional): End date of cumulative range
        """
        try:
            logging.info(f'Sending detailed validation report for {validation_date}')
            deadline_date = datetime.now() + timedelta(days=3)
            enhanced_summary = email_summary.copy()
            if any([current_batch_start, current_batch_end, cumulative_start, cumulative_end]):
                enhanced_summary['date_ranges'] = {'current_batch': f'{current_batch_start} to {current_batch_end}' if current_batch_start and current_batch_end else 'N/A', 'cumulative': f'{cumulative_start} to {cumulative_end}' if cumulative_start and cumulative_end else 'N/A', 'validation_date': validation_date}
            html_body = self.create_professional_html_template(enhanced_summary, deadline_date)
            zip_file = None
            if detailed_report_path and os.path.exists(detailed_report_path):
                validation_period = f'{current_batch_start} to {current_batch_end}' if current_batch_start and current_batch_end else validation_date
                zip_file = self.create_invoice_zip([detailed_report_path], validation_period)
            stats = email_summary.get('statistics', {})
            failed_count = stats.get('failed_invoices', 0)
            total_count = stats.get('total_invoices', 0)
            if failed_count > 0:
                priority = 'ðŸš¨ URGENT'
                urgency = 'HIGH PRIORITY'
            elif stats.get('warning_invoices', 0) > 0:
                priority = 'âš ï¸ ACTION REQUIRED'
                urgency = 'MEDIUM PRIORITY'
            else:
                priority = 'âœ… INFORMATIONAL'
                urgency = 'LOW PRIORITY'
            subject = f"{priority}: Invoice Validation Report ({failed_count} failures, {total_count} total) - {deadline_date.strftime('%b %d, %Y')}"
            success = self.send_email_with_attachments(recipients, subject, html_body, zip_file)
            if success:
                logging.info(f'Detailed validation report sent successfully to {len(recipients)} recipients')
            else:
                logging.error('Failed to send detailed validation report')
            return success
        except Exception as e:
            logging.error(f'Error sending detailed validation report: {str(e)}')
            return False

    def send_validation_report(self, validation_date, recipients, issues_count):
        """
        Send basic validation report with issue count

        Args:
            validation_date (str): Date of validation
            recipients (list): List of email addresses  
            issues_count (int): Number of validation issues found
        """
        try:
            logging.info(f'Sending basic validation report for {validation_date}')
            email_summary = {'statistics': {'total_invoices': issues_count, 'passed_invoices': max(0, issues_count - issues_count), 'warning_invoices': 0, 'failed_invoices': issues_count, 'pass_rate': 0.0 if issues_count > 0 else 100.0, 'total_creators': 0, 'unknown_creators': 0, 'health_status': 'Poor' if issues_count > 0 else 'Excellent'}, 'date_ranges': {'current_batch': validation_date, 'cumulative': validation_date, 'validation_date': validation_date}}
            deadline_date = datetime.now() + timedelta(days=3)
            html_body = self.create_professional_html_template(email_summary, deadline_date)
            if issues_count > 0:
                subject = f"ðŸš¨ URGENT: {issues_count} Invoice Validation Issues - Action Required by {deadline_date.strftime('%b %d, %Y')}"
            else:
                subject = f'âœ… Invoice Validation Passed - {validation_date}'
            success = self.send_email_with_attachments(recipients, subject, html_body)
            if success:
                logging.info(f'Basic validation report sent successfully to {len(recipients)} recipients')
            else:
                logging.error('Failed to send basic validation report')
            return success
        except Exception as e:
            logging.error(f'Error sending basic validation report: {str(e)}')
            return False
EmailNotifier = EnhancedEmailSystem

def main():
    """Test the email system functionality"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    email_system = EnhancedEmailSystem()
    test_validation_data = {'statistics': {'total_invoices': 150, 'passed_invoices': 125, 'warning_invoices': 15, 'failed_invoices': 10, 'pass_rate': 83.3, 'total_creators': 8, 'unknown_creators': 2, 'health_status': 'Good'}, 'date_ranges': {'current_batch': '2024-01-01 to 2024-01-04', 'cumulative': '2023-12-01 to 2024-01-04', 'validation_date': '2024-01-04'}}
    deadline_date = datetime.now() + timedelta(days=3)
    html_body = email_system.create_professional_html_template(test_validation_data, deadline_date)
    print('âœ… HTML template created successfully')
    zip_file = email_system.create_invoice_zip(None, '2024-01-01 to 2024-01-04')
    print(f'âœ… ZIP file created: {zip_file}')
    subject = f"ðŸš¨ URGENT: Invoice Validation - Action Required by {deadline_date.strftime('%b %d, %Y')}"
    success = email_system.send_email_with_attachments(email_system.default_recipients, subject, html_body, zip_file)
    if success:
        print('âœ… Email sent successfully!')
        return 'Success'
    else:
        print('âŒ Email sending failed')
        return 'Failed'
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    result = main()
    print(f'ðŸ“§ Email result: {result}')
    print(f"âœ… Test result: {('Passed' if result == 'Success' else 'Failed')}")
