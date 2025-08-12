import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import zipfile
import os
from datetime import datetime, timedelta
import shutil

class EnhancedEmailSystem:
    def __init__(self, smtp_server, smtp_port, username, password):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
    
    def create_invoice_zip(self, invoice_files, validation_period):
        """Create ZIP file with invoice copies for the validation period"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        zip_filename = f'Invoice_Files_RMS_{validation_period}_{timestamp}.zip'
        
        with zipfile.ZipFile(zip_filename, 'w') as zipf:
            for invoice_file in invoice_files:
                if os.path.exists(invoice_file):
                    zipf.write(invoice_file, os.path.basename(invoice_file))
        
        return zip_filename
    
    def generate_enhanced_email_body(self, validation_summary, changes_detected):
        """Generate enhanced email body with all new features"""
        current_date = datetime.now().strftime('%d %B %Y')
        deadline_date = (datetime.now() + timedelta(days=4)).strftime('%d %B %Y')
        
        # Base email content
        email_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
        
        <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; margin-bottom: 20px;">
            <h2 style="margin: 0;">🧾 Enhanced Invoice Validation Report</h2>
            <p style="margin: 5px 0; font-size: 16px;">Automated Multi-Location GST/VAT Compliance System</p>
        </div>
        
        <div style="background-color: #f8f9fa; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
            <h3 style="color: #495057; margin-top: 0;">📌 Executive Summary</h3>
            <p>Dear Team,</p>
            <p>Please find attached the comprehensive automated invoice validation report for <strong>{current_date}</strong>.</p>
        </div>
        
        <div style="background-color: #e3f2fd; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
            <h3 style="color: #1565c0; margin-top: 0;">🔍 Enhanced Validation Summary</h3>
            <table style="width: 100%; border-collapse: collapse;">
                <tr><td style="padding: 5px; font-weight: bold;">🗓️ Validation Date:</td><td style="padding: 5px;">{current_date}</td></tr>
                <tr><td style="padding: 5px; font-weight: bold;">📊 Report Period:</td><td style="padding: 5px;">Last 4 days</td></tr>
                <tr><td style="padding: 5px; font-weight: bold;">🧾 Historical Check:</td><td style="padding: 5px;">Last 3 months</td></tr>
                <tr><td style="padding: 5px; font-weight: bold;">⚠️ Issues Detected:</td><td style="padding: 5px; color: #d32f2f; font-weight: bold;">{validation_summary.get('total_issues', 0)} invoices flagged</td></tr>
                <tr><td style="padding: 5px; font-weight: bold;">🌍 Locations Covered:</td><td style="padding: 5px;">{validation_summary.get('locations_count', 0)} global locations</td></tr>
                <tr><td style="padding: 5px; font-weight: bold;">💱 Currencies Processed:</td><td style="padding: 5px;">{', '.join(validation_summary.get('currencies', []))}</td></tr>
                <tr><td style="padding: 5px; font-weight: bold;">🔄 Historical Changes:</td><td style="padding: 5px; color: #ff9800; font-weight: bold;">{len(changes_detected)} modifications detected</td></tr>
            </table>
        </div>
        """
        
        # Add GST/VAT breakdown section
        if validation_summary.get('tax_breakdown'):
            email_body += """
            <div style="background-color: #fff3e0; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
                <h3 style="color: #ef6c00; margin-top: 0;">🧮 GST/VAT Compliance Summary</h3>
                <table style="width: 100%; border-collapse: collapse;">
            """
            for location, tax_info in validation_summary['tax_breakdown'].items():
                email_body += f"""
                <tr>
                    <td style="padding: 8px; border-bottom: 1px solid #ddd; font-weight: bold;">{location}</td>
                    <td style="padding: 8px; border-bottom: 1px solid #ddd;">{tax_info['type']}</td>
                    <td style="padding: 8px; border-bottom: 1px solid #ddd; color: #2e7d32;">₹{tax_info['total_tax']:,.2f}</td>
                </tr>
                """
            email_body += "</table></div>"
        
        # Add due date notifications
        if validation_summary.get('due_date_alerts'):
            email_body += f"""
            <div style="background-color: #ffebee; padding: 15px; border-radius: 8px; margin-bottom: 20px; border-left: 4px solid #f44336;">
                <h3 style="color: #c62828; margin-top: 0;">⏰ Critical Due Date Alerts</h3>
                <p><strong>{len(validation_summary['due_date_alerts'])} invoices</strong> are due within the next 5 days:</p>
                <ul>
            """
            for alert in validation_summary['due_date_alerts'][:5]:  # Show top 5
                email_body += f"<li style='color: #d32f2f;'><strong>{alert['invoice_number']}</strong> - Due: {alert['due_date']} ({alert['vendor']})</li>"
            email_body += "</ul></div>"
        
        # Add historical changes section
        if changes_detected:
            email_body += f"""
            <div style="background-color: #fce4ec; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
                <h3 style="color: #ad1457; margin-top: 0;">📊 Historical Data Changes (Last 3 Months)</h3>
                <p><strong>{len(changes_detected)} modifications/deletions detected</strong> in previously validated invoices:</p>
                <ul>
            """
            for change in changes_detected[:10]:  # Show top 10 changes
                email_body += f"""
                <li><strong>{change['invoice_id']}</strong> - {change['field_name']}: 
                    <span style='color: #d32f2f; text-decoration: line-through;'>{change['old_value']}</span> 
                    → <span style='color: #388e3c;'>{change['new_value']}</span>
                </li>
                """
            email_body += "</ul></div>"
        
        # Attachments section
        email_body += """
        <div style="background-color: #f1f8e9; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
            <h3 style="color: #33691e; margin-top: 0;">📎 Enhanced Attachments</h3>
            <ul style="list-style-type: none; padding: 0;">
                <li style="padding: 5px 0;">✅ <strong>Enhanced Invoice Validation Report</strong> (Excel format with new fields)</li>
                <li style="padding: 5px 0;">🗂️ <strong>Invoice Files from RMS</strong> (ZIP folder - validation period only)</li>
                <li style="padding: 5px 0;">📋 <strong>Historical Changes Log</strong> (CSV format - 3 months tracking)</li>
                <li style="padding: 5px 0;">📊 <strong>GST/VAT Compliance Summary</strong> (PDF format)</li>
            </ul>
        </div>
        
        <div style="background-color: #fff8e1; padding: 15px; border-radius: 8px; margin-bottom: 20px; border: 2px solid #ffc107;">
            <h3 style="color: #f57c00; margin-top: 0;">⏳ Action Required</h3>
            <p style="font-size: 16px; font-weight: bold;">Please review and rectify all flagged invoices by <span style="color: #d32f2f;">{deadline_date} (EOD)</span> to ensure:</p>
            <ul>
                <li>✅ Timely GST/VAT compliance across all locations</li>
                <li>✅ Data accuracy and historical integrity</li>
                <li>✅ Due date adherence for payment processing</li>
                <li>✅ Multi-currency validation completeness</li>
            </ul>
            <p style="color: #d32f2f; font-weight: bold;">⚠️ Failure to address discrepancies by the deadline may result in compliance violations and reporting delays.</p>
        </div>
        
        <div style="background-color: #e8f5e8; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
            <h3 style="color: #2e7d32; margin-top: 0;">🌍 Global Coverage</h3>
            <p><strong>Our Enhanced System Now Covers:</strong></p>
            <div style="display: flex; flex-wrap: wrap; gap: 10px;">
                <span style="background-color: #4caf50; color: white; padding: 5px 10px; border-radius: 15px; font-size: 12px;">🇮🇳 India (All Branches)</span>
                <span style="background-color: #2196f3; color: white; padding: 5px 10px; border-radius: 15px; font-size: 12px;">🇺🇸 USA</span>
                <span style="background-color: #9c27b0; color: white; padding: 5px 10px; border-radius: 15px; font-size: 12px;">🇬🇧 UK</span>
                <span style="background-color: #ff5722; color: white; padding: 5px 10px; border-radius: 15px; font-size: 12px;">🇨🇦 Canada</span>
                <span style="background-color: #795548; color: white; padding: 5px 10px; border-radius: 15px; font-size: 12px;">🇩🇪 Germany</span>
                <span style="background-color: #607d8b; color: white; padding: 5px 10px; border-radius: 15px; font-size: 12px;">🇦🇪 Dubai</span>
                <span style="background-color: #e91e63; color: white; padding: 5px 10px; border-radius: 15px; font-size: 12px;">🇸🇬 Singapore</span>
                <span style="background-color: #3f51b5; color: white; padding: 5px 10px; border-radius: 15px; font-size: 12px;">🇦🇺 Australia</span>
            </div>
        </div>
        
        <p>For any clarification or assistance, feel free to reach out to the Finance or Accounts Team.</p>
        
        <div style="margin-top: 30px; padding: 15px; background-color: #f5f5f5; border-radius: 8px; text-align: center;">
            <p style="margin: 0; color: #666;">Best regards,</p>
            <p style="margin: 5px 0; font-weight: bold; color: #1976d2;">🧠 Enhanced Invoice Validation System v2.0</p>
            <p style="margin: 0; color: #666;"><strong>Koenig Solutions Pvt. Ltd.</strong></p>
            <p style="margin: 5px 0; font-size: 12px; color: #999;">Automated Multi-Location GST/VAT Compliance & Historical Tracking</p>
        </div>
        
        </body>
        </html>
        """
        
        return email_body
    
    def send_enhanced_email(self, recipients, validation_summary, changes_detected, attachments):
        """Send enhanced email with all attachments"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.username
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = f"🧾 Enhanced Invoice Validation Report - {datetime.now().strftime('%d %B %Y')} | Multi-Location GST/VAT Compliance"
            
            # Add enhanced email body
            body = self.generate_enhanced_email_body(validation_summary, changes_detected)
            msg.attach(MIMEText(body, 'html'))
            
            # Add all attachments
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
            
            # Send email
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.username, self.password)
            
            text = msg.as_string()
            server.sendmail(self.username, recipients, text)
            server.quit()
            
            return True, "Enhanced email sent successfully"
            
        except Exception as e:
            return False, f"Failed to send email: {str(e)}"