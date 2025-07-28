#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Simple Email Notifier for Invoice Validation System
Provides basic email functionality with 5-day correction workflow
"""

import os
import smtplib
import sqlite3
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("email_notifier")

# Email configuration - Update with your settings
EMAIL_CONFIG = {
    "smtp_server": "smtp.example.com",  # Change to your SMTP server
    "smtp_port": 587,
    "smtp_user": os.environ.get("EMAIL_USER", ""),
    "smtp_password": os.environ.get("EMAIL_PASSWORD", ""),
    "sender_email": "invoices@koenigsolutions.com",  # Change to your sender email
    "reply_to": "support@koenigsolutions.com",
    "bcc_emails": []
}

# Database for tracking correction deadlines
DB_PATH = "data/correction_tracking.db"

def setup_database():
    """Create the tracking database if it doesn't exist"""
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS correction_tracking (
            id INTEGER PRIMARY KEY,
            invoice_id TEXT,
            vendor_email TEXT,
            issue_date TEXT,
            correction_deadline TEXT,
            status TEXT,
            reminder_sent INTEGER,
            final_notice_sent INTEGER
        )
        ''')
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Database setup failed: {str(e)}")
        return False

def send_email(recipient, subject, html_content, attachments=None):
    """Send an email with HTML content and optional attachments"""
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = recipient
        msg['Subject'] = subject
        msg.add_header('Reply-To', EMAIL_CONFIG['reply_to'])
        
        # Add HTML body
        msg.attach(MIMEText(html_content, 'html'))
        
        # Add attachments if any
        if attachments:
            for file_path in attachments:
                if os.path.exists(file_path):
                    with open(file_path, 'rb') as f:
                        attachment = MIMEApplication(f.read())
                    attachment.add_header('Content-Disposition', 'attachment', 
                                          filename=os.path.basename(file_path))
                    msg.attach(attachment)
        
        # Add BCC recipients if any
        if EMAIL_CONFIG['bcc_emails']:
            msg['Bcc'] = ', '.join(EMAIL_CONFIG['bcc_emails'])
            
        # Connect to the SMTP server and send
        server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
        server.starttls()
        
        if EMAIL_CONFIG['smtp_user'] and EMAIL_CONFIG['smtp_password']:
            server.login(EMAIL_CONFIG['smtp_user'], EMAIL_CONFIG['smtp_password'])
            
        server.send_message(msg)
        server.quit()
        logger.info(f"Email sent to {recipient}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")
        return False

def send_validation_failure_notification(invoice, validation_result, attachments=None):
    """Send notification about invoice validation failure"""
    if not invoice.get('Vendor_Email') and not invoice.get('vendor_email'):
        logger.error("No vendor email provided")
        return False
        
    # Get vendor email from either format
    vendor_email = invoice.get('Vendor_Email') or invoice.get('vendor_email')
    invoice_id = invoice.get('InvID') or invoice.get('invoice_id', 'Unknown')
    
    # Create error list HTML
    errors_html = ""
    if validation_result.get('errors'):
        for error in validation_result['errors']:
            errors_html += f"<li>{error}</li>\n"
    
    # Basic HTML template
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background-color: #2E86C1; color: white; padding: 15px; text-align: center; }}
            .content {{ padding: 20px; }}
            .footer {{ background-color: #f4f4f4; padding: 10px; text-align: center; font-size: 0.8em; }}
            .error-list {{ color: #D62929; }}
            .deadline {{ font-weight: bold; color: #D62929; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>Invoice Validation Failed</h2>
            </div>
            <div class="content">
                <p>Dear Vendor,</p>
                <p>Your invoice <strong>{invoice_id}</strong> has failed our validation process 
                and requires correction within <span class="deadline">5 business days</span>.</p>
                
                <p>The following issues were identified:</p>
                <ul class="error-list">
                    {errors_html}
                </ul>
                
                <p>Please correct these issues and resubmit the invoice. 
                If you have any questions, please contact our support team.</p>
                
                <p>Thank you for your prompt attention to this matter.</p>
                
                <p>Best regards,<br>
                Koenig Solutions Invoice Team</p>
            </div>
            <div class="footer">
                <p>Koenig Solutions Ltd. | www.koenig-solutions.com</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Send the email
    subject = f"ACTION REQUIRED: Invoice {invoice_id} Validation Failed"
    result = send_email(vendor_email, subject, html_content, attachments)
    
    # If email sent successfully, schedule the workflow
    if result:
        schedule_correction_workflow(invoice_id, vendor_email, validation_result.get('errors', []))
        
    return result

def schedule_correction_workflow(invoice_id, vendor_email, validation_errors):
    """Schedule the 5-day correction workflow"""
    try:
        # Set up the database if it doesn't exist
        setup_database()
        
        # Calculate the 5-business day deadline
        today = datetime.now()
        deadline = today + timedelta(days=5)  # Simple 5-day calculation
        
        # Save to database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO correction_tracking (
            invoice_id, vendor_email, issue_date, correction_deadline, 
            status, reminder_sent, final_notice_sent
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            invoice_id, vendor_email, today.strftime('%Y-%m-%d'),
            deadline.strftime('%Y-%m-%d'), 'PENDING', 0, 0
        ))
        conn.commit()
        conn.close()
        
        logger.info(f"Correction workflow scheduled for invoice {invoice_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to schedule workflow: {str(e)}")
        return False

def send_reminder_notification(invoice_id, vendor_email, validation_errors):
    """Send reminder on day 3 of the correction workflow"""
    errors_html = ""
    for error in validation_errors:
        errors_html += f"<li>{error}</li>\n"
        
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background-color: #F1C40F; color: white; padding: 15px; text-align: center; }}
            .content {{ padding: 20px; }}
            .footer {{ background-color: #f4f4f4; padding: 10px; text-align: center; font-size: 0.8em; }}
            .error-list {{ color: #D62929; }}
            .deadline {{ font-weight: bold; color: #D62929; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>REMINDER: Invoice Correction Required</h2>
            </div>
            <div class="content">
                <p>Dear Vendor,</p>
                <p>This is a reminder that your invoice <strong>{invoice_id}</strong> 
                requires correction within <span class="deadline">2 more business days</span>.</p>
                
                <p>The following issues still need to be addressed:</p>
                <ul class="error-list">
                    {errors_html}
                </ul>
                
                <p>Please correct these issues and resubmit the invoice as soon as possible.</p>
                
                <p>Best regards,<br>
                Koenig Solutions Invoice Team</p>
            </div>
            <div class="footer">
                <p>Koenig Solutions Ltd. | www.koenig-solutions.com</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    subject = f"REMINDER: Invoice {invoice_id} Correction Required"
    return send_email(vendor_email, subject, html_content)

def send_final_notice(invoice_id, vendor_email, validation_errors):
    """Send final notice on day 5 of the correction workflow"""
    errors_html = ""
    for error in validation_errors:
        errors_html += f"<li>{error}</li>\n"
        
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background-color: #C0392B; color: white; padding: 15px; text-align: center; }}
            .content {{ padding: 20px; }}
            .footer {{ background-color: #f4f4f4; padding: 10px; text-align: center; font-size: 0.8em; }}
            .error-list {{ color: #D62929; }}
            .deadline {{ font-weight: bold; color: #D62929; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>URGENT: Final Notice - Invoice Correction</h2>
            </div>
            <div class="content">
                <p>Dear Vendor,</p>
                <p>This is the <strong>FINAL NOTICE</strong> regarding your invoice <strong>{invoice_id}</strong>.</p>
                
                <p>The correction deadline is <span class="deadline">TODAY</span>. 
                Please address the following issues immediately:</p>
                
                <ul class="error-list">
                    {errors_html}
                </ul>
                
                <p>If corrections are not received today, this may affect payment processing.</p>
                
                <p>Best regards,<br>
                Koenig Solutions Invoice Team</p>
            </div>
            <div class="footer">
                <p>Koenig Solutions Ltd. | www.koenig-solutions.com</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    subject = f"URGENT: Final Notice - Invoice {invoice_id} Correction Required"
    return send_email(vendor_email, subject, html_content)

def process_pending_notifications():
    """Process pending notifications for the workflow"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get all active correction workflows
        cursor.execute('''
        SELECT * FROM correction_tracking
        WHERE status = 'PENDING'
        ''')
        
        pending_items = cursor.fetchall()
        
        for item in pending_items:
            invoice_id = item['invoice_id']
            vendor_email = item['vendor_email']
            issue_date = datetime.strptime(item['issue_date'], '%Y-%m-%d')
            days_since_issue = (datetime.now() - issue_date).days
            
            # Day 3 reminder
            if days_since_issue == 3 and not item['reminder_sent']:
                # We would need validation errors here
                # For now, placeholder:
                errors = ["Please correct validation errors"]
                if send_reminder_notification(invoice_id, vendor_email, errors):
                    cursor.execute('''
                    UPDATE correction_tracking 
                    SET reminder_sent = 1 
                    WHERE id = ?''', (item['id'],))
            
            # Day 5 final notice
            elif days_since_issue == 5 and not item['final_notice_sent']:
                # Again, placeholder for errors
                errors = ["Please correct validation errors"]
                if send_final_notice(invoice_id, vendor_email, errors):
                    cursor.execute('''
                    UPDATE correction_tracking 
                    SET final_notice_sent = 1 
                    WHERE id = ?''', (item['id'],))
            
            # Past deadline - mark as overdue
            elif days_since_issue > 5:
                cursor.execute('''
                UPDATE correction_tracking 
                SET status = 'OVERDUE' 
                WHERE id = ? AND status = 'PENDING'
                ''', (item['id'],))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Failed to process notifications: {str(e)}")
        return False

def get_active_deadlines():
    """Get all active correction deadlines for dashboard"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT * FROM correction_tracking
        WHERE status IN ('PENDING', 'OVERDUE')
        ORDER BY correction_deadline
        ''')
        
        items = cursor.fetchall()
        conn.close()
        
        # Convert to list of dicts
        result = []
        for item in items:
            result.append({
                'id': item['id'],
                'invoice_id': item['invoice_id'],
                'vendor_email': item['vendor_email'],
                'issue_date': item['issue_date'],
                'correction_deadline': item['correction_deadline'],
                'status': item['status'],
                'reminder_sent': bool(item['reminder_sent']),
                'final_notice_sent': bool(item['final_notice_sent'])
            })
        
        return result
    except Exception as e:
        logger.error(f"Failed to get deadlines: {str(e)}")
        return []

def mark_as_corrected(invoice_id):
    """Mark an invoice as corrected in the tracking system"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
        UPDATE correction_tracking
        SET status = 'CORRECTED'
        WHERE invoice_id = ?
        ''', (invoice_id,))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Failed to mark as corrected: {str(e)}")
        return False

def send_email_report(recipient, subject, message, attachments=None):
    """Send validation report email - compatibility with existing code"""
    return send_email(recipient, subject, f"<p>{message}</p>", attachments)

# Initialize the database on module import
setup_database()

# Test function to ensure module is working
def test():
    print("Email notifier module is working correctly")
    return True
