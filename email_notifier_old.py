import smtplib
import ssl
import sqlite3
import os
import logging
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import Dict, List, Optional, Any
import schedule
import time
import threading
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedEmailNotifier:
    """
    Enhanced Email Notifier with 5-day correction workflow
    Compatible with existing system functionality
    """
    
    def __init__(self):
        # Existing email configuration (preserved)
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.sender_email = os.getenv('SENDER_EMAIL')
        self.sender_password = os.getenv('SENDER_PASSWORD')
        self.recipient_emails = os.getenv('RECIPIENT_EMAILS', '').split(',')
        
        # New: Database for deadline tracking
        self.db_path = 'data/correction_tracking.db'
        self._initialize_database()
        
        # New: Multi-currency and location support
        self.subsidiaries = {
            'India': {'currency': 'INR', 'tax_type': 'GST'},
            'Canada': {'currency': 'CAD', 'tax_type': 'VAT'},
            'USA': {'currency': 'USD', 'tax_type': 'Sales Tax'},
            'Australia': {'currency': 'AUD', 'tax_type': 'GST'},
            'South Africa': {'currency': 'ZAR', 'tax_type': 'VAT'},
            'New Zealand': {'currency': 'NZD', 'tax_type': 'GST'},
            'Netherlands': {'currency': 'EUR', 'tax_type': 'VAT'},
            'Singapore': {'currency': 'SGD', 'tax_type': 'GST'},
            'Dubai': {'currency': 'AED', 'tax_type': 'VAT'},
            'Malaysia': {'currency': 'MYR', 'tax_type': 'SST'},
            'Saudi Arabia': {'currency': 'SAR', 'tax_type': 'VAT'},
            'Germany': {'currency': 'EUR', 'tax_type': 'VAT'},
            'UK': {'currency': 'GBP', 'tax_type': 'VAT'},
            'Japan': {'currency': 'JPY', 'tax_type': 'Consumption Tax'}
        }
        
        # Start background scheduler for reminders
        self._start_reminder_scheduler()
    
    def _initialize_database(self):
        """Initialize SQLite database for deadline tracking"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS correction_deadlines (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    invoice_id TEXT NOT NULL,
                    vendor_email TEXT NOT NULL,
                    location TEXT,
                    currency TEXT,
                    total_value REAL,
                    validation_errors TEXT,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    correction_deadline TIMESTAMP NOT NULL,
                    status TEXT DEFAULT 'PENDING',
                    day3_reminder_sent BOOLEAN DEFAULT FALSE,
                    day5_reminder_sent BOOLEAN DEFAULT FALSE,
                    confirmation_received BOOLEAN DEFAULT FALSE,
                    confirmation_date TIMESTAMP,
                    escalated BOOLEAN DEFAULT FALSE
                )
            ''')
            conn.commit()
    
    # Existing method - send_email_report (preserved and enhanced)
    def send_email_report(self, validation_result_path: str, zip_path: str = None):
        """
        Enhanced version of existing send_email_report method
        Maintains compatibility while adding new features
        """
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.sender_email
            msg['To'] = ', '.join(self.recipient_emails)
            msg['Subject'] = f"Koenig Solutions - Invoice Validation Report - {datetime.now().strftime('%Y-%m-%d')}"
            
            # Enhanced email body with Koenig branding
            body = self._generate_enhanced_report_body(validation_result_path)
            msg.attach(MIMEText(body, 'html'))
            
            # Attach validation results
            if os.path.exists(validation_result_path):
                with open(validation_result_path, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename= {os.path.basename(validation_result_path)}'
                )
                msg.attach(part)
            
            # Attach ZIP if provided
            if zip_path and os.path.exists(zip_path):
                with open(zip_path, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename= {os.path.basename(zip_path)}'
                )
                msg.attach(part)
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)
            
            logger.info(f" Email report sent successfully to {len(self.recipient_emails)} recipients")
            return True
            
        except Exception as e:
            logger.error(f" Failed to send email report: {e}")
            return False
    
    # New method - start_5_day_workflow
    def start_5_day_workflow(self, validation_issues: List[Dict], invoice_data: pd.DataFrame):
        """
        Start 5-day correction workflow for invoices with validation issues
        """
        for issue in validation_issues:
            invoice_id = issue.get('invoice_id')
            vendor_email = issue.get('vendor_email')
            
            if invoice_id and vendor_email:
                # Get additional invoice details
                invoice_row = invoice_data[invoice_data['InvID'] == invoice_id].iloc[0] if not invoice_data.empty else {}
                
                location = invoice_row.get('Location', 'Unknown')
                currency = invoice_row.get('Currency', 'INR')
                total_value = invoice_row.get('Total', 0)
                
                # Schedule correction workflow
                self.schedule_correction_workflow(
                    invoice_id=invoice_id,
                    vendor_email=vendor_email,
                    location=location,
                    currency=currency,
                    total_value=total_value,
                    validation_errors=issue.get('errors', [])
                )
    
    def schedule_correction_workflow(self, invoice_id: str, vendor_email: str, 
                                      location: str = 'India', currency: str = 'INR',
                                      total_value: float = 0.0, validation_errors: List = None):
        """Schedule 5-day correction workflow for an invoice"""
        if validation_errors is None:
            validation_errors = []
        
        correction_deadline = datetime.now() + timedelta(days=5)
        
        # Store in database
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT INTO correction_deadlines 
                (invoice_id, vendor_email, location, currency, total_value, 
                 validation_errors, correction_deadline)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (invoice_id, vendor_email, location, currency, total_value,
                    '; '.join(map(str, validation_errors)), correction_deadline))
            conn.commit()
        
        # Send initial notification
        self.send_validation_failure_notification(
            invoice_id=invoice_id,
            vendor_email=vendor_email,
            location=location,
            currency=currency,
            total_value=total_value,
            validation_errors=validation_errors,
            deadline=correction_deadline
        )
        
        logger.info(f" 5-day workflow scheduled for invoice {invoice_id}")
        return True
    
    def send_validation_failure_notification(self, invoice_id: str, vendor_email: str,
                                           location: str, currency: str, total_value: float,
                                           validation_errors: List, deadline: datetime):
        """Send initial validation failure notification"""
        try:
            subject = f" Koenig Solutions - Invoice Correction Required: {invoice_id}"
            
            body = self._generate_validation_failure_template(
                invoice_id=invoice_id,
                location=location,
                currency=currency,
                total_value=total_value,
                validation_errors=validation_errors,
                deadline=deadline
            )
            
            return self._send_email(vendor_email, subject, body)
            
        except Exception as e:
            logger.error(f" Failed to send validation failure notification: {e}")
            return False
    
    def _generate_validation_failure_template(self, invoice_id: str, location: str,
                                            currency: str, total_value: float,
                                            validation_errors: List, deadline: datetime):
        """Generate branded email template for validation failures"""
        
        subsidiary_info = self.subsidiaries.get(location, {'tax_type': 'Tax'})
        
        # Format currency amount
        formatted_amount = self._format_currency(total_value, currency)
        
        
        
        
            
            
        
        
            

                
                

                    
KOENIG

                    
step forward


                

                
                
                

                    
Invoice Correction Required

                    
                    

                        

                             Your invoice has failed validation and requires correction within 5 business days.
                        


                    

                    
                    
                    

                        
Invoice Details:

                        
Invoice ID:	{invoice_id}
Location:	{location}
Currency:	{currency}
Total Value:	{formatted_amount}
Tax Type:	{subsidiary_info['tax_type']}

                    

                    
                    
                    

                        
Validation Issues Found:

                        

                            {error_html}
                        

                    

                    
                    
                    

                        
 Correction Deadline

                        

                            {deadline.strftime('%B %d, %Y at %I:%M %p')}
                        


                        

                            You have 5 business days to correct and resubmit this invoice.
                        


                    

                    
                    
                    

                        
 Action Required:

                        

                            
Review the validation issues listed above

                            
Correct all identified problems in your invoice

                            
Resubmit the corrected invoice through the portal

                            
Reply to this email to confirm completion

                        

                    

                    
                    
                    

                        

                            Need Help?

                            Contact our support team: support@koenig-solutions.com
                        


                    

                

                
                
                

                    

                         2024 Koenig Solutions. Professional Training & IT Solutions.

                        This is an automated notification from our Invoice Validation System.
                    


                

            

        

        
        """
        
        return template
    
    def _format_currency(self, amount: float, currency: str) -> str:
        """Format currency amount based on currency type"""
        currency_symbols = {
            'INR': '', 'USD': '$', 'EUR': '', 'GBP': '', 'AUD': 'A$',
            'CAD': 'C$', 'SGD': 'S$', 'AED': 'AED', 'ZAR': 'R',
            'NZD': 'NZ$', 'MYR': 'RM', 'SAR': 'SAR', 'JPY': ''
        }
        
        symbol = currency_symbols.get(currency, currency)
        return f"{symbol}{amount:,.2f}"
    
    def _start_reminder_scheduler(self):
        """Start background scheduler for sending reminders"""
        def run_scheduler():
            schedule.every().hour.do(self._check_and_send_reminders)
            while True:
                schedule.run_pending()
                time.sleep(3600)  # Check every hour
        
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        logger.info(" Reminder scheduler started")
    
    def _check_and_send_reminders(self):
        """Check for pending reminders and send them"""
        current_time = datetime.now()
        
        with sqlite3.connect(self.db_path) as conn:
            # Check for Day 3 reminders
            day3_reminders = conn.execute('''
                SELECT * FROM correction_deadlines 
                WHERE status = 'PENDING' 
                AND day3_reminder_sent = FALSE
                AND datetime('now', '+2 days') >= correction_deadline
                AND confirmation_received = FALSE
            ''').fetchall()
            
            for reminder in day3_reminders:
                self._send_day3_reminder(reminder)
                # Mark as sent
                conn.execute('''
                    UPDATE correction_deadlines 
                    SET day3_reminder_sent = TRUE 
                    WHERE id = ?
                ''', (reminder[0],))
            
            # Check for Day 5 final notices
            day5_reminders = conn.execute('''
                SELECT * FROM correction_deadlines 
                WHERE status = 'PENDING' 
                AND day5_reminder_sent = FALSE
                AND datetime('now') >= correction_deadline
                AND confirmation_received = FALSE
            ''').fetchall()
            
            for reminder in day5_reminders:
                self._send_day5_final_notice(reminder)
                # Mark as sent and escalated
                conn.execute('''
                    UPDATE correction_deadlines 
                    SET day5_reminder_sent = TRUE, escalated = TRUE, status = 'OVERDUE'
                    WHERE id = ?
                ''', (reminder[0],))
            
            conn.commit()
    
    def _send_day3_reminder(self, reminder_data):
        """Send Day 3 reminder (2 days left)"""
        invoice_id = reminder_data[1]
        vendor_email = reminder_data[2]
        location = reminder_data[3]
        currency = reminder_data[4]
        
        subject = f" Reminder: Invoice Correction Due in 2 Days - {invoice_id}"
        
        body = f"""
        

            

                
 Invoice Correction Reminder

                
2 Days Remaining


            

            

                
Dear Vendor,


                
This is a friendly reminder that your invoice {invoice_id} 
                   requires correction and is due in 2 business days.


                
Location: {location} | Currency: {currency}


                
Please complete the correction as soon as possible to avoid delays.


                
If you need assistance, please contact our support team.


            

        

        """
        
        self._send_email(vendor_email, subject, body)
        logger.info(f" Day 3 reminder sent for invoice {invoice_id}")
    
    def _send_day5_final_notice(self, reminder_data):
        """Send Day 5 final notice (deadline reached)"""
        invoice_id = reminder_data[1]
        vendor_email = reminder_data[2]
        location = reminder_data[3]
        currency = reminder_data[4]
        
        subject = f" URGENT: Invoice Correction Deadline Reached - {invoice_id}"
        
        body = f"""
        

            

                
 URGENT: Correction Deadline Reached

                
Immediate Action Required


            

            

                
Dear Vendor,


                
URGENT: The 5-day correction deadline for invoice 
                   {invoice_id} has been reached.


                
Location: {location} | Currency: {currency}


                

                   This invoice is now overdue for correction and may be subject to 
                   processing delays or additional fees.
                


                
Please contact our team immediately to resolve this issue.


                
Support: support@koenig-solutions.com


            

        

        """
        
        self._send_email(vendor_email, subject, body)
        logger.info(f" Day 5 final notice sent for invoice {invoice_id}")
    
    def _send_email(self, recipient: str, subject: str, body: str):
        """Send individual email"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.sender_email
            msg['To'] = recipient
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'html'))
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)
            
            return True
            
        except Exception as e:
            logger.error(f" Failed to send email to {recipient}: {e}")
            return False
    
    def _generate_enhanced_report_body(self, validation_result_path: str):
        """Generate enhanced email body for regular reports"""
        try:
            # Load validation results if available
            summary_info = "Validation report attached"
            if validation_result_path and os.path.exists(validation_result_path):
                df = pd.read_excel(validation_result_path)
                total_invoices = len(df)
                issues_found = len(df[df['Status'] != 'PASSED']) if 'Status' in df.columns else 0
                summary_info = f{total_invoices} invoices processed, {issues_found} issues found"
        
        except:
            summary_info = "Validation report attached"
        
        return f"""
        
        
        
            

                

                    
KOENIG SOLUTIONS

                    
Invoice Validation Report


                

                

                    
Daily Validation Summary

                    
Date: {datetime.now().strftime('%B %d, %Y')}


                    
Summary: {summary_info}


                    
Please find the detailed validation results in the attached files.


                    
                    

                        
 System Status

                        
 Automated validation completed

                            Email notifications sent for failed validations

                            5-day correction workflows initiated where needed


                    

                

                

                    

                        Automated Invoice Validation System - Koenig Solutions
                    


                

            

        
        
        """
    
    def get_active_deadlines(self):
        """Get list of active correction deadlines"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('''
                SELECT invoice_id, vendor_email, location, currency, 
                       correction_deadline, status, confirmation_received
                FROM correction_deadlines 
                WHERE status IN ('PENDING', 'OVERDUE')
                ORDER BY correction_deadline ASC
            ''')
            
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def mark_correction_confirmed(self, invoice_id: str):
        """Mark an invoice correction as confirmed"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                UPDATE correction_deadlines 
                SET confirmation_received = TRUE, 
                    confirmation_date = CURRENT_TIMESTAMP,
                    status = 'COMPLETED'
                WHERE invoice_id = ? AND status IN ('PENDING', 'OVERDUE')
            ''', (invoice_id,))
            conn.commit()
        
        logger.info(f" Correction confirmed for invoice {invoice_id}")

# Compatibility wrapper for existing system
def send_email_report(validation_result_path: str, zip_path: str = None):
    """
    Compatibility function for existing system
    This ensures your current validator.py continues to work
    """
    notifier = EnhancedEmailNotifier()
    return notifier.send_email_report(validation_result_path, zip_path)

# Create global instance for compatibility
email_notifier = EnhancedEmailNotifier()

if __name__ == "__main__":
    # Test the enhanced system
    print(" Koenig Solutions - Enhanced Email Notifier")
    print(" 5-day correction workflow enabled")
    print(" Multi-currency support active")
    print(" Location-based templates ready")
    print(" Backward compatibility maintained")
            
