import schedule
import time
import logging
import os
from datetime import datetime, timedelta
from main import run_invoice_validation
from email_notifier import EmailNotifier
from late_upload_detector import LateUploadDetector
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

# Configure logging
def setup_logging():
    """Setup structured logging with file and console output"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/scheduler.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def parse_email_list(env_var_name):
    """Parse and validate email list from environment variable"""
    email_list_str = os.getenv(env_var_name, '')
    if not email_list_str:
        logger.warning(f"No email list found for {env_var_name}")
        return []
    
    emails = [email.strip() for email in email_list_str.split(',') if email.strip()]
    valid_emails = [email for email in emails if '@' in email and '.' in email]
    
    if len(valid_emails) != len(emails):
        logger.warning(f"Some invalid emails filtered from {env_var_name}")
    
    logger.info(f"Loaded {len(valid_emails)} valid emails from {env_var_name}")
    return valid_emails

def get_issues_count(report_date):
    """Get issues count from the latest report file with proper error handling"""
    try:
        report_file = os.path.join("data", f"delta_report_{report_date}.xlsx")
        
        if not os.path.exists(report_file):
            logger.warning(f"Report file not found: {report_file}")
            return 0
            
        df = pd.read_excel(report_file)
        issues_count = len(df)
        logger.info(f"Found {issues_count} issues in report {report_file}")
        return issues_count
        
    except (FileNotFoundError, pd.errors.ExcelFileError, pd.errors.EmptyDataError) as e:
        logger.error(f"Error reading Excel file {report_file}: {e}")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error reading report file: {e}")
        return 0

def scheduled_validation_job():
    """Main scheduled job that runs every 4 days at 6 PM"""
    job_start_time = datetime.now()
    logger.info(f"🕒 Starting scheduled validation job at {job_start_time}")
    
    try:
        # Step 1: Run invoice validation
        logger.info("Step 1: Running invoice validation...")
        success = run_invoice_validation()
        
        if not success:
            logger.error("❌ Invoice validation failed - stopping job")
            return False
            
        logger.info("✅ Invoice validation completed successfully")
        
        # Step 2: Check for late uploads
        logger.info("Step 2: Checking for late uploads...")
        try:
            detector = LateUploadDetector()
            late_invoices = detector.check_late_uploads()
            logger.info(f"Found {len(late_invoices) if late_invoices else 0} late uploads")
        except Exception as e:
            logger.error(f"Error checking late uploads: {e}")
            late_invoices = []
        
        # Step 3: Send HR alerts for late uploads
        if late_invoices:
            logger.info("Step 3: Sending HR alerts for late uploads...")
            hr_recipients = parse_email_list('HR_EMAIL_LIST')
            
            if hr_recipients:
                try:
                    notifier = EmailNotifier()
                    notifier.send_late_upload_alert(late_invoices, hr_recipients)
                    logger.info(f"✅ HR alert sent to {len(hr_recipients)} recipients")
                except Exception as e:
                    logger.error(f"Failed to send HR alert: {e}")
            else:
                logger.warning("No HR recipients configured - skipping HR alert")
        else:
            logger.info("Step 3: No late uploads found - skipping HR alerts")
        
        # Step 4: Send validation report to team
        logger.info("Step 4: Sending validation report to team...")
        team_recipients = parse_email_list('TEAM_EMAIL_LIST')
        
        if team_recipients:
            try:
                report_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                issues_count = get_issues_count(report_date)
                
                notifier = EmailNotifier()
                notifier.send_validation_report(report_date, team_recipients, issues_count)
                logger.info(f"✅ Validation report sent to {len(team_recipients)} team members")
                
            except Exception as e:
                logger.error(f"Failed to send validation report: {e}")
        else:
            logger.warning("No team recipients configured - skipping validation report")
        
        # Job completion
        duration = (datetime.now() - job_start_time).total_seconds()
        logger.info(f"✅ Scheduled validation job completed successfully in {duration:.2f} seconds")
        return True
        
    except Exception as e:
        duration = (datetime.now() - job_start_time).total_seconds()
        logger.error(f"❌ Critical error in scheduled job after {duration:.2f} seconds: {str(e)}")
        return False

def start_scheduler():
    """Start the scheduler service with enhanced error handling"""
    logger.info("🚀 Starting Invoice Validator Scheduler")
    logger.info("📅 Scheduled to run every 4 days at 6:00 PM")
    
    # Ensure logs directory exists
    os.makedirs('logs', exist_ok=True)
    
    # Schedule the job every 4 days at 6 PM
    schedule.every(4).days.at("18:00").do(scheduled_validation_job)
    
    # Optional: Run immediately on startup (uncomment if needed)
    # logger.info("Running initial validation job...")
    # scheduled_validation_job()
    
    # For testing - uncomment to run every 2 minutes
    # schedule.every(2).minutes.do(scheduled_validation_job)
    # logger.info("⚠️ TEST MODE: Running every 2 minutes")
    
    logger.info(f"⏰ Next scheduled run: {schedule.next_run()}")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        logger.info("🛑 Scheduler stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"❌ Scheduler error: {e}")
        logger.info("🔄 Restarting scheduler in 60 seconds...")
        time.sleep(60)
        start_scheduler()  # Restart scheduler

def main():
    """Main entry point with startup checks"""
    try:
        # Validate required environment variables
        required_env_vars = ['HR_EMAIL_LIST', 'TEAM_EMAIL_LIST']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        
        if missing_vars:
            logger.warning(f"Missing environment variables: {missing_vars}")
            logger.warning("Some email notifications may not work properly")
        
        # Start the scheduler
        start_scheduler()
        
    except Exception as e:
        logger.error(f"Failed to start scheduler: {e}")
        raise

if __name__ == "__main__":
    main()
