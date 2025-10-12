#!/usr/bin/env python3
"""
Production Invoice Validation System - GitHub Actions Compatible
Complete system with all functionality preserved and production-ready features
"""
# FIXED: Removed PyPDF2 import conflict - using only PyMuPDF
import fitz  # PyMuPDF
import os
import sys
import logging
import sqlite3
import pandas as pd
import numpy as np
import smtplib
import time
import json
import shutil
import glob
import re
import zipfile
import traceback
import tempfile
import hashlib
from typing import List, Optional
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Union
from dataclasses import dataclass, asdict
from contextlib import contextmanager
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
# FIXED: Using fitz for PDF reading instead of PyPDF2
import chardet
import warnings

# Selenium imports with error handling
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait, Select
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.common.exceptions import (
        TimeoutException, NoSuchElementException, WebDriverException,
        ElementClickInterceptedException, StaleElementReferenceException,
        InvalidSessionIdException
    )
    SELENIUM_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Selenium not available: {e}")
    SELENIUM_AVAILABLE = False

# Suppress warnings
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

# Configuration with safe environment variable handling
@dataclass
class Config:
    """Production system configuration with safe environment variable handling"""
    # Database settings
    DB_PATH: str = "invoice_validation.db"
    BACKUP_DB_PATH: str = "backup_invoice_validation.db"

    # File paths
    DOWNLOADS_DIR: str = "downloads"
    ARCHIVE_DIR: str = "archive"
    LOGS_DIR: str = "logs"
    SNAPSHOTS_DIR: str = "snapshots"

    # AFTER (read from env, fall back to placeholders)
    RMS_BASE_URL: str = os.getenv("RMS_BASE_URL", "")
    RMS_LOGIN_URL: str = os.getenv("RMS_LOGIN_URL", "")
    RMS_REPORTS_URL: str = os.getenv("RMS_REPORTS_URL", "")

    # Selenium settings
    SELENIUM_TIMEOUT: int = 30
    SELENIUM_IMPLICIT_WAIT: int = 10
    SELENIUM_PAGE_LOAD_TIMEOUT: int = 60

    # Email settings with safe parsing for production
    EMAIL_SMTP_SERVER: str = (
        os.getenv('EMAIL_SMTP_SERVER')
        or os.getenv('SMTP_HOST')
        or 'smtp.office365.com'
    )
    EMAIL_SMTP_PORT: int = int(
        os.getenv('EMAIL_SMTP_PORT')
        or os.getenv('SMTP_PORT')
        or '587'
    )
    EMAIL_USERNAME: str = (
        os.getenv('EMAIL_USERNAME')
        or os.getenv('SMTP_USER')
        or ''
    )
    EMAIL_PASSWORD: str = (
        os.getenv('EMAIL_PASSWORD')
        or os.getenv('SMTP_PASS')
        or ''
    )
    EMAIL_FROM: str = (
        os.getenv('EMAIL_FROM')
        or os.getenv('SMTP_USER')  # use authenticated address
        or ''
    )
    EMAIL_TO: str = (
        os.getenv('EMAIL_TO')
        or os.getenv('AP_TEAM_EMAIL_LIST')  # your repo secret
        or ''
    )
    EMAIL_CC: str = os.getenv('EMAIL_CC') or ''

    # Processing settings
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 5
    BATCH_SIZE: int = 100

    # GitHub Actions settings
    IS_GITHUB_ACTIONS: bool = os.getenv('GITHUB_ACTIONS', 'false').lower() == 'true'
    HEADLESS_MODE: bool = os.getenv('HEADLESS_MODE', 'true').lower() == 'true'

    def __post_init__(self):
        """Post-initialization validation and warnings"""
        if self.IS_GITHUB_ACTIONS:
            # Log configuration status
            logger = logging.getLogger(__name__)

            if not self.EMAIL_USERNAME:
                logger.warning("EMAIL_USERNAME not configured - email notifications disabled")
            if not self.EMAIL_PASSWORD:
                logger.warning("EMAIL_PASSWORD not configured - email notifications disabled")
            if not self.EMAIL_TO:
                logger.warning("EMAIL_TO not configured - email notifications disabled")

            rms_username = os.getenv('RMS_USERNAME')
            rms_password = os.getenv('RMS_PASSWORD')
            if not rms_username or not rms_password:
                logger.warning("RMS credentials not configured - RMS integration disabled")

# Initialize configuration
config = Config()

class LogManager:
    """Production logging manager with multiple output streams"""

    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.setup_logging()

    def setup_logging(self):
        """Setup comprehensive logging configuration"""
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )

        # Root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG if config.IS_GITHUB_ACTIONS else logging.INFO)

        # Clear existing handlers
        root_logger.handlers.clear()

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)
        root_logger.addHandler(console_handler)

        # Main log file
        main_log_file = self.log_dir / f"invoice_validation_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(main_log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(file_handler)

        # Error log file
        error_log_file = self.log_dir / f"errors_{datetime.now().strftime('%Y%m%d')}.log"
        error_handler = logging.FileHandler(error_log_file, encoding='utf-8')
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(error_handler)

        self.logger = logging.getLogger(__name__)
        self.logger.info("Production logging system initialized")

class DatabaseManager:
    """Production database manager with comprehensive error handling"""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or config.DB_PATH
        self.backup_path = config.BACKUP_DB_PATH
        self.logger = logging.getLogger(__name__)
        self.init_database()

    def init_database(self):
        """Initialize production database with enhanced invoice validation table"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Enhanced Invoice validation table with all required fields
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS invoice_validations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        invoice_number TEXT NOT NULL,
                        vendor_name TEXT,
                        amount REAL,
                        validation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        status TEXT DEFAULT 'pending',
                        rms_status TEXT,
                        discrepancies TEXT,
                        notes TEXT,
                        file_path TEXT,
                        hash_value TEXT,
                        processed_by TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        -- New fields for enhanced reporting
                        gst_no TEXT DEFAULT '',
                        inv_date TEXT DEFAULT '',
                        inv_entry_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        inv_mod_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        due_date TEXT DEFAULT '',
                        scid_number TEXT DEFAULT '',
                        remarks TEXT DEFAULT '',
                        mop TEXT DEFAULT '',
                        account_head TEXT DEFAULT '',
                        inv_currency TEXT DEFAULT 'USD',
                        location TEXT DEFAULT '',
                        vendor_advance REAL DEFAULT 0.00,
                        marked_feedback_issue TEXT DEFAULT '',
                        tp_feedback_by_fm TEXT DEFAULT '',
                        ms_feedback_by_fm TEXT DEFAULT '',
                        fl_feedback TEXT DEFAULT '',
                        inv_created_by TEXT DEFAULT 'System'
                    )
                """)

                # Processing log table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS processing_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id TEXT NOT NULL,
                        operation TEXT NOT NULL,
                        status TEXT NOT NULL,
                        message TEXT,
                        details TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # System settings table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS system_settings (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL,
                        description TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # Archive table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS archived_invoices (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        original_id INTEGER,
                        invoice_number TEXT NOT NULL,
                        vendor_name TEXT,
                        amount REAL,
                        validation_date TIMESTAMP,
                        status TEXT,
                        archive_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        archive_reason TEXT
                    )
                """)

                # Create indexes for production performance
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_invoice_number
                    ON invoice_validations(invoice_number)
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_validation_date
                    ON invoice_validations(validation_date)
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_status
                    ON invoice_validations(status)
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_gst_no
                    ON invoice_validations(gst_no)
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_vendor_name
                    ON invoice_validations(vendor_name)
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_session_logs
                    ON processing_logs(session_id, timestamp)
                """)

                conn.commit()
                self.logger.info("Enhanced production database initialized successfully")

        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
            raise

    def insert_invoice_validation_enhanced(self, invoice_data: Dict) -> int:
        """Insert enhanced invoice validation record with all new fields"""
        query = """
        INSERT INTO invoice_validations
        (invoice_number, vendor_name, amount, status, rms_status,
         discrepancies, notes, file_path, hash_value, processed_by,
         gst_no, inv_date, due_date, mop, account_head, inv_currency,
         location, vendor_advance, remarks, inv_created_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        params = (
            invoice_data.get('invoice_number'),
            invoice_data.get('vendor_name'),
            invoice_data.get('amount'),
            invoice_data.get('status', 'pending'),
            invoice_data.get('rms_status'),
            invoice_data.get('discrepancies'),
            invoice_data.get('notes'),
            invoice_data.get('file_path'),
            invoice_data.get('hash_value'),
            invoice_data.get('processed_by', 'system'),
            # New enhanced fields
            invoice_data.get('gst_no', ''),
            invoice_data.get('inv_date', ''),
            invoice_data.get('due_date', ''),
            invoice_data.get('mop', ''),
            invoice_data.get('account_head', ''),  
            invoice_data.get('currency', 'USD'),
            invoice_data.get('location', ''),
            invoice_data.get('vendor_advance', 0.00),
            invoice_data.get('notes', ''),  # Use notes as remarks
            invoice_data.get('inv_created_by', 'System')
        )

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            self.logger.error(f"Failed to insert enhanced invoice validation: {e}")
            raise

    def migrate_database_schema(self):
        """Migrate existing database to include new fields"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
            
                # Check if new columns exist, if not add them
                cursor.execute("PRAGMA table_info(invoice_validations)")
                existing_columns = [row[1] for row in cursor.fetchall()]
            
                new_columns = {
                    'gst_no': 'TEXT DEFAULT ""',
                    'inv_date': 'TEXT DEFAULT ""',
                    'inv_entry_date': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'inv_mod_date': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'due_date': 'TEXT DEFAULT ""',
                    'scid_number': 'TEXT DEFAULT ""',
                    'remarks': 'TEXT DEFAULT ""',
                    'mop': 'TEXT DEFAULT ""',
                    'account_head': 'TEXT DEFAULT ""',
                    'inv_currency': 'TEXT DEFAULT "USD"',
                    'location': 'TEXT DEFAULT ""',
                    'vendor_advance': 'REAL DEFAULT 0.00',
                    'marked_feedback_issue': 'TEXT DEFAULT ""',
                    'tp_feedback_by_fm': 'TEXT DEFAULT ""',
                    'ms_feedback_by_fm': 'TEXT DEFAULT ""',
                    'fl_feedback': 'TEXT DEFAULT ""',
                    'inv_created_by': 'TEXT DEFAULT "System"'
                }
            
                for column_name, column_definition in new_columns.items():
                    if column_name not in existing_columns:
                        cursor.execute(f"ALTER TABLE invoice_validations ADD COLUMN {column_name} {column_definition}")
                        self.logger.info(f"Added column: {column_name}")
            
                # Create new indexes if they don't exist
                try:
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gst_no ON invoice_validations(gst_no)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_vendor_name ON invoice_validations(vendor_name)")
                except Exception as e:
                    self.logger.debug(f"Index creation skipped: {e}")
            
                conn.commit()
                self.logger.info("Database schema migration completed successfully")
            
        except Exception as e:
            self.logger.error(f"Database migration failed: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """Get database connection with production-grade error handling"""
        conn = None
        try:
            conn = sqlite3.connect(
                self.db_path,
                timeout=30.0,
                check_same_thread=False
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA cache_size = 10000")
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def backup_database(self) -> bool:
        """Create production database backup"""
        try:
            if os.path.exists(self.db_path):
                # Create timestamped backup
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                timestamped_backup = f"backup_invoice_validation_{timestamp}.db"

                shutil.copy2(self.db_path, self.backup_path)
                shutil.copy2(self.db_path, timestamped_backup)

                self.logger.info(f"Database backed up to {self.backup_path} and {timestamped_backup}")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Database backup failed: {e}")
            return False

    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute query with production error handling"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                if query.strip().upper().startswith('SELECT'):
                    return [dict(row) for row in cursor.fetchall()]
                else:
                    conn.commit()
                    return [{'affected_rows': cursor.rowcount}]

        except Exception as e:
            self.logger.error(f"Query execution failed: {query[:100]}... Error: {e}")
            raise

    def insert_invoice_validation(self, invoice_data: Dict) -> int:
        """Insert invoice validation record with production validation"""
        query = """
        INSERT INTO invoice_validations
        (invoice_number, vendor_name, amount, status, rms_status,
         discrepancies, notes, file_path, hash_value, processed_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        params = (
            invoice_data.get('invoice_number'),
            invoice_data.get('vendor_name'),
            invoice_data.get('amount'),
            invoice_data.get('status', 'pending'),
            invoice_data.get('rms_status'),
            invoice_data.get('discrepancies'),
            invoice_data.get('notes'),
            invoice_data.get('file_path'),
            invoice_data.get('hash_value'),
            invoice_data.get('processed_by', 'system')
        )

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            self.logger.error(f"Failed to insert invoice validation: {e}")
            raise

    def log_processing_event(self, session_id: str, operation: str,
                           status: str, message: str = None, details: str = None):
        """Log processing event with production metadata"""
        query = """
        INSERT INTO processing_logs (session_id, operation, status, message, details)
        VALUES (?, ?, ?, ?, ?)
        """

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (session_id, operation, status, message, details))
                conn.commit()
        except Exception as e:
            self.logger.error(f"Failed to log processing event: {e}")

class ProductionSeleniumManager:
    """Creates a Chrome WebDriver configured for GitHub Actions / headless."""

    def __init__(self, logger: logging.Logger = None) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self.options = None
        self.driver = None
        self.setup_chrome_options()

    def setup_chrome_options(self) -> None:
        options = Options()
        # Hardened flags
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-features=VizDisplayCompositor")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-background-timer-throttling")
        options.add_argument("--disable-renderer-backgrounding")
        options.add_argument("--disable-backgrounding-occluded-windows")
        options.add_argument("--window-size=1920,1080")
        
        # Headless for CI
        if getattr(config, "IS_GITHUB_ACTIONS", False) or getattr(config, "HEADLESS_MODE", False):
            options.add_argument("--headless=new")
            options.add_argument("--remote-debugging-port=9222")

        # Downloads
        download_dir = os.path.abspath(getattr(config, "DOWNLOADS_DIR", "downloads"))
        os.makedirs(download_dir, exist_ok=True)
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "plugins.always_open_pdf_externally": True,  # force PDF download, not viewer
            "profile.default_content_settings.popups": 0,
            "profile.default_content_setting_values.notifications": 2,
        }
        options.add_experimental_option("prefs", prefs)
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        options.add_experimental_option("useAutomationExtension", False)

        # 👈 This line was missing
        self.options = options
        
        # Strongly recommended for CI:
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")

    def get_driver(self):
        """Create (or return cached) driver (uses Selenium Manager)."""
        if self.driver is not None:
            return self.driver

        # Let Selenium Manager pick a compatible driver for the installed Chrome.
        # (No Service / chromedriver path required.)
        self.driver = webdriver.Chrome(options=self.options)

        try:
            timeout = int(getattr(config, "SELENIUM_TIMEOUT", 30))
            self.driver.set_page_load_timeout(timeout)
        except Exception:
            pass
        return self.driver
        
    def quit(self) -> None:
        """Close the browser cleanly."""
        try:
            if self.driver is not None:
                self.driver.quit()
        finally:
            self.driver = None

class ProductionEmailNotifier:
    """Production email notification system"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.smtp_server = config.EMAIL_SMTP_SERVER
        self.smtp_port = config.EMAIL_SMTP_PORT
        self.username = config.EMAIL_USERNAME
        self.password = config.EMAIL_PASSWORD
        self.from_email = config.EMAIL_FROM or config.EMAIL_USERNAME

        # Check if email is configured
        self.email_configured = bool(self.username and self.password and config.EMAIL_TO)

        if not self.email_configured:
            self.logger.warning("Email not fully configured - notifications will be logged only")

    def send_processing_summary(self, session_id: str, processing_results: Dict) -> bool:
        """Send production processing summary email"""
        if not self.email_configured:
            self.logger.info("Email not configured - logging summary instead")
            self.logger.info(f"Processing Summary: {json.dumps(processing_results, indent=2, default=str)}")
            return True

        try:
            subject = f"Invoice Validation Report - {datetime.now().strftime('%Y-%m-%d %H:%M')}"

            # Create comprehensive text body
            text_body = f"""
Production Invoice Validation System - Processing Summary
========================================================

Session ID: {session_id}
Processing Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Environment: {'GitHub Actions' if config.IS_GITHUB_ACTIONS else 'Local'}

SUMMARY STATISTICS:
- Total Invoices Processed: {processing_results.get('total_processed', 0)}
- Successful Validations: {processing_results.get('successful', 0)}
- Failed Validations: {processing_results.get('failed', 0)}
- Discrepancies Found: {processing_results.get('discrepancies', 0)}

OVERALL STATUS: {'SUCCESS' if processing_results.get('overall_status') == 'success' else 'FAILED'}

CONFIGURATION:
- RMS Integration: {'Enabled' if os.getenv('RMS_USERNAME') else 'Disabled'}
- Email Notifications: {'Enabled' if self.email_configured else 'Disabled'}
- Database: SQLite with backup enabled
- File Processing: Multiple formats supported

DETAILS:
{json.dumps(processing_results, indent=2, default=str)}

Best regards,
Production Invoice Validation System
"""

            # Get recipients
            to_emails = config.EMAIL_TO.split(',') if config.EMAIL_TO else []
            cc_emails = config.EMAIL_CC.split(',') if config.EMAIL_CC else []

            if not to_emails:
                self.logger.warning("No email recipients configured")
                return False

            # Get attachments
            attachments = processing_results.get('attachment_files', [])

            return self.send_email(
                to_emails=to_emails,
                subject=subject,
                body_text=text_body,
                cc_emails=cc_emails,
                attachments=attachments
            )

        except Exception as e:
            self.logger.error(f"Failed to send production processing summary: {e}")
            return False

    def send_email(self, to_emails: Union[str, List[str]], subject: str,
                  body_text: str, body_html: str = None,
                  cc_emails: Union[str, List[str]] = None,
                  attachments: List[str] = None) -> bool:
        """Send production email with comprehensive error handling"""
        try:
            # Normalize email lists
            if isinstance(to_emails, str):
                to_emails = [email.strip() for email in to_emails.split(',')]

            if cc_emails:
                if isinstance(cc_emails, str):
                    cc_emails = [email.strip() for email in cc_emails.split(',')]
            else:
                cc_emails = []

            # Create message
            msg = MIMEMultipart('alternative')
            msg['From'] = self.from_email
            msg['To'] = ', '.join(to_emails)
            msg['Subject'] = subject

            if cc_emails:
                msg['Cc'] = ', '.join(cc_emails)

            # Add text part
            text_part = MIMEText(body_text, 'plain', 'utf-8')
            msg.attach(text_part)

            # Add HTML part if provided
            if body_html:
                html_part = MIMEText(body_html, 'html', 'utf-8')
                msg.attach(html_part)

            # Add attachments
            if attachments:
                for attachment in attachments:
                    if os.path.exists(attachment):
                        with open(attachment, 'rb') as f:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(f.read())

                        encoders.encode_base64(part)
                        filename = os.path.basename(attachment)
                        part.add_header(
                            'Content-Disposition',
                            f'attachment; filename= {filename}'
                        )
                        msg.attach(part)
                        self.logger.info(f"Added attachment: {filename}")

            # Send email
            all_recipients = to_emails + cc_emails

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg, to_addrs=all_recipients)

            self.logger.info(f"Production email sent successfully to {len(all_recipients)} recipients")
            return True

        except Exception as e:
            self.logger.error(f"Failed to send production email: {e}")
            return False

class ProductionInvoiceValidationSystem:
    """Production invoice validation system orchestrator"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.session_id = f"prod_session_{int(time.time())}"

        # Initialize production components
        self.db_manager = DatabaseManager()
        self.email_notifier = ProductionEmailNotifier()

        # Initialize Selenium if available and configured
        if SELENIUM_AVAILABLE and (os.getenv('RMS_USERNAME') and os.getenv('RMS_PASSWORD')):
            self.selenium_manager = ProductionSeleniumManager()
            self.rms_enabled = True
        else:
            self.selenium_manager = None
            self.rms_enabled = False
            self.logger.warning("RMS integration disabled - missing credentials or Selenium")

        # Production processing state
        self.processing_results = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'discrepancies': 0,
            'errors': [],
            'warnings': [],
            'attachment_files': [],
            'overall_status': 'pending',
            'session_id': self.session_id,
            'start_time': datetime.now().isoformat(),
            'environment': 'GitHub Actions' if config.IS_GITHUB_ACTIONS else 'Local'
        }

        self.logger.info(f"Production Invoice Validation System initialized - Session: {self.session_id}")

    def setup_directories(self):
        """Create required production directories"""
        directories = [
            config.DOWNLOADS_DIR,
            config.ARCHIVE_DIR,
            config.LOGS_DIR,
            config.SNAPSHOTS_DIR
        ]

        for directory in directories:
            Path(directory).mkdir(exist_ok=True, parents=True)
            self.logger.info(f"Production directory ready: {directory}")

    def validate_configuration(self) -> bool:
        """Validate production configuration with graceful degradation"""
        validation_errors = []

        # Check required directories
        try:
            self.setup_directories()
        except Exception as e:
            validation_errors.append(f"Directory setup failed: {e}")

        # Check database
        try:
            self.db_manager.backup_database()
        except Exception as e:
            validation_errors.append(f"Database validation failed: {e}")

        # Check RMS credentials (warning in production)
        rms_username = os.getenv('RMS_USERNAME')
        rms_password = os.getenv('RMS_PASSWORD')

        if not rms_username or not rms_password:
            self.logger.warning("RMS credentials not configured - RMS features will be skipped")
        else:
            self.logger.info("RMS credentials configured - RMS integration enabled")

        # Check email configuration (warning in production)
        if not config.EMAIL_USERNAME or not config.EMAIL_PASSWORD:
            self.logger.warning("Email credentials not configured - email notifications disabled")
        else:
            self.logger.info("Email credentials configured - notifications enabled")

        if not config.EMAIL_TO:
            self.logger.warning("Email recipients not configured - email notifications disabled")
        else:
            self.logger.info(f"Email recipients configured: {len(config.EMAIL_TO.split(','))} recipients")

        # Only fail on critical errors (not missing credentials)
        critical_errors = [e for e in validation_errors if 'credentials' not in e.lower()]

        if critical_errors:
            for error in critical_errors:
                self.logger.error(f"Critical configuration error: {error}")
            return False
        else:
            self.logger.info("Production configuration validation passed")
            return True

    def create_demo_data_if_needed(self):
        """Create demo data only when explicitly enabled and no real files exist."""
        # Gate demo creation behind an env flag (default OFF)
        if os.getenv('FORCE_DEMO', 'false').lower() != 'true':
            self.logger.info("FORCE_DEMO!=true → skipping demo data creation")
            return

        downloads_dir = Path(config.DOWNLOADS_DIR)
        downloads_dir.mkdir(parents=True, exist_ok=True)

        # Consider only data files the pipeline actually ingests
        file_patterns = ['*.xlsx', '*.xls', '*.csv', '*.tsv']
        existing_files = []
        for pattern in file_patterns:
            existing_files.extend(downloads_dir.glob(pattern))

        if existing_files:
            self.logger.info("Data files already present → demo not created")
            return

        self.logger.info("FORCE_DEMO=true and no data files found → creating demo dataset")

        # --- original demo payload unchanged ---
        demo_data = {
            'Invoice_Number': [f'INV-2024-{i:04d}' for i in range(1, 21)],
            'Vendor_Name': [
                'Acme Corp', 'Tech Solutions Inc', 'Global Services Ltd', 'Premier Products',
                'Innovation Systems', 'Quality Supplies Co', 'Advanced Technologies',
                'Professional Services', 'Enterprise Solutions', 'Modern Industries',
                'Strategic Partners', 'Excellence Group', 'Dynamic Solutions',
                'Integrated Systems', 'Optimal Services', 'Premium Vendors',
                'Elite Suppliers', 'Superior Products', 'Leading Technologies', 'Prime Services'
            ],
           'Amount': [
                1250.50, 899.99, 2100.00, 450.75, 1750.25,
                3200.00, 675.80, 1425.30, 2850.00, 990.45,
                1680.75, 2340.20, 758.90, 3150.00, 1125.60,
                2680.40, 892.15, 1935.80, 3500.00, 1475.25
            ],
            'Invoice_Date': [
                f'2024-{((i-1)//7)+1:02d}-{((i-1)%7)+15:02d}' for i in range(1, 21)
            ],
            'Due_Date': [
                f'2024-{((i-1)//7)+2:02d}-{((i-1)%7)+15:02d}' for i in range(1, 21)
            ],
            'Status': [
                'Pending', 'Approved', 'Pending', 'Under Review', 'Approved',
                'Pending', 'Approved', 'Under Review', 'Pending', 'Approved',
                'Under Review', 'Pending', 'Approved', 'Pending', 'Under Review',
                'Approved', 'Pending', 'Under Review', 'Approved', 'Pending'
            ],
            'Category': [
                'Office Supplies', 'IT Services', 'Consulting', 'Equipment', 'Software',
                'Hardware', 'Maintenance', 'Training', 'Licensing', 'Support',
                'Professional Services', 'Office Supplies', 'IT Services', 'Equipment',
                'Software', 'Consulting', 'Hardware', 'Maintenance', 'Training', 'Support'
            ]
        }

        df = pd.DataFrame(demo_data)

        # Save as multiple formats for testing
        demo_csv = downloads_dir / 'demo_invoices_production.csv'
        demo_excel = downloads_dir / 'demo_invoices_production.xlsx'

        df.to_csv(demo_csv, index=False)
        df.to_excel(demo_excel, index=False, engine='openpyxl')

        self.logger.info(f"Created demo data files: {demo_csv}, {demo_excel}")

        # Log demo data creation
        self.db_manager.log_processing_event(
            self.session_id, "demo_data_creation", "success",
            f"Created {len(demo_data['Invoice_Number'])} demo invoices"
        )

    # FIXED: Using fitz instead of PyPDF2 for PDF validation
    def check_pdf_valid(self, pdf_path: str) -> bool:
        """Check if PDF file is valid using PyMuPDF"""
        try:
            doc = fitz.open(pdf_path)
            doc.close()
            return True
        except Exception as e:
            self.logger.warning(f"PDF validation failed for {pdf_path}: {e}")
            return False

    def process_local_files(self) -> List[str]:
        """Process local invoice files with production-grade handling"""
        processed_files = []

        try:
            # Ensure demo data exists if no files found
            self.create_demo_data_if_needed()

            downloads_dir = Path(config.DOWNLOADS_DIR)

            # Find all supported files
            file_patterns = ['*.xlsx', '*.xls', '*.csv', '*.tsv', '*.txt']
            files_to_process = []

            for pattern in file_patterns:
                files_to_process.extend(downloads_dir.glob(pattern))

            if not files_to_process:
                self.logger.warning("No files found to process after demo data creation")
                return processed_files

            self.logger.info(f"Found {len(files_to_process)} files to process")

            for file_path in files_to_process:
                try:
                    self.logger.info(f"Processing production file: {file_path}")

                    # Process file with encoding detection
                    if file_path.suffix.lower() in ['.csv', '.tsv', '.txt']:
                        # Detect encoding
                        with open(file_path, 'rb') as f:
                            raw_data = f.read(10000)
                            encoding_result = chardet.detect(raw_data)
                            encoding = encoding_result['encoding'] or 'utf-8'

                        delimiter = '\t' if file_path.suffix.lower() == '.tsv' else ','
                        df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter)
                    else:
                        # Excel files
                        try:
                            df = pd.read_excel(file_path, engine='openpyxl')
                        except:
                            df = pd.read_excel(file_path, engine='xlrd')

                    # Clean and validate data
                    df = self.clean_dataframe(df)
                    validation_results = self.validate_invoice_data(df)

                    if validation_results['valid']:
                        # Process invoices
                        processed_count = self.process_invoice_dataframe(df, str(file_path))

                        if processed_count > 0:
                            processed_files.append(str(file_path))
                            self.processing_results['successful'] += 1

                            self.logger.info(f"Successfully processed {processed_count} invoices from {file_path.name}")

                            # Log successful processing
                            self.db_manager.log_processing_event(
                                self.session_id, "file_processing", "success",
                                f"Processed {processed_count} invoices from {file_path.name}"
                            )
                        else:
                            self.processing_results['failed'] += 1
                            error_msg = f"No invoices processed from {file_path.name}"
                            self.processing_results['errors'].append(error_msg)

                            self.db_manager.log_processing_event(
                                self.session_id, "file_processing", "failed", error_msg
                            )
                    else:
                        self.processing_results['failed'] += 1
                        error_msg = f"Validation failed for {file_path.name}: {validation_results['errors']}"
                        self.processing_results['errors'].append(error_msg)
                        self.logger.error(error_msg)

                        self.db_manager.log_processing_event(
                            self.session_id, "file_validation", "failed", error_msg
                        )

                    # Update warnings
                    if validation_results.get('warnings'):
                        self.processing_results['warnings'].extend(validation_results['warnings'])

                    self.processing_results['total_processed'] += processed_count

                except Exception as e:
                    self.processing_results['failed'] += 1
                    error_msg = f"Failed to process {file_path.name}: {e}"
                    self.processing_results['errors'].append(error_msg)
                    self.logger.error(error_msg)

                    self.db_manager.log_processing_event(
                        self.session_id, "file_processing", "error", error_msg
                    )

        except Exception as e:
            error_msg = f"Local file processing failed: {e}"
            self.logger.error(error_msg)
            self.processing_results['errors'].append(error_msg)

        return processed_files

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize dataframe for production use."""
        try:
            original_shape = df.shape

            # Remove completely empty rows and columns
            df = df.dropna(how="all").dropna(axis=1, how="all")

            # Standardize column names
            df.columns = [
                str(col).strip().lower().replace(" ", "_").replace("-", "_")
                for col in df.columns
            ]

            # Remove duplicate columns
            df = df.loc[:, ~df.columns.duplicated()]

            # Clean string/object columns
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .replace({"nan": np.nan, "None": np.nan, "": np.nan})
                )

            cleaned_shape = df.shape
            self.logger.info(f"DataFrame cleaned: {original_shape} -> {cleaned_shape}")
            return df

        except Exception as e:
            self.logger.error(f"DataFrame cleaning failed: {e}")
            return df

    def validate_invoice_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate invoice data structure and content for production"""
        validation_results = {'valid': True, 'errors': [], 'warnings': [], 'statistics': {}}
        try:
            required_patterns = {
                'invoice_number': ['invoice', 'inv_no', 'number', 'inv_num'],
                'vendor': ['vendor', 'supplier', 'company'],
                'amount': ['amount', 'total', 'value', 'sum', 'price']
            }
            found_columns = {}
            for req_col, patterns in required_patterns.items():
                matching_cols = [col for col in df.columns if any(p in col.lower() for p in patterns)]
                if matching_cols:
                    found_columns[req_col] = matching_cols[0]
                else:
                    validation_results['warnings'].append(f"Recommended column pattern not found: {req_col}")

            if found_columns.get('invoice_number'):
                empty_invoices = df[found_columns['invoice_number']].isna().sum()
                duplicate_invoices = df[found_columns['invoice_number']].duplicated().sum()
                if empty_invoices > 0:
                    validation_results['warnings'].append(f"{empty_invoices} rows with empty invoice numbers")
                if duplicate_invoices > 0:
                    validation_results['warnings'].append(f"{duplicate_invoices} duplicate invoice numbers")

            if found_columns.get('amount'):
                amount_col = found_columns['amount']
                try:
                    numeric_amounts = pd.to_numeric(df[amount_col], errors='coerce')
                    invalid_amounts = numeric_amounts.isna().sum()
                    negative_amounts = (numeric_amounts < 0).sum()
                    if invalid_amounts > 0:
                        validation_results['warnings'].append(f"{invalid_amounts} rows with invalid amounts")
                    if negative_amounts > 0:
                        validation_results['warnings'].append(f"{negative_amounts} rows with negative amounts")
                except Exception as e:
                    validation_results['warnings'].append(f"Amount validation error: {e}")

            mu = df.memory_usage(deep=True).sum()
            validation_results['statistics'] = {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'found_columns': found_columns,
                'memory_usage': mu,
                'file_size_estimate': f"{mu / 1024:.2f} KB"
            }
            self.logger.info(f"Production validation completed: {validation_results['statistics']}")
        except Exception as e:
            validation_results['valid'] = False
            validation_results['errors'].append(f"Validation process failed: {e}")
            self.logger.error(f"Invoice data validation failed: {e}")
        return validation_results

    def collect_email_attachments(self, summary_file: Optional[str]) -> List[str]:
        """Choose a compact set of useful attachments for the email."""
        attachments: List[str] = []
        try:
            # Always include summary if present
            if summary_file and os.path.exists(summary_file):
                attachments.append(summary_file)

            # Today's logs (at most 2)
            try:
                today_tag = datetime.now().strftime('%Y%m%d')
                log_dir = Path(config.LOGS_DIR)
                if log_dir.exists():
                    todays_logs = sorted(log_dir.glob(f"*{today_tag}*.log"))
                    for f in todays_logs[:2]:
                        attachments.append(str(f))
            except Exception:
                # logs are optional
                pass

            # Useful data outputs from downloads/
            dl = Path(config.DOWNLOADS_DIR)
            if dl.exists():
                # A few CSV/XLSX (max 4)
                for pat in ("*.csv", "*.xlsx"):
                    for f in sorted(dl.glob(pat))[:4]:
                        attachments.append(str(f))
                # A few invoice documents (max 3)
                for pat in ("*.pdf", "*.jpg", "*.jpeg", "*.png"):
                    for f in sorted(dl.glob(pat))[:3]:
                        attachments.append(str(f))

            # Deduplicate while preserving order
            seen: set = set()
            deduped: List[str] = []
            for x in attachments:
                if x not in seen:
                    deduped.append(x)
                    seen.add(x)
            attachments = deduped

            # Size guard -> zip if too large
            def _total_bytes(paths: List[str]) -> int:
                total = 0
                for p in paths:
                    try:
                        if os.path.exists(p):
                            total += os.path.getsize(p)
                    except Exception:
                        pass
                return total

            max_mb = int(os.getenv("EMAIL_MAX_ATTACH_MB", "18"))
            max_bytes = max_mb * 1024 * 1024

            if _total_bytes(attachments) > max_bytes:
                zip_path = Path(config.DOWNLOADS_DIR) / f"email_attachments_{self.session_id}.zip"
                with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    for f in attachments:
                        if os.path.exists(f):
                            zf.write(f, arcname=os.path.basename(f))
                attachments = [str(zip_path)]

            return attachments

        except Exception as e:
            self.logger.warning(f"collect_email_attachments failed: {e}")
            if summary_file and os.path.exists(summary_file):
                return [summary_file]
            return []

    def process_invoice_dataframe(self, df: pd.DataFrame, source_file: str) -> int:
        """Enhanced invoice processing with additional field extraction."""
        processed_count = 0
        try:
            column_mapping = self.map_dataframe_columns(df)

            for index, row in df.iterrows():
                try:
                    # Extract fields via resilient helpers
                    gst_no = self.extract_gst_number(row, column_mapping)
                    inv_date = self.extract_invoice_date(row, column_mapping)
                    due_date = self.extract_due_date(row, column_mapping)
                    currency = self.extract_currency(row, column_mapping)
                    location = self.extract_location(row, column_mapping)
                    mop = self.extract_payment_method(row, column_mapping)
                    account_head = self.extract_account_head(row, column_mapping)

                    invoice_number_col = column_mapping.get("invoice_number", "")
                    vendor_col = column_mapping.get("vendor", "")
                    amount_col = column_mapping.get("amount", "")

                    invoice_number = str(row.get(invoice_number_col, f"AUTO_{index}")).strip()
                    vendor_name = str(row.get(vendor_col, "Unknown Vendor")).strip()
                    amount = self.parse_amount(row.get(amount_col, None))

                    if not invoice_number or invoice_number.lower() == "nan":
                        continue

                    invoice_data = {
                        "invoice_number": invoice_number,
                        "vendor_name": vendor_name,
                        "amount": amount,
                        "status": "processed",
                        "rms_status": "pending" if getattr(self, "rms_enabled", False) else "n/a",
                        "file_path": source_file,
                        "hash_value": self.calculate_row_hash(row),
                        "processed_by": f"production_{self.session_id}",
                        "discrepancies": self.check_discrepancies(row, column_mapping),
                        "notes": f"Processed in production mode at {datetime.now().isoformat()}",
                        # extended fields
                        "gst_no": gst_no,
                        "inv_date": inv_date,
                        "due_date": due_date,
                        "currency": currency,
                        "location": location,
                        "mop": mop,
                        "account_head": account_head,
                    }

                    record_id = self.db_manager.insert_invoice_validation_enhanced(invoice_data)
                    if record_id:
                        processed_count += 1
                        if processed_count <= 5:
                            self.logger.debug(f"Processed invoice: {invoice_number}")

                except Exception as e:
                    self.logger.error(f"Failed to process row {index}: {e}")
                    continue

        except Exception as e:
            self.logger.error(f"DataFrame processing failed: {e}")

        return processed_count

    # ----------------------- Extraction helpers (row + mapping) -----------------------

    def extract_gst_number(self, row: pd.Series, column_mapping: Dict[str, str]) -> str:
        """Extract GST number from likely fields or text columns."""
        try:
            patt = re.compile(r"\b\d{2}[A-Z]{5}\d{4}[A-Z][A-Z0-9]Z[A-Z0-9]\b", re.I)
            # try GST-ish headers first
            for col in row.index:
                lc = str(col).lower()
                if any(k in lc for k in ("gst", "gstin", "gst_no", "gstnumber", "tax_id", "tin")):
                    val = str(row.get(col, "")).strip()
                    m = patt.search(val)
                    if m:
                        return m.group(0).upper()
            # fallback: scan vendor/notes columns
            vendor = str(row.get(column_mapping.get("vendor", ""), "")).strip()
            m = patt.search(vendor)
            if m:
                return m.group(0).upper()
        except Exception as e:
            self.logger.debug(f"GST extraction failed: {e}")
        return ""

    def _parse_any_date_str(self, value) -> Optional[str]:
        """Try common date formats; return 'YYYY-MM-DD' or None."""
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return None
        # already a datetime-like?
        try:
            if hasattr(value, "strftime"):
                return value.strftime("%Y-%m-%d")
        except Exception:
            pass
        s = str(value).strip()
        if not s or s.lower() in ("nan", "none", "nat"):
            return None
        fmts = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"]
        for f in fmts:
            try:
                return datetime.strptime(s, f).strftime("%Y-%m-%d")
            except Exception:
                continue
        return None

    def extract_invoice_date(self, row: pd.Series, column_mapping: Dict[str, str]) -> str:
        try:
            for col in row.index:
                lc = str(col).lower()
                if any(k in lc for k in ("invoice_date", "inv_date", "bill_date", "date", "created_date")):
                    d = self._parse_any_date_str(row.get(col))
                    if d:
                        return d
        except Exception as e:
            self.logger.debug(f"Invoice date extraction failed: {e}")
        return ""

    def extract_due_date(self, row: pd.Series, column_mapping: Dict[str, str]) -> str:
        try:
            for col in row.index:
                lc = str(col).lower()
                if any(k in lc for k in ("due", "due_date", "payment_due", "pay_date", "expiry")):
                    d = self._parse_any_date_str(row.get(col))
                    if d:
                        return d
        except Exception as e:
            self.logger.debug(f"Due date extraction failed: {e}")
        return ""

    def extract_currency(self, row: pd.Series, column_mapping: Dict[str, str]) -> str:
        try:
            for col in row.index:
                lc = str(col).lower()
                if any(k in lc for k in ("currency", "curr", "ccy", "cur_code")):
                    val = str(row.get(col, "")).strip().upper()
                    if val and val.lower() not in ("nan", "none"):
                        return val
        except Exception as e:
            self.logger.debug(f"Currency extraction failed: {e}")
        return "USD"

    def extract_location(self, row: pd.Series, column_mapping: Dict[str, str]) -> str:
        try:
            for col in row.index:
                lc = str(col).lower()
                if any(k in lc for k in ("location", "site", "branch", "office", "city", "state")):
                    val = str(row.get(col, "")).strip()
                    if val and val.lower() not in ("nan", "none"):
                        return val
        except Exception as e:
            self.logger.debug(f"Location extraction failed: {e}")
        return ""

    def extract_payment_method(self, row: pd.Series, column_mapping: Dict[str, str]) -> str:
        try:
            for col in row.index:
                lc = str(col).lower()
                if any(k in lc for k in ("payment", "pay_method", "mop", "method", "pay_mode")):
                    val = str(row.get(col, "")).strip()
                    if val and val.lower() not in ("nan", "none"):
                        return val
        except Exception as e:
            self.logger.debug(f"Payment method extraction failed: {e}")
        return ""

    def extract_account_head(self, row: pd.Series, column_mapping: Dict[str, str]) -> str:
        try:
            for col in row.index:
                lc = str(col).lower()
                if any(k in lc for k in ("account", "acc_head", "gl_code", "cost_center", "dept")):
                    val = str(row.get(col, "")).strip()
                    if val and val.lower() not in ("nan", "none"):
                        return val
        except Exception as e:
            self.logger.debug(f"Account head extraction failed: {e}")
        return ""

    # ------------------------- Column mapping / validation --------------------------

    def map_dataframe_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Map dataframe columns to standard fields with robust heuristics.
        Targets: invoice_number, vendor, amount
        """
        cols: List[str] = [str(c).strip() for c in df.columns if str(c).strip()]
        if not cols:
            self.logger.warning("No columns found in DataFrame for mapping.")
            return {}

        cols_lower = {c.lower(): c for c in cols}

        def norm_key(s: str) -> str:
            return re.sub(r"[^a-z0-9]+", "", s.lower())

        norm_map = {norm_key(c): c for c in cols}

        rules = {
            "invoice_number": {
                "exact": [
                    "invoice_number", "invoice number", "invoice_no", "invoice no",
                    "inv_no", "inv no", "inv_num", "inv#", "invoiceid", "voucherno",
                    "voucher_no", "doc_no", "document_no", "purchaseinvno",
                ],
                "syn": ["invoice", "inv", "voucher", "doc", "bill"],
            },
            "vendor": {
                "exact": [
                    "vendor", "vendor_name", "vendor name", "supplier", "supplier_name",
                    "supplier name", "partyname", "party", "payee", "company", "beneficiary",
                ],
                "syn": ["vendor", "supplier", "party", "company", "payee", "beneficiary"],
            },
            "amount": {
                "exact": [
                    "amount", "invoice_amount", "invoice amount", "total", "total_amount",
                    "grand_total", "net_amount", "gross_amount", "paytyamt", "taxablevalue",
                ],
                "syn": ["amount", "total", "grand", "value", "sum", "amt"],
            },
        }

        bad_words = {
            "invoice_number": {"date", "time", "entry", "created", "updated", "month", "year"},
            "vendor": {"address", "gst", "pan", "state", "city", "country", "code", "id"},
            "amount": {
                "igst", "cgst", "sgst", "vat", "tax", "tds", "discount", "round", "roundoff",
                "cess", "rate", "qty", "quantity", "price_per", "unit",
            },
        }

        def numeric_ratio(series: pd.Series) -> float:
            try:
                conv = pd.to_numeric(series, errors="coerce")
                return float(conv.notna().mean())
            except Exception:
                return 0.0

        def find_best(target: str) -> Optional[str]:
            rule = rules[target]
            # exact lower
            for name in rule["exact"]:
                key = name.lower()
                if key in cols_lower:
                    return cols_lower[key]
            # exact normalized
            for name in rule["exact"]:
                keyn = norm_key(name)
                if keyn in norm_map:
                    return norm_map[keyn]
            # synonyms
            candidates: List[str] = []
            for syn in rule["syn"]:
                s = syn.lower()
                for c in cols:
                    lc = c.lower()
                    if s in lc and not any(b in lc for b in bad_words[target]):
                        candidates.append(c)
            if not candidates:
                return None

            if target == "invoice_number":
                def inv_score(c: str) -> int:
                    lc = c.lower(); score = 0
                    if any(tok in lc for tok in ["no", "num", "id", "#"]): score += 3
                    if any(tok in lc for tok in ["invoice", "voucher", "doc"]): score += 2
                    if df[c].dtype == object: score += 1
                    try:
                        if numeric_ratio(df[c]) < 0.5: score += 1
                    except Exception:
                        pass
                    return score
                candidates.sort(key=inv_score, reverse=True)
                return candidates[0]

            if target == "vendor":
                def ven_score(c: str) -> int:
                    lc = c.lower(); score = 0
                    if any(tok in lc for tok in ["vendor", "supplier", "party", "company"]): score += 2
                    if df[c].dtype == object: score += 2
                    try:
                        if pd.Series(df[c].astype(str)).str.len().mean() >= 6: score += 1
                    except Exception:
                        pass
                    return score
                candidates.sort(key=ven_score, reverse=True)
                return candidates[0]

            if target == "amount":
                def amt_score(c: str) -> int:
                    lc = c.lower(); score = 0
                    if "grand" in lc: score += 3
                    if "total" in lc: score += 2
                    if any(tok in lc for tok in ["amount", "amt", "value"]): score += 1
                    try:
                        if numeric_ratio(df[c]) >= 0.6: score += 3
                    except Exception:
                        pass
                    if any(b in lc for b in bad_words["amount"]): score -= 3
                    return score
                candidates.sort(key=amt_score, reverse=True)
                return candidates[0]

            return candidates[0]

        mapping: Dict[str, str] = {}
        for key in ["invoice_number", "vendor", "amount"]:
            got = find_best(key)
            if got:
                mapping[key] = got

        # avoid collisions
        if len(set(mapping.values())) < len(mapping):
            inv = mapping.get("invoice_number")
            ven = mapping.get("vendor")
            amt = mapping.get("amount")
            if amt in {inv, ven}:
                self.logger.warning("Amount column collided with another target — dropping amount mapping.")
                mapping.pop("amount", None)

        self.logger.info(f"Production column mapping: {mapping}")
        return mapping

    def parse_amount(self, amount_value) -> Optional[float]:
        """Parse amount to float with guardrails."""
        if pd.isna(amount_value):
            return None
        try:
            amount_str = re.sub(r"[^\d.-]", "", str(amount_value).strip())
            if amount_str:
                parsed = float(amount_str)
                # keep bounds warning, but still return parsed
                if not (0 <= parsed <= 1_000_000):
                    self.logger.warning(f"Amount outside reasonable range: {parsed}")
                return parsed
        except Exception as e:
            self.logger.warning(f"Failed to parse amount '{amount_value}': {e}")
        return None

    def check_discrepancies(self, row: pd.Series, column_mapping: Dict[str, str]) -> Optional[str]:
        """Detect basic issues for later review."""
        discrepancies: List[str] = []
        try:
            if column_mapping.get("amount"):
                amount = self.parse_amount(row.get(column_mapping["amount"]))
                if amount is None:
                    discrepancies.append("Invalid or missing amount")
                elif amount <= 0:
                    discrepancies.append("Zero or negative amount")
            if column_mapping.get("vendor"):
                vendor = str(row.get(column_mapping["vendor"], "")).strip()
                if not vendor or vendor.lower() in ("", "nan", "unknown"):
                    discrepancies.append("Missing vendor information")
            return "; ".join(discrepancies) if discrepancies else None
        except Exception as e:
            self.logger.error(f"Discrepancy check failed: {e}")
            return f"Discrepancy check error: {e}"

    def calculate_row_hash(self, row: pd.Series) -> str:
        """Stable-ish row hash for dedupe/tracking."""
        try:
            row_tuple = tuple(map(str, row.values))
            row_str = f"{self.session_id}_{hash(row_tuple)}"
            return hashlib.md5(row_str.encode("utf-8")).hexdigest()
        except Exception:
            return hashlib.md5(f"{self.session_id}_{time.time()}".encode("utf-8")).hexdigest()

    # ------------------------------- RMS downloading -------------------------------

    def download_rms_exports(self, start_date: Optional[str], end_date: Optional[str]) -> List[str]:
        """
        Log in to RMS and download:
          1) the grid export (Excel/XLS)
          2) invoice documents (PDF/JPG) after selecting header checkbox.
        Uses the exact element IDs you provided.
        """
        files: List[str] = []
        if not getattr(self, "rms_enabled", False):
            self.logger.info("RMS disabled → skipping RMS downloads")
            return files

        download_dir = Path(getattr(config, "DOWNLOADS_DIR", "downloads"))
        download_dir.mkdir(parents=True, exist_ok=True)

        def _enable_downloads(drv):
            payload = {"behavior": "allow", "downloadPath": str(download_dir)}
            try:
                drv.execute_cdp_cmd("Page.setDownloadBehavior", payload)
            except Exception:
                try:
                    drv.execute_cdp("Page.setDownloadBehavior", payload)  # alt API
                except Exception as e:
                    self.logger.warning(f"CDP downloads not enabled: {e}")

        def _snapshot() -> set:
            return {p.name for p in download_dir.glob("*")}
            
        def _new_completed_files(before: set) -> List[Path]:
            wanted = (".csv", ".xlsx", ".xls", ".pdf", ".jpg", ".jpeg")
            out: List[Path] = []
            for p in download_dir.glob("*"):
                if p.name in before:
                    continue
                if p.suffix.lower() in wanted and not p.name.endswith((".crdownload", ".tmp")):
                    out.append(p)
            return out

        def _wait_for_downloads(before: set, timeout: int = 180) -> List[Path]:
            end = time.time() + timeout
            while time.time() < end:
                partials = list(download_dir.glob("*.crdownload")) + list(download_dir.glob("*.tmp"))
                news = _new_completed_files(before)
                if not partials and news:
                    return news
                time.sleep(1)
            return _new_completed_files(before)

        # Locators (from your HTML)
        LOGIN_USER      = (By.ID, "txtUser")
        LOGIN_PWD       = (By.ID, "txtPwd")
        LOGIN_BTN       = (By.ID, "btnSubmit")

        DATE_FROM       = (By.ID, "cphMainContent_mainContent_txtDateFrom")
        DATE_TO         = (By.ID, "cphMainContent_mainContent_txtDateTo")
        COMBINE_RADIO   = (By.ID, "cphMainContent_mainContent_rbPaidUnPaid_2")
        SEARCH_BTN      = (By.ID, "cphMainContent_mainContent_btnSearch")
        EXPORT_LINK     = (By.ID, "cphMainContent_mainContent_ExportToExcel")
        HEADER_CHECKBOX = (By.ID, "cphMainContent_mainContent_rptShowAss_chkHeader")

        try:
            drv = self.selenium_manager.get_driver()
            wait = WebDriverWait(drv, int(getattr(config, "SELENIUM_TIMEOUT", 30)))
            _enable_downloads(drv)

            login_url   = os.getenv("RMS_LOGIN_URL")   or getattr(config, "RMS_LOGIN_URL", None)
            reports_url = os.getenv("RMS_REPORTS_URL") or getattr(config, "RMS_REPORTS_URL", None)
            if not login_url and not reports_url:
                self.logger.error("No RMS URLs configured (RMS_LOGIN_URL / RMS_REPORTS_URL).")
                return files

            # If only one is set, use it and allow server to redirect between pages.
            target_login   = login_url   or reports_url
            target_reports = reports_url or login_url

            # --- 1) Login ---
            self.logger.info(f"Navigating to login: {target_login}")
            drv.get(target_login)

            user = wait.until(EC.visibility_of_element_located(LOGIN_USER))
            pwd  = wait.until(EC.visibility_of_element_located(LOGIN_PWD))
            user.clear(); user.send_keys(os.getenv("RMS_USERNAME", ""))
            pwd.clear();  pwd.send_keys(os.getenv("RMS_PASSWORD", ""))

            try:
                wait.until(EC.element_to_be_clickable(LOGIN_BTN)).click()
            except Exception:
                drv.find_element(By.ID, "btnSubmit").submit()
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            # --- 2) Invoice list page ---
            if target_reports:
                self.logger.info(f"Navigating to invoice list: {target_reports}")
                drv.get(target_reports)

            # --- 3) Filters: from/to + combine + search ---
            def _set_input(locator, value: str):
                el = wait.until(EC.element_to_be_clickable(locator))
                drv.execute_script("arguments[0].focus();", el)
                el.clear()
                el.send_keys(value)
                el.send_keys(Keys.ENTER)

            if start_date:
                try:
                    _set_input(DATE_FROM, start_date)
                except Exception as e:
                    self.logger.warning(f"Could not set From date: {e}")
            if end_date:
                try:
                    _set_input(DATE_TO, end_date)
                except Exception as e:
                    self.logger.warning(f"Could not set To date: {e}")

            # Combine radio
            try:
                r = wait.until(EC.element_to_be_clickable(COMBINE_RADIO))
                if not r.is_selected():
                    r.click()
            except Exception as e:
                self.logger.info(f"Combine radio not set: {e}")

            # Search
            try:
                wait.until(EC.element_to_be_clickable(SEARCH_BTN)).click()
            except Exception as e:
                self.logger.error(f"Search click failed: {e}")

            # Wait for results: header checkbox (or any table rows)
            try:
                wait.until(EC.presence_of_element_located(HEADER_CHECKBOX))
            except Exception:
                # as a fallback, wait for any grid rows under main content
                wait.until(EC.presence_of_element_located((By.XPATH, "//table//tr")))

            # --- 4) Export to Excel ---
            try:
                export_el = wait.until(EC.element_to_be_clickable(EXPORT_LINK))
                before = _snapshot()
                drv.execute_script("arguments[0].click();", export_el)  # anchor does __doPostBack
                for p in _wait_for_downloads(before, timeout=180):
                    if p.suffix.lower() in (".csv", ".xlsx", ".xls"):
                        files.append(str(p))
                        self.logger.info(f"Downloaded export: {p.name}")
            except Exception as e:
                self.logger.error(f"Export to Excel failed: {e}")

            # --- 5) Select all invoices and try bulk download of PDFs/JPGs ---
            try:
                # select the header checkbox
                try:
                    hdr = wait.until(EC.element_to_be_clickable(HEADER_CHECKBOX))
                    if not hdr.is_selected():
                        hdr.click()
                    time.sleep(0.5)
                except Exception as e:
                    self.logger.info(f"Header checkbox not available: {e}")

                # Try common “Download” triggers (buttons/links)
                candidates = [
                    (By.XPATH, "//a[contains(.,'Download') or contains(.,'download') or contains(.,'ZIP') or contains(.,'Zip')]"),
                    (By.XPATH, "//input[@type='button' and (contains(@value,'Download') or contains(@value,'ZIP') or contains(@value,'Zip'))]"),
                    (By.XPATH, "//button[contains(.,'Download') or contains(.,'ZIP') or contains(.,'Zip')]"),
                ]
                clicked_any = False
                for loc in candidates:
                    try:
                        btns = drv.find_elements(*loc)
                        for b in btns[:2]:
                            drv.execute_script("arguments[0].scrollIntoView({block:'center'});", b)
                            before = _snapshot()
                            try:
                                drv.execute_script("arguments[0].click();", b)
                            except Exception:
                                b.click()
                            # collect any docs
                            for p in _wait_for_downloads(before, timeout=120):
                                if p.suffix.lower() in (".pdf", ".jpg", ".jpeg"):
                                    files.append(str(p))
                                    clicked_any = True
                        if clicked_any:
                            break
                    except Exception:
                        pass

                if not clicked_any:
                    self.logger.info("No obvious bulk download control found. Attempting to click visible invoice document links.")
                    # Fallback: click links that look like invoice documents
                    links = drv.find_elements(
                        By.XPATH,
                        "//a[contains(translate(@href,'PDFJGP','pdfjgp'),'.pdf') or "
                        "     contains(translate(@href,'PDFJGP','pdfjgp'),'.jpg')]"
                    )[:100]
                    if links:
                        before = _snapshot()
                        for a in links:
                            try:
                                drv.execute_script("arguments[0].scrollIntoView({block:'center'})", a)
                                ActionChains(drv).key_down(Keys.CONTROL).click(a).key_up(Keys.CONTROL).perform()
                            except Exception:
                                try: a.click()
                                except Exception: pass
                        for p in _wait_for_downloads(before, timeout=180):
                            if p.suffix.lower() in (".pdf", ".jpg", ".jpeg"):
                                files.append(str(p))

            except Exception as e:
                self.logger.warning(f"Invoice document download step skipped: {e}")

            self.logger.info(f"RMS downloaded: {len(files)} file(s)")
            return files

        except Exception as e:
            self.logger.error(f"RMS download failed: {e}")
            return files

            # 6) Bundle all invoice docs into a single ZIP for emailing
            try:
                bundle = download_dir / "invoices_bundle.zip"
                import zipfile
                with zipfile.ZipFile(bundle, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    for p in sorted(download_dir.glob("*")):
                        if p.suffix.lower() in (".pdf", ".jpg", ".jpeg"):
                            zf.write(p, arcname=p.name)
                if bundle.exists():
                    files.append(str(bundle))
                    self.logger.info(f"Created bundle: {bundle}")
            except Exception as e:
                 self.logger.warning(f"Could not create invoices bundle: {e}")

            self.logger.info(f"RMS downloaded: {len(files)} file(s)")
            return files

        except Exception as e:
            self.logger.error(f"RMS download failed: {e}")
            return files

    # --------------------------------- Packaging ------------------------------------

    def build_invoices_zip(self) -> Optional[str]:
        """Zip any PDFs/JPGs found in downloads/ for emailing."""
        download_dir = Path(getattr(config, "DOWNLOADS_DIR", "downloads"))
        docs = list(download_dir.glob("*.pdf")) + list(download_dir.glob("*.jpg")) + list(download_dir.glob("*.jpeg"))
        if not docs:
            return None
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        zpath = download_dir / f"invoices_{ts}.zip"
        with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
            for p in docs:
                z.write(p, arcname=p.name)
        self.logger.info(f"Built invoices archive: {zpath}")
        return str(zpath)

    # --------------------------------- Reporting -----------------------------------

    def write_validation_csvs(self, df_all: Optional[pd.DataFrame] = None, out_dir: Optional[Path] = None) -> None:
        """Write validation results to CSV files (validation_report.csv + discrepancy_report.csv)."""
        try:
            out_dir = out_dir or Path(getattr(config, "DOWNLOADS_DIR", "downloads"))
            out_dir.mkdir(parents=True, exist_ok=True)

            if df_all is None:
                with self.db_manager.get_connection() as conn:
                    df_all = pd.read_sql_query(
                        "SELECT * FROM invoice_validations WHERE processed_by LIKE ? ORDER BY created_at DESC",
                        conn,
                        params=(f"%{self.session_id}%",),
                    )

            if df_all is None or df_all.empty:
                self.logger.warning("No data available for CSV export")
                return

            valid_csv = out_dir / "validation_report.csv"
            disc_csv = out_dir / "discrepancy_report.csv"

            df_all.to_csv(valid_csv, index=False)

            if "discrepancies" in df_all.columns:
                df_disc = df_all[df_all["discrepancies"].notna()]
                df_disc.to_csv(disc_csv, index=False)
            else:
                pd.DataFrame().to_csv(disc_csv, index=False)

            # track attachments if the dict exists
            try:
                self.processing_results.setdefault("attachment_files", [])
                self.processing_results["attachment_files"] += [str(valid_csv), str(disc_csv)]
            except Exception:
                pass

            self.logger.info(f"Wrote CSVs: {valid_csv.name}, {disc_csv.name}")
        except Exception as e:
            self.logger.warning(f"write_validation_csvs skipped: {e}")

    def generate_summary_report(self) -> Optional[str]:
        """Generate an Excel summary from whatever columns exist in the DB."""
        try:
            out_dir = Path(getattr(config, "DOWNLOADS_DIR", "downloads"))
            out_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = out_dir / f"production_summary_{ts}.xlsx"

            with self.db_manager.get_connection() as conn:
                df = pd.read_sql_query(
                    "SELECT * FROM invoice_validations ORDER BY created_at DESC",
                    conn,
                )

            if df is None or df.empty:
                self.logger.info("No validation records found for report generation")
                return None

            with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name="Invoice Validations", index=False)

                # Autosize columns
                ws = writer.sheets["Invoice Validations"]
                for col in ws.columns:
                    max_len = 0
                    col_letter = col[0].column_letter
                    for cell in col:
                        try:
                            max_len = max(max_len, len(str(cell.value)) if cell.value is not None else 0)
                        except Exception:
                            pass
                    ws.column_dimensions[col_letter].width = min(max_len + 2, 50)

            self.logger.info(f"Excel report generated: {report_path}")
            return str(report_path)
        except Exception as e:
            self.logger.error(f"Error generating Excel report: {e}")
            return None

    # ----------------------------- Orchestrator (run) ------------------------------

    def run_validation_process(self) -> bool:
        """End-to-end production run: setup, (optional) RMS fetch, local processing, reports, email."""
        try:
            self.logger.info(
                f"Starting production invoice validation process - Session: {getattr(self, 'session_id', 'unknown')}"
            )

            # 1) Setup and config validation
            try:
                if hasattr(self, "setup_directories"):
                    self.setup_directories()
                if hasattr(self, "validate_configuration"):
                    self.validate_configuration()
            except Exception as e:
                self.logger.error(f"Initial setup/config failed: {e}")

            # 2) Dates from env
            start_date = os.getenv("PROCESSING_START_DATE") or None
            end_date = os.getenv("PROCESSING_END_DATE") or None
            force_demo = (os.getenv("FORCE_DEMO") or "").strip().lower() in ("1", "true", "yes")

            # 3) RMS download (optional)
            files_downloaded: List[str] = []
            try:
                if getattr(self, "rms_enabled", False) and not force_demo:
                    have_urls = bool(getattr(config, "RMS_LOGIN_URL", "") and getattr(config, "RMS_REPORTS_URL", ""))
                    have_creds = bool(os.getenv("RMS_USERNAME") and os.getenv("RMS_PASSWORD"))
                    if have_urls and have_creds:
                        files_downloaded = self.download_rms_exports(start_date, end_date) or []
                    else:
                        self.logger.warning("RMS not fully configured (urls/creds missing); skipping RMS downloads.")
                else:
                    self.logger.info("RMS disabled or FORCE_DEMO active; skipping RMS downloads.")
            except Exception as e:
                self.logger.error(f"RMS download step failed (continuing with local files): {e}")

            # 4) Seed demo data if nothing present
            try:
                if hasattr(self, "create_demo_data_if_needed"):
                    self.create_demo_data_if_needed()
            except Exception as e:
                self.logger.warning(f"Demo data creation skipped: {e}")

            # 5) Process local files
            processed_count = 0
            try:
                if hasattr(self, "process_local_files"):
                    processed_files = self.process_local_files()
                    processed_count = len(processed_files) if processed_files else 0
                else:
                    self.logger.error("process_local_files method not found.")
            except Exception as e:
                self.logger.error(f"Local file processing failed: {e}")

            # 6) Reports
            attachments: List[str] = []
            try:
                self.write_validation_csvs()
            except Exception as e:
                self.logger.warning(f"write_validation_csvs skipped: {e}")

            try:
                summary_path = self.generate_summary_report()
                if summary_path:
                    attachments.append(str(summary_path))
            except Exception as e:
                self.logger.warning(f"Summary report generation failed: {e}")

            # Include invoices ZIP if any docs exist
            try:
                z = self.build_invoices_zip()
                if z:
                    attachments.append(z)
            except Exception as e:
                self.logger.debug(f"No invoice ZIP built: {e}")

            # Include error log if present
            try:
                err_log = Path("logs") / f"errors_{datetime.now():%Y%m%d}.log"
                if err_log.exists():
                    attachments.append(str(err_log))
            except Exception:
                pass

            # 7) Email results (best-effort)
            try:
                if getattr(self, "email_notifier", None):
                    self.processing_results['attachment_files'] = attachments
                    self.processing_results['overall_status'] = 'success' if processed_count > 0 else 'failed'
                    self.email_notifier.send_processing_summary(self.session_id, self.processing_results)
            except Exception as e:
                self.logger.warning(f"Email notification failed: {e}")

            self.logger.info(f"Production validation process finished successfully - Processed: {processed_count}")
            return True

        except Exception as e:
            if hasattr(self.logger, "exception"):
                self.logger.exception(f"Fatal error in run_validation_process: {e}")
            else:
                print(f"Fatal error in run_validation_process: {e}")
            return False
            
def main():
    """Production main entry point"""
    try:
        # Initialize production logging
        log_manager = LogManager()
        logger = logging.getLogger(__name__)

        logger.info("=== Production Invoice Validation System Starting ===")
        logger.info(f"Python version: {sys.version}")
        logger.info(f"GitHub Actions mode: {config.IS_GITHUB_ACTIONS}")
        logger.info(f"Selenium available: {SELENIUM_AVAILABLE}")
        logger.info(f"Current working directory: {os.getcwd()}")
        logger.info(f"Environment configuration: {config.IS_GITHUB_ACTIONS and 'Production GitHub Actions' or 'Local Development'}")

        # Create and run production validation system
        validation_system = ProductionInvoiceValidationSystem()

        success = validation_system.run_validation_process()

        if success:
            logger.info("=== Production Invoice Validation System Completed Successfully ===")
            sys.exit(0)
        else:
            logger.error("=== Production Invoice Validation System Failed ===")
            sys.exit(1)

    except Exception as e:
        print(f"Critical production error: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
