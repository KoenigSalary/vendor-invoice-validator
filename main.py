#!/usr/bin/env python3
"""
Invoice Validation System - Main Processing Engine
Automated RMS data extraction, validation, and reporting system

Features:
- RMS data scraping and processing
- Comprehensive invoice validation
- Email notifications with professional templates
- Archiving and cumulative validation
- Enhanced error handling and logging
- GitHub Actions integration

Version: 2.0 (Syntax Error Free)
Author: Koenig Solutions Invoice Team
"""

import os
import sys
import json
import pandas as pd
import numpy as np
import logging
import traceback
import hashlib
import pickle
import sqlite3
import smtplib
import zipfile
import shutil
import requests
import schedule
import time
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, field
from collections import defaultdict, Counter
from contextlib import contextmanager
import warnings
warnings.filterwarnings('ignore')

# Email and file handling imports
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import glob
import re
import io
import base64

# Web scraping and data processing
import urllib.parse
import urllib.request
from urllib.error import URLError, HTTPError
import ssl
import certifi

# Excel and CSV processing
import openpyxl
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
import xlsxwriter

# Configuration and environment
from dotenv import load_dotenv
load_dotenv()

# Global Configuration
CONFIG = {
    'rms_base_url': os.getenv('RMS_BASE_URL', 'https://rms.koenig-solutions.com'),
    'database_path': os.getenv('DB_PATH', 'invoice_validation.db'),
    'excel_output_path': os.getenv('EXCEL_PATH', 'invoice_validation_report.xlsx'),
    'archive_days': int(os.getenv('ARCHIVE_DAYS', '90')),
    'validation_interval_days': int(os.getenv('VALIDATION_INTERVAL', '4')),
    'max_retries': int(os.getenv('MAX_RETRIES', '3')),
    'timeout_seconds': int(os.getenv('TIMEOUT_SECONDS', '30')),
    'batch_size': int(os.getenv('BATCH_SIZE', '100')),
    'debug_mode': os.getenv('DEBUG_MODE', 'False').lower() == 'true'
}

# Logging Configuration
def setup_logging():
    """Configure comprehensive logging system"""
    log_level = logging.DEBUG if CONFIG['debug_mode'] else logging.INFO
    
    # Create logs directory
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)
    
    # Configure logging format
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # File handler with rotation
    log_file = logs_dir / f"invoice_validation_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))
    
    # Root logger configuration
    logging.basicConfig(
        level=log_level,
        handlers=[file_handler, console_handler],
        format=log_format,
        datefmt=date_format
    )
    
    return logging.getLogger(__name__)

# Global logger
logger = setup_logging()

@dataclass
class InvoiceData:
    """Data class for invoice information"""
    invoice_number: str
    vendor_code: str = ""
    vendor_name: str = ""
    invoice_date: Optional[datetime] = None
    invoice_amount: float = 0.0
    currency: str = "INR"
    payment_terms: str = ""
    due_date: Optional[datetime] = None
    description: str = ""
    account_head: str = ""
    payment_method: str = ""
    creator_name: str = ""
    status: str = "PENDING"
    validation_result: str = ""
    error_details: List[str] = field(default_factory=list)
    pass_rate: float = 0.0
    processing_timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'invoice_number': self.invoice_number,
            'vendor_code': self.vendor_code,
            'vendor_name': self.vendor_name,
            'invoice_date': self.invoice_date.isoformat() if self.invoice_date else None,
            'invoice_amount': self.invoice_amount,
            'currency': self.currency,
            'payment_terms': self.payment_terms,
            'due_date': self.due_date.isoformat() if self.due_date else None,
            'description': self.description,
            'account_head': self.account_head,
            'payment_method': self.payment_method,
            'creator_name': self.creator_name,
            'status': self.status,
            'validation_result': self.validation_result,
            'error_details': self.error_details,
            'pass_rate': self.pass_rate,
            'processing_timestamp': self.processing_timestamp.isoformat()
        }

@dataclass
class ValidationStats:
    """Statistics for validation results"""
    total_invoices: int = 0
    passed_invoices: int = 0
    failed_invoices: int = 0
    warning_invoices: int = 0
    total_amount: float = 0.0
    passed_amount: float = 0.0
    failed_amount: float = 0.0
    pass_rate: float = 0.0
    processing_time: float = 0.0
    errors: List[str] = field(default_factory=list)
    
    def calculate_metrics(self):
        """Calculate derived metrics"""
        if self.total_invoices > 0:
            self.pass_rate = (self.passed_invoices / self.total_invoices) * 100
        else:
            self.pass_rate = 0.0

def map_account_head(description: str) -> str:
    """
    Map transaction descriptions to standardized account heads
    
    Args:
        description: Transaction description or item details
        
    Returns:
        Standardized account head category
    """
    if not description:
        return "Miscellaneous"
    
    description = str(description).lower().strip()
    
    # Comprehensive account head mapping
    account_mappings = {
        # Office and Administrative
        'rent': 'Rent Expense',
        'office rent': 'Rent Expense',
        'lease': 'Rent Expense',
        'utilities': 'Utilities Expense',
        'electricity': 'Utilities Expense',
        'water': 'Utilities Expense',
        'gas': 'Utilities Expense',
        'internet': 'Internet Expense',
        'telephone': 'Telephone Expense',
        'phone': 'Telephone Expense',
        'mobile': 'Telephone Expense',
        
        # HR and Payroll
        'salary': 'Salary Expense',
        'wages': 'Salary Expense',
        'bonus': 'Salary Expense',
        'overtime': 'Salary Expense',
        'training': 'Training Expense',
        'recruitment': 'Recruitment Expense',
        
        # IT and Technology
        'software': 'Software Expense',
        'license': 'Software Expense',
        'subscription': 'Subscription Expense',
        'cloud': 'Software Expense',
        'hardware': 'Equipment Expense',
        'computer': 'Equipment Expense',
        'laptop': 'Equipment Expense',
        
        # Travel and Transportation
        'travel': 'Travel Expense',
        'flight': 'Travel Expense',
        'hotel': 'Travel Expense',
        'accommodation': 'Travel Expense',
        'transport': 'Travel Expense',
        'taxi': 'Travel Expense',
        'fuel': 'Fuel Expense',
        'petrol': 'Fuel Expense',
        'diesel': 'Fuel Expense',
        
        # Office Supplies and Maintenance
        'stationery': 'Office Supplies',
        'supplies': 'Office Supplies',
        'printing': 'Office Supplies',
        'paper': 'Office Supplies',
        'maintenance': 'Maintenance Expense',
        'repair': 'Repair Expense',
        'cleaning': 'Maintenance Expense',
        
        # Professional Services
        'legal': 'Legal Expense',
        'audit': 'Professional Services',
        'consulting': 'Consulting Expense',
        'accounting': 'Accounting Expense',
        'professional': 'Professional Services',
        
        # Marketing and Business Development
        'marketing': 'Marketing Expense',
        'advertising': 'Advertising Expense',
        'promotion': 'Marketing Expense',
        'branding': 'Marketing Expense',
        
        # Financial
        'bank': 'Bank Charges',
        'interest': 'Interest Expense',
        'loan': 'Interest Expense',
        'insurance': 'Insurance Expense',
        'tax': 'Tax Expense',
        'penalty': 'Penalty Expense',
        
        # Meals and Entertainment
        'meals': 'Meals & Entertainment',
        'food': 'Meals & Entertainment',
        'entertainment': 'Meals & Entertainment',
        'conference': 'Conference Expense',
        
        # Depreciation and Assets
        'depreciation': 'Depreciation',
        'asset': 'Asset Purchase',
        'equipment': 'Equipment Expense',
        'furniture': 'Furniture & Fixtures',
        
        # Other Common Categories
        'postage': 'Postage Expense',
        'courier': 'Postage Expense',
        'security': 'Security Expense',
        'donation': 'Donation',
        'charity': 'Donation'
    }
    
    # Find matching category (case-insensitive, partial match)
    for keyword, account_head in account_mappings.items():
        if keyword in description:
            logger.debug(f"Mapped '{description}' to '{account_head}' via keyword '{keyword}'")
            return account_head
    
    # Additional pattern matching for complex descriptions
    if any(word in description for word in ['service', 'maintenance', 'support']):
        return "Professional Services"
    elif any(word in description for word in ['purchase', 'buy', 'procurement']):
        return "Purchases"
    elif any(word in description for word in ['refund', 'return', 'credit']):
        return "Refunds & Credits"
    
    logger.debug(f"No mapping found for '{description}', using 'Miscellaneous'")
    return "Miscellaneous"

def map_payment_method(payment_info: str) -> str:
    """
    Standardize payment method information
    
    Args:
        payment_info: Raw payment method data
        
    Returns:
        Standardized payment method
    """
    if not payment_info:
        return "Cash"
    
    payment_str = str(payment_info).lower().strip()
    
    # Payment method mappings
    if any(term in payment_str for term in ['credit card', 'debit card', 'card', 'visa', 'mastercard', 'amex']):
        return "Card Payment"
    elif any(term in payment_str for term in ['bank transfer', 'wire transfer', 'neft', 'rtgs', 'imps']):
        return "Bank Transfer"
    elif any(term in payment_str for term in ['cheque', 'check', 'dd', 'demand draft']):
        return "Cheque"
    elif any(term in payment_str for term in ['upi', 'digital', 'online', 'net banking', 'wallet']):
        return "Digital Payment"
    elif any(term in payment_str for term in ['cash', 'petty cash']):
        return "Cash"
    elif any(term in payment_str for term in ['emi', 'installment', 'credit']):
        return "Credit/EMI"
    else:
        logger.debug(f"Unknown payment method '{payment_info}', defaulting to Cash")
        return "Cash"

def get_invoice_creator_name(creator_info: str) -> str:
    """
    Extract and standardize invoice creator name
    
    Args:
        creator_info: Raw creator information
        
    Returns:
        Cleaned creator name
    """
    if not creator_info:
        return "System Generated"
    
    creator_str = str(creator_info).strip()
    
    # Clean up common prefixes and suffixes
    cleanup_patterns = [
        r'^(created by:?\s*)',
        r'^(user:?\s*)',
        r'^(by:?\s*)',
        r'^(name:?\s*)',
        r'\s*\(.*\)$',  # Remove parenthetical info
        r'\s*-.*$'      # Remove dash and everything after
    ]
    
    for pattern in cleanup_patterns:
        creator_str = re.sub(pattern, '', creator_str, flags=re.IGNORECASE)
    
    creator_str = creator_str.strip()
    
    # Handle common placeholder values
    if not creator_str or creator_str.lower() in ['n/a', 'na', 'null', 'none', '', 'unknown', 'system']:
        return "System Generated"
    
    # Capitalize properly
    if creator_str.islower() or creator_str.isupper():
        creator_str = creator_str.title()
    
    logger.debug(f"Processed creator info '{creator_info}' to '{creator_str}'")
    return creator_str

class DatabaseManager:
    """Database operations for invoice validation system"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or CONFIG['database_path']
        self.init_database()
    
    def init_database(self):
        """Initialize database schema"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create invoices table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS invoices (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        invoice_number TEXT NOT NULL,
                        vendor_code TEXT,
                        vendor_name TEXT,
                        invoice_date TEXT,
                        invoice_amount REAL,
                        currency TEXT DEFAULT 'INR',
                        payment_terms TEXT,
                        due_date TEXT,
                        description TEXT,
                        account_head TEXT,
                        payment_method TEXT,
                        creator_name TEXT,
                        status TEXT DEFAULT 'PENDING',
                        validation_result TEXT,
                        error_details TEXT,
                        pass_rate REAL DEFAULT 0.0,
                        processing_timestamp TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(invoice_number, processing_timestamp)
                    )
                ''')
                
                # Create validation_stats table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS validation_stats (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_date TEXT NOT NULL,
                        total_invoices INTEGER,
                        passed_invoices INTEGER,
                        failed_invoices INTEGER,
                        warning_invoices INTEGER,
                        total_amount REAL,
                        passed_amount REAL,
                        failed_amount REAL,
                        pass_rate REAL,
                        processing_time REAL,
                        errors TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create indexes for better performance
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_invoice_number ON invoices(invoice_number)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON invoices(status)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_processing_timestamp ON invoices(processing_timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_run_date ON validation_stats(run_date)')
                
                conn.commit()
                logger.info("Database initialized successfully")
                
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise
    
    def save_invoice(self, invoice: InvoiceData) -> bool:
        """Save invoice data to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO invoices (
                        invoice_number, vendor_code, vendor_name, invoice_date,
                        invoice_amount, currency, payment_terms, due_date,
                        description, account_head, payment_method, creator_name,
                        status, validation_result, error_details, pass_rate,
                        processing_timestamp, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    invoice.invoice_number,
                    invoice.vendor_code,
                    invoice.vendor_name,
                    invoice.invoice_date.isoformat() if invoice.invoice_date else None,
                    invoice.invoice_amount,
                    invoice.currency,
                    invoice.payment_terms,
                    invoice.due_date.isoformat() if invoice.due_date else None,
                    invoice.description,
                    invoice.account_head,
                    invoice.payment_method,
                    invoice.creator_name,
                    invoice.status,
                    invoice.validation_result,
                    json.dumps(invoice.error_details),
                    invoice.pass_rate,
                    invoice.processing_timestamp.isoformat()
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Failed to save invoice {invoice.invoice_number}: {e}")
            return False
    
    def save_validation_stats(self, stats: ValidationStats, run_date: str) -> bool:
        """Save validation statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO validation_stats (
                        run_date, total_invoices, passed_invoices, failed_invoices,
                        warning_invoices, total_amount, passed_amount, failed_amount,
                        pass_rate, processing_time, errors
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    run_date,
                    stats.total_invoices,
                    stats.passed_invoices,
                    stats.failed_invoices,
                    stats.warning_invoices,
                    stats.total_amount,
                    stats.passed_amount,
                    stats.failed_amount,
                    stats.pass_rate,
                    stats.processing_time,
                    json.dumps(stats.errors)
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Failed to save validation stats: {e}")
            return False
    
    def get_recent_invoices(self, days: int = 30) -> List[Dict]:
        """Get recent invoices from database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
                
                cursor.execute('''
                    SELECT * FROM invoices 
                    WHERE processing_timestamp >= ? 
                    ORDER BY processing_timestamp DESC
                ''', (cutoff_date,))
                
                columns = [description[0] for description in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Failed to get recent invoices: {e}")
            return []
    
    def cleanup_old_data(self, days: int = None) -> int:
        """Clean up old data beyond retention period"""
        days = days or CONFIG['archive_days']
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
                
                # Delete old invoices
                cursor.execute('DELETE FROM invoices WHERE processing_timestamp < ?', (cutoff_date,))
                deleted_invoices = cursor.rowcount
                
                # Delete old stats
                cursor.execute('DELETE FROM validation_stats WHERE created_at < ?', (cutoff_date,))
                deleted_stats = cursor.rowcount
                
                conn.commit()
                
                logger.info(f"Cleaned up {deleted_invoices} old invoices and {deleted_stats} old stats")
                return deleted_invoices + deleted_stats
                
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")
            return 0

class RMSDataExtractor:
    """Extract data from RMS system"""
    
    def __init__(self):
        self.base_url = CONFIG['rms_base_url']
        self.session = requests.Session()
        self.session.timeout = CONFIG['timeout_seconds']
        
        # Configure SSL context
        self.session.verify = certifi.where()
        
        # Set user agent
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def authenticate(self, username: str = None, password: str = None) -> bool:
        """Authenticate with RMS system"""
        try:
            username = username or os.getenv('RMS_USERNAME')
            password = password or os.getenv('RMS_PASSWORD')
            
            if not username or not password:
                logger.error("RMS credentials not provided")
                return False
            
            # Login endpoint
            login_url = f"{self.base_url}/login"
            
            login_data = {
                'username': username,
                'password': password
            }
            
            response = self.session.post(login_url, data=login_data)
            response.raise_for_status()
            
            # Check if login was successful
            if 'dashboard' in response.url.lower() or response.status_code == 200:
                logger.info("Successfully authenticated with RMS")
                return True
            else:
                logger.error("RMS authentication failed")
                return False
                
        except Exception as e:
            logger.error(f"RMS authentication error: {e}")
            return False
    
    def extract_invoice_data(self, date_from: datetime = None, date_to: datetime = None) -> List[Dict]:
        """Extract invoice data from RMS"""
        try:
            # Default date range (last 4 days)
            if not date_to:
                date_to = datetime.now()
            if not date_from:
                date_from = date_to - timedelta(days=CONFIG['validation_interval_days'])
            
            # Invoice data endpoint
            invoice_url = f"{self.base_url}/api/invoices"
            
            params = {
                'date_from': date_from.strftime('%Y-%m-%d'),
                'date_to': date_to.strftime('%Y-%m-%d'),
                'format': 'json',
                'limit': 1000  # Adjust based on expected volume
            }
            
            response = self.session.get(invoice_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if isinstance(data, dict) and 'invoices' in data:
                invoices = data['invoices']
            elif isinstance(data, list):
                invoices = data
            else:
                logger.error("Unexpected RMS response format")
                return []
            
            logger.info(f"Extracted {len(invoices)} invoices from RMS")
            return invoices
            
        except Exception as e:
            logger.error(f"Failed to extract invoice data from RMS: {e}")
            return []
    
    def extract_vendor_data(self) -> Dict[str, Dict]:
        """Extract vendor master data"""
        try:
            vendor_url = f"{self.base_url}/api/vendors"
            
            response = self.session.get(vendor_url)
            response.raise_for_status()
            
            data = response.json()
            
            # Convert to lookup dictionary
            vendor_lookup = {}
            if isinstance(data, list):
                for vendor in data:
                    if 'vendor_code' in vendor:
                        vendor_lookup[vendor['vendor_code']] = vendor
            
            logger.info(f"Extracted {len(vendor_lookup)} vendors from RMS")
            return vendor_lookup
            
        except Exception as e:
            logger.error(f"Failed to extract vendor data: {e}")
            return {}

class InvoiceValidator:
    """Core invoice validation logic"""
    
    def __init__(self, vendor_lookup: Dict = None):
        self.vendor_lookup = vendor_lookup or {}
        self.validation_rules = self._load_validation_rules()
    
    def _load_validation_rules(self) -> Dict:
        """Load validation rules configuration"""
        return {
            'required_fields': ['invoice_number', 'vendor_code', 'invoice_amount'],
            'amount_limits': {
                'min': 0.01,
                'max': 10000000.0  # 1 Crore
            },
            'date_range': {
                'past_days': 365,
                'future_days': 30
            },
            'currency_codes': ['INR', 'USD', 'EUR', 'GBP'],
            'payment_terms': ['NET30', 'NET45', 'NET60', 'IMMEDIATE', 'COD'],
            'status_values': ['PENDING', 'APPROVED', 'REJECTED', 'PAID']
        }
    
    def validate_invoice(self, invoice_data: Dict) -> InvoiceData:
        """Validate a single invoice"""
        try:
            # Create invoice object
            invoice = InvoiceData(
                invoice_number=str(invoice_data.get('invoice_number', '')),
                vendor_code=str(invoice_data.get('vendor_code', '')),
                vendor_name=str(invoice_data.get('vendor_name', '')),
                invoice_amount=float(invoice_data.get('invoice_amount', 0)),
                currency=str(invoice_data.get('currency', 'INR')),
                payment_terms=str(invoice_data.get('payment_terms', '')),
                description=str(invoice_data.get('description', '')),
                status=str(invoice_data.get('status', 'PENDING'))
            )
            
            # Parse dates
            try:
                if invoice_data.get('invoice_date'):
                    invoice.invoice_date = self._parse_date(invoice_data['invoice_date'])
            except:
                pass
            
            try:
                if invoice_data.get('due_date'):
                    invoice.due_date = self._parse_date(invoice_data['due_date'])
            except:
                pass
            
            # Map fields using helper functions
            invoice.account_head = map_account_head(invoice.description)
            invoice.payment_method = map_payment_method(invoice_data.get('payment_method', ''))
            invoice.creator_name = get_invoice_creator_name(invoice_data.get('creator_name', ''))
            
            # Perform validation
            errors = []
            warnings = []
            
            # Required field validation
            for field in self.validation_rules['required_fields']:
                if not getattr(invoice, field, None):
                    errors.append(f"Missing required field: {field}")
            
            # Amount validation
            if invoice.invoice_amount < self.validation_rules['amount_limits']['min']:
                errors.append(f"Amount too low: {invoice.invoice_amount}")
            elif invoice.invoice_amount > self.validation_rules['amount_limits']['max']:
                warnings.append(f"High amount requires approval: {invoice.invoice_amount}")
            
            # Date validation
            if invoice.invoice_date:
                date_diff = (datetime.now().date() - invoice.invoice_date.date()).days
                if date_diff > self.validation_rules['date_range']['past_days']:
                    warnings.append(f"Invoice date is {date_diff} days old")
                elif date_diff < -self.validation_rules['date_range']['future_days']:
                    errors.append(f"Invoice date is {abs(date_diff)} days in future")
            
            # Vendor validation
            if invoice.vendor_code and invoice.vendor_code not in self.vendor_lookup:
                warnings.append(f"Vendor code not found in master: {invoice.vendor_code}")
            elif invoice.vendor_code in self.vendor_lookup:
                # Update vendor name from master data
                vendor_info = self.vendor_lookup[invoice.vendor_code]
                if vendor_info.get('vendor_name'):
                    invoice.vendor_name = vendor_info['vendor_name']
            
            # Currency validation
            if invoice.currency not in self.validation_rules['currency_codes']:
                warnings.append(f"Unsupported currency: {invoice.currency}")
            
            # Set validation results
            invoice.error_details = errors + warnings
            
            if errors:
                invoice.status = "FAILED"
                invoice.validation_result = "FAIL"
                invoice.pass_rate = 0.0
            elif warnings:
                invoice.status = "WARNING"
                invoice.validation_result = "WARNING"
                invoice.pass_rate = 0.5
            else:
                invoice.status = "PASSED"
                invoice.validation_result = "PASS"
                invoice.pass_rate = 1.0
            
            logger.debug(f"Validated invoice {invoice.invoice_number}: {invoice.validation_result}")
            return invoice
            
        except Exception as e:
            logger.error(f"Validation error for invoice {invoice_data.get('invoice_number', 'Unknown')}: {e}")
            # Return failed invoice object
            invoice = InvoiceData(
                invoice_number=str(invoice_data.get('invoice_number', 'ERROR')),
                status="FAILED",
                validation_result="FAIL",
                error_details=[f"Validation exception: {str(e)}"],
                pass_rate=0.0
            )
            return invoice
    
    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string to datetime object"""
        if not date_str:
            return None
        
        # Common date formats
        date_formats = [
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%m/%d/%Y',
            '%Y-%m-%d %H:%M:%S',
            '%d-%m-%Y',
            '%d.%m.%Y'
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(str(date_str), fmt)
            except ValueError:
                continue
        
        # If no format matches, try pandas
        try:
            return pd.to_datetime(date_str)
        except:
            raise ValueError(f"Unable to parse date: {date_str}")
    
    def validate_batch(self, invoice_list: List[Dict]) -> Tuple[List[InvoiceData], ValidationStats]:
        """Validate a batch of invoices"""
        start_time = time.time()
        
        validated_invoices = []
        stats = ValidationStats()
        
        for invoice_data in invoice_list:
            try:
                validated_invoice = self.validate_invoice(invoice_data)
                validated_invoices.append(validated_invoice)
                
                # Update statistics
                stats.total_invoices += 1
                stats.total_amount += validated_invoice.invoice_amount
                
                if validated_invoice.validation_result == "PASS":
                    stats.passed_invoices += 1
                    stats.passed_amount += validated_invoice.invoice_amount
                elif validated_invoice.validation_result == "FAIL":
                    stats.failed_invoices += 1
                    stats.failed_amount += validated_invoice.invoice_amount
                elif validated_invoice.validation_result == "WARNING":
                    stats.warning_invoices += 1
                
            except Exception as e:
                error_msg = f"Batch validation error: {e}"
                stats.errors.append(error_msg)
                logger.error(error_msg)
        
        # Calculate final metrics
        stats.processing_time = time.time() - start_time
        stats.calculate_metrics()
        
        logger.info(f"Batch validation completed: {stats.total_invoices} invoices, "
                   f"{stats.pass_rate:.1f}% pass rate, {stats.processing_time:.2f}s")
        
        return validated_invoices, stats

class ExcelReportGenerator:
    """Generate comprehensive Excel reports"""
    
    def __init__(self):
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.report_filename = f"invoice_validation_report_{self.timestamp}.xlsx"
    
    def create_report(self, invoices: List[InvoiceData], stats: ValidationStats) -> str:
        """Create comprehensive Excel report"""
        try:
            with pd.ExcelWriter(self.report_filename, engine='xlsxwriter') as writer:
                workbook = writer.book
                
                # Define formats
                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'top',
                    'fg_color': '#4F81BD',
                    'font_color': 'white',
                    'border': 1
                })
                
                pass_format = workbook.add_format({
                    'fg_color': '#C6EFCE',
                    'font_color': '#006100',
                    'border': 1
                })
                
                fail_format = workbook.add_format({
                    'fg_color': '#FFC7CE',
                    'font_color': '#9C0006',
                    'border': 1
                })
                
                warning_format = workbook.add_format({
                    'fg_color': '#FFEB9C',
                    'font_color': '#9C6500',
                    'border': 1
                })
                
                currency_format = workbook.add_format({
                    'num_format': '#,##0.00',
                    'border': 1
                })
                
                # Create summary sheet
                self._create_summary_sheet(writer, stats, header_format)
                
                # Create detailed data sheet
                self._create_detailed_sheet(writer, invoices, header_format, 
                                          pass_format, fail_format, warning_format, currency_format)
                
                # Create pivot analysis
                self._create_analysis_sheet(writer, invoices, header_format)
                
                # Create charts sheet
                self._create_charts_sheet(writer, invoices, stats)
            
            logger.info(f"Excel report generated: {self.report_filename}")
            return self.report_filename
            
        except Exception as e:
            logger.error(f"Failed to create Excel report: {e}")
            return None
    
    def _create_summary_sheet(self, writer, stats: ValidationStats, header_format):
        """Create executive summary sheet"""
        summary_data = [
            ['Metric', 'Value'],
            ['Report Generated', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
            ['Total Invoices Processed', stats.total_invoices],
            ['Passed Invoices', stats.passed_invoices],
            ['Failed Invoices', stats.failed_invoices],
            ['Warning Invoices', stats.warning_invoices],
            ['Overall Pass Rate', f"{stats.pass_rate:.1f}%"],
            ['Total Invoice Amount', f"₹{stats.total_amount:,.2f}"],
            ['Passed Amount', f"₹{stats.passed_amount:,.2f}"],
            ['Failed Amount', f"₹{stats.failed_amount:,.2f}"],
            ['Processing Time', f"{stats.processing_time:.2f} seconds"],
        ]
        
        summary_df = pd.DataFrame(summary_data[1:], columns=summary_data[0])
        summary_df.to_excel(writer, sheet_name='Executive Summary', index=False)
        
        # Format the summary sheet
        worksheet = writer.sheets['Executive Summary']
        worksheet.set_column('A:A', 25)
        worksheet.set_column('B:B', 20)
        
        # Apply header format
        for col_num, value in enumerate(summary_data[0]):
            worksheet.write(0, col_num, value, header_format)
    
    def _create_detailed_sheet(self, writer, invoices: List[InvoiceData], 
                             header_format, pass_format, fail_format, warning_format, currency_format):
        """Create detailed invoice data sheet"""
        # Convert invoices to DataFrame
        invoice_data = []
        for invoice in invoices:
            invoice_data.append({
                'Invoice Number': invoice.invoice_number,
                'Vendor Code': invoice.vendor_code,
                'Vendor Name': invoice.vendor_name,
                'Invoice Date': invoice.invoice_date.strftime('%Y-%m-%d') if invoice.invoice_date else '',
                'Invoice Amount': invoice.invoice_amount,
                'Currency': invoice.currency,
                'Payment Terms': invoice.payment_terms,
                'Due Date': invoice.due_date.strftime('%Y-%m-%d') if invoice.due_date else '',
                'Description': invoice.description,
                'Account Head': invoice.account_head,
                'Payment Method': invoice.payment_method,
                'Creator Name': invoice.creator_name,
                'Validation Status': invoice.validation_result,
                'Pass Rate': invoice.pass_rate,
                'Error Details': '; '.join(invoice.error_details) if invoice.error_details else '',
                'Processing Time': invoice.processing_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        df = pd.DataFrame(invoice_data)
        df.to_excel(writer, sheet_name='Detailed Results', index=False)
        
        # Format the detailed sheet
        worksheet = writer.sheets['Detailed Results']
        
        # Auto-adjust column widths
        for i, col in enumerate(df.columns):
            max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, min(max_len, 50))
        
        # Apply conditional formatting
        for row_num in range(1, len(df) + 1):
            status = df.iloc[row_num - 1]['Validation Status']
            if status == 'PASS':
                worksheet.set_row(row_num, cell_format=pass_format)
            elif status == 'FAIL':
                worksheet.set_row(row_num, cell_format=fail_format)
            elif status == 'WARNING':
                worksheet.set_row(row_num, cell_format=warning_format)
        
        # Apply header format
        for col_num, value in enumerate(df.columns):
            worksheet.write(0, col_num, value, header_format)
    
    def _create_analysis_sheet(self, writer, invoices: List[InvoiceData], header_format):
        """Create analysis and pivot sheet"""
        try:
            # Status analysis
            status_counts = {}
            amount_by_status = {}
            
            for invoice in invoices:
                status = invoice.validation_result
                status_counts[status] = status_counts.get(status, 0) + 1
                amount_by_status[status] = amount_by_status.get(status, 0) + invoice.invoice_amount
            
            # Create analysis DataFrame
            analysis_data = []
            for status in ['PASS', 'FAIL', 'WARNING']:
                analysis_data.append({
                    'Status': status,
                    'Count': status_counts.get(status, 0),
                    'Percentage': (status_counts.get(status, 0) / len(invoices) * 100) if invoices else 0,
                    'Total Amount': amount_by_status.get(status, 0),
                    'Average Amount': (amount_by_status.get(status, 0) / status_counts.get(status, 1)) if status_counts.get(status, 0) > 0 else 0
                })
            
            analysis_df = pd.DataFrame(analysis_data)
            analysis_df.to_excel(writer, sheet_name='Analysis', index=False, startrow=1)
            
            # Format analysis sheet
            worksheet = writer.sheets['Analysis']
            worksheet.write(0, 0, 'Status Analysis', header_format)
            
            # Auto-adjust columns
            for i, col in enumerate(analysis_df.columns):
                worksheet.set_column(i, i, 15)
            
        except Exception as e:
            logger.error(f"Failed to create analysis sheet: {e}")
    
    def _create_charts_sheet(self, writer, invoices: List[InvoiceData], stats: ValidationStats):
        """Create charts and visualizations"""
        try:
            workbook = writer.book
            worksheet = workbook.add_worksheet('Charts')
            
            # Status distribution pie chart
            chart = workbook.add_chart({'type': 'pie'})
            
            chart.add_series({
                'name': 'Validation Status',
                'categories': ['Analysis', 1, 0, 3, 0],  # Status column
                'values': ['Analysis', 1, 1, 3, 1],      # Count column
                'data_labels': {'percentage': True},
            })
            
            chart.set_title({'name': 'Invoice Validation Status Distribution'})
            chart.set_size({'width': 480, 'height': 288})
            
            worksheet.insert_chart('A1', chart)
            
        except Exception as e:
            logger.error(f"Failed to create charts: {e}")

class EmailNotificationSystem:
    """Enhanced email notification system"""
    
    def __init__(self):
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.office365.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.username = os.getenv('EMAIL_USERNAME')
        self.password = os.getenv('EMAIL_PASSWORD')
        
        # Recipients
        recipients_str = os.getenv('AP_TEAM_EMAIL_LIST', '')
        self.recipients = [email.strip() for email in recipients_str.split(',') if email.strip()]
    
    def send_validation_report(self, excel_file: str, stats: ValidationStats) -> bool:
        """Send validation report via email"""
        try:
            if not self.username or not self.password:
                logger.error("Email credentials not configured")
                return False
            
            if not self.recipients:
                logger.error("No email recipients configured")
                return False
            
            # Create email message
            msg = MIMEMultipart()
            msg['From'] = self.username
            msg['To'] = ', '.join(self.recipients)
            msg['Subject'] = f"Invoice Validation Report - {datetime.now().strftime('%Y-%m-%d')} - {stats.pass_rate:.1f}% Pass Rate"
            
            # Create HTML body
            html_body = self._create_html_body(stats)
            msg.attach(MIMEText(html_body, 'html'))
            
            # Attach Excel file
            if excel_file and os.path.exists(excel_file):
                with open(excel_file, 'rb') as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {os.path.basename(excel_file)}'
                    )
                    msg.attach(part)
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)
            
            logger.info(f"Validation report sent to {len(self.recipients)} recipients")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            return False
    
    def _create_html_body(self, stats: ValidationStats) -> str:
        """Create professional HTML email body"""
        deadline_date = datetime.now() + timedelta(days=3)
        
        return f"""
        
        
        
            
                
                    📊 Invoice Validation Report
                    Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}
                
                
                
                    📈 Summary Statistics
                    
                        
                            Total Invoices
                            {stats.total_invoices}
                        
                        
                            Passed
                            {stats.passed_invoices} ({stats.pass_rate:.1f}%)
                        
                        
                            Failed
                            {stats.failed_invoices}
                        
                        
                            Warnings
                            {stats.warning_invoices}
                        
                        
                            Total Amount
                            ₹{stats.total_amount:,.2f}
                        
                    
                
                
                
                    📎 Report Attachment
                    Please find the detailed Excel validation report attached to this email.
                
                
                
                    Koenig Solutions Pvt. Ltd. | Invoice Validation System
                    This is an automated report
                
            
        
        
        """

class InvoiceValidationSystem:
    """Main system orchestrator"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.rms_extractor = RMSDataExtractor()
        self.validator = None  # Will be initialized with vendor data
        self.excel_generator = ExcelReportGenerator()
        self.email_system = EmailNotificationSystem()
        
        logger.info("Invoice Validation System initialized")
    
    def run_validation_cycle(self) -> bool:
        """Execute complete validation cycle"""
        try:
            logger.info("Starting invoice validation cycle")
            cycle_start_time = time.time()
            
            # Step 1: Authenticate with RMS
            logger.info("Step 1: Authenticating with RMS")
            if not self.rms_extractor.authenticate():
                logger.error("RMS authentication failed - aborting validation cycle")
                return False
            
            # Step 2: Extract vendor master data
            logger.info("Step 2: Extracting vendor master data")
            vendor_lookup = self.rms_extractor.extract_vendor_data()
            
            # Initialize validator with vendor data
            self.validator = InvoiceValidator(vendor_lookup)
            
            # Step 3: Extract invoice data
            logger.info("Step 3: Extracting invoice data from RMS")
            invoice_data_list = self.rms_extractor.extract_invoice_data()
            
            if not invoice_data_list:
                logger.warning("No invoice data extracted from RMS")
                return False
            
            # Step 4: Validate invoices
            logger.info(f"Step 4: Validating {len(invoice_data_list)} invoices")
            validated_invoices, validation_stats = self.validator.validate_batch(invoice_data_list)
            
            # Step 5: Save to database
            logger.info("Step 5: Saving validation results to database")
            for invoice in validated_invoices:
                self.db_manager.save_invoice(invoice)
            
            # Save validation statistics
            run_date = datetime.now().strftime('%Y-%m-%d')
            self.db_manager.save_validation_stats(validation_stats, run_date)
            
            # Step 6: Generate Excel report
            logger.info("Step 6: Generating Excel report")
            excel_file = self.excel_generator.create_report(validated_invoices, validation_stats)
            
            # Step 7: Send email notification
            if excel_file:
                logger.info("Step 7: Sending email notification")
                self.email_system.send_validation_report(excel_file, validation_stats)
            
            # Step 8: Cleanup old data
            logger.info("Step 8: Cleaning up old data")
            cleaned_records = self.db_manager.cleanup_old_data()
            
            # Calculate total cycle time
            cycle_time = time.time() - cycle_start_time
            
            # Log final summary
            logger.info(f"Validation cycle completed successfully in {cycle_time:.2f}s")
            logger.info(f"Summary: {validation_stats.total_invoices} total, "
                       f"{validation_stats.passed_invoices} passed, "
                       f"{validation_stats.failed_invoices} failed, "
                       f"{validation_stats.warning_invoices} warnings, "
                       f"{validation_stats.pass_rate:.1f}% pass rate")
            
            return True
            
        except Exception as e:
            logger.error(f"Validation cycle failed: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def run_scheduled_validation(self):
        """Run validation on schedule"""
        logger.info("Scheduled validation triggered")
        
        try:
            success = self.run_validation_cycle()
            
            if success:
                logger.info("Scheduled validation completed successfully")
            else:
                logger.error("Scheduled validation failed")
                # Send error notification
                self._send_error_notification("Scheduled validation failed")
                
        except Exception as e:
            logger.error(f"Scheduled validation error: {e}")
            self._send_error_notification(f"Scheduled validation error: {e}")
    
    def _send_error_notification(self, error_message: str):
        """Send error notification email"""
        try:
            if not self.email_system.username or not self.email_system.recipients:
                return
            
            msg = MIMEText(f"""
            Invoice Validation System Error
            
            An error occurred during the scheduled validation process:
            
            Error: {error_message}
            Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            
            Please check the system logs for more details.
            
            Koenig Solutions Invoice Validation System
            """)
            
            msg['Subject'] = f"Invoice Validation System Error - {datetime.now().strftime('%Y-%m-%d')}"
            msg['From'] = self.email_system.username
            msg['To'] = ', '.join(self.email_system.recipients)
            
            with smtplib.SMTP(self.email_system.smtp_server, self.email_system.smtp_port) as server:
                server.starttls()
                server.login(self.email_system.username, self.email_system.password)
                server.send_message(msg)
            
            logger.info("Error notification sent")
            
        except Exception as e:
            logger.error(f"Failed to send error notification: {e}")

def setup_scheduler(validation_system: InvoiceValidationSystem):
    """Setup scheduled validation"""
    try:
        # Schedule validation every 4 days at 6 AM
        schedule.every(CONFIG['validation_interval_days']).days.at("06:00").do(
            validation_system.run_scheduled_validation
        )
        
        logger.info(f"Scheduler configured: validation every {CONFIG['validation_interval_days']} days at 6:00 AM")
        
        # Keep the scheduler running
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.error(f"Scheduler error: {e}")

def main():
    """Main entry point"""
    try:
        logger.info("=== Invoice Validation System Starting ===")
        logger.info(f"Configuration: {CONFIG}")
        
        # Create validation system
        validation_system = InvoiceValidationSystem()
        
        # Check command line arguments
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            
            if command == 'run':
                # Run single validation cycle
                logger.info("Running single validation cycle")
                success = validation_system.run_validation_cycle()
                sys.exit(0 if success else 1)
                
            elif command == 'schedule':
                # Run scheduler
                logger.info("Starting scheduled validation service")
                setup_scheduler(validation_system)
                
            elif command == 'test':
                # Test mode - validate sample data
                logger.info("Running in test mode")
                test_validation_system(validation_system)
                
            else:
                print(f"Unknown command: {command}")
                print("Usage: python main.py [run|schedule|test]")
                sys.exit(1)
        else:
            # Default: run single validation cycle
            logger.info("Running default single validation cycle")
            success = validation_system.run_validation_cycle()
            sys.exit(0 if success else 1)
            
    except Exception as e:
        logger.error(f"Main execution error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

def test_validation_system(validation_system: InvoiceValidationSystem):
    """Test the validation system with sample data"""
    logger.info("Testing validation system with sample data")
    
    # Sample invoice data
    sample_invoices = [
        {
            'invoice_number': 'INV-2024-001',
            'vendor_code': 'V001',
            'vendor_name': 'Test Vendor 1',
            'invoice_date': '2024-01-15',
            'invoice_amount': 15000.00,
            'currency': 'INR',
            'payment_terms': 'NET30',
            'description': 'Office supplies and stationery',
            'payment_method': 'Bank Transfer',
            'creator_name': 'John Doe',
            'status': 'PENDING'
        },
        {
            'invoice_number': 'INV-2024-002',
            'vendor_code': 'V002',
            'vendor_name': 'Test Vendor 2',
            'invoice_date': '2024-01-16',
            'invoice_amount': 25000.00,
            'currency': 'INR',
            'payment_terms': 'NET45',
            'description': 'Software license and maintenance',
            'payment_method': 'Credit Card',
            'creator_name': 'Jane Smith',
            'status': 'PENDING'
        },
        {
            'invoice_number': 'INV-2024-003',
            'vendor_code': '',  # Missing vendor code to trigger validation error
            'vendor_name': 'Test Vendor 3',
            'invoice_date': '2024-01-17',
            'invoice_amount': -1000.00,  # Negative amount to trigger validation error
            'currency': 'USD',
            'payment_terms': 'IMMEDIATE',
            'description': 'Travel expenses',
            'payment_method': 'Cash',
            'creator_name': 'Bob Wilson',
            'status': 'PENDING'
        }
    ]
    
    try:
        # Initialize validator with empty vendor lookup
        validation_system.validator = InvoiceValidator({})
        
        # Validate sample invoices
        validated_invoices, stats = validation_system.validator.validate_batch(sample_invoices)
        
        # Generate test report
        excel_file = validation_system.excel_generator.create_report(validated_invoices, stats)
        
        logger.info(f"Test completed: {stats.total_invoices} invoices processed")
        logger.info(f"Pass rate: {stats.pass_rate:.1f}%")
        logger.info(f"Report generated: {excel_file}")
        
        # Print detailed results
        print("\n=== Test Results ===")
        for invoice in validated_invoices:
            print(f"Invoice: {invoice.invoice_number}")
            print(f"  Status: {invoice.validation_result}")
            print(f"  Errors: {invoice.error_details}")
            print()
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()
