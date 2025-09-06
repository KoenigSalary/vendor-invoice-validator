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
from dataclasses import dataclass
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import warnings
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import queue
import asyncio
import aiohttp
import configparser
import yaml
from decimal import Decimal, ROUND_HALF_UP
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from io import StringIO, BytesIO
import base64
from urllib.parse import urljoin, urlparse
import ssl
import socket
from contextlib import contextmanager
import signal
import subprocess
from collections import defaultdict, Counter, deque
import itertools
import functools
import operator
from math import ceil, floor
import random
import string
from uuid import uuid4
import tempfile
import mimetypes
from werkzeug.utils import secure_filename

# Configuration and Constants
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# Global Configuration
CONFIG = {
    'RMS_BASE_URL': 'https://your-rms-system.com/api',
    'DATABASE_PATH': 'invoice_validation.db',
    'ARCHIVE_PATH': 'archives',
    'REPORTS_PATH': 'reports',
    'LOGS_PATH': 'logs',
    'MAX_RETRIES': 3,
    'TIMEOUT': 30,
    'BATCH_SIZE': 100,
    'VALIDATION_INTERVAL_DAYS': 4,
    'ARCHIVE_RETENTION_MONTHS': 3,
    'EMAIL_SMTP_SERVER': 'smtp.gmail.com',
    'EMAIL_SMTP_PORT': 587,
    'ENABLE_NOTIFICATIONS': True,
    'DEBUG_MODE': False,
    'PARALLEL_PROCESSING': True,
    'MAX_WORKERS': 4
}

# Environment Variables Override
for key, default_value in CONFIG.items():
    env_value = os.getenv(key)
    if env_value:
        if isinstance(default_value, bool):
            CONFIG[key] = env_value.lower() in ['true', '1', 'yes', 'on']
        elif isinstance(default_value, int):
            CONFIG[key] = int(env_value)
        else:
            CONFIG[key] = env_value

@dataclass
class InvoiceRecord:
    """Enhanced invoice record structure"""
    invoice_id: str
    invoice_number: str
    invoice_date: str
    supplier_name: str
    supplier_code: str
    amount: float
    currency: str
    status: str
    description: str
    account_head: str
    cost_center: str
    payment_method: str
    due_date: str
    created_by: str
    created_date: str
    modified_by: str
    modified_date: str
    approval_status: str
    approval_date: str
    payment_status: str
    payment_date: str
    reference_number: str
    tax_amount: float
    discount_amount: float
    net_amount: float
    department: str
    project_code: str
    gl_account: str
    vendor_id: str
    purchase_order_number: str
    receipt_number: str
    line_items: List[Dict]
    attachments: List[str]
    validation_errors: List[str]
    processing_timestamp: str
    hash_value: str

@dataclass 
class ValidationResult:
    """Validation result structure"""
    invoice_id: str
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    score: float
    timestamp: str
    processing_time: float
    validation_rules_applied: List[str]
    corrected_fields: Dict[str, Any]
    confidence_level: float

class DatabaseManager:
    """Enhanced database operations manager"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or CONFIG['DATABASE_PATH']
        self.connection = None
        self.lock = threading.Lock()
        self.initialize_database()
    
    def initialize_database(self):
        """Initialize database with enhanced schema"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''CREATE TABLE IF NOT EXISTS invoices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    invoice_id TEXT UNIQUE NOT NULL,
                    invoice_number TEXT,
                    invoice_date TEXT,
                    supplier_name TEXT,
                    supplier_code TEXT,
                    amount REAL,
                    currency TEXT,
                    status TEXT,
                    description TEXT,
                    account_head TEXT,
                    cost_center TEXT,
                    payment_method TEXT,
                    due_date TEXT,
                    created_by TEXT,
                    created_date TEXT,
                    modified_by TEXT,
                    modified_date TEXT,
                    approval_status TEXT,
                    approval_date TEXT,
                    payment_status TEXT,
                    payment_date TEXT,
                    reference_number TEXT,
                    tax_amount REAL,
                    discount_amount REAL,
                    net_amount REAL,
                    department TEXT,
                    project_code TEXT,
                    gl_account TEXT,
                    vendor_id TEXT,
                    purchase_order_number TEXT,
                    receipt_number TEXT,
                    line_items TEXT,
                    attachments TEXT,
                    processing_timestamp TEXT,
                    hash_value TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )''')
                
                conn.execute('''CREATE TABLE IF NOT EXISTS validation_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    invoice_id TEXT NOT NULL,
                    is_valid BOOLEAN,
                    errors TEXT,
                    warnings TEXT,
                    score REAL,
                    timestamp TEXT,
                    processing_time REAL,
                    validation_rules_applied TEXT,
                    corrected_fields TEXT,
                    confidence_level REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (invoice_id) REFERENCES invoices (invoice_id)
                )''')
                
                conn.execute('''CREATE TABLE IF NOT EXISTS processing_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT,
                    operation TEXT,
                    status TEXT,
                    message TEXT,
                    details TEXT,
                    timestamp TEXT,
                    processing_time REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )''')
                
                conn.execute('''CREATE TABLE IF NOT EXISTS system_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric_name TEXT,
                    metric_value TEXT,
                    category TEXT,
                    timestamp TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )''')
                
                # Create indexes for better performance
                conn.execute('CREATE INDEX IF NOT EXISTS idx_invoice_id ON invoices(invoice_id)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_invoice_date ON invoices(invoice_date)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_supplier_code ON invoices(supplier_code)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_status ON invoices(status)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_validation_invoice ON validation_results(invoice_id)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_processing_session ON processing_logs(session_id)')
                
                conn.commit()
                logging.info("Database initialized successfully")
                
        except Exception as e:
            logging.error(f"Database initialization failed: {str(e)}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Thread-safe database connection context manager"""
        with self.lock:
            try:
                conn = sqlite3.connect(self.db_path)
                conn.row_factory = sqlite3.Row
                yield conn
            finally:
                conn.close()
    
    def insert_invoice(self, invoice: InvoiceRecord) -> bool:
        """Insert or update invoice record"""
        try:
            with self.get_connection() as conn:
                # Convert complex fields to JSON
                line_items_json = json.dumps(invoice.line_items) if invoice.line_items else '[]'
                attachments_json = json.dumps(invoice.attachments) if invoice.attachments else '[]'
                validation_errors_json = json.dumps(invoice.validation_errors) if invoice.validation_errors else '[]'
                
                conn.execute('''INSERT OR REPLACE INTO invoices (
                    invoice_id, invoice_number, invoice_date, supplier_name, supplier_code,
                    amount, currency, status, description, account_head, cost_center,
                    payment_method, due_date, created_by, created_date, modified_by,
                    modified_date, approval_status, approval_date, payment_status,
                    payment_date, reference_number, tax_amount, discount_amount,
                    net_amount, department, project_code, gl_account, vendor_id,
                    purchase_order_number, receipt_number, line_items, attachments,
                    processing_timestamp, hash_value, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)''',
                (invoice.invoice_id, invoice.invoice_number, invoice.invoice_date,
                 invoice.supplier_name, invoice.supplier_code, invoice.amount,
                 invoice.currency, invoice.status, invoice.description,
                 invoice.account_head, invoice.cost_center, invoice.payment_method,
                 invoice.due_date, invoice.created_by, invoice.created_date,
                 invoice.modified_by, invoice.modified_date, invoice.approval_status,
                 invoice.approval_date, invoice.payment_status, invoice.payment_date,
                 invoice.reference_number, invoice.tax_amount, invoice.discount_amount,
                 invoice.net_amount, invoice.department, invoice.project_code,
                 invoice.gl_account, invoice.vendor_id, invoice.purchase_order_number,
                 invoice.receipt_number, line_items_json, attachments_json,
                 invoice.processing_timestamp, invoice.hash_value))
                
                conn.commit()
                return True
        except Exception as e:
            logging.error(f"Failed to insert invoice {invoice.invoice_id}: {str(e)}")
            return False
    
    def insert_validation_result(self, result: ValidationResult) -> bool:
        """Insert validation result"""
        try:
            with self.get_connection() as conn:
                errors_json = json.dumps(result.errors) if result.errors else '[]'
                warnings_json = json.dumps(result.warnings) if result.warnings else '[]'
                rules_json = json.dumps(result.validation_rules_applied) if result.validation_rules_applied else '[]'
                corrected_json = json.dumps(result.corrected_fields) if result.corrected_fields else '{}'
                
                conn.execute('''INSERT INTO validation_results (
                    invoice_id, is_valid, errors, warnings, score, timestamp,
                    processing_time, validation_rules_applied, corrected_fields, confidence_level
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (result.invoice_id, result.is_valid, errors_json, warnings_json,
                 result.score, result.timestamp, result.processing_time,
                 rules_json, corrected_json, result.confidence_level))
                
                conn.commit()
                return True
        except Exception as e:
            logging.error(f"Failed to insert validation result for {result.invoice_id}: {str(e)}")
            return False
    
    def get_invoices(self, limit: int = None, offset: int = 0, filters: Dict = None) -> List[Dict]:
        """Retrieve invoices with optional filtering"""
        try:
            with self.get_connection() as conn:
                query = "SELECT * FROM invoices"
                params = []
                
                if filters:
                    conditions = []
                    for key, value in filters.items():
                        if key == 'date_from':
                            conditions.append("invoice_date >= ?")
                            params.append(value)
                        elif key == 'date_to':
                            conditions.append("invoice_date <= ?")
                            params.append(value)
                        elif key == 'status':
                            conditions.append("status = ?")
                            params.append(value)
                        elif key == 'supplier_code':
                            conditions.append("supplier_code = ?")
                            params.append(value)
                        elif key == 'amount_min':
                            conditions.append("amount >= ?")
                            params.append(value)
                        elif key == 'amount_max':
                            conditions.append("amount <= ?")
                            params.append(value)
                    
                    if conditions:
                        query += " WHERE " + " AND ".join(conditions)
                
                query += " ORDER BY created_at DESC"
                
                if limit:
                    query += f" LIMIT {limit} OFFSET {offset}"
                
                cursor = conn.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Failed to retrieve invoices: {str(e)}")
            return []
    
    def get_validation_summary(self) -> Dict:
        """Get validation summary statistics"""
        try:
            with self.get_connection() as conn:
                # Total invoices
                total_count = conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
                
                # Validation results summary
                valid_count = conn.execute(
                    "SELECT COUNT(*) FROM validation_results WHERE is_valid = 1"
                ).fetchone()[0]
                
                invalid_count = conn.execute(
                    "SELECT COUNT(*) FROM validation_results WHERE is_valid = 0"
                ).fetchone()[0]
                
                # Average processing time
                avg_processing_time = conn.execute(
                    "SELECT AVG(processing_time) FROM validation_results"
                ).fetchone()[0] or 0
                
                # Most common errors
                cursor = conn.execute('''
                    SELECT errors, COUNT(*) as count 
                    FROM validation_results 
                    WHERE errors != '[]' 
                    GROUP BY errors 
                    ORDER BY count DESC 
                    LIMIT 10
                ''')
                common_errors = [dict(row) for row in cursor.fetchall()]
                
                return {
                    'total_invoices': total_count,
                    'valid_invoices': valid_count,
                    'invalid_invoices': invalid_count,
                    'validation_rate': (valid_count / total_count * 100) if total_count > 0 else 0,
                    'average_processing_time': round(avg_processing_time, 2),
                    'common_errors': common_errors
                }
        except Exception as e:
            logging.error(f"Failed to generate validation summary: {str(e)}")
            return {}
    
    def cleanup_old_records(self, retention_days: int = 90):
        """Clean up old records based on retention policy"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=retention_days)).isoformat()
            
            with self.get_connection() as conn:
                # Archive old records before deletion
                old_records = conn.execute(
                    "SELECT * FROM invoices WHERE created_at < ?", (cutoff_date,)
                ).fetchall()
                
                if old_records:
                    # Create archive
                    archive_path = Path(CONFIG['ARCHIVE_PATH'])
                    archive_path.mkdir(exist_ok=True)
                    
                    archive_file = archive_path / f"archived_invoices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    with open(archive_file, 'w') as f:
                        json.dump([dict(row) for row in old_records], f, indent=2)
                    
                    # Delete old records
                    conn.execute("DELETE FROM invoices WHERE created_at < ?", (cutoff_date,))
                    conn.execute("DELETE FROM validation_results WHERE timestamp < ?", (cutoff_date,))
                    conn.execute("DELETE FROM processing_logs WHERE created_at < ?", (cutoff_date,))
                    
                    conn.commit()
                    logging.info(f"Archived and cleaned up {len(old_records)} old records")
                
        except Exception as e:
            logging.error(f"Failed to cleanup old records: {str(e)}")

class RMSDataExtractor:
    """Enhanced RMS data extraction and processing"""
    
    def __init__(self):
        self.base_url = CONFIG['RMS_BASE_URL']
        self.session = requests.Session()
        self.session.timeout = CONFIG['TIMEOUT']
        self.max_retries = CONFIG['MAX_RETRIES']
        self.batch_size = CONFIG['BATCH_SIZE']
        
        # Setup session headers
        self.session.headers.update({
            'User-Agent': 'InvoiceValidationSystem/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
    
    def authenticate(self, username: str = None, password: str = None, api_key: str = None) -> bool:
        """Authenticate with RMS system"""
        try:
            auth_url = urljoin(self.base_url, '/auth/login')
            
            if api_key:
                self.session.headers['Authorization'] = f'Bearer {api_key}'
                # Test authentication
                test_response = self.session.get(urljoin(self.base_url, '/auth/verify'))
                return test_response.status_code == 200
            
            elif username and password:
                auth_data = {
                    'username': username,
                    'password': password
                }
                response = self.session.post(auth_url, json=auth_data)
                
                if response.status_code == 200:
                    auth_result = response.json()
                    if 'token' in auth_result:
                        self.session.headers['Authorization'] = f'Bearer {auth_result["token"]}'
                        return True
            
            return False
            
        except Exception as e:
            logging.error(f"Authentication failed: {str(e)}")
            return False
    
    def extract_invoice_data(self, date_from: str = None, date_to: str = None, 
                           batch_size: int = None) -> List[Dict]:
        """Extract invoice data from RMS system"""
        try:
            batch_size = batch_size or self.batch_size
            all_invoices = []
            offset = 0
            
            # Set default date range if not provided
            if not date_from:
                date_from = (datetime.now() - timedelta(days=CONFIG['VALIDATION_INTERVAL_DAYS'])).strftime('%Y-%m-%d')
            if not date_to:
                date_to = datetime.now().strftime('%Y-%m-%d')
            
            while True:
                # Build request parameters
                params = {
                    'date_from': date_from,
                    'date_to': date_to,
                    'limit': batch_size,
                    'offset': offset,
                    'include_details': True,
                    'include_line_items': True,
                    'include_attachments': True
                }
                
                # Make request with retry logic
                response = self._make_request_with_retry(
                    'GET', urljoin(self.base_url, '/invoices'), params=params
                )
                
                if not response or response.status_code != 200:
                    break
                
                data = response.json()
                invoices = data.get('invoices', [])
                
                if not invoices:
                    break
                
                # Process and validate invoice data
                processed_invoices = self._process_invoice_batch(invoices)
                all_invoices.extend(processed_invoices)
                
                offset += batch_size
                
                # Check if we've retrieved all available records
                if len(invoices) < batch_size:
                    break
                
                # Prevent infinite loops
                if offset > 10000:  # Reasonable limit
                    logging.warning("Reached maximum extraction limit")
                    break
            
            logging.info(f"Extracted {len(all_invoices)} invoices from RMS system")
            return all_invoices
            
        except Exception as e:
            logging.error(f"Failed to extract invoice data: {str(e)}")
            return []
    
    def _make_request_with_retry(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """Make HTTP request with retry logic"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.request(method, url, **kwargs)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Rate limit
                    wait_time = 2 ** attempt
                    logging.warning(f"Rate limited, waiting {wait_time} seconds")
                    time.sleep(wait_time)
                    continue
                elif response.status_code in [401, 403]:  # Auth issues
                    logging.error("Authentication failed, attempting to re-authenticate")
                    # Re-authentication logic would go here
                    return None
                else:
                    logging.warning(f"Request failed with status {response.status_code}")
                    return response
                    
            except requests.exceptions.RequestException as e:
                logging.warning(f"Request attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logging.error("All retry attempts failed")
        
        return None
    
    def _process_invoice_batch(self, invoices: List[Dict]) -> List[InvoiceRecord]:
        """Process and validate a batch of invoice data"""
        processed_invoices = []
        
        for invoice_data in invoices:
            try:
                # Create InvoiceRecord with data validation and cleaning
                invoice = InvoiceRecord(
                    invoice_id=str(invoice_data.get('id', '')),
                    invoice_number=str(invoice_data.get('invoice_number', '')),
                    invoice_date=self._normalize_date(invoice_data.get('invoice_date')),
                    supplier_name=str(invoice_data.get('supplier_name', '')),
                    supplier_code=str(invoice_data.get('supplier_code', '')),
                    amount=self._safe_float(invoice_data.get('amount', 0)),
                    currency=str(invoice_data.get('currency', 'USD')),
                    status=str(invoice_data.get('status', 'Unknown')),
                    description=str(invoice_data.get('description', '')),
                    account_head=map_account_head(invoice_data.get('description', '')),
                    cost_center=str(invoice_data.get('cost_center', '')),
                    payment_method=map_payment_method(invoice_data.get('payment_method', '')),
                    due_date=self._normalize_date(invoice_data.get('due_date')),
                    created_by=get_invoice_creator_name(invoice_data.get('created_by', '')),
                    created_date=self._normalize_date(invoice_data.get('created_date')),
                    modified_by=str(invoice_data.get('modified_by', '')),
                    modified_date=self._normalize_date(invoice_data.get('modified_date')),
                    approval_status=str(invoice_data.get('approval_status', 'Pending')),
                    approval_date=self._normalize_date(invoice_data.get('approval_date')),
                    payment_status=str(invoice_data.get('payment_status', 'Unpaid')),
                    payment_date=self._normalize_date(invoice_data.get('payment_date')),
                    reference_number=str(invoice_data.get('reference_number', '')),
                    tax_amount=self._safe_float(invoice_data.get('tax_amount', 0)),
                    discount_amount=self._safe_float(invoice_data.get('discount_amount', 0)),
                    net_amount=self._safe_float(invoice_data.get('net_amount', 0)),
                    department=str(invoice_data.get('department', '')),
                    project_code=str(invoice_data.get('project_code', '')),
                    gl_account=str(invoice_data.get('gl_account', '')),
                    vendor_id=str(invoice_data.get('vendor_id', '')),
                    purchase_order_number=str(invoice_data.get('purchase_order_number', '')),
                    receipt_number=str(invoice_data.get('receipt_number', '')),
                    line_items=invoice_data.get('line_items', []),
                    attachments=invoice_data.get('attachments', []),
                    validation_errors=[],
                    processing_timestamp=datetime.now().isoformat(),
                    hash_value=self._generate_hash(invoice_data)
                )
                
                processed_invoices.append(invoice)
                
            except Exception as e:
                logging.error(f"Failed to process invoice {invoice_data.get('id', 'unknown')}: {str(e)}")
                continue
        
        return processed_invoices
    
    def _normalize_date(self, date_value: Any) -> str:
        """Normalize various date formats to ISO format"""
        if not date_value:
            return ''
        
        if isinstance(date_value, str):
            # Try common date formats
            date_formats = [
                '%Y-%m-%d',
                '%Y-%m-%d %H:%M:%S',
                '%d/%m/%Y',
                '%m/%d/%Y',
                '%d-%m-%Y',
                '%Y/%m/%d'
            ]
            
            for fmt in date_formats:
                try:
                    return datetime.strptime(date_value, fmt).strftime('%Y-%m-%d')
                except ValueError:
                    continue
        
        elif isinstance(date_value, (datetime, date)):
            return date_value.strftime('%Y-%m-%d')
        
        return str(date_value)
    
    def _safe_float(self, value: Any) -> float:
        """Safely convert value to float"""
        try:
            if value is None or value == '':
                return 0.0
            return float(str(value).replace(',', '').replace('$', ''))
        except (ValueError, TypeError):
            return 0.0
    
    def _generate_hash(self, data: Dict) -> str:
        """Generate hash for invoice data integrity"""
        # Create a string representation of key fields
        key_fields = [
            str(data.get('id', '')),
            str(data.get('invoice_number', '')),
            str(data.get('amount', '')),
            str(data.get('supplier_code', ''))
        ]
        
        hash_input = '|'.join(key_fields).encode('utf-8')
        return hashlib.md5(hash_input).hexdigest()

def map_account_head(description: str) -> str:
    """Map transaction descriptions to appropriate account heads"""
    if not description:
        return "Miscellaneous"
    
    description = str(description).lower().strip()
    
    # Comprehensive account head mapping
    mappings = {
        # Operating Expenses
        'rent': 'Rent Expense',
        'lease': 'Rent Expense',
        'rental': 'Rent Expense',
        
        # Personnel Costs
        'salary': 'Salary Expense',
        'wage': 'Salary Expense',
        'payroll': 'Salary Expense',
        'bonus': 'Salary Expense',
        'commission': 'Commission Expense',
        
        # Utilities
        'electricity': 'Utilities Expense',
        'water': 'Utilities Expense',
        'gas': 'Utilities Expense',
        'utilities': 'Utilities Expense',
        
        # Office Operations
        'office supplies': 'Office Supplies',
        'stationery': 'Office Supplies',
        'supplies': 'Office Supplies',
        'printing': 'Office Supplies',
        
        # Travel & Transport
        'travel': 'Travel Expense',
        'flight': 'Travel Expense',
        'hotel': 'Travel Expense',
        'taxi': 'Travel Expense',
        'fuel': 'Fuel Expense',
        'petrol': 'Fuel Expense',
        'diesel': 'Fuel Expense',
        
        # Technology
        'software': 'Software Expense',
        'license': 'Software Expense',
        'subscription': 'Subscription Expense',
        'internet': 'Internet Expense',
        'telephone': 'Telephone Expense',
        'mobile': 'Telephone Expense',
        
        # Maintenance
        'maintenance': 'Maintenance Expense',
        'repair': 'Repair Expense',
        'service': 'Maintenance Expense',
        
        # Insurance
        'insurance': 'Insurance Expense',
        'premium': 'Insurance Expense',
        
        # Professional Services
        'legal': 'Legal Expense',
        'accounting': 'Accounting Expense',
        'audit': 'Accounting Expense',
        'consulting': 'Consulting Expense',
        'advisory': 'Consulting Expense',
        
        # Marketing & Sales
        'marketing': 'Marketing Expense',
        'advertising': 'Advertising Expense',
        'promotion': 'Marketing Expense',
        'branding': 'Marketing Expense',
        
        # Training & Development
        'training': 'Training Expense',
        'education': 'Training Expense',
        'course': 'Training Expense',
        'seminar': 'Training Expense',
        
        # Entertainment
        'meals': 'Meals & Entertainment',
        'entertainment': 'Meals & Entertainment',
        'dinner': 'Meals & Entertainment',
        'lunch': 'Meals & Entertainment',
        
        # Banking & Finance
        'bank': 'Bank Charges',
        'fee': 'Bank Charges',
        'charge': 'Bank Charges',
        'interest': 'Interest Expense',
        'loan': 'Interest Expense',
        
        # Taxes
        'tax': 'Tax Expense',
        'vat': 'Tax Expense',
        'gst': 'Tax Expense',
        
        # Assets
        'equipment': 'Equipment Expense',
        'furniture': 'Equipment Expense',
        'computer': 'Equipment Expense',
        'laptop': 'Equipment Expense',
        
        # Other
        'depreciation': 'Depreciation',
        'amortization': 'Amortization'
    }
    
    # Find matching category
    for keyword, account_head in mappings.items():
        if keyword in description:
            return account_head
    
    # Additional pattern matching for complex descriptions
    if any(word in description for word in ['purchase', 'buy', 'procurement']):
        return 'Purchases'
    elif any(word in description for word in ['sell', 'sale', 'revenue']):
        return 'Sales Revenue'
    elif any(word in description for word in ['refund', 'return']):
        return 'Refunds'
    
    return "Miscellaneous"

def map_payment_method(payment_info: Any) -> str:
    """Standardize payment method information"""
    if not payment_info:
        return "Cash"
    
    payment_str = str(payment_info).lower().strip()
    
    # Payment method mappings
    if any(term in payment_str for term in ['credit card', 'creditcard', 'cc', 'visa', 'mastercard', 'amex']):
        return "Credit Card"
    elif any(term in payment_str for term in ['debit card', 'debitcard', 'debit']):
        return "Debit Card"
    elif any(term in payment_str for term in ['bank transfer', 'wire transfer', 'eft', 'ach', 'neft', 'rtgs']):
        return "Bank Transfer"
    elif any(term in payment_str for term in ['cheque', 'check']):
        return "Cheque"
    elif any(term in payment_str for term in ['online', 'digital', 'upi', 'paytm', 'gpay', 'paypal']):
        return "Online Payment"
    elif any(term in payment_str for term in ['cash']):
        return "Cash"
    else:
        return "Other"

def get_invoice_creator_name(creator_info: Any) -> str:
    """Extract and standardize invoice creator name"""
    if not creator_info:
        return "System Generated"
    
    creator_str = str(creator_info).strip()
    
    # Clean up common prefixes/suffixes
    creator_str = creator_str.replace("Created by:", "").strip()
    creator_str = creator_str.replace("User:", "").strip()
    creator_str = creator_str.replace("By:", "").strip()
    
    # Handle special cases
    if creator_str.lower() in ['', 'n/a', 'na', 'null', 'none', 'unknown']:
        return "System Generated"
    elif creator_str.lower() in ['admin', 'administrator']:
        return "Administrator"
    elif creator_str.lower() in ['system', 'auto', 'automatic']:
        return "System Generated"
    elif len(creator_str) > 50:  # Truncate very long names
        return creator_str[:50] + "..."
    else:
        return creator_str

class InvoiceValidator:
    """Enhanced invoice validation with comprehensive rules"""
    
    def __init__(self):
        self.validation_rules = self._initialize_validation_rules()
        self.field_validators = self._initialize_field_validators()
        
    def _initialize_validation_rules(self) -> Dict:
        """Initialize validation rules configuration"""
        return {
            'required_fields': [
                'invoice_id', 'invoice_number', 'supplier_name', 
                'amount', 'invoice_date'
            ],
            'numeric_fields': [
                'amount', 'tax_amount', 'discount_amount', 'net_amount'
            ],
            'date_fields': [
                'invoice_date', 'due_date', 'created_date', 'approval_date', 'payment_date'
            ],
            'amount_thresholds': {
                'min_amount': 0.01,
                'max_amount': 1000000.00,
                'warning_amount': 50000.00
            },
            'date_validations': {
                'max_future_days': 30,
                'max_past_days': 365
            },
            'text_field_limits': {
                'invoice_number': 50,
                'supplier_name': 100,
                'description': 500,
                'reference_number': 50
            }
        }
    
    def _initialize_field_validators(self) -> Dict:
        """Initialize field-specific validators"""
        return {
            'invoice_number': self._validate_invoice_number,
            'amount': self._validate_amount,
            'invoice_date': self._validate_date,
            'supplier_code': self._validate_supplier_code,
            'currency': self._validate_currency,
            'status': self._validate_status,
            'email': self._validate_email,
            'phone': self._validate_phone
        }
    
    def validate_invoice(self, invoice: InvoiceRecord) -> ValidationResult:
        """Comprehensive invoice validation"""
        start_time = time.time()
        errors = []
        warnings = []
        corrected_fields = {}
        validation_rules_applied = []
        
        try:
            # Required field validation
            missing_fields = self._validate_required_fields(invoice)
            if missing_fields:
                errors.extend([f"Missing required field: {field}" for field in missing_fields])
                validation_rules_applied.append('required_fields')
            
            # Data type validation
            type_errors = self._validate_data_types(invoice)
            if type_errors:
                errors.extend(type_errors)
                validation_rules_applied.append('data_types')
            
            # Business logic validation
            business_errors, business_warnings = self._validate_business_logic(invoice)
            errors.extend(business_errors)
            warnings.extend(business_warnings)
            if business_errors or business_warnings:
                validation_rules_applied.append('business_logic')
            
            # Field-specific validation
            field_errors, field_corrections = self._validate_specific_fields(invoice)
            errors.extend(field_errors)
            corrected_fields.update(field_corrections)
            if field_errors:
                validation_rules_applied.append('field_specific')
            
            # Cross-field validation
            cross_errors = self._validate_cross_fields(invoice)
            errors.extend(cross_errors)
            if cross_errors:
                validation_rules_applied.append('cross_field')
            
            # Calculate validation score
            score = self._calculate_validation_score(invoice, errors, warnings)
            
            # Calculate confidence level
            confidence = self._calculate_confidence_level(invoice, errors, warnings)
            
            processing_time = time.time() - start_time
            
            return ValidationResult(
                invoice_id=invoice.invoice_id,
                is_valid=len(errors) == 0,
                errors=errors,
                warnings=warnings,
                score=score,
                timestamp=datetime.now().isoformat(),
                processing_time=processing_time,
                validation_rules_applied=validation_rules_applied,
                corrected_fields=corrected_fields,
                confidence_level=confidence
            )
            
        except Exception as e:
            logging.error(f"Validation failed for invoice {invoice.invoice_id}: {str(e)}")
            return ValidationResult(
                invoice_id=invoice.invoice_id,
                is_valid=False,
                errors=[f"Validation system error: {str(e)}"],
                warnings=[],
                score=0.0,
                timestamp=datetime.now().isoformat(),
                processing_time=time.time() - start_time,
                validation_rules_applied=['error_handling'],
                corrected_fields={},
                confidence_level=0.0
            )
    
    def _validate_required_fields(self, invoice: InvoiceRecord) -> List[str]:
        """Validate required fields are present and not empty"""
        missing_fields = []
        
        for field in self.validation_rules['required_fields']:
            value = getattr(invoice, field, None)
            if not value or (isinstance(value, str) and value.strip() == ''):
                missing_fields.append(field)
        
        return missing_fields
    
    def _validate_data_types(self, invoice: InvoiceRecord) -> List[str]:
        """Validate data types for numeric and date fields"""
        errors = []
        
        # Numeric field validation
        for field in self.validation_rules['numeric_fields']:
            value = getattr(invoice, field, None)
            if value is not None:
                try:
                    float_value = float(value)
                    if not isinstance(float_value, (int, float)) or math.isnan(float_value):
                        errors.append(f"Invalid numeric value for {field}: {value}")
                except (ValueError, TypeError):
                    errors.append(f"Invalid numeric format for {field}: {value}")
        
        # Date field validation
        for field in self.validation_rules['date_fields']:
            value = getattr(invoice, field, None)
            if value and value.strip():
                if not self._is_valid_date(value):
                    errors.append(f"Invalid date format for {field}: {value}")
        
        return errors
    
    def _validate_business_logic(self, invoice: InvoiceRecord) -> Tuple[List[str], List[str]]:
        """Validate business logic rules"""
        errors = []
        warnings = []
        
        # Amount validations
        amount = invoice.amount
        if amount is not None:
            min_amount = self.validation_rules['amount_thresholds']['min_amount']
            max_amount = self.validation_rules['amount_thresholds']['max_amount']
            warning_amount = self.validation_rules['amount_thresholds']['warning_amount']
            
            if amount < min_amount:
                errors.append(f"Amount too low: {amount} (minimum: {min_amount})")
            elif amount > max_amount:
                errors.append(f"Amount too high: {amount} (maximum: {max_amount})")
            elif amount > warning_amount:
                warnings.append(f"High amount detected: {amount}")
        
        # Date logic validations
        if invoice.invoice_date:
            invoice_date = self._parse_date(invoice.invoice_date)
            if invoice_date:
                today = datetime.now().date()
                max_future = self.validation_rules['date_validations']['max_future_days']
                max_past = self.validation_rules['date_validations']['max_past_days']
                
                if invoice_date > today + timedelta(days=max_future):
                    errors.append(f"Invoice date too far in future: {invoice.invoice_date}")
                elif invoice_date < today - timedelta(days=max_past):
                    warnings.append(f"Invoice date is very old: {invoice.invoice_date}")
        
        # Due date logic
        if invoice.due_date and invoice.invoice_date:
            due_date = self._parse_date(invoice.due_date)
            invoice_date = self._parse_date(invoice.invoice_date)
            
            if due_date and invoice_date and due_date < invoice_date:
                errors.append("Due date cannot be before invoice date")
        
        # Amount consistency
        if all(x is not None for x in [invoice.amount, invoice.tax_amount, invoice.discount_amount, invoice.net_amount]):
            calculated_net = invoice.amount + invoice.tax_amount - invoice.discount_amount
            if abs(calculated_net - invoice.net_amount) > 0.01:
                warnings.append(f"Net amount calculation mismatch: expected {calculated_net}, got {invoice.net_amount}")
        
        return errors, warnings
    
    def _validate_specific_fields(self, invoice: InvoiceRecord) -> Tuple[List[str], Dict]:
        """Validate specific field formats and values"""
        errors = []
        corrections = {}
        
        # Invoice number validation
        if invoice.invoice_number:
            if not self._validate_invoice_number(invoice.invoice_number):
                errors.append(f"Invalid invoice number format: {invoice.invoice_number}")
        
        # Currency validation
        if invoice.currency:
            valid_currencies = ['USD', 'EUR', 'GBP', 'INR', 'CAD', 'AUD', 'JPY']
            if invoice.currency.upper() not in valid_currencies:
                warnings = getattr(self, '_current_warnings', [])
                warnings.append(f"Unusual currency code: {invoice.currency}")
                self._current_warnings = warnings
        
        # Status validation
        if invoice.status:
            valid_statuses = ['Draft', 'Pending', 'Approved', 'Rejected', 'Paid', 'Cancelled']
            if invoice.status not in valid_statuses:
                corrections['status'] = 'Pending'
        
        # Text field length validation
        for field, max_length in self.validation_rules['text_field_limits'].items():
            value = getattr(invoice, field, '')
            if value and len(str(value)) > max_length:
                errors.append(f"{field} exceeds maximum length of {max_length} characters")
        
        return errors, corrections
    
    def _validate_cross_fields(self, invoice: InvoiceRecord) -> List[str]:
        """Validate relationships between fields"""
        errors = []
        
        # Payment status vs payment date
        if invoice.payment_status == 'Paid' and not invoice.payment_date:
            errors.append("Payment date required when payment status is 'Paid'")
        
        # Approval status vs approval date
        if invoice.approval_status == 'Approved' and not invoice.approval_date:
            errors.append("Approval date required when approval status is 'Approved'")
        
        # Supplier consistency
        if invoice.supplier_name and invoice.supplier_code:
            # This would typically check against a master supplier database
            pass
        
        return errors
    
    def _validate_invoice_number(self, invoice_number: str) -> bool:
        """Validate invoice number format"""
        if not invoice_number:
            return False
        
        # Common invoice number patterns
        patterns = [
            r'^INV-\d{4,}$',  # INV-1234
            r'^\d{4,}$',      # 1234
            r'^[A-Z]{2,}-\d{4,}$',  # AB-1234
            r'^[A-Z]+\d{4,}$',      # ABC1234
        ]
        
        return any(re.match(pattern, invoice_number) for pattern in patterns)
    
    def _validate_amount(self, amount: Any) -> bool:
        """Validate amount value"""
        try:
            float_amount = float(amount)
            return float_amount >= 0 and not math.isnan(float_amount)
        except (ValueError, TypeError):
            return False
    
    def _validate_date(self, date_value: str) -> bool:
        """Validate date format"""
        return self._is_valid_date(date_value)
    
    def _validate_supplier_code(self, supplier_code: str) -> bool:
        """Validate supplier code format"""
        if not supplier_code:
            return False
        
        # Typical supplier code patterns
        patterns = [
            r'^[A-Z]{2,6}\d{2,}$',  # ABC123
            r'^\d{4,}$',            # 1234
            r'^[A-Z]{3}-\d{3}$',    # ABC-123
        ]
        
        return any(re.match(pattern, supplier_code) for pattern in patterns)
    
    def _validate_currency(self, currency: str) -> bool:
        """Validate currency code"""
        valid_currencies = ['USD', 'EUR', 'GBP', 'INR', 'CAD', 'AUD', 'JPY', 'CHF', 'CNY']
        return currency and currency.upper() in valid_currencies
    
    def _validate_status(self, status: str) -> bool:
        """Validate status value"""
        valid_statuses = ['Draft', 'Pending', 'Approved', 'Rejected', 'Paid', 'Cancelled', 'Processing']
        return status in valid_statuses
    
    def _validate_email(self, email: str) -> bool:
        """Validate email format"""
        if not email:
            return True  # Email might be optional
        
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(email_pattern, email) is not None
    
    def _validate_phone(self, phone: str) -> bool:
        """Validate phone number format"""
        if not phone:
            return True  # Phone might be optional
        
        # Remove common formatting
        clean_phone = re.sub(r'[^\d+]', '', phone)
        
        # Basic phone validation (10-15 digits, optional + prefix)
        phone_pattern = r'^\+?\d{10,15}$'
        return re.match(phone_pattern, clean_phone) is not None
    
    def _is_valid_date(self, date_str: str) -> bool:
        """Check if date string is in valid format"""
        if not date_str:
            return False
        
        date_formats = [
            '%Y-%m-%d',
            '%Y-%m-%d %H:%M:%S',
            '%d/%m/%Y',
            '%m/%d/%Y',
            '%d-%m-%Y',
            '%Y/%m/%d'
        ]
        
        for fmt in date_formats:
            try:
                datetime.strptime(date_str, fmt)
                return True
            except ValueError:
                continue
        
        return False
    
    def _parse_date(self, date_str: str) -> Optional[date]:
        """Parse date string to date object"""
        if not date_str:
            return None
        
        date_formats = [
            '%Y-%m-%d',
            '%Y-%m-%d %H:%M:%S',
            '%d/%m/%Y',
            '%m/%d/%Y',
            '%d-%m-%Y',
            '%Y/%m/%d'
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        return None
    
    def _calculate_validation_score(self, invoice: InvoiceRecord, errors: List[str], warnings: List[str]) -> float:
        """Calculate validation score (0-100)"""
        total_checks = 20  # Total number of validation checks
        error_weight = 2
        warning_weight = 1
        
        penalty = len(errors) * error_weight + len(warnings) * warning_weight
        score = max(0, (total_checks - penalty) / total_checks * 100)
        
        return round(score, 2)
    
    def _calculate_confidence_level(self, invoice: InvoiceRecord, errors: List[str], warnings: List[str]) -> float:
        """Calculate confidence level in validation result"""
        base_confidence = 100.0
        
        # Reduce confidence based on missing data
        required_fields = self.validation_rules['required_fields']
        missing_count = sum(1 for field in required_fields if not getattr(invoice, field, None))
        confidence = base_confidence - (missing_count * 15)
        
        # Reduce confidence based on errors and warnings
        confidence -= len(errors) * 10
        confidence -= len(warnings) * 5
        
        return max(0.0, min(100.0, confidence))

class ReportGenerator:
    """Enhanced report generation and analytics"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.reports_path = Path(CONFIG['REPORTS_PATH'])
        self.reports_path.mkdir(exist_ok=True)
    
    def generate_validation_report(self, date_from: str = None, date_to: str = None) -> Dict:
        """Generate comprehensive validation report"""
        try:
            # Set default date range
            if not date_from:
                date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            if not date_to:
                date_to = datetime.now().strftime('%Y-%m-%d')
            
            # Get validation summary
            summary = self.db_manager.get_validation_summary()
            
            # Get detailed invoice data
            filters = {'date_from': date_from, 'date_to': date_to}
            invoices = self.db_manager.get_invoices(filters=filters)
            
            # Generate analytics
            analytics = self._generate_analytics(invoices)
            
            # Create report data
            report_data = {
                'report_metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'date_range': {'from': date_from, 'to': date_to},
                    'total_invoices': len(invoices)
                },
                'validation_summary': summary,
                'analytics': analytics,
                'detailed_results': self._format_detailed_results(invoices)
            }
            
            # Save report
            report_filename = f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            report_path = self.reports_path / report_filename
            
            with open(report_path, 'w') as f:
                json.dump(report_data, f, indent=2, default=str)
            
            logging.info(f"Validation report generated: {report_path}")
            return report_data
            
        except Exception as e:
            logging.error(f"Failed to generate validation report: {str(e)}")
            return {}
    
    def generate_dashboard_data(self) -> Dict:
        """Generate data for dashboard visualization"""
        try:
            # Get recent data
            date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            filters = {'date_from': date_from}
            invoices = self.db_manager.get_invoices(filters=filters)
            
            # Process data for visualization
            dashboard_data = {
                'summary_metrics': self._calculate_summary_metrics(invoices),
                'validation_trends': self._calculate_validation_trends(invoices),
                'supplier_analysis': self._analyze_suppliers(invoices),
                'amount_distribution': self._analyze_amount_distribution(invoices),
                'error_patterns': self._analyze_error_patterns(invoices),
                'processing_performance': self._analyze_processing_performance()
            }
            
            return dashboard_data
            
        except Exception as e:
            logging.error(f"Failed to generate dashboard data: {str(e)}")
            return {}
    
    def _generate_analytics(self, invoices: List[Dict]) -> Dict:
        """Generate analytics from invoice data"""
        if not invoices:
            return {}
        
        df = pd.DataFrame(invoices)
        
        analytics = {
            'amount_statistics': {
                'total_amount': df['amount'].sum(),
                'average_amount': df['amount'].mean(),
                'median_amount': df['amount'].median(),
                'min_amount': df['amount'].min(),
                'max_amount': df['amount'].max(),
                'std_amount': df['amount'].std()
            },
            'supplier_statistics': {
                'total_suppliers': df['supplier_code'].nunique(),
                'top_suppliers': df['supplier_name'].value_counts().head(10).to_dict(),
                'supplier_amount_distribution': df.groupby('supplier_name')['amount'].sum().head(10).to_dict()
            },
            'temporal_analysis': {
                'invoices_by_month': df.groupby(df['invoice_date'].str[:7]).size().to_dict(),
                'amount_by_month': df.groupby(df['invoice_date'].str[:7])['amount'].sum().to_dict()
            },
            'status_distribution': df['status'].value_counts().to_dict(),
            'currency_distribution': df['currency'].value_counts().to_dict(),
            'account_head_distribution': df['account_head'].value_counts().to_dict()
        }
        
        return analytics
    
    def _format_detailed_results(self, invoices: List[Dict]) -> List[Dict]:
        """Format detailed results for reporting"""
        formatted_results = []
        
        for invoice in invoices[:100]:  # Limit to first 100 for detailed view
            formatted_results.append({
                'invoice_id': invoice.get('invoice_id'),
                'invoice_number': invoice.get('invoice_number'),
                'supplier_name': invoice.get('supplier_name'),
                'amount': invoice.get('amount'),
                'status': invoice.get('status'),
                'validation_status': 'Valid' if invoice.get('validation_errors', '[]') == '[]' else 'Invalid',
                'processing_date': invoice.get('processing_timestamp', '')[:10]
            })
        
        return formatted_results
    
    def _calculate_summary_metrics(self, invoices: List[Dict]) -> Dict:
        """Calculate summary metrics for dashboard"""
        if not invoices:
            return {}
        
        df = pd.DataFrame(invoices)
        
        # Validation results
        valid_invoices = df[df['validation_errors'] == '[]']
        invalid_invoices = df[df['validation_errors'] != '[]']
        
        return {
            'total_invoices': len(df),
            'valid_invoices': len(valid_invoices),
            'invalid_invoices': len(invalid_invoices),
            'validation_rate': len(valid_invoices) / len(df) * 100 if len(df) > 0 else 0,
            'total_amount': df['amount'].sum(),
            'average_amount': df['amount'].mean(),
            'unique_suppliers': df['supplier_code'].nunique(),
            'processing_date_range': {
                'from': df['processing_timestamp'].min()[:10] if len(df) > 0 else '',
                'to': df['processing_timestamp'].max()[:10] if len(df) > 0 else ''
            }
        }
    
    def _calculate_validation_trends(self, invoices: List[Dict]) -> Dict:
        """Calculate validation trends over time"""
        if not invoices:
            return {}
        
        df = pd.DataFrame(invoices)
        df['processing_date'] = pd.to_datetime(df['processing_timestamp']).dt.date
        
        # Group by date and calculate validation rates
        daily_stats = df.groupby('processing_date').agg({
            'invoice_id': 'count',
            'validation_errors': lambda x: (x == '[]').sum()
        }).rename(columns={'invoice_id': 'total', 'validation_errors': 'valid'})
        
        daily_stats['validation_rate'] = daily_stats['valid'] / daily_stats['total'] * 100
        
        return {
            'daily_totals': daily_stats['total'].to_dict(),
            'daily_valid': daily_stats['valid'].to_dict(),
            'daily_validation_rates': daily_stats['validation_rate'].to_dict()
        }
    
    def _analyze_suppliers(self, invoices: List[Dict]) -> Dict:
        """Analyze supplier performance"""
        if not invoices:
            return {}
        
        df = pd.DataFrame(invoices)
        
        # Supplier-wise analysis
        supplier_stats = df.groupby('supplier_name').agg({
            'invoice_id': 'count',
            'amount': ['sum', 'mean'],
            'validation_errors': lambda x: (x == '[]').sum()
        })
        
        supplier_stats.columns = ['total_invoices', 'total_amount', 'avg_amount', 'valid_invoices']
        supplier_stats['validation_rate'] = supplier_stats['valid_invoices'] / supplier_stats['total_invoices'] * 100
        
        return {
            'top_suppliers_by_volume': supplier_stats.sort_values('total_invoices', ascending=False).head(10).to_dict('index'),
            'top_suppliers_by_amount': supplier_stats.sort_values('total_amount', ascending=False).head(10).to_dict('index'),
            'suppliers_by_validation_rate': supplier_stats.sort_values('validation_rate', ascending=False).head(10).to_dict('index')
        }
    
    def _analyze_amount_distribution(self, invoices: List[Dict]) -> Dict:
        """Analyze amount distribution patterns"""
        if not invoices:
            return {}
        
        df = pd.DataFrame(invoices)
        amounts = df['amount'].values
        
        # Create amount ranges
        ranges = [0, 100, 500, 1000, 5000, 10000, float('inf')]
        range_labels = ['0-100', '100-500', '500-1K', '1K-5K', '5K-10K', '10K+']
        
        amount_distribution = pd.cut(amounts, bins=ranges, labels=range_labels).value_counts().to_dict()
        
        return {
            'amount_ranges': amount_distribution,
            'percentiles': {
                '25th': float(np.percentile(amounts, 25)),
                '50th': float(np.percentile(amounts, 50)),
                '75th': float(np.percentile(amounts, 75)),
                '90th': float(np.percentile(amounts, 90)),
                '95th': float(np.percentile(amounts, 95))
            }
        }
    
    def _analyze_error_patterns(self, invoices: List[Dict]) -> Dict:
        """Analyze common error patterns"""
        if not invoices:
            return {}
        
        error_counts = defaultdict(int)
        
        for invoice in invoices:
            errors_str = invoice.get('validation_errors', '[]')
            try:
                errors = json.loads(errors_str)
                for error in errors:
                    error_counts[error] += 1
            except (json.JSONDecodeError, TypeError):
                continue
        
        return {
            'most_common_errors': dict(sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:10]),
            'total_error_types': len(error_counts),
            'error_frequency': sum(error_counts.values())
        }
    
    def _analyze_processing_performance(self) -> Dict:
        """Analyze processing performance metrics"""
        try:
            with self.db_manager.get_connection() as conn:
                # Get recent validation results
                cursor = conn.execute('''
                    SELECT processing_time, timestamp, confidence_level
                    FROM validation_results 
                    WHERE timestamp >= date('now', '-30 days')
                    ORDER BY timestamp DESC
                ''')
                
                results = cursor.fetchall()
                
                if not results:
                    return {}
                
                processing_times = [row['processing_time'] for row in results if row['processing_time']]
                confidence_levels = [row['confidence_level'] for row in results if row['confidence_level']]
                
                return {
                    'average_processing_time': np.mean(processing_times) if processing_times else 0,
                    'median_processing_time': np.median(processing_times) if processing_times else 0,
                    'max_processing_time': max(processing_times) if processing_times else 0,
                    'min_processing_time': min(processing_times) if processing_times else 0,
                    'average_confidence': np.mean(confidence_levels) if confidence_levels else 0,
                    'total_processed': len(results)
                }
        
        except Exception as e:
            logging.error(f"Failed to analyze processing performance: {str(e)}")
            return {}

class NotificationManager:
    """Enhanced notification and alerting system"""
    
    def __init__(self):
        self.smtp_server = CONFIG.get('EMAIL_SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = CONFIG.get('EMAIL_SMTP_PORT', 587)
        self.enabled = CONFIG.get('ENABLE_NOTIFICATIONS', True)
        
        # Load email configuration from environment
        self.email_user = os.getenv('EMAIL_USER')
        self.email_password = os.getenv('EMAIL_PASSWORD')
        self.notification_recipients = os.getenv('NOTIFICATION_RECIPIENTS', '').split(',')
        
    def send_validation_summary(self, report_data: Dict) -> bool:
        """Send validation summary email notification"""
        if not self.enabled or not self.email_user:
            logging.info("Email notifications disabled or not configured")
            return False
        
        try:
            # Create email content
            subject = f"Invoice Validation Report - {datetime.now().strftime('%Y-%m-%d')}"
            body = self._create_summary_email_body(report_data)
            
            # Send email
            return self._send_email(subject, body, html=True)
            
        except Exception as e:
            logging.error(f"Failed to send validation summary: {str(e)}")
            return False
    
    def send_error_alert(self, error_details: Dict) -> bool:
        """Send error alert notification"""
        if not self.enabled:
            return False
        
        try:
            subject = f"Invoice Validation System Alert - {error_details.get('error_type', 'Unknown Error')}"
            body = self._create_error_alert_body(error_details)
            
            return self._send_email(subject, body, priority='high')
            
        except Exception as e:
            logging.error(f"Failed to send error alert: {str(e)}")
            return False
    
    def send_processing_complete(self, processing_stats: Dict) -> bool:
        """Send processing completion notification"""
        if not self.enabled:
            return False
        
        try:
            subject = f"Invoice Processing Complete - {processing_stats.get('total_processed', 0)} invoices"
            body = self._create_processing_complete_body(processing_stats)
            
            return self._send_email(subject, body)
            
        except Exception as e:
            logging.error(f"Failed to send processing complete notification: {str(e)}")
            return False
    
    def _send_email(self, subject: str, body: str, html: bool = False, priority: str = 'normal') -> bool:
        """Send email notification"""
        if not self.notification_recipients or not any(self.notification_recipients):
            logging.warning("No notification recipients configured")
            return False
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.email_user
            msg['To'] = ', '.join([r.strip() for r in self.notification_recipients if r.strip()])
            
            # Set priority
            if priority == 'high':
                msg['X-Priority'] = '1'
                msg['X-MSMail-Priority'] = 'High'
            
            # Add body
            if html:
                msg.attach(MIMEText(body, 'html'))
            else:
                msg.attach(MIMEText(body, 'plain'))
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.email_user, self.email_password)
                server.send_message(msg)
            
            logging.info(f"Email notification sent: {subject}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to send email: {str(e)}")
            return False
    
    def _create_summary_email_body(self, report_data: Dict) -> str:
        """Create HTML email body for validation summary"""
        summary = report_data.get('validation_summary', {})
        metadata = report_data.get('report_metadata', {})
        
        html_body = f"""
        
        
            
        
        
            

                
Invoice Validation Report

                
Generated on {metadata.get('generated_at', 'N/A')}


            

            
            

                
Validation Summary

                
                

                    
{summary.get('total_invoices', 0)}

                    
Total Invoices

                

                
                

                    
{summary.get('valid_invoices', 0)}

                    
Valid Invoices

                

                
                

                    
{summary.get('invalid_invoices', 0)}

                    
Invalid Invoices

                

                
                

                    
{summary.get('validation_rate', 0):.1f}%

                    
Validation Rate

                

                
                {"
✓ All invoices validated successfully!
" if summary.get('validation_rate', 0) == 100 else ""}
                {"
⚠ Some invoices failed validation. Please review the detailed report.
" if summary.get('invalid_invoices', 0) > 0 else ""}
                
                
Processing Performance

                
Average processing time: {summary.get('average_processing_time', 0)} seconds


                
                
Most Common Errors

                

        """
        
        # Add common errors
        for error in summary.get('common_errors', [])[:5]:
            html_body += f"
{error.get('errors', 'Unknown error')} (Count: {error.get('count', 0)})
"
        
        html_body += """
                

                
                
For detailed analysis and full report, please check the system dashboard.


            

        
        
        """
        
        return html_body
    
    def _create_error_alert_body(self, error_details: Dict) -> str:
        """Create error alert email body"""
        return f"""
        INVOICE VALIDATION SYSTEM ALERT
        
        Error Type: {error_details.get('error_type', 'Unknown')}
        Timestamp: {error_details.get('timestamp', datetime.now().isoformat())}
        
        Error Details:
        {error_details.get('message', 'No details available')}
        
        Stack Trace:
        {error_details.get('stack_trace', 'No stack trace available')}
        
        System Information:
        - Total invoices in queue: {error_details.get('queue_size', 'Unknown')}
        - Processing status: {error_details.get('processing_status', 'Unknown')}
        
        Please investigate this issue promptly.
        
        ---
        Invoice Validation System
        """
    
    def _create_processing_complete_body(self, processing_stats: Dict) -> str:
        """Create processing completion email body"""
        return f"""
        Invoice Processing Complete
        
        Processing Summary:
        - Total invoices processed: {processing_stats.get('total_processed', 0)}
        - Valid invoices: {processing_stats.get('valid_count', 0)}
        - Invalid invoices: {processing_stats.get('invalid_count', 0)}
        - Processing time: {processing_stats.get('total_time', 0):.2f} seconds
        - Success rate: {processing_stats.get('success_rate', 0):.1f}%
        
        Next scheduled run: {processing_stats.get('next_run', 'Not scheduled')}
        
        ---
        Invoice Validation System
        """

class InvoiceProcessingSystem:
    """Main invoice processing system coordinator"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.data_extractor = RMSDataExtractor()
        self.validator = InvoiceValidator()
        self.report_generator = ReportGenerator(self.db_manager)
        self.notification_manager = NotificationManager()
        
        self.processing_stats = {
            'total_processed': 0,
            'valid_count': 0,
            'invalid_count': 0,
            'start_time': None,
            'end_time': None,
            'errors': []
        }
        
        # Setup logging
        self._setup_logging()
        
        # Initialize session ID for tracking
        self.session_id = str(uuid4())
    
    def _setup_logging(self):
        """Setup comprehensive logging"""
        logs_path = Path(CONFIG['LOGS_PATH'])
        logs_path.mkdir(exist_ok=True)
        
        log_file = logs_path / f"invoice_processing_{datetime.now().strftime('%Y%m%d')}.log"
        
        logging.basicConfig(
            level=logging.INFO if not CONFIG['DEBUG_MODE'] else logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        # Log system startup
        logging.info(f"Invoice Processing System started - Session ID: {self.session_id}")
        logging.info(f"Configuration: {CONFIG}")
    
    def run_full_processing_cycle(self) -> Dict:
        """Run complete invoice processing cycle"""
        self.processing_stats['start_time'] = datetime.now()
        logging.info("Starting full invoice processing cycle")
        
        try:
            # Step 1: Extract data from RMS
            logging.info("Step 1: Extracting invoice data from RMS")
            invoices = self._extract_invoice_data()
            
            if not invoices:
                logging.warning("No invoices extracted from RMS system")
                return self._finalize_processing()
            
            # Step 2: Process and validate invoices
            logging.info(f"Step 2: Processing and validating {len(invoices)} invoices")
            validation_results = self._process_invoices(invoices)
            
            # Step 3: Generate reports
            logging.info("Step 3: Generating reports and analytics")
            report_data = self.report_generator.generate_validation_report()
            
            # Step 4: Send notifications
            logging.info("Step 4: Sending notifications")
            self._send_notifications(report_data)
            
            # Step 5: Cleanup and archiving
            logging.info("Step 5: Performing cleanup and archiving")
            self._perform_cleanup()
            
            logging.info("Full processing cycle completed successfully")
            return self._finalize_processing()
            
        except Exception as e:
            logging.error(f"Processing cycle failed: {str(e)}")
            self.processing_stats['errors'].append(str(e))
            
            # Send error notification
            self.notification_manager.send_error_alert({
                'error_type': 'Processing Cycle Failure',
                'message': str(e),
                'stack_trace': traceback.format_exc(),
                'timestamp': datetime.now().isoformat(),
                'session_id': self.session_id
            })
            
            return self._finalize_processing()
    
    def _extract_invoice_data(self) -> List[InvoiceRecord]:
        """Extract invoice data from RMS system"""
        try:
            # Authenticate with RMS
            api_key = os.getenv('RMS_API_KEY')
            username = os.getenv('RMS_USERNAME')
            password = os.getenv('RMS_PASSWORD')
            
            if api_key:
                authenticated = self.data_extractor.authenticate(api_key=api_key)
            elif username and password:
                authenticated = self.data_extractor.authenticate(username=username, password=password)
            else:
                logging.warning("No RMS authentication credentials provided")
                authenticated = False
            
            if not authenticated:
                logging.error("RMS authentication failed")
                return []
            
            # Extract invoice data
            date_from = (datetime.now() - timedelta(days=CONFIG['VALIDATION_INTERVAL_DAYS'])).strftime('%Y-%m-%d')
            invoices = self.data_extractor.extract_invoice_data(date_from=date_from)
            
            logging.info(f"Successfully extracted {len(invoices)} invoices from RMS")
            return invoices
            
        except Exception as e:
            logging.error(f"Invoice data extraction failed: {str(e)}")
            return []
    
    def _process_invoices(self, invoices: List[InvoiceRecord]) -> List[ValidationResult]:
        """Process and validate invoices"""
        validation_results = []
        
        try:
            if CONFIG['PARALLEL_PROCESSING']:
                validation_results = self._process_invoices_parallel(invoices)
            else:
                validation_results = self._process_invoices_sequential(invoices)
            
            # Update processing stats
            self.processing_stats['total_processed'] = len(validation_results)
            self.processing_stats['valid_count'] = sum(1 for r in validation_results if r.is_valid)
            self.processing_stats['invalid_count'] = sum(1 for r in validation_results if not r.is_valid)
            
            logging.info(f"Processing complete: {self.processing_stats['valid_count']} valid, {self.processing_stats['invalid_count']} invalid")
            
            return validation_results
            
        except Exception as e:
            logging.error(f"Invoice processing failed: {str(e)}")
            return []
    
    def _process_invoices_sequential(self, invoices: List[InvoiceRecord]) -> List[ValidationResult]:
        """Process invoices sequentially"""
        validation_results = []
        
        for i, invoice in enumerate(invoices, 1):
            try:
                # Log progress
                if i % 50 == 0:
                    logging.info(f"Processing invoice {i}/{len(invoices)}")
                
                # Validate invoice
                validation_result = self.validator.validate_invoice(invoice)
                validation_results.append(validation_result)
                
                # Store in database
                self.db_manager.insert_invoice(invoice)
                self.db_manager.insert_validation_result(validation_result)
                
            except Exception as e:
                logging.error(f"Failed to process invoice {invoice.invoice_id}: {str(e)}")
                continue
        
        return validation_results
    
    def _process_invoices_parallel(self, invoices: List[InvoiceRecord]) -> List[ValidationResult]:
        """Process invoices in parallel"""
        validation_results = []
        max_workers = CONFIG['MAX_WORKERS']
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_invoice = {
                executor.submit(self._process_single_invoice, invoice): invoice 
                for invoice in invoices
            }
            
            # Collect results
            for i, future in enumerate(as_completed(future_to_invoice), 1):
                invoice = future_to_invoice[future]
                
                try:
                    validation_result = future.result()
                    if validation_result:
                        validation_results.append(validation_result)
                    
                    # Log progress
                    if i % 50 == 0:
                        logging.info(f"Completed {i}/{len(invoices)} invoices")
                        
                except Exception as e:
                    logging.error(f"Failed to process invoice {invoice.invoice_id}: {str(e)}")
                    continue
        
        return validation_results
    
    def _process_single_invoice(self, invoice: InvoiceRecord) -> Optional[ValidationResult]:
        """Process a single invoice (for parallel processing)"""
        try:
            # Validate invoice
            validation_result = self.validator.validate_invoice(invoice)
            
            # Store in database (with thread safety)
            self.db_manager.insert_invoice(invoice)
            self.db_manager.insert_validation_result(validation_result)
            
            return validation_result
            
        except Exception as e:
            logging.error(f"Single invoice processing failed for {invoice.invoice_id}: {str(e)}")
            return None
    
    def _send_notifications(self, report_data: Dict):
        """Send processing notifications"""
        try:
            # Send validation summary
            self.notification_manager.send_validation_summary(report_data)
            
            # Send processing complete notification
            processing_stats = {
                'total_processed': self.processing_stats['total_processed'],
                'valid_count': self.processing_stats['valid_count'],
                'invalid_count': self.processing_stats['invalid_count'],
                'total_time': (datetime.now() - self.processing_stats['start_time']).total_seconds(),
                'success_rate': (self.processing_stats['valid_count'] / max(1, self.processing_stats['total_processed'])) * 100,
                'next_run': (datetime.now() + timedelta(days=CONFIG['VALIDATION_INTERVAL_DAYS'])).strftime('%Y-%m-%d %H:%M:%S')
            }
            
            self.notification_manager.send_processing_complete(processing_stats)
            
        except Exception as e:
            logging.error(f"Failed to send notifications: {str(e)}")
    
    def _perform_cleanup(self):
        """Perform system cleanup and maintenance"""
        try:
            # Clean up old records
            retention_days = CONFIG['ARCHIVE_RETENTION_MONTHS'] * 30
            self.db_manager.cleanup_old_records(retention_days)
            
            # Clean up old log files
            self._cleanup_old_logs()
            
            # Clean up old report files
            self._cleanup_old_reports()
            
            logging.info("Cleanup and maintenance completed")
            
        except Exception as e:
            logging.error(f"Cleanup failed: {str(e)}")
    
    def _cleanup_old_logs(self):
        """Clean up old log files"""
        try:
            logs_path = Path(CONFIG['LOGS_PATH'])
            if not logs_path.exists():
                return
            
            cutoff_date = datetime.now() - timedelta(days=30)
            
            for log_file in logs_path.glob("*.log"):
                if log_file.stat().st_mtime < cutoff_date.timestamp():
                    log_file.unlink()
                    logging.debug(f"Removed old log file: {log_file}")
                    
        except Exception as e:
            logging.error(f"Log cleanup failed: {str(e)}")
    
    def _cleanup_old_reports(self):
        """Clean up old report files"""
        try:
            reports_path = Path(CONFIG['REPORTS_PATH'])
            if not reports_path.exists():
                return
            
            cutoff_date = datetime.now() - timedelta(days=90)
            
            for report_file in reports_path.glob("*.json"):
                if report_file.stat().st_mtime < cutoff_date.timestamp():
                    report_file.unlink()
                    logging.debug(f"Removed old report file: {report_file}")
                    
        except Exception as e:
            logging.error(f"Report cleanup failed: {str(e)}")
    
    def _finalize_processing(self) -> Dict:
        """Finalize processing and return summary"""
        self.processing_stats['end_time'] = datetime.now()
        
        if self.processing_stats['start_time']:
            total_time = (self.processing_stats['end_time'] - self.processing_stats['start_time']).total_seconds()
            self.processing_stats['total_time'] = total_time
        
        # Log final statistics
        logging.info(f"Processing cycle completed:")
        logging.info(f"- Total processed: {self.processing_stats['total_processed']}")
        logging.info(f"- Valid: {self.processing_stats['valid_count']}")
        logging.info(f"- Invalid: {self.processing_stats['invalid_count']}")
        logging.info(f"- Total time: {self.processing_stats.get('total_time', 0):.2f} seconds")
        logging.info(f"- Errors: {len(self.processing_stats['errors'])}")
        
        return self.processing_stats.copy()
    
    def get_dashboard_data(self) -> Dict:
        """Get data for dashboard display"""
        return self.report_generator.generate_dashboard_data()
    
    def get_validation_summary(self) -> Dict:
        """Get validation summary statistics"""
        return self.db_manager.get_validation_summary()

def setup_scheduled_processing():
    """Setup scheduled processing using GitHub Actions or local scheduler"""
    processing_system = InvoiceProcessingSystem()
    
    # Schedule processing every N days
    schedule.every(CONFIG['VALIDATION_INTERVAL_DAYS']).days.do(
        processing_system.run_full_processing_cycle
    )
    
    logging.info(f"Scheduled processing every {CONFIG['VALIDATION_INTERVAL_DAYS']} days")
    
    # Keep the scheduler running
    while True:
        schedule.run_pending()
        time.sleep(3600)  # Check every hour

def main():
    """Main entry point for the invoice processing system"""
    try:
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
        signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
        
        # Parse command line arguments
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            
            if command == 'process':
                # Run single processing cycle
                processing_system = InvoiceProcessingSystem()
                result = processing_system.run_full_processing_cycle()
                print(json.dumps(result, indent=2, default=str))
                
            elif command == 'schedule':
                # Run scheduled processing
                setup_scheduled_processing()
                
            elif command == 'dashboard':
                # Generate dashboard data
                processing_system = InvoiceProcessingSystem()
                dashboard_data = processing_system.get_dashboard_data()
                print(json.dumps(dashboard_data, indent=2, default=str))
                
            elif command == 'validate':
                # Validate configuration
                print("Validating system configuration...")
                
                # Check database connection
                try:
                    db_manager = DatabaseManager()
                    print("✓ Database connection successful")
                except Exception as e:
                    print(f"✗ Database connection failed: {e}")
                
                # Check RMS connection
                try:
                    extractor = RMSDataExtractor()
                    print("✓ RMS extractor initialized")
                except Exception as e:
                    print(f"✗ RMS extractor failed: {e}")
                
                # Check email configuration
                if os.getenv('EMAIL_USER'):
                    print("✓ Email configuration found")
                else:
                    print("⚠ Email configuration not found")
                
                print("Configuration validation complete")
                
            else:
                print(f"Unknown command: {command}")
                print("Available commands: process, schedule, dashboard, validate")
        
        else:
            # Default: run single processing cycle
            processing_system = InvoiceProcessingSystem()
            result = processing_system.run_full_processing_cycle()
            print("Processing completed successfully!")
            print(f"Processed: {result.get('total_processed', 0)} invoices")
            print(f"Valid: {result.get('valid_count', 0)}")
            print(f"Invalid: {result.get('invalid_count', 0)}")
    
    except KeyboardInterrupt:
        logging.info("Processing interrupted by user")
        sys.exit(0)
    
    except Exception as e:
        logging.error(f"System error: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
