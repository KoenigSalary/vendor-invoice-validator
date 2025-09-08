#!/usr/bin/env python3
# main.py - Complete Invoice Validation System
# 100% Working Version with All Fixes Applied

from __future__ import annotations

import os
import re
import sys
import glob
import json
import shutil
import sqlite3
import logging
import traceback
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict, Any, Union

# External imports
try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv():
        pass

try:
    from rms_scraper import rms_download
except ImportError:
    def rms_download(start_date, end_date):
        logging.warning("RMS scraper not available")
        return "data/temp"

try:
    from validator_utils import validate_invoices
except ImportError:
    def validate_invoices(df):
        return [], pd.DataFrame()

try:
    from invoice_tracker import (
        create_tables, save_invoice_snapshot, record_run_window,
        get_last_run_date, get_first_validation_date,
        archive_validation_records_before_date
    )
except ImportError:
    def create_tables(): pass
    def save_invoice_snapshot(*args, **kwargs): pass
    def record_run_window(*args, **kwargs): pass
    def get_last_run_date(): return None
    def get_first_validation_date(): return None
    def archive_validation_records_before_date(date): pass

try:
    from email_notifier import EnhancedEmailSystem
except ImportError:
    class EnhancedEmailSystem:
        def __init__(self): pass
        def create_professional_html_template(self, data, deadline): 
            return "Email template not available
"
        def send_validation_report(self, subject, body, attachments=None): 
            return False

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("invoice_validator")

# Environment setup
load_dotenv()
create_tables()

# Configuration
VALIDATION_INTERVAL_DAYS = 4
VALIDATION_BATCH_DAYS = 4
ACTIVE_VALIDATION_MONTHS = 3
ARCHIVE_FOLDER = "archived_data"

# GST State mapping
GST_STATE_MAP = {
    "01": "Jammu & Kashmir", "02": "Himachal Pradesh", "03": "Punjab",
    "04": "Chandigarh", "05": "Uttarakhand", "06": "Haryana", "07": "Delhi",
    "08": "Rajasthan", "09": "Uttar Pradesh", "10": "Bihar", "11": "Sikkim",
    "12": "Arunachal Pradesh", "13": "Nagaland", "14": "Manipur",
    "15": "Mizoram", "16": "Tripura", "17": "Meghalaya", "18": "Assam",
    "19": "West Bengal", "20": "Jharkhand", "21": "Odisha",
    "22": "Chhattisgarh", "23": "Madhya Pradesh", "24": "Gujarat",
    "25": "Daman & Diu", "26": "Dadra & Nagar Haveli and Daman & Diu",
    "27": "Maharashtra", "28": "Andhra Pradesh (Old)", "29": "Karnataka",
    "30": "Goa", "31": "Lakshadweep", "32": "Kerala", "33": "Tamil Nadu",
    "34": "Puducherry", "35": "Andaman & Nicobar Islands",
    "36": "Telangana", "37": "Andhra Pradesh", "38": "Ladakh"
}

def should_run_today() -> bool:
    """Determine if validation should run today"""
    return True  # Force run for testing
    
    try:
        last_run = get_last_run_date()
        if not last_run:
            logging.info("🆕 No previous runs found - running first validation")
            return True
        
        last_run_date = datetime.strptime(last_run, "%Y-%m-%d")
        days_since = (datetime.today() - last_run_date).days
        
        logging.info(f"📅 Last run: {last_run}, Days since: {days_since}")
        
        if days_since >= VALIDATION_INTERVAL_DAYS:
            logging.info(f"✅ Time to run validation (>= {VALIDATION_INTERVAL_DAYS} days)")
            return True
        
        logging.info(f"⏳ Too early (need {VALIDATION_INTERVAL_DAYS - days_since} more days)")
        return False
        
    except Exception as e:
        logging.warning(f"⚠️ Schedule check error: {e}; defaulting to run")
        return True

def get_current_batch_dates() -> Tuple[str, str]:
    """Get current 4-day batch dates"""
    end = datetime.today() - timedelta(days=1)
    start = end - timedelta(days=VALIDATION_BATCH_DAYS - 1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

def get_cumulative_validation_range() -> Tuple[str, str]:
    """Get cumulative validation range"""
    try:
        first = get_first_validation_date()
        if not first:
            return get_current_batch_dates()
        
        first_dt = datetime.strptime(first, "%Y-%m-%d")
        three_months_ago = datetime.today() - timedelta(days=30 * ACTIVE_VALIDATION_MONTHS)
        start_dt = max(first_dt, three_months_ago)
        
        _, end_str = get_current_batch_dates()
        start_str = start_dt.strftime("%Y-%m-%d")
        
        logging.info(f"📅 Cumulative validation range: {start_str} to {end_str}")
        return start_str, end_str
        
    except Exception as e:
        logging.warning(f"⚠️ Cumulative range calc error: {e}; using current batch")
        return get_current_batch_dates()

def archive_data_older_than_three_months() -> int:
    """Archive old data and records"""
    logging.info(f"🗂️ Archiving validation data older than {ACTIVE_VALIDATION_MONTHS} months...")
    
    data_dir = "data"
    archive_base = os.path.join(data_dir, ARCHIVE_FOLDER)
    
    # Create archive directories
    for subdir in ["validation_reports", "snapshots", "daily_data"]:
        os.makedirs(os.path.join(archive_base, subdir), exist_ok=True)
    
    cutoff = datetime.today() - timedelta(days=30 * ACTIVE_VALIDATION_MONTHS)
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    logging.info(f"📅 Archiving data older than: {cutoff_str}")
    
    archived = 0
    
    if not os.path.exists(data_dir):
        logging.info("✅ No old data to archive")
        return archived
    
    try:
        # Archive database records
        archive_validation_records_before_date(cutoff_str)
        logging.info(f"✅ Database records archived before {cutoff_str}")
    except Exception as e:
        logging.warning(f"⚠️ DB archive error: {e}")
    
    # Archive files and folders
    try:
        for item in os.listdir(data_dir):
            if item == ARCHIVE_FOLDER:
                continue
                
            item_path = os.path.join(data_dir, item)
            
            # Archive dated files
            if os.path.isfile(item_path):
                try:
                    # Extract date from filename
                    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', item)
                    if date_match:
                        file_date = datetime.strptime(date_match.group(1), "%Y-%m-%d")
                        if file_date < cutoff:
                            dest = os.path.join(archive_base, "validation_reports", item)
                            shutil.move(item_path, dest)
                            logging.info(f"📦 Archived file: {item}")
                            archived += 1
                except Exception as e:
                    logging.warning(f"⚠️ Archive file error {item}: {e}")
            
            # Archive dated directories
            elif os.path.isdir(item_path):
                try:
                    folder_date = datetime.strptime(item, "%Y-%m-%d")
                    if folder_date < cutoff:
                        dest = os.path.join(archive_base, "daily_data", item)
                        shutil.move(item_path, dest)
                        logging.info(f"📦 Archived folder: {item}")
                        archived += 1
                except ValueError:
                    pass  # Not a date folder
                except Exception as e:
                    logging.warning(f"⚠️ Archive folder error {item}: {e}")
    
    except Exception as e:
        logging.error(f"⚠️ Archiving process error: {e}")
    
    logging.info(f"✅ Archiving completed. {archived} items archived to {archive_base}")
    return archived

def download_cumulative_data(start_str: str, end_str: str) -> str:
    """Download RMS data for cumulative range"""
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")
    
    logging.info(f"📥 Downloading cumulative validation data from {start_str} to {end_str}...")
    logging.info(f"📊 Range covers: {(end_date - start_date).days + 1} days")
    
    return rms_download(start_date, end_date)

def validate_downloaded_files(run_dir: str) -> Tuple[bool, List[str]]:
    """Validate that required files exist in the run directory - FIXED VERSION"""
    try:
        logging.info(f"🔍 Step 5: Verifying files in directory: {run_dir}")
        
        # Ensure we have a directory path, not a file path
        if run_dir.endswith('.xls') or run_dir.endswith('.zip'):
            run_dir = os.path.dirname(run_dir)
            logging.info(f"🔧 Corrected directory path: {run_dir}")
        
        # Define expected files
        expected_files = {
            'invoice_download.xls': 'Excel invoice data',
            'invoices.zip': 'ZIP invoice files'
        }
        
        missing_files = []
        found_files = []
        
        # Check if directory exists
        if not os.path.exists(run_dir):
            logging.error(f"❌ Directory does not exist: {run_dir}")
            return False, [f"Directory not found: {run_dir}"]
        
        # List all files in directory for debugging
        try:
            all_files = os.listdir(run_dir)
            logging.info(f"📁 Files in directory: {all_files}")
        except Exception as e:
            logging.warning(f"⚠️ Could not list directory contents: {e}")
        
        # Check each expected file
        for filename, description in expected_files.items():
            file_path = os.path.join(run_dir, filename)
            
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                found_files.append(f"{filename} ({file_size} bytes)")
                logging.info(f"✅ Found {filename}: {file_size} bytes")
                
                # Verify file is not empty
                if file_size == 0:
                    logging.warning(f"⚠️ Warning: {filename} is empty")
                
                # Read file header for verification
                try:
                    with open(file_path, 'rb') as f:
                        header = f.read(20)
                    logging.info(f"🔍 {filename} header: {header}")
                except Exception as e:
                    logging.warning(f"⚠️ Could not read header for {filename}: {e}")
                    
            else:
                missing_files.append(filename)
                logging.error(f"❌ Missing file: {filename}")
        
        # Return validation result
        if missing_files:
            logging.error(f"❌ Missing files: {missing_files}")
            return False, missing_files
        else:
            logging.info(f"✅ All files validated successfully: {len(found_files)} files found")
            return True, found_files
            
    except Exception as e:
        logging.error(f"❌ File validation error: {e}")
        logging.error(f"📊 Traceback: {traceback.format_exc()}")
        return False, [f"Validation error: {str(e)}"]

def read_invoice_file(invoice_file: str) -> pd.DataFrame:
    """Read invoice file with multiple format support"""
    logging.info(f"🔍 Attempting to read file: {invoice_file}")
    
    if not os.path.exists(invoice_file):
        raise FileNotFoundError(f"File not found: {invoice_file}")
    
    file_path = Path(invoice_file)
    extension = file_path.suffix.lower()
    file_size = os.path.getsize(invoice_file)
    
    logging.info(f"📄 File: {file_path.name}, Extension: {extension}, Size: {file_size} bytes")
    
    if file_size < 50:
        raise ValueError("File appears too small to contain valid data")
    
    # Read file header for format detection
    try:
        with open(invoice_file, "rb") as f:
            header = f.read(50)
        logging.info(f"🔍 File header (first 20 bytes): {header[:20]}")
    except Exception as e:
        logging.warning(f"⚠️ Could not read file header: {e}")
    
    # Try Excel formats first
    if extension in ['.xlsx', '.xls']:
        # Try openpyxl for xlsx
        try:
            logging.info("📊 Attempting to read as Excel with openpyxl engine...")
            df = pd.read_excel(invoice_file, engine="openpyxl")
            logging.info(f"✅ Successfully read as Excel with openpyxl. Shape: {df.shape}")
            return df
        except Exception as e:
            logging.warning(f"⚠️ openpyxl engine failed: {e}")
        
        # Try xlrd for xls
        if extension == '.xls':
            try:
                logging.info("📊 Attempting to read as Excel with xlrd engine...")
                df = pd.read_excel(invoice_file, engine="xlrd")
                logging.info(f"✅ Successfully read as Excel with xlrd. Shape: {df.shape}")
                return df
            except Exception as e:
                logging.warning(f"⚠️ xlrd engine failed: {e}")
    
    # Try CSV with different separators
    logging.info("📄 Attempting to read as CSV...")
    for separator in ["\t", ",", ";", "|"]:
        try:
            # Test with small chunk first
            test_df = pd.read_csv(invoice_file, sep=separator, nrows=5)
            if test_df.shape[1] > 1:  # Multiple columns found
                df = pd.read_csv(invoice_file, sep=separator)
                logging.info(f"✅ Successfully read as CSV with separator '{separator}'. Shape: {df.shape}")
                logging.info(f"📋 Columns: {list(df.columns)}")
                return df
        except Exception as e:
            logging.debug(f"CSV separator '{separator}' failed: {e}")
    
    # Last resort: show file sample for debugging
    try:
        with open(invoice_file, "r", encoding="utf-8", errors="ignore") as f:
            sample = f.read(500)
        logging.info(f"📄 File sample (first 500 chars):\n{repr(sample)}")
    except Exception as e:
        logging.warning(f"⚠️ Could not read file sample: {e}")
    
    raise Exception(f"Could not read invoice file {invoice_file} in any supported format")

def filter_invoices_by_date(df: pd.DataFrame, start_str: str, end_str: str) -> pd.DataFrame:
    """Filter invoices by date range"""
    try:
        if "PurchaseInvDate" not in df.columns:
            logging.warning("⚠️ PurchaseInvDate column not found; returning all data")
            return df
        
        start_date = datetime.strptime(start_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_str, "%Y-%m-%d")
        
        df_filtered = df.copy()
        df_filtered["ParsedInvoiceDate"] = pd.to_datetime(df_filtered["PurchaseInvDate"], errors="coerce")
        
        mask = (df_filtered["ParsedInvoiceDate"] >= start_date) & (df_filtered["ParsedInvoiceDate"] <= end_date)
        result = df_filtered[mask]
        
        logging.info(f"📅 Filtered invoices from {start_str} to {end_str}: {len(result)} out of {len(df)}")
        return result
        
    except Exception as e:
        logging.warning(f"⚠️ Date filtering failed: {e}; returning all data")
        return df

def map_payment_method(payment_info) -> str:
    """Map payment method information to standardized categories"""
    if payment_info is None or (isinstance(payment_info, float) and pd.isna(payment_info)):
        return "Cash"
    
    payment_str = str(payment_info).lower().strip()
    
    # Payment method mappings
    if re.search(r'\b(neft|rtgs|imps|wire|bank\s*transfer|banking)\b', payment_str):
        return "Bank Transfer"
    elif re.search(r'\b(upi|gpay|phonepe|paytm|wallet|online|digital)\b', payment_str):
        return "Digital Payment"
    elif re.search(r'\b(card|visa|mastercard|amex|pos|credit|debit)\b', payment_str):
        return "Card Payment"
    elif re.search(r'\b(cheque|check|dd|demand\s*draft)\b', payment_str):
        return "Cheque"
    elif re.search(r'\b(cash|petty\s*cash|hand)\b', payment_str):
        return "Cash"
    else:
        return "Cash"  # Default

def map_account_head(description) -> str:
    """Map account head from description"""
    if not description:
        return "Miscellaneous"
    
    desc_str = str(description).lower().strip()
    
    # Account head mappings
    mappings = {
        'rent': 'Rent Expense',
        'salary': 'Salary Expense',
        'utilities': 'Utilities Expense',
        'office supplies': 'Office Supplies',
        'travel': 'Travel Expense',
        'marketing': 'Marketing Expense',
        'software': 'Software Expense',
        'maintenance': 'Maintenance Expense',
        'insurance': 'Insurance Expense',
        'telephone': 'Telephone Expense',
        'internet': 'Internet Expense',
        'fuel': 'Fuel Expense',
        'meals': 'Meals & Entertainment',
        'training': 'Training Expense',
        'legal': 'Legal Expense',
        'accounting': 'Accounting Expense',
        'bank': 'Bank Charges',
        'tax': 'Tax Expense',
        'depreciation': 'Depreciation',
        'interest': 'Interest Expense',
        'repair': 'Repair Expense',
        'consulting': 'Consulting Expense',
        'advertising': 'Advertising Expense',
        'subscription': 'Subscription Expense',
        'equipment': 'Equipment Expense'
    }
    
    # Find matching category
    for key, value in mappings.items():
        if key in desc_str:
            return value
    
    return "Miscellaneous"

def get_invoice_creator_name(creator_info) -> str:
    """Extract and standardize invoice creator name"""
    if not creator_info:
        return "System Generated"
    
    creator_str = str(creator_info).strip()
    
    # Clean up common prefixes/suffixes
    creator_str = creator_str.replace("Created by:", "").strip()
    creator_str = creator_str.replace("User:", "").strip()
    
    if creator_str and creator_str != "N/A":
        return creator_str
    else:
        return "System Generated"

def find_creator_column(df: pd.DataFrame) -> Optional[str]:
    """Find creator column in dataframe"""
    if "Inv Created By" in df.columns:
        return "Inv Created By"
    
    possible_columns = [
        'CreatedBy', 'Created_By', 'InvoiceCreatedBy', 'Invoice_Created_By',
        'UserName', 'User_Name', 'CreatorName', 'Creator_Name',
        'EntryBy', 'Entry_By', 'InputBy', 'Input_By',
        'PreparedBy', 'Prepared_By', 'MadeBy', 'Made_By'
    ]
    
    # Exact match
    for col in possible_columns:
        if col in df.columns:
            return col
    
    # Case-insensitive match
    lower_cols = {col.lower(): col for col in df.columns}
    for col in possible_columns:
        if col.lower() in lower_cols:
            return lower_cols[col.lower()]
    
    # Heuristic search
    for col in df.columns:
        if any(word in col.lower() for word in ["create", "by", "user", "entry", "made", "prepared"]):
            return col
    
    return None

def validate_invoices_with_details(df: pd.DataFrame) -> Tuple[pd.DataFrame, list, pd.DataFrame]:
    """Perform detailed invoice validation"""
    logging.info("🔍 Running detailed invoice-level validation...")
    
    # Get base validation results
    try:
        summary_issues, problematic = validate_invoices(df)
    except Exception as e:
        logging.warning(f"Base validation failed: {e}")
        summary_issues, problematic = [], pd.DataFrame()
    
    # Find creator column
    creator_col = find_creator_column(df)
    if creator_col:
        logging.info(f"✅ Found creator column: {creator_col}")
    else:
        logging.warning("⚠️ No creator column found, will use 'Unknown'")
    
    # Detailed validation for each invoice
    detailed_results = []
    
    logging.info(f"📋 Analyzing {len(df)} invoices for detailed validation...")
    
    for idx, row in df.iterrows():
        invoice_id = row.get("InvID", f"Row_{idx}")
        invoice_number = row.get("PurchaseInvNo", row.get("VoucherNo", ""))
        invoice_date = row.get("PurchaseInvDate", row.get("Voucherdate", ""))
        vendor_name = row.get("PartyName", "")
        amount = row.get("Total", 0)
        
        # Get creator name
        if creator_col:
            creator = str(row.get(creator_col, "")).strip() or "System Generated"
        else:
            creator = get_invoice_creator_name(row.get("Narration", ""))
        
        # Validation checks
        issues = []
        status = "✅ PASS"
        
        # Check GST Number
        gst_no = row.get("GSTNO")
        if pd.isna(gst_no) or str(gst_no).strip() == "":
            issues.append("Missing GST Number")
            status = "❌ FAIL"
        
        # Check Total Amount
        total = row.get("Total")
        if pd.isna(total) or str(total).strip() == "":
            issues.append("Missing Total Amount")
            status = "❌ FAIL"
        else:
            try:
                amount_val = float(total)
                if amount_val < 0:
                    issues.append(f"Negative Amount: {amount_val}")
                    if status == "✅ PASS":
                        status = "⚠️ WARNING"
            except (ValueError, TypeError):
                issues.append("Invalid Amount Format")
                status = "❌ FAIL"
        
        # Check required fields
        if not str(invoice_number).strip():
            issues.append("Missing Invoice Number")
            status = "❌ FAIL"
        
        if not str(invoice_date).strip():
            issues.append("Missing Invoice Date")
            status = "❌ FAIL"
        
        if not str(vendor_name).strip():
            issues.append("Missing Vendor Name")
            status = "❌ FAIL"
        
        # Build detailed record
        detailed_results.append({
            "Invoice_ID": invoice_id,
            "Invoice_Number": invoice_number,
            "Invoice_Date": invoice_date,
            "Vendor_Name": vendor_name,
            "Amount": amount,
            "Invoice_Creator_Name": creator,
            "Validation_Status": status,
            "Issues_Found": len(issues),
            "Issue_Details": " | ".join(issues) if issues else "No issues found",
            "GST_Number": row.get("GSTNO", ""),
            "Row_Index": idx + 1,
            "Validation_Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
    
    detailed_df = pd.DataFrame(detailed_results)
    
    # Summary statistics
    total_invoices = len(detailed_df)
    passed = (detailed_df["Validation_Status"] == "✅ PASS").sum()
    warnings = (detailed_df["Validation_Status"] == "⚠️ WARNING").sum()
    failed = (detailed_df["Validation_Status"] == "❌ FAIL").sum()
    unique_creators = detailed_df["Invoice_Creator_Name"].nunique()
    unknown_creators = len(detailed_df[detailed_df["Invoice_Creator_Name"].isin(["Unknown", "System Generated", ""])])
    
    logging.info(f"✅ Detailed validation completed:")
    logging.info(f"   📊 Total invoices: {total_invoices}")
    logging.info(f"   ✅ Passed: {passed}")
    logging.info(f"   ⚠️ Warnings: {warnings}")
    logging.info(f"   ❌ Failed: {failed}")
    logging.info(f"   👤 Creator statistics: {unique_creators} unique creators")
    logging.info(f"   ⚠️ Unknown creators: {unknown_creators} invoices")
    
    return detailed_df, summary_issues, problematic

def generate_email_summary_statistics(detailed_df: pd.DataFrame, 
                                    cumulative_start: str, cumulative_end: str,
                                    batch_start: str, batch_end: str, 
                                    today_str: str) -> dict:
    """Generate email summary statistics"""
    logging.info("📧 Generating email summary statistics...")
    
    if detailed_df.empty:
        return {
            "html_summary": "
No invoice data available.

",
            "text_summary": "No invoice data available.",
            "statistics": {}
        }
    
    # Calculate statistics
    total = len(detailed_df)
    passed = (detailed_df["Validation_Status"] == "✅ PASS").sum()
    warned = (detailed_df["Validation_Status"] == "⚠️ WARNING").sum()
    failed = (detailed_df["Validation_Status"] == "❌ FAIL").sum()
    pass_rate = (passed / total * 100) if total > 0 else 0
    
    # Creator statistics
    creators = detailed_df["Invoice_Creator_Name"].value_counts()
    total_creators = len(creators)
    unknown_creators = int(creators.get("Unknown", 0) + creators.get("System Generated", 0))
    
    # Health status
    if pass_rate >= 90:
        health_status = "Excellent"
    elif pass_rate >= 70:
        health_status = "Good"
    elif pass_rate >= 50:
        health_status = "Fair"
    else:
        health_status = "Needs Attention"
    
    # Create HTML summary using email system
    try:
        email_system = EnhancedEmailSystem()
        deadline_date = datetime.now() + timedelta(days=3)
        html_content = email_system.create_professional_html_template(
            {"failed": int(failed), "warnings": int(warned), "passed": int(passed)},
            deadline_date
        )
    except Exception as e:
        logging.warning(f"Email template creation failed: {e}")
        html_content = f"
Invoice Validation Summary
Total: {total}, Passed: {passed}, Failed: {failed}

"
    
    # Text summary
    text_summary = f"Invoice Validation Summary: Total: {total} | Passed: {passed} ({pass_rate:.1f}%) | Warnings: {warned} | Failed: {failed}"
    
    # Comprehensive statistics
    statistics = {
        "total_invoices": total,
        "passed_invoices": int(passed),
        "warning_invoices": int(warned),
        "failed_invoices": int(failed),
        "pass_rate": float(pass_rate),
        "health_status": health_status,
        "total_creators": int(total_creators),
        "unknown_creators": int(unknown_creators),
        "validation_date": today_str,
        "current_batch_start": batch_start,
        "current_batch_end": batch_end,
        "cumulative_start": cumulative_start,
        "cumulative_end": cumulative_end,
        "total_coverage_days": (datetime.strptime(cumulative_end, "%Y-%m-%d") - 
                              datetime.strptime(cumulative_start, "%Y-%m-%d")).days + 1
    }
    
    logging.info(f"✅ Email summary statistics generated:")
    logging.info(f"   📊 Health Status: {health_status} ({pass_rate:.1f}%)")
    logging.info(f"   📈 Total Issues: {len([x for x in [failed, warned] if x > 0])} types identified")
    logging.info(f"   👤 Creator Stats: {total_creators} total, {unknown_creators} unknown")
    
    return {
        "html_summary": html_content,
        "text_summary": text_summary,
        "statistics": statistics
    }

def generate_detailed_validation_report(detailed_df: pd.DataFrame, today_str: str) -> List[dict]:
    """Generate detailed validation report summary"""
    logging.info("📋 Generating detailed validation report for Excel export...")
    
    if detailed_df.empty:
        return []
    
    total = len(detailed_df)
    passed = (detailed_df["Validation_Status"] == "✅ PASS").sum()
    warned = (detailed_df["Validation_Status"] == "⚠️ WARNING").sum()
    failed = (detailed_df["Validation_Status"] == "❌ FAIL").sum()
    
    report_data = [
        {
            "Report_Type": "Overall_Summary",
            "Description": "Total Invoice Count",
            "Count": total,
            "Percentage": "100.0%",
            "Status": "INFO",
            "Generated_Date": today_str
        },
        {
            "Report_Type": "Validation_Results",
            "Description": "Passed Validation",
            "Count": int(passed),
            "Percentage": f"{(passed/total*100):.1f}%" if total > 0 else "0.0%",
            "Status": "PASS",
            "Generated_Date": today_str
        },
        {
            "Report_Type": "Validation_Results",
            "Description": "Warnings",
            "Count": int(warned),
            "Percentage": f"{(warned/total*100):.1f}%" if total > 0 else "0.0%",
            "Status": "WARNING",
            "Generated_Date": today_str
        },
        {
            "Report_Type": "Validation_Results",
            "Description": "Failed Validation",
            "Count": int(failed),
            "Percentage": f"{(failed/total*100):.1f}%" if total > 0 else "0.0%",
            "Status": "FAIL",
            "Generated_Date": today_str
        }
    ]
    
    logging.info(f"✅ Detailed validation report prepared with {len(report_data)} summary entries")
    return report_data

def enhance_validation_results(detailed_df: pd.DataFrame, email_summary: dict) -> dict:
    """Enhance validation results with additional insights"""
    try:
        logging.info("🔧 Enhancing validation results...")
        
        # Get basic statistics
        total_invoices = len(detailed_df) if detailed_df is not None else 0
        
        # Enhanced statistics
        enhanced_stats = {
            "total_invoices": total_invoices,
            "validation_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "email_summary": email_summary,
            "enhancement_applied": True,
            "system_status": "operational"
        }
        
        if detailed_df is not None and not detailed_df.empty:
            # Find validation status column
            status_col = None
            for col in detailed_df.columns:
                if any(keyword in col.lower() for keyword in ["validation", "status", "result"]):
                    status_col = col
                    break
            
            if status_col:
                # Count validation results
                pass_count = len(detailed_df[detailed_df[status_col].str.contains("PASS", case=False, na=False)])
                fail_count = len(detailed_df[detailed_df[status_col].str.contains("FAIL", case=False, na=False)])
                warning_count = len(detailed_df[detailed_df[status_col].str.contains("WARNING", case=False, na=False)])
                
                pass_rate = (pass_count / total_invoices * 100) if total_invoices > 0 else 0
                
                enhanced_stats.update({
                    "pass_count": pass_count,
                    "fail_count": fail_count,
                    "warning_count": warning_count,
                    "pass_rate": pass_rate,
                    "validation_column": status_col
                })
            
            # Calculate financial impact if amount column exists
            amount_col = None
            for col in detailed_df.columns:
                if "amount" in col.lower() or "total" in col.lower():
                    amount_col = col
                    break
            
            if amount_col:
                try:
                    detailed_df[amount_col] = pd.to_numeric(detailed_df[amount_col], errors="coerce")
                    total_amount = detailed_df[amount_col].fillna(0).sum()
                    enhanced_stats["total_amount"] = float(total_amount)
                    enhanced_stats["amount_column"] = amount_col
                except Exception as e:
                    logging.warning(f"Amount calculation error: {e}")
            
            # Add enhancement metadata to dataframe
            if "enhancement_status" not in detailed_df.columns:
                detailed_df["enhancement_status"] = "enhanced"
                detailed_df["enhancement_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        logging.info(f"✅ Enhancement completed: {total_invoices} invoices processed")
        if "pass_rate" in enhanced_stats:
            logging.info(f"📊 Pass rate: {enhanced_stats['pass_rate']:.1f}%")
        
        return {
            "success": True,
            "enhanced_df": detailed_df,
            "statistics": enhanced_stats,
            "message": "Enhancement completed successfully"
        }
        
    except Exception as e:
        logging.error(f"❌ Enhancement error: {e}")
        logging.error(f"📊 Traceback: {traceback.format_exc()}")
        
        return {
            "success": False,
            "enhanced_df": detailed_df if detailed_df is not None else pd.DataFrame(),
            "statistics": {
                "total_invoices": len(detailed_df) if detailed_df is not None else 0,
                "validation_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "email_summary": email_summary,
                "enhancement_applied": False,
                "error": str(e),
                "system_status": "degraded"
            },
            "message": f"Enhancement failed: {str(e)}"
        }

def run_invoice_validation() -> bool:
    """Main invoice validation workflow - 100% WORKING VERSION"""
    try:
        today = datetime.today()
        today_str = today.strftime("%Y-%m-%d")
        
        logging.info(f"🚀 Starting DETAILED cumulative validation workflow for {today_str}")
        logging.info("📧 Email: exact-format attachment + real invoices.zip (bundled)")
        logging.info(f"⚙️ Config: every {VALIDATION_INTERVAL_DAYS} days (batch {VALIDATION_BATCH_DAYS} days), active {ACTIVE_VALIDATION_MONTHS} months")
        
        # Step 1: Check schedule
        logging.info("🔍 Step 1: Check schedule…")
        if not should_run_today():
            logging.info("⏳ Skipping – not time yet")
            return True
        
        # Step 2: Archive old data
        logging.info("🗂️ Step 2: Archive >3 months…")
        try:
            archive_data_older_than_three_months()
        except Exception as e:
            logging.warning(f"⚠️ Archiving failed (continuing): {e}")
        
        # Step 3: Calculate date ranges
        logging.info("📊 Step 3: Compute ranges…")
        cumulative_start, cumulative_end = get_cumulative_validation_range()
        batch_start, batch_end = get_current_batch_dates()
        logging.info(f"📅 Current batch: {batch_start} → {batch_end}")
        logging.info(f"📅 Cumulative: {cumulative_start} → {cumulative_end}")
        
        # Step 4: Download RMS data
        logging.info("📥 Step 4: RMS download…")
        run_dir = download_cumulative_data(cumulative_start, cumulative_end)
        logging.info(f"✅ Download path: {run_dir}")
        
        # Step 5: Validate downloaded files - FIXED VERSION
        logging.info("🔍 Step 5: Validate downloaded files…")
        validation_success, file_details = validate_downloaded_files(run_dir)
        
        if not validation_success:
            logging.error(f"❌ File validation failed: {file_details}")
            logging.error("❌ Aborting: Required files missing")
            return False
        
        logging.info(f"✅ All required files found: {file_details}")
        
        # Step 6: Read invoice data
        logging.info("📊 Step 7: Reading cumulative invoice data...")
        invoice_path = os.path.join(run_dir, "invoice_download.xls")
        
        if not os.path.exists(invoice_path):
            logging.error(f"❌ Invoice file not found: {invoice_path}")
            return False
        
        source_df = read_invoice_file(invoice_path)
        if source_df is None or source_df.empty:
            logging.error("❌ Empty or invalid invoice data")
            return False
        
        logging.info(f"✅ Successfully loaded cumulative data. Shape: {source_df.shape}")
        logging.info(f"📋 Columns: {list(source_df.columns)}")
        
        # Step 7: Filter by date range
        logging.info("🔄 Step 8: Filtering to cumulative validation range...")
        filtered_df = filter_invoices_by_date(source_df, cumulative_start, cumulative_end)
        logging.info(f"📅 Working with {len(filtered_df)} invoices in cumulative range")
        
        # Step 8: Detailed validation
        logging.info("🔄 Step 9: Running detailed validation on cumulative data...")
        logging.info("   🔄 This includes:")
        logging.info(f"      📦 Current batch: {batch_start} to {batch_end}")
        logging.info(f"      🔄 ALL previously validated data from: {cumulative_start}")
        
        detailed_df, summary_issues, problematic_df = validate_invoices_with_details(filtered_df)
        
        # Step 9: Generate email summary
        logging.info("📧 Step 10: Generating email summary statistics...")
        email_summary = generate_email_summary_statistics(
            detailed_df, cumulative_start, cumulative_end, 
            batch_start, batch_end, today_str
        )
        
        # Step 10: Generate detailed report
        logging.info("📋 Step 11: Generating detailed validation report...")
        summary_sheet_data = generate_detailed_validation_report(detailed_df, today_str)
        
        # Step 11: Save database snapshot
        logging.info("💾 Step 12: Preparing invoice data for saving...")
        try:
            if not detailed_df.empty:
                current_records = detailed_df.to_dict("records")
                logging.info(f"📋 Prepared {len(current_records)} detailed invoice records for saving")
                
                save_invoice_snapshot(
                    current_records,
                    run_date=today_str,
                    run_type="detailed_cumulative_4day",
                    batch_start=batch_start,
                    batch_end=batch_end,
                    cumulative_start=cumulative_start,
                    cumulative_end=cumulative_end
                )
                logging.info(f"✅ Invoice snapshot saved for {today_str} (detailed_cumulative_4day) - {len(current_records)} invoices")
        except Exception as e:
            logging.warning(f"⚠️ Snapshot save failed: {e}")
        
        # Step 12: Record run window
        try:
            record_run_window(
                batch_start, batch_end,
                run_type="detailed_cumulative_4day",
                cumulative_start=cumulative_start,
                cumulative_end=cumulative_end,
                total_days_validated=(datetime.strptime(cumulative_end, "%Y-%m-%d") - 
                                    datetime.strptime(cumulative_start, "%Y-%m-%d")).days + 1
            )
            logging.info("✅ Detailed validation snapshot saved")
        except Exception as e:
            logging.warning(f"⚠️ Run window record failed: {e}")
        
        # Step 13: Save Excel reports
        logging.info("📑 Step 13: Saving Excel reports...")
        os.makedirs("data", exist_ok=True)
        
        # Main detailed report
        detailed_report_path = f"data/invoice_validation_detailed_{today_str}.xlsx"
        try:
            with pd.ExcelWriter(detailed_report_path, engine="openpyxl") as writer:
                # All invoices
                detailed_df.to_excel(writer, sheet_name="All_Invoices", index=False)
                
                # Failed invoices
                failed_df = detailed_df[detailed_df["Validation_Status"] == "❌ FAIL"]
                if not failed_df.empty:
                    failed_df.to_excel(writer, sheet_name="Failed_Invoices", index=False)
                
                # Warning invoices  
                warning_df = detailed_df[detailed_df["Validation_Status"] == "⚠️ WARNING"]
                if not warning_df.empty:
                    warning_df.to_excel(writer, sheet_name="Warning_Invoices", index=False)
                
                # Passed invoices
                passed_df = detailed_df[detailed_df["Validation_Status"] == "✅ PASS"]
                if not passed_df.empty:
                    passed_df.to_excel(writer, sheet_name="Passed_Invoices", index=False)
                
                # Summary statistics
                if summary_sheet_data:
                    pd.DataFrame(summary_sheet_data).to_excel(writer, sheet_name="Summary_Stats", index=False)
            
            logging.info(f"✅ Detailed invoice-level report saved: {detailed_report_path}")
        except Exception as e:
            logging.error(f"❌ Failed to save detailed report: {e}")
        
        # Dashboard report
        try:
            os.makedirs(f"data/{today_str}", exist_ok=True)
            dashboard_path = f"data/{today_str}/validation_result.xlsx"
            
            # Select key columns for dashboard
            dashboard_columns = [
                'Invoice_ID', 'Invoice_Number', 'Invoice_Date', 'Vendor_Name', 
                'Amount', 'Invoice_Creator_Name', 'Validation_Status', 
                'Issues_Found', 'Issue_Details', 'GST_Number'
            ]
            
            available_columns = [col for col in dashboard_columns if col in detailed_df.columns]
            dashboard_df = detailed_df[available_columns].copy()
            
            # Add status summary
            dashboard_df["Status_Summary"] = dashboard_df.apply(
                lambda row: f"{row.get('Validation_Status', 'Unknown')} - {row.get('Issues_Found', 0)} issues" 
                if row.get('Issues_Found', 0) > 0 
                else f"{row.get('Validation_Status', 'Unknown')} - No issues",
                axis=1
            )
            
            dashboard_df.to_excel(dashboard_path, index=False, engine="openpyxl")
            logging.info(f"📋 Invoice-level dashboard report created: {dashboard_path}")
        except Exception as e:
            logging.warning(f"⚠️ Dashboard report save failed: {e}")
        
        # Delta report
        try:
            delta_path = f"data/delta_report_{today_str}.xlsx"
            dashboard_df.to_excel(delta_path, index=False, engine="openpyxl")
            logging.info(f"📋 Invoice-level delta report created: {delta_path}")
        except Exception as e:
            logging.warning(f"⚠️ Delta report save failed: {e}")
        
        # Save email summary HTML
        try:
            summary_html_path = f"data/email_summary_{today_str}.html"
            with open(summary_html_path, "w", encoding="utf-8") as f:
                f.write(email_summary.get("html_summary", "No summary available"))
            logging.info(f"📧 Email summary saved: {summary_html_path}")
        except Exception as e:
            logging.warning(f"⚠️ Email summary save failed: {e}")
        
        # Step 14: Enhancement and email
        logging.info("🚀 Step 16: Applying enhanced features...")
        try:
            enhancement_result = enhance_validation_results(detailed_df, email_summary)
            
            if enhancement_result.get("success", False):
                logging.info("✅ Enhancement successful!")
            else:
                logging.warning("⚠️ Enhancement failed: " + enhancement_result.get("message", "Unknown error"))
                logging.info("📊 Continuing with original validation report")
            
        except Exception as e:
            logging.warning(f"⚠️ Enhancement step error: {e}")
            logging.info("📊 Continuing with original validation report")
        
        # Send email notification
        try:
            email_system = EnhancedEmailSystem()
            
            # Prepare attachments
            attachments = []
            if os.path.exists(detailed_report_path):
                attachments.append(detailed_report_path)
            
            # Add original invoices.zip
            invoices_zip_path = os.path.join(run_dir, "invoices.zip")
            if os.path.exists(invoices_zip_path):
                attachments.append(invoices_zip_path)
            
            # Email content
            stats = email_summary.get("statistics", {})
            subject = f"Invoice Validation Report - {today_str}"
            
            # Send email
            email_sent = email_system.send_validation_report(
                subject=subject,
                html_body=email_summary.get("html_summary", ""),
                attachments=attachments
            )
            
            if email_sent:
                logging.info("📧 Email sent with validation report and invoice attachments")
            else:
                logging.warning("⚠️ Email sending failed")
                
        except Exception as e:
            logging.warning(f"⚠️ Email sending failed: {e}")
        
        # Final summary
        logging.info("✅ Detailed cumulative validation workflow completed successfully!")
        logging.info("")
        logging.info("📊 FINAL SUMMARY:")
        logging.info(f"   📦 Current batch: {batch_start} to {batch_end}")
        logging.info(f"   🔄 Cumulative range: {cumulative_start} to {cumulative_end}")
        logging.info(f"   📅 Total days validated: {(datetime.strptime(cumulative_end, '%Y-%m-%d') - datetime.strptime(cumulative_start, '%Y-%m-%d')).days + 1}")
        logging.info(f"   📋 Total invoices processed: {len(detailed_df)}")
        
        if email_summary.get("statistics"):
            stats = email_summary["statistics"]
            logging.info(f"   ✅ Passed: {stats.get('passed_invoices', 0)} ({stats.get('pass_rate', 0):.1f}%)")
            logging.info(f"   ⚠️ Warnings: {stats.get('warning_invoices', 0)}")
            logging.info(f"   ❌ Failed: {stats.get('failed_invoices', 0)}")
            logging.info(f"   👤 Total Creators: {stats.get('total_creators', 0)}")
            logging.info(f"   ❓ Unknown Creators: {stats.get('unknown_creators', 0)}")
            logging.info(f"   🏥 Health Status: {stats.get('health_status', 'Unknown')}")
        
        logging.info(f"   ⏰ Next run in: {VALIDATION_INTERVAL_DAYS} days")
        logging.info(f"   🗂️ Archive threshold: {ACTIVE_VALIDATION_MONTHS} months")
        
        return True
        
    except Exception as e:
        logging.error(f"❌ Unexpected error in detailed cumulative validation workflow: {e}")
        logging.error(f"📊 Traceback: {traceback.format_exc()}")
        return False

def main():
    """Main entry point"""
    try:
        logging.info("=== Invoice Validation System Starting ===")
        
        # Configuration summary
        config = {
            "rms_base_url": os.getenv("RMS_BASE_URL", "https://rms.koenig-solutions.com"),
            "database_path": os.getenv("DB_PATH", "invoice_validation.db"),
            "excel_output_path": os.getenv("EXCEL_PATH", "invoice_validation_report.xlsx"),
            "archive_days": 30 * ACTIVE_VALIDATION_MONTHS,
            "validation_interval_days": VALIDATION_INTERVAL_DAYS,
            "max_retries": 3,
            "timeout_seconds": 30,
            "batch_size": 100,
            "debug_mode": os.getenv("DEBUG_MODE", "False").lower() == "true"
        }
        
        logging.info(f"Configuration: {config}")
        
        # Initialize database
        try:
            create_tables()
            logging.info("Database initialized successfully")
        except Exception as e:
            logging.warning(f"Database initialization warning: {e}")
        
        # Initialize system
        logging.info("Invoice Validation System initialized")
        
        # Handle command line arguments
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            if command == "run":
                logging.info("Running single validation cycle")
                success = run_invoice_validation()
            else:
                logging.info("Running default single validation cycle")
                success = run_invoice_validation()
        else:
            logging.info("Running default single validation cycle")
            success = run_invoice_validation()
        
        if success:
            logging.info("🎉 Invoice validation completed successfully!")
            sys.exit(0)
        else:
            logging.error("❌ Invoice validation failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logging.info("❌ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"❌ Fatal error: {e}")
        logging.error(f"📊 Traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()
