# main.py
# Complete workflow runner for RMS invoice validation + exact-format email report.

from __future__ import annotations

import os
import re
import glob
import json
import shutil
import logging
import traceback
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

import pandas as pd
from dotenv import load_dotenv

# --- External module imports from your repo (expected to exist) ---
from rms_scraper import rms_download
from validator_utils import validate_invoices
from updater import update_invoice_status
from reporter import save_snapshot_report
from invoice_tracker import (
    create_tables,
    save_invoice_snapshot,
    record_run_window,
    get_all_run_windows,
    get_last_run_date,
    get_first_validation_date,
    get_validation_date_ranges,
    archive_validation_records_before_date,
)

# --- Email system ---
from email_notifier import EnhancedEmailSystem

# ============== Logging bootstrap ==============
logger = logging.getLogger("invoice_validator")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

# ============== Environment & DB bootstrap ==============
load_dotenv()
create_tables()

# ============== Config ==============
VALIDATION_INTERVAL_DAYS = 4      # run validation every 4 days
VALIDATION_BATCH_DAYS    = 4      # each batch covers 4 days
ACTIVE_VALIDATION_MONTHS = 3      # maintain 3 months active data
ARCHIVE_FOLDER           = "archived_data"

# ============== Helpers ==============

def should_run_today() -> bool:
    """4-day cadence; override to always run by returning True at the top if desired."""
    # Force-run (comment next line if you want to revert to cadence logic)
    return True

    try:
        last_run = get_last_run_date()
        if not last_run:
            print("🆕 No previous runs found - running first validation")
            return True
        last_run_date = datetime.strptime(last_run, "%Y-%m-%d")
        days_since = (datetime.today() - last_run_date).days
        print(f"📅 Last run: {last_run}, Days since: {days_since}")
        if days_since >= VALIDATION_INTERVAL_DAYS:
            print(f"✅ Time to run validation (>= {VALIDATION_INTERVAL_DAYS} days)")
            return True
        print(f"⏳ Too early (need {VALIDATION_INTERVAL_DAYS - days_since} more days)")
        return False
    except Exception as e:
        print(f"⚠️ Schedule check error: {e}; defaulting to run")
        return True

def get_current_batch_dates() -> Tuple[str, str]:
    """Get the current 4-day batch: (start, end) as 'YYYY-MM-DD' strings; end is yesterday."""
    end = datetime.today() - timedelta(days=1)
    start = end - timedelta(days=VALIDATION_BATCH_DAYS - 1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

def get_cumulative_validation_range() -> Tuple[str, str]:
    """From first validation (but no older than 3 months) to current batch end."""
    try:
        first = get_first_validation_date()
        if not first:
            return get_current_batch_dates()
        first_dt = datetime.strptime(first, "%Y-%m-%d")
        three_months_ago = datetime.today() - timedelta(days=30 * ACTIVE_VALIDATION_MONTHS)
        start_dt = max(first_dt, three_months_ago)
        _, end_str = get_current_batch_dates()
        s = start_dt.strftime("%Y-%m-%d")
        print(f"📅 Cumulative validation range: {s} to {end_str}")
        return s, end_str
    except Exception as e:
        print(f"⚠️ Cumulative range calc error: {e}; using current batch")
        return get_current_batch_dates()

def archive_data_older_than_three_months() -> int:
    """Move files/folders older than 3 months into data/archived_data/* and mark DB records."""
    print(f"🗂️ Archiving validation data older than {ACTIVE_VALIDATION_MONTHS} months...")
    data_dir = "data"
    archive_base = os.path.join(data_dir, ARCHIVE_FOLDER)
    validation_archive = os.path.join(archive_base, "validation_reports")
    snapshot_archive = os.path.join(archive_base, "snapshots")
    daily_data_archive = os.path.join(archive_base, "daily_data")
    for d in (archive_base, validation_archive, snapshot_archive, daily_data_archive):
        os.makedirs(d, exist_ok=True)

    cutoff = datetime.today() - timedelta(days=30 * ACTIVE_VALIDATION_MONTHS)
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    print(f"📅 Archiving data older than: {cutoff_str}")
    archived = 0

    if not os.path.exists(data_dir):
        return archived

    # move dated report files
    for filename in os.listdir(data_dir):
        fp = os.path.join(data_dir, filename)
        if not os.path.isfile(fp):
            continue
        try:
            date_extracted = None
            for prefix in ("invoice_validation_detailed_", "validation_summary_", "delta_report_"):
                if filename.startswith(prefix) and filename.endswith(".xlsx"):
                    d = filename.replace(prefix, "").replace(".xlsx", "")
                    date_extracted = datetime.strptime(d, "%Y-%m-%d")
                    break
            if date_extracted and date_extracted < cutoff:
                shutil.move(fp, os.path.join(validation_archive, filename))
                print(f"📦 Archived report: {filename}")
                archived += 1
        except Exception as e:
            print(f"⚠️ Archive skip {filename}: {e}")

    # move daily folders
    for item in os.listdir(data_dir):
        if item == ARCHIVE_FOLDER:
            continue
        p = os.path.join(data_dir, item)
        if not os.path.isdir(p):
            continue
        try:
            folder_dt = datetime.strptime(item, "%Y-%m-%d")
            if folder_dt < cutoff:
                shutil.move(p, os.path.join(daily_data_archive, item))
                print(f"📦 Archived folder: {item}")
                archived += 1
        except ValueError:
            pass
        except Exception as e:
            print(f"⚠️ Archive folder error {item}: {e}")

    try:
        archive_validation_records_before_date(cutoff_str)
        print(f"✅ Database records archived before {cutoff_str}")
    except Exception as e:
        print(f"⚠️ DB archive mark failed: {e}")

    print(f"✅ Archiving completed. {archived} items archived to {archive_base}")
    return archived

def download_cumulative_data(start_str: str, end_str: str) -> str:
    """Kick off RMS download for the cumulative window; returns the run directory path."""
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")
    print(f"📥 Downloading cumulative validation data from {start_str} to {end_str}...")
    print(f"📊 Range covers: {(end_date - start_date).days + 1} days")
    return rms_download(start_date, end_date)

def validate_downloaded_files(run_dir):
    """
    Validate that required files exist in the run directory
    """
    try:
        logging.info(f"🔍 Step 5: Verifying files in directory: {run_dir}")
        
        # Define expected files
        expected_files = {
            'invoice_download.xls': 'Excel invoice data',
            'invoices.zip': 'ZIP invoice files'
        }
        
        missing_files = []
        found_files = []
        
        # Check each expected file
        for filename, description in expected_files.items():
            file_path = os.path.join(run_dir, filename)
            
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                found_files.append((filename, file_size))
                logging.info(f"✅ Found {filename}: {file_size} bytes")
                
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
        return False, [f"Validation error: {str(e)}"]
        
def read_invoice_file(invoice_file: str) -> pd.DataFrame:
    """Robust reader: try Excel engines, then CSV sniffing (handles tabular XLS-TSV export)."""
    print(f"🔍 Attempting to read file: {invoice_file}")
    if not os.path.exists(invoice_file):
        raise FileNotFoundError(invoice_file)
    p = Path(invoice_file)
    ext = p.suffix.lower()
    size = os.path.getsize(invoice_file)
    print(f"📄 File: {p.name}, Extension: {ext}, Size: {size} bytes")
    if size < 50:
        raise ValueError("File appears too small")

    try:
        with open(invoice_file, "rb") as f:
            header = f.read(50)
        print(f"🔍 File header (first 20 bytes): {header[:20]}")
    except Exception as e:
        print(f"⚠️ Could not read header: {e}")

    # Try openpyxl (xlsx)
    try:
        print("📊 Attempting to read as Excel with openpyxl engine...")
        df = pd.read_excel(invoice_file, engine="openpyxl")
        print(f"✅ Excel(openpyxl) read. Shape: {df.shape}")
        print(f"📋 Columns: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"⚠️ openpyxl engine failed: {e}")

    # Try xlrd (xls)
    if ext == ".xls":
        try:
            print("📊 Attempting to read as Excel with xlrd engine...")
            df = pd.read_excel(invoice_file, engine="xlrd")
            print(f"✅ Excel(xlrd) read. Shape: {df.shape}")
            print(f"📋 Columns: {list(df.columns)}")
            return df
        except Exception as e:
            print(f"⚠️ xlrd engine failed: {e}")

    # Try CSV sniff (often it's actually tab-separated)
    print("📄 Attempting to read as CSV...")
    for sep in [",", ";", "\t", "|"]:
        try:
            head = pd.read_csv(invoice_file, sep=sep, nrows=5)
            if head.shape[1] > 1:
                df = pd.read_csv(invoice_file, sep=sep)
                print(f"✅ Successfully read as CSV with separator '{sep}'. Shape: {df.shape}")
                print(f"📋 Columns: {list(df.columns)}")
                return df
        except Exception:
            continue
    print("⚠️ CSV reading failed with all separators")

    raise Exception("Could not read invoice file in any supported format")

def filter_invoices_by_date(df: pd.DataFrame, start_str: str, end_str: str) -> pd.DataFrame:
    """Filter by PurchaseInvDate in [start, end]."""
    try:
        if "PurchaseInvDate" not in df.columns:
            print("⚠️ PurchaseInvDate not found; returning all data")
            return df
        s = datetime.strptime(start_str, "%Y-%m-%d")
        e = datetime.strptime(end_str, "%Y-%m-%d")
        df = df.copy()
        df["ParsedInvoiceDate"] = pd.to_datetime(df["PurchaseInvDate"], errors="coerce")
        out = df[(df["ParsedInvoiceDate"] >= s) & (df["ParsedInvoiceDate"] <= e)]
        print(f"📅 Filtered invoices from {start_str} to {end_str}: {len(out)} out of {len(df)}")
        return out
    except Exception as e:
        print(f"⚠️ Date filtering failed: {e}; returning all data")
        return df

# ====== Mapping/Derivation helpers ======

GST_STATE_MAP = {
    "01":"Jammu & Kashmir","02":"Himachal Pradesh","03":"Punjab","04":"Chandigarh","05":"Uttarakhand",
    "06":"Haryana","07":"Delhi","08":"Rajasthan","09":"Uttar Pradesh","10":"Bihar","11":"Sikkim",
    "12":"Arunachal Pradesh","13":"Nagaland","14":"Manipur","15":"Mizoram","16":"Tripura","17":"Meghalaya",
    "18":"Assam","19":"West Bengal","20":"Jharkhand","21":"Odisha","22":"Chhattisgarh","23":"Madhya Pradesh",
    "24":"Gujarat","25":"Daman & Diu","26":"Dadra & Nagar Haveli and Daman & Diu","27":"Maharashtra",
    "28":"Andhra Pradesh (Old)","29":"Karnataka","30":"Goa","31":"Lakshadweep","32":"Kerala","33":"Tamil Nadu",
    "34":"Puducherry","35":"Andaman & Nicobar Islands","36":"Telangana","37":"Andhra Pradesh","38":"Ladakh"
}

def map_payment_method(payment_info) -> str:
    """Standardize payment method information"""
    if payment_info is None or (isinstance(payment_info, float) and pd.isna(payment_info)):
        return "Cash"
    
    payment_str = str(payment_info).lower().strip()
    
    # Define payment method mappings
    payment_mappings = {
        'Card Payment': ['card', 'credit', 'debit', 'visa', 'mastercard'],
        'Bank Transfer': ['bank', 'transfer', 'wire', 'neft', 'rtgs', 'imps'],
        'Cheque': ['cheque', 'check', 'dd', 'demand draft'],
        'Digital Payment': ['online', 'digital', 'upi', 'paytm', 'gpay', 'phonepe'],
        'Cash': ['cash', 'hand', 'direct']
    }
    
    # Check for matches
    for method, keywords in payment_mappings.items():
        if any(keyword in payment_str for keyword in keywords):
            return method
    
    return "Cash"

def _derive_payment_method(row) -> str:
    pieces = []
    for c in ("MOP","VoucherTypeName","Narration","PurchaseLEDGER","OtherLedger1","OtherLedger2","OtherLedger3"):
        v = row.get(c)
        if v is not None and str(v).strip():
            pieces.append(str(v))
    return map_payment_method(" ".join(pieces))

def _derive_account_head(row) -> str:
    if "A/C Head" in row and str(row.get("A/C Head") or "").strip():
        return str(row.get("A/C Head")).strip()
    for c in ("PurchaseLEDGER","Narration"):
        v = row.get(c)
        if v and str(v).strip():
            return str(v).strip()
    return ""

def _derive_location(row) -> str:
    for c in ("Location", "Branch", "State"):
        v = row.get(c)
        if isinstance(v, str) and v.strip():
            return v.strip()
    gst = str(row.get("GSTNO", "")).strip()
    m = re.match(r"^(\d{2})", gst)
    if m and m.group(1) in GST_STATE_MAP:
        return GST_STATE_MAP[m.group(1)]
    narr = str(row.get("Narration", ""))
    m = re.search(r"(?:Location|Loc)[:\- ]+([A-Za-z .]+)", narr, flags=re.I)
    if m:
        return m.group(1).strip().title()
    return ""

def _try_load_creator_map(run_dir: str) -> dict:
    creators = {}
    for p in glob.glob(os.path.join(run_dir, "*creator*.*")):
        try:
            if p.lower().endswith(".json"):
                with open(p, "r", encoding="utf-8") as f:
                    creators.update(json.load(f))
            else:
                cdf = pd.read_csv(p)
                key_col = next((c for c in cdf.columns if c.lower() in ("purchaseinvno","invid","voucherno","invoice_number")), None)
                val_col = next((c for c in cdf.columns if "creator" in c.lower()), None)
                if key_col and val_col:
                    creators.update(dict(zip(cdf[key_col].astype(str), cdf[val_col].astype(str))))
        except Exception as e:
            logging.warning(f"Creator map load failed for {p}: {e}")
    return creators

def _derive_creator(row, creators_map: dict) -> str:
    for k in [row.get("VoucherNo"), row.get("PurchaseInvNo"), row.get("InvID")]:
        k = str(k) if k is not None else ""
        if k and k in creators_map:
            return str(creators_map[k]).strip().title()
    narr = str(row.get("Narration", ""))
    m = re.search(r"(?:Inv(?:oice)?\s*Created\s*By|Created\s*By|Prepared\s*By|Maker|User)[:\- ]+([A-Za-z .]+)", narr, flags=re.I)
    if m:
        return m.group(1).strip().title()
    return "System Generated"

def find_creator_column(df: pd.DataFrame) -> Optional[str]:
    if "Inv Created By" in df.columns:
        return "Inv Created By"
    possible = [
        'CreatedBy','Created_By','InvoiceCreatedBy','Invoice_Created_By','UserName','User_Name',
        'CreatorName','Creator_Name','EntryBy','Entry_By','InputBy','Input_By',
        'PreparedBy','Prepared_By','MadeBy','Made_By'
    ]
    for c in possible:
        if c in df.columns:
            return c
    # case-insensitive fallback
    lower = {c.lower(): c for c in df.columns}
    for c in possible:
        if c.lower() in lower:
            return lower[c.lower()]
    # heuristic
    for c in df.columns:
        if any(w in c.lower() for w in ("create","by","user","entry","made","prepared")):
            return c
    return None

def validate_invoices_with_details(df: pd.DataFrame) -> Tuple[pd.DataFrame, list, pd.DataFrame]:
    """Augment each invoice with a pass/warn/fail and include Invoice_Creator_Name."""
    print("🔍 Running detailed invoice-level validation...")
    try:
        summary_issues, problematic = validate_invoices(df)
    except Exception as e:
        logging.warning(f"Base validation failed (continuing with detailed only): {e}")
        summary_issues, problematic = [], pd.DataFrame()

    creator_col = find_creator_column(df)
    detailed = []
    for idx, row in df.iterrows():
        invoice_id   = row.get("InvID", f"Row_{idx}")
        inv_num      = row.get("PurchaseInvNo", row.get("VoucherNo", ""))
        inv_date     = row.get("PurchaseInvDate", row.get("Voucherdate", ""))
        vendor       = row.get("PartyName", "")
        amount       = row.get("Total", 0)

        if creator_col:
            creator = str(row.get(creator_col, "")).strip() or "System Generated"
        else:
            creator = _derive_creator(row, {})

        issues = []
        status = "✅ PASS"

        # GST
        if pd.isna(row.get("GSTNO")) or str(row.get("GSTNO")).strip() == "":
            issues.append("Missing GST Number"); status = "❌ FAIL"
        # Amount
        if pd.isna(row.get("Total")) or str(row.get("Total")).strip() == "":
            issues.append("Missing Total Amount"); status = "❌ FAIL"
        else:
            try:
                val = float(row.get("Total") or 0)
                if val < 0:
                    issues.append(f"Negative Amount: {val}")
                    if status == "✅ PASS": status = "⚠️ WARNING"
            except Exception:
                issues.append("Invalid Amount Format"); status = "❌ FAIL"
        # Invoice number/date/vendor
        if not str(inv_num).strip():
            issues.append("Missing Invoice Number"); status = "❌ FAIL"
        if not str(inv_date).strip():
            issues.append("Missing Invoice Date"); status = "❌ FAIL"
        if not str(vendor).strip():
            issues.append("Missing Vendor Name"); status = "❌ FAIL"

        detailed.append({
            "Invoice_ID": invoice_id,
            "Invoice_Number": inv_num,
            "Invoice_Date": inv_date,
            "Vendor_Name": vendor,
            "Amount": amount,
            "Invoice_Creator_Name": creator,
            "Validation_Status": status,
            "Issues_Found": len(issues),
            "Issue_Details": " | ".join(issues) if issues else "No issues found",
            "GST_Number": row.get("GSTNO", ""),
            "Row_Index": idx + 1,
            "Validation_Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    detailed_df = pd.DataFrame(detailed)
    print(f"✅ Detailed validation completed on {len(detailed_df)} invoices")
    return detailed_df, summary_issues, problematic

def generate_email_summary_statistics(detailed_df: pd.DataFrame,
                                      cumulative_start: str, cumulative_end: str,
                                      batch_start: str, batch_end: str,
                                      today_str: str) -> dict:
    print("📧 Generating email summary statistics...")
    if detailed_df.empty:
        return {"html_summary": "

No invoice data.

", "text_summary": "No invoice data.", "statistics": {}}

    total = len(detailed_df)
    passed  = (detailed_df["Validation_Status"] == "✅ PASS").sum()
    warned  = (detailed_df["Validation_Status"] == "⚠️ WARNING").sum()
    failed  = (detailed_df["Validation_Status"] == "❌ FAIL").sum()
    pass_rate = (passed / total * 100) if total else 0

    creators = detailed_df["Invoice_Creator_Name"].value_counts()
    unknown = int(creators.get("Unknown", 0) + creators.get("", 0))

    html = EnhancedEmailSystem().create_professional_html_template(
        {"failed": int(failed), "warnings": int(warned), "passed": int(passed)},
        datetime.now() + timedelta(days=3)
    )
    text = f"Total: {total} | Passed: {passed} | Warnings: {warned} | Failed: {failed} | Pass rate: {pass_rate:.1f}%"

    stats = {
        "total_invoices": total,
        "passed_invoices": int(passed),
        "warning_invoices": int(warned),
        "failed_invoices": int(failed),
        "pass_rate": pass_rate,
        "total_creators": int(len(creators)),
        "unknown_creators": int(unknown),
        "validation_date": today_str,
        "current_batch_start": batch_start,
        "current_batch_end": batch_end,
        "cumulative_start": cumulative_start,
        "cumulative_end": cumulative_end,
        "total_coverage_days": (datetime.strptime(cumulative_end, "%Y-%m-%d") - datetime.strptime(cumulative_start, "%Y-%m-%d")).days + 1
    }
    print(f"✅ Email summary statistics generated:")
    print(f"   📊 Health Status: {'Needs Attention' if pass_rate < 80 else 'Good'} ({pass_rate:.1f}%)")
    print(f"   📈 Total Issues: {len([x for x in [failed, warned] if x > 0])} types identified")
    print(f"   👤 Creator Stats: {len(creators)} total, {unknown} unknown")
    return {"html_summary": html, "text_summary": text, "statistics": stats}

def generate_detailed_validation_report(detailed_df: pd.DataFrame, today_str: str) -> List[dict]:
    print("📋 Generating detailed validation report for Excel export...")
    if detailed_df.empty:
        return []
    total = len(detailed_df)
    passed  = (detailed_df["Validation_Status"] == "✅ PASS").sum()
    warned  = (detailed_df["Validation_Status"] == "⚠️ WARNING").sum()
    failed  = (detailed_df["Validation_Status"] == "❌ FAIL").sum()
    print(f"✅ Detailed validation report prepared with 6 summary entries")
    return [
        {"Report_Type": "Overall_Summary", "Description": "Total Invoice Count", "Count": total, "Percentage": "100.0%", "Status": "INFO"},
        {"Report_Type": "Overall_Summary", "Description": "Passed Validation",   "Count": int(passed), "Percentage": f"{(passed/total*100):.1f}%", "Status": "PASS"},
        {"Report_Type": "Overall_Summary", "Description": "Warnings",            "Count": int(warned), "Percentage": f"{(warned/total*100):.1f}%", "Status": "WARNING"},
        {"Report_Type": "Overall_Summary", "Description": "Failed Validation",   "Count": int(failed), "Percentage": f"{(failed/total*100):.1f}%", "Status": "FAIL"},
        {"Report_Type": "Data_Quality", "Description": "Data Completeness", "Count": int(passed + warned), "Percentage": f"{((passed+warned)/total*100):.1f}%", "Status": "INFO"},
        {"Report_Type": "Process_Health", "Description": "System Health Score", "Count": int(pass_rate if total else 0), "Percentage": f"{(passed/total*100):.1f}%", "Status": "HEALTH"}
    ]

def enhance_validation_results(detailed_df: pd.DataFrame, email_summary: dict) -> dict:
    """
    Enhance validation results with additional insights and formatting
    """
    try:
        logging.info("🔧 Enhancing validation results...")
        
        # Get basic statistics
        total_invoices = len(detailed_df) if detailed_df is not None else 0
        
        # Create enhanced summary
        enhanced_summary = {
            'total_invoices': total_invoices,
            'validation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'email_summary': email_summary,
            'enhancement_applied': True,
            'system_status': 'operational'
        }
        
        if detailed_df is not None and len(detailed_df) > 0:
            # Calculate validation statistics
            validation_col = None
            for col in detailed_df.columns:
                if 'validation' in col.lower() or 'result' in col.lower() or 'status' in col.lower():
                    validation_col = col
                    break
            
            if validation_col:
                pass_count = len(detailed_df[detailed_df[validation_col].str.contains('PASS|pass', case=False, na=False)])
                fail_count = len(detailed_df[detailed_df[validation_col].str.contains('FAIL|fail', case=False, na=False)])
                warning_count = len(detailed_df[detailed_df[validation_col].str.contains('WARN|warning', case=False, na=False)])
            else:
                pass_count = 0
                fail_count = total_invoices  # Assume all failed if no validation column
                warning_count = 0
            
            # Calculate financial impact if amount column exists
            total_amount = 0
            amount_col = None
            for col in detailed_df.columns:
                if 'total' in col.lower() or 'amount' in col.lower():
                    amount_col = col
                    break
            
            if amount_col:
                try:
                    detailed_df[amount_col] = pd.to_numeric(detailed_df[amount_col], errors='coerce')
                    total_amount = detailed_df[amount_col].fillna(0).sum()
                except:
                    total_amount = 0
            
            enhanced_summary.update({
                'pass_count': pass_count,
                'fail_count': fail_count,
                'warning_count': warning_count,
                'pass_rate': (pass_count / total_invoices * 100) if total_invoices > 0 else 0,
                'total_amount': total_amount,
                'validation_column': validation_col,
                'amount_column': amount_col
            })
            
            # Add enhancement flag to dataframe
            if 'enhancement_status' not in detailed_df.columns:
                detailed_df['enhancement_status'] = 'enhanced'
                detailed_df['enhancement_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        logging.info(f"✅ Enhancement completed: {total_invoices} invoices processed")
        logging.info(f"📊 Pass rate: {enhanced_summary.get('pass_rate', 0):.1f}%")
        
        return enhanced_summary
        
    except Exception as e:
        logging.error(f"❌ Enhancement error: {e}")
        logging.error(f"📊 Traceback: {traceback.format_exc()}")
        
        # Return basic summary on error
        return {
            'total_invoices': len(detailed_df) if detailed_df is not None else 0,
            'validation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'email_summary': email_summary if email_summary else {},
            'enhancement_applied': False,
            'error': str(e),
            'system_status': 'degraded'
        }

# ============== MAIN RUNNER ==============

def run_invoice_validation() -> bool:
    try:
        today = datetime.today()
        today_str = today.strftime("%Y-%m-%d")

        print(f"🚀 Starting DETAILED cumulative validation workflow for {today_str}")
        print("📧 Email: exact-format attachment + real invoices.zip (bundled)")
        print(f"⚙️ Config: every {VALIDATION_INTERVAL_DAYS} days (batch {VALIDATION_BATCH_DAYS} days), active {ACTIVE_VALIDATION_MONTHS} months")

        # Step 1
        print("🔍 Step 1: Check schedule…")
        if not should_run_today():
            print("⏳ Skipping – not time yet")
            return True

        # Step 2
        print("🗂️ Step 2: Archive >3 months…")
        try:
            archive_data_older_than_three_months()
        except Exception as e:
            print(f"⚠️ Archiving failed (continuing): {e}")

        # Step 3
        print("📊 Step 3: Compute ranges…")
        cumulative_start, cumulative_end = get_cumulative_validation_range()
        batch_start, batch_end = get_current_batch_dates()
        print(f"📅 Current batch: {batch_start} → {batch_end}")
        print(f"📅 Cumulative: {cumulative_start} → {cumulative_end}")

        # Step 4
        print("📥 Step 4: RMS download…")
        run_dir = download_cumulative_data(cumulative_start, cumulative_end)
        print(f"✅ Download path: {run_dir}")

        # Step 5
        print(f"🔍 Step 5: Validate files in {run_dir} …")
        validation_success, file_details = validate_downloaded_files(run_dir)
        
        if not validation_success:
            logging.error(f"❌ File validation failed: {file_details}")
            logging.error("❌ Aborting: Required files missing")
            return False
        
        logging.info(f"✅ All required files found: {file_details}")

        # Step 7
        print("📊 Step 7: Reading cumulative invoice data...")
        invoice_path = os.path.join(run_dir, "invoice_download.xls")
        src_df = read_invoice_file(invoice_path)
        if src_df is None or src_df.empty:
            print("❌ Empty dataframe after read")
            return False
        print(f"✅ Successfully loaded cumulative data. Shape: {src_df.shape}")
        print(f"📋 Columns: {list(src_df.columns)}")

        # Step 8
        print("🔄 Step 8: Filtering to cumulative validation range...")
        filtered_df = filter_invoices_by_date(src_df, cumulative_start, cumulative_end)
        print(f"📅 Working with {len(filtered_df)} invoices in cumulative range")

        # Step 9
        print("🔄 Step 9: Running detailed validation on cumulative data...")
        print("   🔄 This includes:")
        print(f"      📦 Current batch: {batch_start} to {batch_end}")
        print(f"      🔄 ALL previously validated data from: {cumulative_start}")
        detailed_df, summary_issues, problematic_df = validate_invoices_with_details(filtered_df)

        # Step 10
        print("📧 Step 10: Generating email summary statistics...")
        email_summary = generate_email_summary_statistics(
            detailed_df, cumulative_start, cumulative_end, batch_start, batch_end, today_str
        )

        # Step 11
        print("📋 Step 11: Generating detailed validation report...")
        summary_sheet_rows = generate_detailed_validation_report(detailed_df, today_str)

        # Step 12: Preparing invoice data for saving
        print("💾 Step 12: Preparing invoice data for saving...")
        current_records = detailed_df.to_dict("records") if not detailed_df.empty else []
        print(f"📋 Prepared {len(current_records)} detailed invoice records for saving")
        
        try:
            save_invoice_snapshot(
                current_records,
                run_date=today_str,
                run_type="detailed_cumulative_4day",
                batch_start=batch_start,
                batch_end=batch_end,
                cumulative_start=cumulative_start,
                cumulative_end=cumulative_end
            )
            print(f"✅ Invoice snapshot saved for {today_str} (detailed_cumulative_4day) - {len(current_records)} invoices")
            print("✅ Detailed validation snapshot saved")
        except Exception as e:
            print(f"⚠️ Snapshot save failed: {e}")

        try:
            record_run_window(
                batch_start, batch_end,
                run_type="detailed_cumulative_4day",
                cumulative_start=cumulative_start,
                cumulative_end=cumulative_end,
                total_days_validated=(datetime.strptime(cumulative_end, "%Y-%m-%d") - datetime.strptime(cumulative_start, "%Y-%m-%d")).days + 1
            )
            print(f"✅ Run window recorded: {batch_start} to {batch_end} (detailed_cumulative_4day)")
            print("✅ Detailed cumulative run recorded")
        except Exception as e:
            print(f"⚠️ Run window record failed: {e}")

        # Save reports
        os.makedirs("data", exist_ok=True)
        detailed_report_path = f"data/invoice_validation_detailed_{today_str}.xlsx"
        with pd.ExcelWriter(detailed_report_path, engine="openpyxl") as writer:
            detailed_df.to_excel(writer, sheet_name="All_Invoices", index=False)
            failed_df = detailed_df[detailed_df["Validation_Status"] == "❌ FAIL"]
            if not failed_df.empty:
                failed_df.to_excel(writer, sheet_name="Failed_Invoices", index=False)
            warn_df = detailed_df[detailed_df["Validation_Status"] == "⚠️ WARNING"]
            if not warn_df.empty:
                warn_df.to_excel(writer, sheet_name="Warning_Invoices", index=False)
            pass_df = detailed_df[detailed_df["Validation_Status"] == "✅ PASS"]
            if not pass_df.empty:
                pass_df.to_excel(writer, sheet_name="Passed_Invoices", index=False)
            if summary_sheet_rows:
                pd.DataFrame(summary_sheet_rows).to_excel(writer, sheet_name="Summary_Stats", index=False)
        print(f"✅ Detailed invoice-level report saved: {detailed_report_path}")

        os.makedirs(f"data/{today_str}", exist_ok=True)
        dashboard_path = f"data/{today_str}/validation_result.xlsx"
        keep_cols = ['Invoice_ID','Invoice_Number','Invoice_Date','Vendor_Name','Amount','Invoice_Creator_Name',
                     'Validation_Status','Issues_Found','Issue_Details','GST_Number']
        dashboard_df = detailed_df[keep_cols].copy()
        dashboard_df["Status_Summary"] = dashboard_df.apply(
            lambda r: f"{r['Validation_Status']} - {r['Issues_Found']} issues" if r['Issues_Found'] > 0 else f"{r['Validation_Status']} - No issues",
            axis=1
        )
        dashboard_df.to_excel(dashboard_path, index=False, engine="openpyxl")
        print(f"📋 Invoice-level dashboard report created: {dashboard_path}")

        delta_path = f"data/delta_report_{today_str}.xlsx"
        dashboard_df.to_excel(delta_path, index=False, engine="openpyxl")
        print(f"📋 Invoice-level delta report created: {delta_path}")

        summary_html_path = f"data/email_summary_{today_str}.html"
        with open(summary_html_path, "w", encoding="utf-8") as f:
            f.write(email_summary["html_summary"])
        print(f"📧 Email summary saved: {summary_html_path}")

        # Step 16: Enhance + exact-format final + email
        print("🚀 Step 16: Applying enhanced features...")
        try:
            enhancement_result = enhance_validation_results(detailed_df, email_summary)
            if enhancement_result.get('enhancement_applied'):
                print("✅ Enhancement successful!")
            else:
                print(f"⚠️ Enhancement failed: {enhancement_result.get('error', 'Unknown error')}")
        except Exception as e:
            logging.error(f"⚠️ Enhancement step error: {e}")
            print("📊 Continuing with original validation report")

        # Email sending
        try:
            from email_notifier import EnhancedEmailSystem
            email_system = EnhancedEmailSystem()
            
            # Prepare email data
            stats = email_summary.get("statistics", {})
            validation_data = {
                "failed": stats.get("failed_invoices", 0),
                "warnings": stats.get("warning_invoices", 0),
                "passed": stats.get("passed_invoices", 0)
            }
            
            deadline_date = datetime.now() + timedelta(days=3)
            html_body = email_system.create_professional_html_template(validation_data, deadline_date)
            
            # Create and attach files
            zip_file = email_system.create_invoice_zip(validation_period=f"{cumulative_start}_to_{cumulative_end}")
            
            subject = f"🚨 URGENT: Invoice Validation Report - Action Required by {deadline_date.strftime('%B %d, %Y')}"
            
            if email_system.default_recipients:
                success = email_system.send_email_with_attachments(
                    email_system.default_recipients,
                    subject,
                    html_body,
                    zip_file
                )
                
                if success:
                    print("📧 Professional email sent successfully with validation report and invoice attachments!")
                else:
                    print("⚠️ Email sending failed")
            else:
                print("⚠️ No email recipients configured")
                
        except Exception as e:
            print(f"⚠️ Email sending failed: {e}")

        print("✅ Detailed cumulative validation workflow completed successfully!")
        
        # Final summary to console
        print("")
        print("📊 FINAL SUMMARY:")
        print(f"   📦 Current batch: {batch_start} to {batch_end}")
        print(f"   🔄 Cumulative range: {cumulative_start} to {cumulative_end}")
        print(f"   📅 Total days validated: {(datetime.strptime(cumulative_end, '%Y-%m-%d') - datetime.strptime(cumulative_start, '%Y-%m-%d')).days + 1}")
        print(f"   📋 Total invoices processed: {len(detailed_df)}")
        if email_summary.get("statistics"):
            stats = email_summary["statistics"]
            print(f"   ✅ Passed: {stats.get('passed_invoices',0)} ({stats.get('pass_rate',0):.1f}%)")
            print(f"   ⚠️ Warnings: {stats.get('warning_invoices',0)}")
            print(f"   ❌ Failed: {stats.get('failed_invoices',0)}")
            print(f"   👤 Total Creators: {stats.get('total_creators',0)}")
            print(f"   ❓ Unknown Creators: {stats.get('unknown_creators',0)}")
            print(f"   🏥 Health Status: {'Needs Attention' if stats.get('pass_rate',0) < 80 else 'Good'}")
        print(f"   ⏰ Next run in: {VALIDATION_INTERVAL_DAYS} days")
        print(f"   🗂️ Archive threshold: {ACTIVE_VALIDATION_MONTHS} months")
        return True

    except Exception as e:
        logging.error(f"❌ Unexpected error in detailed cumulative validation workflow: {e}")
        logging.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    ok = run_invoice_validation()
    if not ok:
        print("❌ Detailed cumulative validation failed!")
        raise SystemExit(1)
    print("🎉 Detailed cumulative validation completed successfully!")
    raise SystemExit(0)
