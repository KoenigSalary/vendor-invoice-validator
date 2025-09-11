from __future__ import annotations

# main.py
# Complete workflow runner for RMS invoice validation + exact-format email report.

import os
import re
import glob
import json
import shutil
import sys
import zipfile
import tempfile
import logging
import traceback
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

import pandas as pd
from dotenv import load_dotenv

# --- External module imports from your repo (expected to exist) ---
from rms_scraper import rms_download  # returns either run folder OR full path to invoice_download.xls
from validator_utils import validate_invoices  # returns (summary_issues, problematic_df)
from updater import update_invoice_status  # (kept for parity, not invoked directly below)
from reporter import save_snapshot_report  # (kept for parity, not invoked directly below)
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
from email_notifier import EnhancedEmailSystem, EmailNotifier

# ============== Logging bootstrap ==============
logger = logging.getLogger("invoice_validator")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

# ============== Environment & DB bootstrap ==============
if os.getenv("GITHUB_ACTIONS") != "true":
    load_dotenv()
    
# ============== Config ==============
VALIDATION_INTERVAL_DAYS = 4
VALIDATION_BATCH_DAYS    = 4
ACTIVE_VALIDATION_MONTHS = 3
ARCHIVE_FOLDER           = "archived_data"
SEND_EMAIL               = os.getenv("SEND_EMAIL", "0") == "1"   # OFF by default

# ============== Environment Check Function ==============
def check_environment():
    """Check if all required environment variables and modules are available"""
    print("🔍 Checking environment setup...")
    
    # Check required environment variables - support both naming conventions
    rms_user = os.getenv('RMS_USER') or os.getenv('RMS_USERNAME')
    rms_pass = os.getenv('RMS_PASS') or os.getenv('RMS_PASSWORD')
    rms_base_url = os.getenv('RMS_BASE_URL')
    
    missing_env = []
    if not rms_user:
        missing_env.append('RMS_USER or RMS_USERNAME')
    if not rms_pass:
        missing_env.append('RMS_PASS or RMS_PASSWORD')
    if not rms_base_url:
        missing_env.append('RMS_BASE_URL')
    
    if missing_env:
        print(f"❌ Missing required environment variables: {missing_env}")
        return False
    
    # Check optional but recommended variables
    optional_env = ['SMTP_USER', 'SMTP_PASS', 'AP_TEAM_EMAIL_LIST']
    missing_optional = [var for var in optional_env if not os.getenv(var)]
    
    if missing_optional:
        print(f"⚠️ Missing optional environment variables (email disabled): {missing_optional}")
    
    # Test module imports
    print("🔍 Testing module imports...")
    try:
        from rms_scraper import rms_download
        print("✅ rms_scraper imported successfully")
    except ImportError as e:
        print(f"❌ rms_scraper import failed: {e}")
        return False
        
    try:
        from validator_utils import validate_invoices
        print("✅ validator_utils imported successfully")
    except ImportError as e:
        print(f"❌ validator_utils import failed: {e}")
        return False
        
    try:
        from email_notifier import EnhancedEmailSystem, EmailNotifier
        print("✅ email_notifier imported successfully")
    except ImportError as e:
        print(f"❌ email_notifier import failed: {e}")
        return False
    
    print("✅ Environment check passed")
    print(f"🔧 Using RMS credentials: RMS_USER={'✅' if rms_user else '❌'}, RMS_PASS={'✅' if rms_pass else '❌'}")
    return True
    
# ============== Helpers ==============

def should_run_today() -> bool:
    # Force-run; keep cadence code below for reference if you want to re-enable later
    return True

def get_current_batch_dates() -> Tuple[str, str]:
    end = datetime.today() - timedelta(days=1)
    start = end - timedelta(days=VALIDATION_BATCH_DAYS - 1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

def get_cumulative_validation_range() -> Tuple[str, str]:
    try:
        first = get_first_validation_date()
        if not first:
            return get_current_batch_dates()
        first_dt = datetime.strptime(first, "%Y-%m-%d")
        three_months_ago = datetime.today() - timedelta(days=30 * ACTIVE_VALIDATION_MONTHS)
        start_dt = max(first_dt, three_months_ago)
        _, end_str = get_current_batch_dates()
        return start_dt.strftime("%Y-%m-%d"), end_str
    except Exception:
        return get_current_batch_dates()

def archive_data_older_than_three_months() -> int:
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
    archived = 0

    if os.path.isdir(data_dir):
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
                    archived += 1
            except Exception:
                pass

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
                    archived += 1
            except Exception:
                pass

    try:
        archive_validation_records_before_date(cutoff_str)
    except Exception:
        pass

    print(f"✅ Archiving completed. {archived} items archived to {archive_base}")
    return archived

def download_cumulative_data(start_str: str, end_str: str) -> str:
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")
    print(f"📥 Downloading cumulative validation data from {start_str} to {end_str}...")
    print(f"📊 Range covers: {(end_date - start_date).days + 1} days")
    return rms_download(start_date, end_date)

def _normalize_run_dir(run_path: str) -> Tuple[str, str]:
    """Accept directory OR full file path; return (run_dir, invoice_path)."""
    if not run_path:
        raise ValueError("Empty run_path provided")
    
    try:
        # Convert to absolute path
        abs_path = os.path.abspath(run_path)
        
        if os.path.isdir(abs_path):
            # It's a directory - look for invoice file
            run_dir = abs_path
            
            # Try multiple possible invoice file names
            possible_files = [
                "invoice_download.xls",
                "invoice_download.xlsx", 
                "invoices.xls",
                "invoices.xlsx",
                "export.xls",
                "export.xlsx"
            ]
            
            invoice_path = None
            for filename in possible_files:
                candidate = os.path.join(run_dir, filename)
                if os.path.exists(candidate):
                    invoice_path = candidate
                    break
            
            # Default to expected name even if not found yet
            if not invoice_path:
                invoice_path = os.path.join(run_dir, "invoice_download.xls")
            
            return run_dir, invoice_path
            
        elif os.path.isfile(abs_path):
            # It's a file - extract directory and use the file
            return os.path.dirname(abs_path), abs_path
        else:
            # Path doesn't exist yet - assume it will be a directory
            return abs_path, os.path.join(abs_path, "invoice_download.xls")
            
    except Exception as e:
        logging.error(f"Error normalizing path '{run_path}': {e}")
        # Return safe defaults
        return str(run_path), os.path.join(str(run_path), "invoice_download.xls")

def validate_downloaded_files(run_dir: str) -> Tuple[bool, List[str]]:
    """
    Only require invoice_download.xls.
    invoices.zip is OPTIONAL (logged if missing).
    """
    try:
        logging.info(f"🔍 Step 5: Verifying files in directory: {run_dir}")

        required = ["invoice_download.xls"]
        optional = ["invoices.zip"]

        missing_req, found = [], []

        for name in required:
            p = os.path.join(run_dir, name)
            if os.path.exists(p):
                found.append(f"{name} ({os.path.getsize(p)} bytes)")
                logging.info(f"✅ Found {name}")
            else:
                missing_req.append(name)
                logging.error(f"❌ Missing required file: {name}")

        for name in optional:
            p = os.path.join(run_dir, name)
            if os.path.exists(p):
                logging.info(f"ℹ️ Optional file present: {name}")
            else:
                logging.warning(f"ℹ️ Optional file missing (ok): {name}")

        if missing_req:
            return False, missing_req
        return True, found
    except Exception as e:
        logging.error(f"❌ File validation error: {e}")
        return False, [f"Validation error: {str(e)}"]

def read_invoice_file(invoice_file: str) -> pd.DataFrame:
    print(f"🔍 Attempting to read file: {invoice_file}")
    if not os.path.exists(invoice_file):
        raise FileNotFoundError(invoice_file)
    p = Path(invoice_file)
    ext = p.suffix.lower()
    try:
        with open(invoice_file, "rb") as f:
            f.read(32)
    except Exception:
        pass

    # Try openpyxl (xlsx)
    try:
        df = pd.read_excel(invoice_file, engine="openpyxl")
        return df
    except Exception:
        pass

    # Try xlrd (xls)
    if ext == ".xls":
        try:
            df = pd.read_excel(invoice_file, engine="xlrd")
            return df
        except Exception:
            pass

    # Try CSV sniff
    for sep in [",", ";", "\t", "|"]:
        try:
            head = pd.read_csv(invoice_file, sep=sep, nrows=5)
            if head.shape[1] > 1:
                return pd.read_csv(invoice_file, sep=sep)
        except Exception:
            continue

    # Try HTML
    try:
        tables = pd.read_html(invoice_file, flavor="lxml")
        if tables:
            return tables[0]
    except Exception:
        pass

    # Last resort: empty df with no rows (so pipeline still completes)
    logging.warning("⚠️ Could not parse file reliably; proceeding with empty DataFrame.")
    return pd.DataFrame()

def filter_invoices_by_date(df: pd.DataFrame, start_str: str, end_str: str) -> pd.DataFrame:
    try:
        if "PurchaseInvDate" not in df.columns:
            return df
        s = datetime.strptime(start_str, "%Y-%m-%d")
        e = datetime.strptime(end_str, "%Y-%m-%d")
        out = df.copy()
        out["ParsedInvoiceDate"] = pd.to_datetime(out["PurchaseInvDate"], errors="coerce")
        return out[(out["ParsedInvoiceDate"] >= s) & (out["ParsedInvoiceDate"] <= e)]
    except Exception:
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

_MOP_ALIASES = [
    "MOP","Mop","ModeOfPayment","Mode Of Payment","Mode_of_Payment",
    "Payment Mode","Payment_Mode","PaymentType","Payment Type"
]

def _try_load_creator_map(run_dir: str) -> dict:
    creators = {}
    patterns = ["*creator*.*", "*created_by*.*", "*inv_created*.*"]
    for patt in patterns:
        for p in glob.glob(os.path.join(run_dir, patt)):
            try:
                if p.lower().endswith(".json"):
                    with open(p, "r", encoding="utf-8") as f:
                        creators.update(json.load(f))
                else:
                    cdf = pd.read_csv(p)
                    key_col = next((c for c in cdf.columns if c.lower() in ("purchaseinvno","invid","voucherno","invoice_number")), None)
                    val_col = next((c for c in cdf.columns if "creator" in c.lower() or "created" in c.lower()), None)
                    if key_col and val_col:
                        creators.update(dict(zip(cdf[key_col].astype(str), cdf[val_col].astype(str))))
            except Exception:
                pass
    return creators

def map_payment_method(payment_info) -> str:
    if payment_info is None or (isinstance(payment_info, float) and pd.isna(payment_info)):
        return ""
    s = str(payment_info).lower()
    if re.search(r"\b(neft|rtgs|imps|wire|bank\s*transfer)\b", s): return "Bank Transfer"
    if re.search(r"\bupi|gpay|phonepe|paytm|wallet|online\b", s):   return "Digital Payment"
    if re.search(r"\b(card|visa|mastercard|amex|pos)\b", s):        return "Card Payment"
    if re.search(r"\bcheque|check|dd|demand\s*draft\b", s):         return "Cheque"
    if re.search(r"\bcash|petty\s*cash\b", s):                      return "Cash"
    return s.strip().title()

def _derive_payment_method(row) -> str:
    for c in _MOP_ALIASES:
        if c in row and str(row.get(c) or "").strip():
            return map_payment_method(row.get(c))
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

def _derive_creator(row, creators_map: dict) -> str:
    for k in [row.get("VoucherNo"), row.get("PurchaseInvNo"), row.get("InvID")]:
        k = str(k) if k is not None else ""
        if k and k in creators_map:
            return str(creators_map[k]).strip().title()
    for col in ["Inv Created By","CreatedBy","Created_By","InvoiceCreatedBy","Invoice_Created_By","UserName","User_Name",
                "CreatorName","Creator_Name","EntryBy","Entry_By","InputBy","Input_By","PreparedBy","Prepared_By",
                "MadeBy","Made_By","Created By","Entered By"]:
        if col in row and str(row.get(col) or "").strip():
            return str(row.get(col)).strip().title()
    narr = str(row.get("Narration", ""))
    m = re.search(r"(?:Inv(?:oice)?\s*Created\s*By|Created\s*By|Prepared\s*By|Maker|User)[:\- ]+([A-Za-z .]+)", narr, flags=re.I)
    if m:
        return m.group(1).strip().title()
    return "System Generated"

def _derive_scid(row) -> str:
    for c in ("SCID#", "SCID", "Scid", "scid"):
        v = row.get(c)
        if isinstance(v, str) and v.strip():
            return v.strip()
    narr = str(row.get("Narration", ""))
    m = re.search(r"\bSCID[#:]?\s*([A-Za-z0-9\-_/]+)", narr, flags=re.I)
    return m.group(1).strip() if m else ""

def _derive_due_date(row) -> str:
    for c in ("DueDate","Due Date","InvoiceDueDate","Invoice_Due_Date"):
        if c in row and str(row.get(c) or "").strip():
            return str(row.get(c)).strip()
    narr = str(row.get("Narration",""))
    m = re.search(r"(?:Due\s*Date|Payment\s*Due)\s*[:\-]?\s*([0-9]{1,2}[-/][0-9]{1,2}[-/][0-9]{2,4})", narr, re.I)
    return m.group(1).strip() if m else ""

def _due_date_notification(due_date_str: str) -> str:
    if not str(due_date_str).strip():
        return ""
    for fmt in ("%d-%m-%Y","%d/%m/%Y","%Y-%m-%d","%d-%b-%Y","%d/%b/%Y","%m-%d-%Y","%m/%d/%Y"):
        try:
            dd = datetime.strptime(str(due_date_str).strip(), fmt)
            break
        except Exception:
            dd = None
    if not dd:
        return ""
    today = datetime.today().date()
    d = (dd.date() - today).days
    if d < 0:  return f"Overdue by {-d} day(s)"
    if d == 0: return "Due today"
    if d <= 7: return f"Due in {d} day(s)"
    return ""

def _pick_key(df: pd.DataFrame) -> Optional[str]:
    for k in ["InvID","VoucherNo","PurchaseInvNo"]:
        if k in df.columns:
            return k
    return None

# ====== Validation-detail builder ======

def find_creator_column(df: pd.DataFrame) -> Optional[str]:
    if "Inv Created By" in df.columns:
        return "Inv Created By"
    candidates = ['CreatedBy','Created_By','InvoiceCreatedBy','Invoice_Created_By','UserName','User_Name',
                  'CreatorName','Creator_Name','EntryBy','Entry_By','InputBy','Input_By',
                  'PreparedBy','Prepared_By','MadeBy','Made_By']
    for c in candidates:
        if c in df.columns:
            return c
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    for c in df.columns:
        if any(w in c.lower() for w in ("create","by","user","entry","made","prepared")):
            return c
    return None

def validate_invoices_with_details(df: pd.DataFrame) -> Tuple[pd.DataFrame, list, pd.DataFrame]:
    print("🔍 Running detailed invoice-level validation…")
    if df is None or df.empty:
        # produce an empty but well-typed frame with expected columns
        cols = ["Invoice_ID","Invoice_Number","Invoice_Date","Vendor_Name","Amount",
                "Invoice_Creator_Name","Validation_Status","Issues_Found","Issue_Details",
                "GST_Number","Row_Index","Validation_Date"]
        return pd.DataFrame(columns=cols), [], pd.DataFrame()

    try:
        summary_issues, problematic = validate_invoices(df)
    except Exception as e:
        logger.warning(f"Base validation failed (continuing with detailed only): {e}")
        summary_issues, problematic = [], pd.DataFrame()

    creators_map = _try_load_creator_map(".")
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
            creator = _derive_creator(row, creators_map)

        issues = []
        status = "✅ PASS"

        if pd.isna(row.get("GSTNO")) or str(row.get("GSTNO")).strip() == "":
            issues.append("Missing GST Number"); status = "❌ FAIL"
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

    return pd.DataFrame(detailed), summary_issues, problematic

def generate_email_summary_statistics(detailed_df: pd.DataFrame,
                                      cumulative_start: str, cumulative_end: str,
                                      batch_start: str, batch_end: str,
                                      today_str: str) -> dict:
    total = len(detailed_df) if detailed_df is not None else 0
    passed  = int((detailed_df["Validation_Status"] == "✅ PASS").sum()) if total else 0
    warned  = int((detailed_df["Validation_Status"] == "⚠️ WARNING").sum()) if total else 0
    failed  = int((detailed_df["Validation_Status"] == "❌ FAIL").sum()) if total else 0
    pass_rate = (passed / total * 100) if total else 0.0

    html = EnhancedEmailSystem().create_professional_html_template(
        {"failed": failed, "warnings": warned, "passed": passed},
        datetime.now() + timedelta(days=3)
    )
    text = f"Total: {total} | Passed: {passed} | Warnings: {warned} | Failed: {failed} | Pass rate: {pass_rate:.1f}%"

    return {"html_summary": html, "text_summary": text, "statistics": {
        "total_invoices": total, "passed_invoices": passed, "warning_invoices": warned, "failed_invoices": failed,
        "pass_rate": pass_rate, "validation_date": today_str,
        "current_batch_start": batch_start, "current_batch_end": batch_end,
        "cumulative_start": cumulative_start, "cumulative_end": cumulative_end,
        "total_coverage_days": (datetime.strptime(cumulative_end, "%Y-%m-%d") - datetime.strptime(cumulative_start, "%Y-%m-%d")).days + 1
    }}

def generate_detailed_validation_report(detailed_df: pd.DataFrame, today_str: str) -> List[dict]:
    if detailed_df is None or detailed_df.empty:
        return []
    total = len(detailed_df)
    passed  = int((detailed_df["Validation_Status"] == "✅ PASS").sum())
    warned  = int((detailed_df["Validation_Status"] == "⚠️ WARNING").sum())
    failed  = int((detailed_df["Validation_Status"] == "❌ FAIL").sum())
    return [
        {"Report_Type": "Overall_Summary", "Description": "Total Invoice Count", "Count": total, "Percentage": "100.0%", "Status": "INFO"},
        {"Report_Type": "Overall_Summary", "Description": "Passed Validation",   "Count": passed, "Percentage": f"{(passed/total*100):.1f}%", "Status": "PASS"},
        {"Report_Type": "Overall_Summary", "Description": "Warnings",            "Count": warned, "Percentage": f"{(warned/total*100):.1f}%", "Status": "WARNING"},
        {"Report_Type": "Overall_Summary", "Description": "Failed Validation",   "Count": failed, "Percentage": f"{(failed/total*100):.1f}%", "Status": "FAIL"},
    ]

def build_final_validation_report(src_df: pd.DataFrame,
                                  detailed_df: pd.DataFrame,
                                  run_dir: str,
                                  validation_dt: datetime) -> pd.DataFrame:
    # Normalize empties
    src = src_df.copy() if src_df is not None else pd.DataFrame()
    det = detailed_df.copy() if detailed_df is not None else pd.DataFrame()

    # Merge key
    key_src = _pick_key(src) or "Row#"
    key_det = _pick_key(det) or "Row#"
    src["_merge_key"] = src.get(key_src, pd.Series(range(1, len(src)+1)))
    det["_merge_key"] = det.get(key_det, pd.Series(range(1, len(det)+1)))
    merged = pd.merge(src, det, how="left", on="_merge_key", suffixes=("","_det"))

    cols = {c.lower(): c for c in merged.columns}
    def col(name: str) -> Optional[str]:
        return cols.get(name.lower())

    # Common columns
    c_voucher_no       = col("VoucherNo")
    c_purchase_inv_no  = col("PurchaseInvNo")
    c_purchase_inv_dt  = col("PurchaseInvDate")
    c_voucher_dt       = col("Voucherdate")
    c_party            = col("PartyName")
    c_total            = col("Total")
    c_state            = col("State")
    c_currency_legacy  = col("Currency")
    c_narration        = col("Narration")
    c_gst              = col("GSTNO")
    c_invid            = col("InvID")
    c_vat              = col("VAT")
    c_igst_amt         = col("IGST/VATInputAmt")
    c_cgst_amt         = col("CGSTInputAmt")
    c_sgst_amt         = col("SGSTInputAmt")
    c_mop              = next((col(n) for n in _MOP_ALIASES if col(n)), None)

    c_inv_created_by   = col("Inv Created By") or col("Invoice_Creator_Name") or col("Invoice_Creator_Name_det")
    c_inv_currency     = col("Inv Currency") or col("Invoice_Currency") or col("Invoice currency")
    c_inv_entry_date   = col("Inv Entry Date") or c_voucher_dt or c_purchase_inv_dt
    c_inv_mod_date     = col("Inv Mod Date")
    c_due_date_col     = col("DueDate") or col("Due Date")
    c_remarks          = col("Remarks")
    c_location_pref    = col("Location") or col("Branch") or col("State")
    c_scid_hash        = col("SCID#") or col("SCID")

    invoice_id     = merged[c_invid] if c_invid else (merged[c_voucher_no] if c_voucher_no else pd.Series([""]*len(merged)))
    invoice_number = merged[c_purchase_inv_no] if c_purchase_inv_no else (merged[c_voucher_no] if c_voucher_no else pd.Series([""]*len(merged)))
    invoice_date   = merged[c_purchase_inv_dt] if c_purchase_inv_dt else (merged[c_voucher_dt] if c_voucher_dt else pd.Series([""]*len(merged)))
    entry_date     = merged[c_inv_entry_date] if c_inv_entry_date else invoice_date
    mod_date       = merged[c_inv_mod_date] if c_inv_mod_date else pd.Series([""]*len(merged))
    remarks        = merged[c_remarks] if c_remarks else (merged[c_narration] if c_narration else pd.Series([""]*len(merged)))

    creators_map = _try_load_creator_map(run_dir)
    if c_inv_created_by:
        creator_series = merged[c_inv_created_by].astype(str).apply(lambda s: s.strip() or "System Generated")
    else:
        creator_series = merged.apply(lambda r: _derive_creator(r, creators_map), axis=1)

    if c_location_pref:
        location_series = merged[c_location_pref].astype(str).apply(lambda s: s.strip())
    else:
        location_series = merged.apply(_derive_location, axis=1)

    if c_mop:
        mop_series = merged[c_mop].apply(map_payment_method)
    else:
        mop_series = merged.apply(_derive_payment_method, axis=1)

    if c_inv_currency:
        currency_series = merged[c_inv_currency]
    elif c_currency_legacy:
        currency_series = merged[c_currency_legacy]
    else:
        currency_series = pd.Series([""]*len(merged))

    vstatus_col = col("Validation_Status") or col("Validation_Status_det")
    issues_count_col = col("Issues_Found") or col("Issues_Found_det")
    issues_detail_col = col("Issue_Details") or col("Issue_Details_det")
    validation_status = merged[vstatus_col] if vstatus_col in merged.columns else pd.Series([""]*len(merged))
    issues_found = pd.to_numeric(merged[issues_count_col], errors="coerce").fillna(0).astype(int) if issues_count_col in merged.columns else pd.Series([0]*len(merged))
    issue_details = merged[issues_detail_col].fillna("") if issues_detail_col in merged.columns else pd.Series([""]*len(merged))

    cgst = pd.to_numeric(merged[c_cgst_amt], errors="coerce").fillna(0) if c_cgst_amt else pd.Series([0]*len(merged))
    sgst = pd.to_numeric(merged[c_sgst_amt], errors="coerce").fillna(0) if c_sgst_amt else pd.Series([0]*len(merged))
    igst = pd.to_numeric(merged[c_igst_amt], errors="coerce").fillna(0) if c_igst_amt else pd.Series([0]*len(merged))
    vat  = pd.to_numeric(merged[c_vat],  errors="coerce").fillna(0) if c_vat else pd.Series([0]*len(merged))
    total_tax = cgst + sgst + igst + vat

    def _tax_type_row(i: int) -> str:
        if float(igst.iloc[i]) > 0: return "IGST"
        if (float(cgst.iloc[i]) > 0) or (float(sgst.iloc[i]) > 0): return "GST"
        if float(vat.iloc[i])  > 0: return "VAT"
        return ""
    tax_type = pd.Series([_tax_type_row(i) for i in range(len(merged))]) if len(merged) else pd.Series(dtype=object)

    if c_scid_hash:
        scid_series = merged[c_scid_hash].astype(str)
    else:
        scid_series = merged.apply(_derive_scid, axis=1) if len(merged) else pd.Series(dtype=object)

    amount_series = merged[c_total] if c_total else pd.Series([0]*len(merged))

    if c_due_date_col:
        due_series = merged[c_due_date_col].astype(str).fillna("")
    else:
        due_series = merged.apply(_derive_due_date, axis=1) if len(merged) else pd.Series(dtype=object)
    notify_series = due_series.apply(_due_date_notification) if len(due_series) else pd.Series(dtype=object)

    final = pd.DataFrame({
        "Invoice_ID":             invoice_id,
        "Invoice_Number":         invoice_number,
        "Invoice_Date":           invoice_date,
        "Invoice_Entry_Date":     entry_date,
        "Vendor_Name":            merged[c_party] if c_party else pd.Series([""]*len(merged)),
        "Amount":                 amount_series,
        "Invoice_Creator_Name":   creator_series,
        "Location":               location_series,
        "Invoice_Currency":       currency_series,     # single currency column
        "Method_of_Payment":      mop_series,
        "Account_Head":           "",                  # derived via _derive_account_head if needed
        "Validation_Status":      validation_status,
        "Issues_Found":           issues_found,
        "Issue_Details":          issue_details,
        "GST_Number":             merged[c_gst] if c_gst else pd.Series([""]*len(merged)),
        "Row_Index":              merged.index + 1,
        "Validation_Date":        validation_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "Tax_Type":               tax_type,
        "Due_Date":               due_series,
        "Due_Date_Notification":  notify_series,
        "Total_Tax_Calculated":   total_tax,
        "CGST_Amount":            cgst,
        "SGST_Amount":            sgst,
        "IGST_Amount":            igst,
        "VAT_Amount":             vat,
        "TDS_Status":             pd.Series(["Coming Soon"]*len(merged)),
        "RMS_Invoice_ID":         invoice_id,
        "SCID":                   scid_series,
        # Traceability (kept at end)
        "Inv Entry Date":         entry_date,
        "Inv Mod Date":           mod_date,
        "Remarks":                remarks,
    })

    ordered_cols = [
        "Invoice_ID","Invoice_Number","Invoice_Date","Invoice_Entry_Date",
        "Vendor_Name","Amount","Invoice_Creator_Name","Location",
        "Invoice_Currency","Method_of_Payment","Account_Head","Validation_Status",
        "Issues_Found","Issue_Details","GST_Number","Row_Index","Validation_Date",
        "Tax_Type","Due_Date","Due_Date_Notification",
        "Total_Tax_Calculated","CGST_Amount","SGST_Amount","IGST_Amount",
        "VAT_Amount","TDS_Status","RMS_Invoice_ID","SCID",
        "Inv Entry Date","Inv Mod Date","Remarks",
    ]
    for c in ordered_cols:
        if c not in final.columns:
            final[c] = ""
    return final[ordered_cols]
                                      
def analyze_zip_contents(zip_path: str) -> dict:
    """Analyze the contents of the invoices ZIP file"""
    analysis = {
        "total_files": 0,
        "pdf_files": 0,
        "other_files": 0,
        "file_types": {},
        "sample_files": [],
        "total_size_mb": 0,
        "extraction_status": "success"
    }
    
    try:
        if not os.path.exists(zip_path):
            analysis["extraction_status"] = "zip_not_found"
            return analysis
        
        # Get ZIP file size
        zip_size = os.path.getsize(zip_path) / (1024 * 1024)  # MB
        analysis["total_size_mb"] = round(zip_size, 2)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            analysis["total_files"] = len(file_list)
            
            for file_name in file_list[:10]:  # Sample first 10 files
                analysis["sample_files"].append(file_name)
            
            # Count file types
            for file_name in file_list:
                if file_name.lower().endswith('.pdf'):
                    analysis["pdf_files"] += 1
                else:
                    analysis["other_files"] += 1
                
                # Track file extensions
                ext = os.path.splitext(file_name)[1].lower()
                if ext:
                    analysis["file_types"][ext] = analysis["file_types"].get(ext, 0) + 1
        
        print(f"📦 ZIP Analysis Results:")
        print(f"   📁 Total files: {analysis['total_files']}")
        print(f"   📄 PDF files: {analysis['pdf_files']}")
        print(f"   📋 Other files: {analysis['other_files']}")
        print(f"   💾 ZIP size: {analysis['total_size_mb']} MB")
        print(f"   🗂️ File types: {analysis['file_types']}")
        print(f"   📋 Sample files: {analysis['sample_files'][:3]}")
        
    except Exception as e:
        print(f"❌ ZIP analysis failed: {e}")
        analysis["extraction_status"] = f"error: {str(e)}"
    
    return analysis

def add_zip_info_to_report(detailed_report_path: str, run_dir: str):
    """Add ZIP analysis as a new sheet in the detailed report"""
    zip_path = os.path.join(run_dir, "invoices.zip")
    
    if os.path.exists(zip_path):
        zip_analysis = analyze_zip_contents(zip_path)
        
        # Create ZIP summary DataFrame
        zip_summary = pd.DataFrame([
            {"Metric": "Total Files in ZIP", "Value": zip_analysis["total_files"]},
            {"Metric": "PDF Invoice Files", "Value": zip_analysis["pdf_files"]},
            {"Metric": "Other Files", "Value": zip_analysis["other_files"]},
            {"Metric": "ZIP Size (MB)", "Value": zip_analysis["total_size_mb"]},
            {"Metric": "File Types", "Value": str(zip_analysis["file_types"])},
            {"Metric": "Extraction Status", "Value": zip_analysis["extraction_status"]},
        ])
        
        # Add to existing Excel file
        with pd.ExcelWriter(detailed_report_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            zip_summary.to_excel(writer, sheet_name="ZIP_Analysis", index=False)
        
        print(f"📦 ZIP analysis added to report: {zip_analysis['pdf_files']} PDFs found")

def extract_invoice_pdfs(zip_path: str, extract_to: str, max_files: int = 50) -> dict:
    """Extract a sample of PDF files for verification"""
    extraction_info = {
        "extracted_count": 0,
        "extraction_dir": extract_to,
        "sample_pdfs": [],
        "status": "success"
    }
    
    try:
        os.makedirs(extract_to, exist_ok=True)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            pdf_files = [f for f in zip_ref.namelist() if f.lower().endswith('.pdf')]
            
            # Extract a sample of PDFs (not all to save space)
            for i, pdf_file in enumerate(pdf_files[:max_files]):
                try:
                    zip_ref.extract(pdf_file, extract_to)
                    extraction_info["extracted_count"] += 1
                    extraction_info["sample_pdfs"].append(pdf_file)
                except Exception as extract_error:
                    print(f"⚠️ Failed to extract {pdf_file}: {extract_error}")
            
        print(f"📂 Extracted {extraction_info['extracted_count']} PDF samples to {extract_to}")
        
    except Exception as e:
        print(f"❌ PDF extraction failed: {e}")
        extraction_info["status"] = f"error: {str(e)}"
    
    return extraction_info
    
# ============== MAIN RUNNER ==============

def run_invoice_validation() -> bool:
    try:
        today = datetime.today()
        today_str = today.strftime("%Y-%m-%d")

        print(f"🚀 Starting DETAILED cumulative validation workflow for {today_str}")
        print("📧 Email: disabled by default (SEND_EMAIL=1 to enable)")
        print(f"⚙️ Config: every {VALIDATION_INTERVAL_DAYS} days (batch {VALIDATION_BATCH_DAYS} days), active {ACTIVE_VALIDATION_MONTHS} months")

        # Step 1: schedule
        if not should_run_today():
            print("⏳ Skipping – not time yet")
            return True

        # Step 2: archive
        archive_data_older_than_three_months()

        # Step 3: ranges
        cumulative_start, cumulative_end = get_cumulative_validation_range()
        batch_start, batch_end = get_current_batch_dates()
        print(f"📅 Current batch: {batch_start} → {batch_end}")
        print(f"📅 Cumulative: {cumulative_start} → {cumulative_end}")

        # Step 4: download (CORRECTED)
        print("📥 Step 4: Starting download process...")
        try:
            # Validate date range before download
            start_dt = datetime.strptime(cumulative_start, "%Y-%m-%d")
            end_dt = datetime.strptime(cumulative_end, "%Y-%m-%d")
            
            if start_dt > end_dt:
                raise ValueError(f"Invalid date range: start date {cumulative_start} is after end date {cumulative_end}")
            
            days_span = (end_dt - start_dt).days + 1
            print(f"📊 Downloading data for {days_span} days ({cumulative_start} to {cumulative_end})")
            
            # Call the download function with proper error handling
            print("🔄 Initiating RMS download...")
            run_path = download_cumulative_data(cumulative_start, cumulative_end)
            
            # Validate the returned path
            if not run_path:
                raise ValueError("Download function returned empty/None path")
            
            if not isinstance(run_path, str):
                raise TypeError(f"Download function returned invalid type: {type(run_path)}, expected string")
            
            print(f"📂 Raw download path received: {run_path}")
            
            # Normalize the path and validate it exists
            run_dir, invoice_path = _normalize_run_dir(run_path)
            
            # Validate run directory exists
            if not os.path.exists(run_dir):
                raise FileNotFoundError(f"Download directory does not exist: {run_dir}")
            
            if not os.path.isdir(run_dir):
                raise NotADirectoryError(f"Path is not a directory: {run_dir}")
            
            print(f"✅ Run directory validated: {run_dir}")
            print(f"📄 Expected invoice file: {invoice_path}")
            
            # List contents of download directory for debugging
            try:
                dir_contents = os.listdir(run_dir)
                print(f"📋 Directory contents: {dir_contents}")
            except Exception as list_error:
                print(f"⚠️ Could not list directory contents: {list_error}")
            
        except Exception as download_error:
            logging.error(f"❌ Download process failed: {download_error}")
            print(f"💥 Download error details: {download_error}")
            
            # Create fallback empty structure to allow workflow to continue
            print("🔄 Creating fallback structure for graceful degradation...")
            fallback_dir = f"data/fallback_{today_str}"
            os.makedirs(fallback_dir, exist_ok=True)
            
            # Create empty invoice file for testing
            fallback_invoice = os.path.join(fallback_dir, "invoice_download.xls")
            pd.DataFrame().to_excel(fallback_invoice, index=False, engine="openpyxl")
            
            run_dir = fallback_dir
            invoice_path = fallback_invoice
            
            print(f"⚠️ Using fallback directory: {run_dir}")
            print("⚠️ Workflow will continue with empty dataset for testing")

            # Step 5: verify files and PDF completeness - ENHANCED
        print("🔍 Step 5: Validating downloaded files...")
        ok, details = validate_downloaded_files(run_dir)
        if not ok:
            logging.error(f"❌ File validation failed: {details}")
            # ... existing error handling ...
        else:
            logging.info(f"✅ Required files found: {details}")
            
            # Read Excel file first to get expected count
            excel_path = os.path.join(run_dir, "invoice_download.xls")
            if os.path.exists(excel_path):
                try:
                    excel_df = pd.read_excel(excel_path)
                    print(f"📊 Excel contains {len(excel_df)} invoice records")
                    
                    # Verify PDF completeness
                    zip_path = os.path.join(run_dir, "invoices.zip")
                    if os.path.exists(zip_path):
                        print("📦 Step 5b: Verifying PDF download completeness...")
                        
                        pdf_complete = verify_pdf_download_completeness(run_dir, excel_df)
                        
                        if not pdf_complete:
                            print("⚠️ PDF download appears incomplete, attempting regeneration...")
                            regeneration_success = request_pdf_regeneration(zip_path, run_dir)
                            
                            if regeneration_success:
                                # Re-verify after regeneration
                                pdf_complete = verify_pdf_download_completeness(run_dir, excel_df, max_wait_minutes=2)
                        
                        if pdf_complete:
                            # Analyze ZIP contents
                            zip_analysis = analyze_zip_contents(zip_path)
                            
                            # Extract sample PDFs
                            pdf_extract_dir = os.path.join(run_dir, "sample_pdfs")
                            extraction_info = extract_invoice_pdfs(zip_path, pdf_extract_dir, max_files=25)
                            
                            print(f"✅ PDF verification complete: {zip_analysis['pdf_files']} PDFs available")
                        else:
                            print("⚠️ Continuing with incomplete PDF set - Excel data will still be processed")
                    else:
                        print("ℹ️ No invoices.zip file found - Excel data only")
                        
                except Exception as excel_error:
                    print(f"❌ Error reading Excel for PDF verification: {excel_error}")
                    
        # Step 6: read export (do NOT fail the run if empty) - IMPROVED
        print("📊 Step 6: Reading RMS export file...")
        
        # Double-check file existence before reading
        if not os.path.exists(invoice_path):
            # Try common alternative paths
            alternative_paths = [
                os.path.join(run_dir, "invoice_download.xls"),
                os.path.join(run_dir, "invoice_download.xlsx"),
                os.path.join(run_dir, "invoices.xls"),
                os.path.join(run_dir, "invoices.xlsx"),
                os.path.join(run_dir, "export.xls"),
                os.path.join(run_dir, "export.xlsx")
            ]
            
            found_file = None
            for alt_path in alternative_paths:
                if os.path.exists(alt_path):
                    found_file = alt_path
                    break
            
            if found_file:
                invoice_path = found_file
                print(f"🔄 Using alternative file path: {invoice_path}")
            else:
                print(f"⚠️ Invoice file not found at: {invoice_path}")
                print(f"📋 Checked alternatives: {alternative_paths}")
                # Create empty DataFrame to continue workflow
                src_df = pd.DataFrame()
                print("⚠️ Continuing with empty dataset")
        
        if os.path.exists(invoice_path):
            try:
                print(f"📖 Reading file: {invoice_path}")
                file_size = os.path.getsize(invoice_path)
                print(f"📏 File size: {file_size} bytes")
                
                src_df = read_invoice_file(invoice_path)
                if src_df is None:
                    src_df = pd.DataFrame()
                    
                print(f"✅ Loaded: {src_df.shape if hasattr(src_df,'shape') else (0,0)} rows/columns")
                
                # Log column information if data exists
                if not src_df.empty:
                    print(f"📊 Columns found: {list(src_df.columns)}")
                    print(f"📈 Data sample: {len(src_df)} rows total")
                else:
                    print("⚠️ File loaded but contains no data")
                    
            except Exception as read_error:
                logging.error(f"❌ Error reading invoice file: {read_error}")
                print(f"💥 Read error details: {read_error}")
                src_df = pd.DataFrame()
                print("⚠️ Using empty DataFrame due to read error")
        else:
            src_df = pd.DataFrame()

        # Step 7: filter
        print("🔄 Step 7: Filter to cumulative range…")
        filtered_df = filter_invoices_by_date(src_df, cumulative_start, cumulative_end)
        print(f"📦 Working rows: {len(filtered_df)}")

        # Step 8: validate
        print("🔎 Step 8: Detailed validation on cumulative…")
        detailed_df, summary_issues, problematic_df = validate_invoices_with_details(filtered_df)

        # Step 9: email stats (computed even if not sent)
        print("📧 Step 9: Build email summary…")
        email_summary = generate_email_summary_statistics(
            detailed_df, cumulative_start, cumulative_end, batch_start, batch_end, today_str
        )

        # Step 10: summary sheet data
        print("📋 Step 10: Build summary sheet data…")
        summary_sheet_rows = generate_detailed_validation_report(detailed_df, today_str)

        # Step 11/12: DB persistence (best-effort)
        print("💾 Step 11: Save snapshot…")
        try:
            save_invoice_snapshot(
                detailed_df.to_dict("records") if not detailed_df.empty else [],
                run_date=today_str,
                run_type="detailed_cumulative_4day",
                batch_start=batch_start,
                batch_end=batch_end,
                cumulative_start=cumulative_start,
                cumulative_end=cumulative_end
            )
        except Exception as e:
            logging.warning(f"Snapshot save skipped: {e}")

        print("📝 Step 12: Record run window…")
        try:
            record_run_window(
                batch_start, batch_end,
                run_type="detailed_cumulative_4day",
                cumulative_start=cumulative_start,
                cumulative_end=cumulative_end,
                total_days_validated=(datetime.strptime(cumulative_end, "%Y-%m-%d") - datetime.strptime(cumulative_start, "%Y-%m-%d")).days + 1
            )
        except Exception as e:
            logging.warning(f"Run window record skipped: {e}")

        # Step 13: Save ONLY the 3 requested files
        print("📑 Step 13: Save reports (3 files only)…")
        os.makedirs("data", exist_ok=True)

        # 13a) Invoice validation report (with multiple tabs + final 'Validation Report' later)
        detailed_report_path = f"data/invoice_validation_detailed_{today_str}.xlsx"
        with pd.ExcelWriter(detailed_report_path, engine="openpyxl") as writer:
            detailed_df.to_excel(writer, sheet_name="All_Invoices", index=False)
            failed_df = detailed_df[detailed_df["Validation_Status"] == "❌ FAIL"] if not detailed_df.empty else pd.DataFrame()
            if not failed_df.empty:
                failed_df.to_excel(writer, sheet_name="Failed_Invoices", index=False)
            warn_df = detailed_df[detailed_df["Validation_Status"] == "⚠️ WARNING"] if not detailed_df.empty else pd.DataFrame()
            if not warn_df.empty:
                warn_df.to_excel(writer, sheet_name="Warning_Invoices", index=False)
            pass_df = detailed_df[detailed_df["Validation_Status"] == "✅ PASS"] if not detailed_df.empty else pd.DataFrame()
            if not pass_df.empty:
                pass_df.to_excel(writer, sheet_name="Passed_Invoices", index=False)
            if summary_sheet_rows:
                pd.DataFrame(summary_sheet_rows).to_excel(writer, sheet_name="Summary_Stats", index=False)
        print(f"✅ Saved: {detailed_report_path}")

        # 13b) Validation result (compact dashboard)
        os.makedirs(f"data/{today_str}", exist_ok=True)
        dashboard_path = f"data/{today_str}/validation_result.xlsx"
        keep_cols = ['Invoice_ID','Invoice_Number','Invoice_Date','Vendor_Name','Amount','Invoice_Creator_Name',
                     'Validation_Status','Issues_Found','Issue_Details','GST_Number']
        dashboard_df = detailed_df[keep_cols].copy() if not detailed_df.empty else pd.DataFrame(columns=keep_cols)
        if not dashboard_df.empty:
            dashboard_df["Status_Summary"] = dashboard_df.apply(
                lambda r: f"{r['Validation_Status']} - {r['Issues_Found']} issue(s)"
                          if r['Issues_Found'] > 0 else f"{r['Validation_Status']} - No issues",
                axis=1
            )
        dashboard_df.to_excel(dashboard_path, index=False, engine="openpyxl")
        print(f"📊 Dashboard saved: {dashboard_path}")

        # 13c) Delta
        delta_path = f"data/delta_report_{today_str}.xlsx"
        dashboard_df.to_excel(delta_path, index=False, engine="openpyxl")
        print(f"📈 Delta saved: {delta_path}")

        # Step 14: Build exact-format final sheet and append to invoice_validation_detailed
        print("🧩 Step 14: Build exact-format final sheet…")
        final_df = build_final_validation_report(filtered_df, detailed_df, run_dir, datetime.now())
        with pd.ExcelWriter(detailed_report_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as xw:
            final_df.to_excel(xw, sheet_name="Validation Report", index=False)
        print(f"✅ Final sheet 'Validation Report' written into: {detailed_report_path}")

        # Step 15: Optional email (3 attachments) - ENHANCED DEBUG
        print("📧 Step 15: Email notification...")
        debug_email_settings()
        
        if SEND_EMAIL:
            try:
                stats = email_summary.get("statistics", {})
                html_body = EnhancedEmailSystem().create_professional_html_template(
                    {"failed": stats.get("failed_invoices", 0),
                     "warnings": stats.get("warning_invoices", 0),
                     "passed": stats.get("passed_invoices", 0)},
                    datetime.now() + timedelta(days=3)
                )
                
                print("📧 Creating email notifier...")
                notifier = EmailNotifier()
                
                attachments = [detailed_report_path, dashboard_path, delta_path]
                print(f"📎 Email attachments: {attachments}")
                
                print("📤 Sending email...")
                email_sent = notifier.send_validation_report(
                    f"Invoice Validation Results - {today_str}", 
                    html_body, 
                    attachments=attachments
                )
                
                if email_sent:
                    print("✅ Email sent successfully with 3 attachments.")
                else:
                    print("❌ Email sending failed (returned False)")
                    
            except Exception as email_error:
                print(f"💥 Email error: {email_error}")
                logging.error(f"Email sending error: {email_error}")
        else:
            print("✉️ Email sending skipped (SEND_EMAIL not set or disabled).")

def main():
    """Main entry point that handles command line arguments"""
    print(f"🚀 Invoice Validation Agent starting at {datetime.now()}")
    print(f"📁 Working directory: {os.getcwd()}")
    print(f"🐍 Python version: {sys.version}")
    
    # Handle command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "run":
            print("📋 Running in GitHub Actions mode")
        elif sys.argv[1] == "--help" or sys.argv[1] == "-h":
            print("Usage: python main.py [run]")
            sys.exit(0)
        else:
            print(f"⚠️ Unknown argument: {sys.argv[1]}")
    
    # Check environment
    if not check_environment():
        print("❌ Environment check failed!")
        sys.exit(1)
    
    # Test database functions (best effort)
    try:
        create_tables()
        print("✅ Database tables created/verified")
    except Exception as e:
        print(f"⚠️ Database warning: {e}")
        # Continue anyway - database is optional for basic validation
    
    # Run validation
    try:
        ok = run_invoice_validation()
        if ok:
            print("🎉 Invoice validation completed successfully!")
            sys.exit(0)
        else:
            print("❌ Invoice validation failed!")
            sys.exit(1)
    except Exception as e:
        print(f"💥 Unexpected error: {e}")
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if os.getenv("SEND_EMAIL", "0") == "1":
        email_test_passed = test_email_configuration()
        if not email_test_passed:
            print("⚠️ Email test failed, but continuing with validation...")

if __name__ == "__main__":
    main()
