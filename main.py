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
# These are referenced by the original project structure.
from rms_scraper import rms_download                   # returns run folder path OR invoice_download.xls fullpath
from validator_utils import validate_invoices          # returns (summary_issues, problematic_df)
from updater import update_invoice_status              # (kept for parity, not invoked directly below)
from reporter import save_snapshot_report              # (kept for parity, not invoked directly below)
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
load_dotenv()
create_tables()

# ============== Config ==============
VALIDATION_INTERVAL_DAYS = 4      # run validation every 4 days
VALIDATION_BATCH_DAYS    = 4      # each batch covers 4 days
ACTIVE_VALIDATION_MONTHS = 3      # maintain 3 months active data
ARCHIVE_FOLDER           = "archived_data"

# ============== Helpers ==============

def should_run_today() -> bool:
    """4-day cadence; safe default to always run when scheduling is unclear."""
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
    """Kick off RMS download for the cumulative window; returns a directory path."""
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")
    print(f"📥 Downloading cumulative validation data from {start_str} to {end_str}...")
    print(f"📊 Range covers: {(end_date - start_date).days + 1} days")
    returned_path = rms_download(start_date, end_date)

    # Normalize: the scraper may return the xls file path OR the run directory.
    p = Path(returned_path)
    run_dir = str(p if p.is_dir() else p.parent)
    if p.is_file():
        print(f"ℹ️ rms_download returned a file; using its parent directory: {run_dir}")
    return run_dir


def validate_downloaded_files(run_dir: str) -> Tuple[bool, List]:
    """Validate that required files exist in the run directory."""
    try:
        logging.info(f"🔍 Step 5: Verifying files in directory: {run_dir}")
        expected_files = {
            'invoice_download.xls': 'Excel invoice data',
            'invoices.zip': 'ZIP invoice files'
        }
        missing_files, found_files = [], []
        for filename, _desc in expected_files.items():
            file_path = os.path.join(run_dir, filename)
            if os.path.exists(file_path):
                size = os.path.getsize(file_path)
                found_files.append((filename, size))
                logging.info(f"✅ Found {filename}: {size} bytes")
                try:
                    with open(file_path, 'rb') as f:
                        header = f.read(20)
                    logging.info(f"🔍 {filename} header: {header}")
                except Exception as e:
                    logging.warning(f"⚠️ Could not read header for {filename}: {e}")
            else:
                missing_files.append(filename)
                logging.error(f"❌ Missing file: {filename}")
        if missing_files:
            logging.error(f"❌ Missing files: {missing_files}")
            return False, missing_files
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
        # Still return empty frame rather than crash the whole run.
        print("⚠️ File appears too small; continuing with empty dataset")
        return pd.DataFrame()

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
                print(f"✅ CSV read with '{sep}'. Shape: {df.shape}")
                print(f"📋 Columns: {list(df.columns)}")
                return df
        except Exception:
            continue
    print("⚠️ CSV reading failed with all separators")

    # Try HTML (rare)
    try:
        print("🌐 Attempting to read as HTML...")
        tables = pd.read_html(invoice_file, flavor="lxml")
        if tables:
            df = tables[0]
            print(f"✅ HTML table read. Shape: {df.shape}")
            print(f"📋 Columns: {list(df.columns)}")
            return df
    except Exception as e:
        print(f"⚠️ HTML parsing failed: {e}")

    # Last resort: show sample and return empty
    try:
        with open(invoice_file, "r", encoding="utf-8", errors="ignore") as f:
            sample = f.read(500)
        print(f"📄 File sample (first 500 chars):\n{repr(sample)}")
    except Exception as e:
        print(f"⚠️ Could not read file content: {e}")

    print("⚠️ Could not parse invoice file; continuing with empty dataset")
    return pd.DataFrame()


def filter_invoices_by_date(df: pd.DataFrame, start_str: str, end_str: str) -> pd.DataFrame:
    """Filter by PurchaseInvDate in [start, end]."""
    try:
        if df is None or df.empty:
            return pd.DataFrame()
        if "PurchaseInvDate" not in df.columns:
            print("⚠️ PurchaseInvDate not found; returning all data")
            return df
        s = datetime.strptime(start_str, "%Y-%m-%d")
        e = datetime.strptime(end_str, "%Y-%m-%d")
        df = df.copy()
        df["ParsedInvoiceDate"] = pd.to_datetime(df["PurchaseInvDate"], errors="coerce")
        out = df[(df["ParsedInvoiceDate"] >= s) & (df["ParsedInvoiceDate"] <= e)]
        print(f"📅 Filtered {len(out)}/{len(df)} between {start_str} and {end_str}")
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
            logger.warning(f"Creator map load failed for {p}: {e}")
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
    return ""

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

def _derive_scid(row) -> str:
    for c in ("SCID#", "SCID", "Scid", "scid"):
        v = row.get(c)
        if isinstance(v, str) and v.strip():
            return v.strip()
    narr = str(row.get("Narration", ""))
    m = re.search(r"\bSCID[#:]?\s*([A-Za-z0-9\-_/]+)", narr, flags=re.I)
    return m.group(1).strip() if m else ""


# ====== Validation-detail builder ======

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
    lower = {c.lower(): c for c in df.columns}
    for c in possible:
        if c.lower() in lower:
            return lower[c.lower()]
    for c in df.columns:
        if any(w in c.lower() for w in ("create","by","user","entry","made","prepared")):
            return c
    return None


def validate_invoices_with_details(df: pd.DataFrame) -> Tuple[pd.DataFrame, list, pd.DataFrame]:
    """Augment each invoice with a pass/warn/fail and include Invoice_Creator_Name."""
    print("🔍 Running detailed invoice-level validation…")
    try:
        summary_issues, problematic = validate_invoices(df)  # existing rules from your module
    except Exception as e:
        logger.warning(f"Base validation failed (continuing with detailed only): {e}")
        summary_issues, problematic = [], pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "Invoice_ID","Invoice_Number","Invoice_Date","Vendor_Name","Amount",
            "Invoice_Creator_Name","Validation_Status","Issues_Found","Issue_Details",
            "GST_Number","Row_Index","Validation_Date"
        ]), summary_issues, problematic

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
    print(f"✅ Detailed validation completed: {len(detailed_df)} rows")
    return detailed_df, summary_issues, problematic


def generate_email_summary_statistics(detailed_df: pd.DataFrame,
                                      cumulative_start: str, cumulative_end: str,
                                      batch_start: str, batch_end: str,
                                      today_str: str) -> dict:
    print("📧 Generating email summary statistics…")
    if detailed_df is None or detailed_df.empty:
        html = EnhancedEmailSystem().create_professional_html_template(
            {"failed": 0, "warnings": 0, "passed": 0},
            datetime.now() + timedelta(days=3)
        )
        return {"html_summary": html, "text_summary": "No invoice data.", "statistics": {}}

    total = len(detailed_df)
    passed  = (detailed_df["Validation_Status"] == "✅ PASS").sum()
    warned  = (detailed_df["Validation_Status"] == "⚠️ WARNING").sum()
    failed  = (detailed_df["Validation_Status"] == "❌ FAIL").sum()
    pass_rate = (passed / total * 100) if total else 0

    creators = detailed_df["Invoice_Creator_Name"].value_counts() if "Invoice_Creator_Name" in detailed_df else pd.Series(dtype=int)
    unknown = int(creators.get("Unknown", 0) + creators.get("", 0)) if not creators.empty else 0

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
    print(f"✅ Email summary generated. Pass rate {pass_rate:.1f}%")
    return {"html_summary": html, "text_summary": text, "statistics": stats}


def generate_detailed_validation_report(detailed_df: pd.DataFrame, today_str: str) -> List[dict]:
    print("📋 Generating detailed validation summary sheet…")
    if detailed_df is None or detailed_df.empty:
        return []
    total = len(detailed_df)
    passed  = (detailed_df["Validation_Status"] == "✅ PASS").sum()
    warned  = (detailed_df["Validation_Status"] == "⚠️ WARNING").sum()
    failed  = (detailed_df["Validation_Status"] == "❌ FAIL").sum()
    return [
        {"Report_Type": "Overall_Summary", "Description": "Total Invoice Count", "Count": int(total), "Percentage": "100.0%", "Status": "INFO"},
        {"Report_Type": "Overall_Summary", "Description": "Passed Validation",   "Count": int(passed), "Percentage": f"{(passed/total*100):.1f}%", "Status": "PASS"},
        {"Report_Type": "Overall_Summary", "Description": "Warnings",            "Count": int(warned), "Percentage": f"{(warned/total*100):.1f}%", "Status": "WARNING"},
        {"Report_Type": "Overall_Summary", "Description": "Failed Validation",   "Count": int(failed), "Percentage": f"{(failed/total*100):.1f}%", "Status": "FAIL"},
    ]


def enhance_validation_results(detailed_df: pd.DataFrame, email_summary: dict) -> dict:
    """Stable enhancement wrapper; safe defaults."""
    try:
        logging.info("🔧 Enhancing validation results…")
        df = detailed_df.copy() if detailed_df is not None else pd.DataFrame()
        total = len(df)
        if total:
            col = next((c for c in df.columns if any(k in c.lower() for k in ("validation","status","result"))), None)
            passed  = df[col].astype(str).str.contains(r"\bpass\b", case=False, na=False).sum() if col else 0
            failed  = df[col].astype(str).str.contains(r"\bfail\b", case=False, na=False).sum() if col else total
            warned  = df[col].astype(str).str.contains(r"warn", case=False, na=False).sum() if col else 0
            amount_col = next((c for c in df.columns if ("total" in c.lower()) or ("amount" in c.lower())), None)
            total_amount = float(pd.to_numeric(df[amount_col], errors="coerce").fillna(0).sum()) if amount_col else 0.0
            pass_rate = (passed/total*100) if total else 0.0
        else:
            passed = failed = warned = 0
            total_amount = 0.0
            pass_rate = 0.0

        return {
            "success": True,
            "enhanced_df": df,
            "email_summary": email_summary,
            "message": "Enhancement completed",
            "total_invoices": total,
            "pass_count": int(passed),
            "fail_count": int(failed),
            "warning_count": int(warned),
            "pass_rate": float(pass_rate),
            "total_amount": float(total_amount),
            "enhancement_applied": True,
            "system_status": "operational",
        }
    except Exception as e:
        logging.error(f"⚠️ Enhancement error: {e}")
        logging.error(traceback.format_exc())
        return {
            "success": False,
            "enhanced_df": detailed_df if detailed_df is not None else pd.DataFrame(),
            "email_summary": email_summary,
            "message": f"Enhancement error: {e}",
            "enhancement_applied": False,
            "system_status": "degraded",
        }


def build_final_validation_report(df: pd.DataFrame, run_dir: str, validation_dt: datetime) -> pd.DataFrame:
    """
    EXACT final attachment with RMS inspected headers:
      Inv Entry Date, Inv Mod Date, DueDate, Remarks, MOP, Location, SCID#, Inv Created By, A/C Head, Inv Currency
    Produces the requested schema (incl. duplicates: 'Invoice currency' and 'Invoice_Currency', and Location).
    """
    src = df.copy() if df is not None else pd.DataFrame()
    if src.empty:
        return pd.DataFrame(columns=[
            "Invoice_ID","Invoice_Number","Invoice_Date","Invoice_Entry_Date","Vendor_Name","Amount",
            "Invoice_Creator_Name","Location","Invoice currency","Method_of_Payment","Account_Head",
            "Validation_Status","Issues_Found","Issue_Details","GST_Number","Row_Index","Validation_Date",
            "Invoice_Currency","Tax_Type","Due_Date","Due_Date_Notification","Total_Tax_Calculated",
            "CGST_Amount","SGST_Amount","IGST_Amount","VAT_Amount","TDS_Status","RMS_Invoice_ID","SCID",
            "Inv Entry Date","Inv Mod Date","DueDate","Remarks"
        ])

    cols = {c.lower(): c for c in src.columns}
    def col(name: str) -> Optional[str]:
        return cols.get(name.lower())

    # Base
    c_voucher_no       = col("VoucherNo")
    c_purchase_inv_no  = col("PurchaseInvNo")
    c_purchase_inv_dt  = col("PurchaseInvDate")
    c_voucher_dt       = col("Voucherdate")
    c_party            = col("PartyName")
    c_total            = col("Total")
    c_state            = col("State")
    c_currency_legacy  = col("Currency")
    c_narration        = col("Narration")
    c_ledger           = col("PurchaseLEDGER")
    c_gst              = col("GSTNO")
    c_invid            = col("InvID")
    c_vat              = col("VAT")
    c_igst_amt         = col("IGST/VATInputAmt")
    c_cgst_amt         = col("CGSTInputAmt")
    c_sgst_amt         = col("SGSTInputAmt")

    # Inspected fields
    c_inv_created_by   = col("Inv Created By")
    c_mop              = col("MOP")
    c_ac_head          = col("A/C Head")
    c_inv_currency     = col("Inv Currency")
    c_inv_entry_date   = col("Inv Entry Date")
    c_inv_mod_date     = col("Inv Mod Date")
    c_due_date         = col("DueDate") or col("Due Date")
    c_remarks          = col("Remarks")
    c_location_pref    = col("Location")
    c_scid_hash        = col("SCID#") or col("SCID")

    # Compose
    invoice_id     = src[c_invid] if c_invid else (src[c_voucher_no] if c_voucher_no else pd.Series([""]*len(src)))
    invoice_number = src[c_purchase_inv_no] if c_purchase_inv_no else (src[c_voucher_no] if c_voucher_no else pd.Series([""]*len(src)))
    invoice_date   = src[c_purchase_inv_dt] if c_purchase_inv_dt else (src[c_voucher_dt] if c_voucher_dt else pd.Series([""]*len(src)))
    entry_date     = src[c_inv_entry_date] if c_inv_entry_date else (src[c_voucher_dt] if c_voucher_dt else invoice_date)
    mod_date       = src[c_inv_mod_date] if c_inv_mod_date else pd.Series([""]*len(src))
    due_date       = src[c_due_date] if c_due_date else pd.Series([""]*len(src))
    remarks        = src[c_remarks] if c_remarks else (src[c_narration] if c_narration else pd.Series([""]*len(src)))

    if c_inv_created_by:
        creator_series = src[c_inv_created_by].astype(str).apply(lambda s: s.strip() or "System Generated")
    else:
        creator_series = src.apply(lambda r: _derive_creator(r, _try_load_creator_map(run_dir)), axis=1)

    if c_location_pref:
        location_series = src[c_location_pref].astype(str)
    elif c_state:
        location_series = src[c_state].astype(str)
    else:
        location_series = src.apply(_derive_location, axis=1)

    method_series = src[c_mop].astype(str) if c_mop else src.apply(_derive_payment_method, axis=1)
    account_series = src[c_ac_head].astype(str) if c_ac_head else src.apply(_derive_account_head, axis=1)

    if c_inv_currency:
        currency_series = src[c_inv_currency]
    elif c_currency_legacy:
        currency_series = src[c_currency_legacy]
    else:
        currency_series = pd.Series([""]*len(src))

    validation_col = next((c for c in src.columns if any(k in c.lower() for k in ("validation","status","result"))), None)
    validation_status = src[validation_col] if validation_col else pd.Series([""]*len(src))
    issue_details_col = next((c for c in src.columns if "issue" in c.lower()), None)
    issue_details = src[issue_details_col].fillna("") if issue_details_col else pd.Series([""]*len(src))
    issues_found = issue_details.apply(lambda s: 0 if str(s).strip() in ("", "No issues found") else 1)

    cgst = pd.to_numeric(src[c_cgst_amt], errors="coerce").fillna(0) if c_cgst_amt else pd.Series([0]*len(src))
    sgst = pd.to_numeric(src[c_sgst_amt], errors="coerce").fillna(0) if c_sgst_amt else pd.Series([0]*len(src))
    igst = pd.to_numeric(src[c_igst_amt], errors="coerce").fillna(0) if c_igst_amt else pd.Series([0]*len(src))
    vat  = pd.to_numeric(src[c_vat],  errors="coerce").fillna(0) if c_vat else pd.Series([0]*len(src))
    total_tax = cgst + sgst + igst + vat

    def _tax_type_row(i: int) -> str:
        if float(igst.iloc[i]) > 0: return "IGST"
        if (float(cgst.iloc[i]) > 0) or (float(sgst.iloc[i]) > 0): return "GST"
        if float(vat.iloc[i])  > 0: return "VAT"
        return ""
    tax_type = pd.Series([_tax_type_row(i) for i in range(len(src))])

    if c_scid_hash:
        scid_series = src[c_scid_hash].astype(str)
    else:
        scid_series = src.apply(_derive_scid, axis=1)

    amount_series = src[c_total] if c_total else pd.Series([0]*len(src))

    final = pd.DataFrame({
        "Invoice_ID":             invoice_id,
        "Invoice_Number":         invoice_number,
        "Invoice_Date":           invoice_date,
        "Invoice_Entry_Date":     entry_date,
        "Inv Entry Date":         entry_date,   # inspected header (kept for traceability)
        "Inv Mod Date":           mod_date,     # inspected header
        "Vendor_Name":            src[c_party] if c_party else pd.Series([""]*len(src)),
        "Amount":                 amount_series,
        "Invoice_Creator_Name":   creator_series,
        "Location":               location_series,
        "Invoice currency":       currency_series,    # spec duplicate spelling
        "Method_of_Payment":      method_series,
        "Account_Head":           account_series,
        "Validation_Status":      validation_status,
        "Issues_Found":           issues_found,
        "Issue_Details":          issue_details,
        "GST_Number":             src[c_gst] if c_gst else pd.Series([""]*len(src)),
        "Row_Index":              (src.index + 1),
        "Validation_Date":        validation_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "Invoice_Currency":       currency_series,    # mirror of "Invoice currency"
        "Tax_Type":               tax_type,
        "Due_Date":               due_date,
        "DueDate":                due_date,           # inspected header spelling
        "Remarks":                remarks,
        "Due_Date_Notification":  pd.Series([""]*len(src)),
        "Total_Tax_Calculated":   total_tax,
        "CGST_Amount":            cgst,
        "SGST_Amount":            sgst,
        "IGST_Amount":            igst,
        "VAT_Amount":             vat,
        "TDS_Status":             pd.Series(["Coming Soon"]*len(src)),
        "RMS_Invoice_ID":         invoice_id,
        "SCID":                   scid_series,
    })

    ordered_cols = [
        "Invoice_ID","Invoice_Number","Invoice_Date","Invoice_Entry_Date",
        "Vendor_Name","Amount","Invoice_Creator_Name","Location",
        "Invoice currency","Method_of_Payment","Account_Head","Validation_Status",
        "Issues_Found","Issue_Details","GST_Number","Row_Index","Validation_Date",
        "Invoice_Currency","Tax_Type","Due_Date","Due_Date_Notification",
        "Total_Tax_Calculated","CGST_Amount","SGST_Amount","IGST_Amount",
        "VAT_Amount","TDS_Status","RMS_Invoice_ID","SCID",
        "Inv Entry Date","Inv Mod Date","DueDate","Remarks",
    ]
    for c in ordered_cols:
        if c not in final.columns:
            final[c] = ""
    return final[ordered_cols]


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
        raw_download_path = rms_download(datetime.strptime(cumulative_start, "%Y-%m-%d"),
                                         datetime.strptime(cumulative_end, "%Y-%m-%d"))
        print(f"✅ Download path (raw): {raw_download_path}")
        # Normalize to directory
        p = Path(raw_download_path)
        run_dir = str(p if p.is_dir() else p.parent)
        if p.is_file():
            print(f"ℹ️ Using parent directory for validation: {run_dir}")

        # Step 5: Validate downloaded files
        validation_success, file_details = validate_downloaded_files(run_dir)
        if not validation_success:
            logging.error(f"❌ File validation failed: {file_details}")
            logging.error("❌ Aborting: Required files missing")
            return False
        logging.info(f"✅ All required files found: {file_details}")

        # Step 6: Read RMS export…
        invoice_path = os.path.join(run_dir, "invoice_download.xls")
        if not os.path.isfile(invoice_path):
            print("❌ Aborting: invoice_download.xls missing")
            return False

        print("📊 Step 7: Read RMS export…")
        src_df = read_invoice_file(invoice_path)
        if src_df is None:
            print("❌ Could not read invoice file")
            return False
        print(f"✅ Loaded: {src_df.shape if not src_df.empty else (0,0)}")

        # Step 8: Filter to cumulative range…
        print("🔄 Step 8: Filter to cumulative range…")
        filtered_df = filter_invoices_by_date(src_df, cumulative_start, cumulative_end)
        print(f"📦 Working rows: {len(filtered_df)}")

        # Step 9: Detailed validation on cumulative…
        print("🔎 Step 9: Detailed validation on cumulative…")
        detailed_df, summary_issues, problematic_df = validate_invoices_with_details(filtered_df)

        # Step 10: Build email summary…
        print("📧 Step 10: Build email summary…")
        email_summary = generate_email_summary_statistics(
            detailed_df, cumulative_start, cumulative_end, batch_start, batch_end, today_str
        )

        # Step 11: Build summary sheet data…
        print("📋 Step 11: Build summary sheet data…")
        summary_sheet_rows = generate_detailed_validation_report(detailed_df, today_str)

        # Step 12/13: Persist snapshot + run window
        print("💾 Step 12: Save snapshot…")
        try:
            current_records = detailed_df.to_dict("records") if not detailed_df.empty else []
            save_invoice_snapshot(
                current_records,
                run_date=today_str,
                run_type="detailed_cumulative_4day",
                batch_start=batch_start,
                batch_end=batch_end,
                cumulative_start=cumulative_start,
                cumulative_end=cumulative_end
            )
            print("✅ Snapshot saved")
        except Exception as e:
            print(f"⚠️ Snapshot save failed: {e}")

        print("📝 Step 13: Record run window…")
        try:
            record_run_window(
                batch_start, batch_end,
                run_type="detailed_cumulative_4day",
                cumulative_start=cumulative_start,
                cumulative_end=cumulative_end,
                total_days_validated=(datetime.strptime(cumulative_end, "%Y-%m-%d") - datetime.strptime(cumulative_start, "%Y-%m-%d")).days + 1
            )
            print("✅ Run window recorded")
        except Exception as e:
            print(f"⚠️ Run window record failed: {e}")

        # Step 14: Save reports (creator-inclusive)
        print("📑 Step 14: Save reports…")
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
        print(f"✅ Saved: {detailed_report_path}")

        os.makedirs(f"data/{today_str}", exist_ok=True)
        dashboard_path = f"data/{today_str}/validation_result.xlsx"
        keep_cols = ['Invoice_ID','Invoice_Number','Invoice_Date','Vendor_Name','Amount','Invoice_Creator_Name',
                     'Validation_Status','Issues_Found','Issue_Details','GST_Number']
        dashboard_df = detailed_df[keep_cols].copy() if not detailed_df.empty else pd.DataFrame(columns=keep_cols)
        if not dashboard_df.empty:
            dashboard_df["Status_Summary"] = dashboard_df.apply(
                lambda r: f"{r['Validation_Status']} - {r['Issues_Found']} issues" if r['Issues_Found'] > 0 else f"{r['Validation_Status']} - No issues",
                axis=1
            )
        dashboard_df.to_excel(dashboard_path, index=False, engine="openpyxl")
        print(f"📊 Dashboard saved: {dashboard_path}")

        delta_path = f"data/delta_report_{today_str}.xlsx"
        dashboard_df.to_excel(delta_path, index=False, engine="openpyxl")
        print(f"📈 Delta saved: {delta_path}")

        summary_html_path = f"data/email_summary_{today_str}.html"
        with open(summary_html_path, "w", encoding="utf-8") as f:
            f.write(email_summary["html_summary"])
        print(f"📧 Email summary html saved: {summary_html_path}")

        # Step 15: Enhance + exact-format final + SINGLE email
        print("📮 Step 15: Enhance + build exact-format + send email…")
        _ = enhance_validation_results(detailed_df, email_summary)

        # exact-format final attachment (based on filtered_df)
        final_df = build_final_validation_report(filtered_df, run_dir, datetime.now())
        final_path = os.path.join("data", f"invoice_validation_detailed_{today_str}_FINAL.xlsx")
        with pd.ExcelWriter(final_path, engine="openpyxl") as xw:
            final_df.to_excel(xw, sheet_name="Validation Report", index=False)

        # real invoices.zip from this run dir
        invoices_zip_path = os.path.join(run_dir, "invoices.zip")

        # email contents
        stats = email_summary.get("statistics", {})
        html_body = EnhancedEmailSystem().create_professional_html_template(
            {"failed": stats.get("failed_invoices", 0),
             "warnings": stats.get("warning_invoices", 0),
             "passed": stats.get("passed_invoices", 0)},
            datetime.now() + timedelta(days=3)
        )

        # Single send; wrapper will zip the two files together (if both exist)
        notifier = EmailNotifier()
        attachments = []
        if os.path.isfile(final_path): attachments.append(final_path)
        if os.path.isfile(invoices_zip_path): attachments.append(invoices_zip_path)
        subject = f"Invoice Validation Report - {today_str}"
        sent = notifier.send_validation_report(subject, html_body, attachments=attachments)
        if sent:
            print("📧 Email sent (final report + original invoices.zip bundled).")
        else:
            print("⚠️ Email send failed (returned False)")

        # Final summary to console
        print("✅ Detailed cumulative validation workflow completed successfully!\n")
        print("📊 FINAL SUMMARY:")
        print(f"   📦 Current batch: {batch_start} → {batch_end}")
        print(f"   🔄 Cumulative: {cumulative_start} → {cumulative_end}")
        print(f"   🗓️ Days covered: {(datetime.strptime(cumulative_end, '%Y-%m-%d') - datetime.strptime(cumulative_start, '%Y-%m-%d')).days + 1}")
        print(f"   📋 Processed: {len(detailed_df)}")
        if stats:
            print(f"   ✅ Passed: {stats.get('passed_invoices',0)} ({stats.get('pass_rate',0):.1f}%)")
            print(f"   ⚠️ Warnings: {stats.get('warning_invoices',0)}")
            print(f"   ❌ Failed: {stats.get('failed_invoices',0)}")
            print(f"   👤 Creators: {stats.get('total_creators',0)} (Unknown: {stats.get('unknown_creators',0)})")
        print(f"   ⏰ Next run: {VALIDATION_INTERVAL_DAYS} days")
        print(f"   🗂️ Archive threshold: {ACTIVE_VALIDATION_MONTHS} months")
        return True

    except Exception as e:
        logging.error(f"❌ Unexpected error: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        return False


if __name__ == "__main__":
    ok = run_invoice_validation()
    if not ok:
        print("❌ Detailed cumulative validation failed!")
        raise SystemExit(1)
    print("🎉 Detailed cumulative validation completed successfully!")
    raise SystemExit(0)
