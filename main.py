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
load_dotenv()
create_tables()

# ============== Config ==============
VALIDATION_INTERVAL_DAYS = 4      # run validation every 4 days
VALIDATION_BATCH_DAYS    = 4      # each batch covers 4 days
ACTIVE_VALIDATION_MONTHS = 3      # maintain 3 months active data
ARCHIVE_FOLDER           = "archived_data"
SEND_EMAIL               = os.getenv("SEND_EMAIL", "0") == "1"   # OFF by default

# ============== Helpers ==============

def should_run_today() -> bool:
    """4-day cadence; override to always run by returning True at the top if desired."""
    # Force-run
    return True

    # -- cadence logic (kept for reference)
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
    """Kick off RMS download for the cumulative window; returns the run path (folder or file)."""
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")
    print(f"📥 Downloading cumulative validation data from {start_str} to {end_str}...")
    print(f"📊 Range covers: {(end_date - start_date).days + 1} days")
    return rms_download(start_date, end_date)


def _normalize_run_dir(run_path: str) -> Tuple[str, str]:
    """
    Accepts the return of rms_download which might be a directory OR a full path to invoice_download.xls.
    Returns (run_dir, invoice_path).
    """
    if os.path.isdir(run_path):
        run_dir = run_path
        invoice_path = os.path.join(run_dir, "invoice_download.xls")
    else:
        # assume file-like
        invoice_path = run_path
        run_dir = os.path.dirname(run_path)
    return run_dir, invoice_path


def validate_downloaded_files(run_dir: str) -> Tuple[bool, List[str]]:
    """Validate that required files exist in the run directory."""
    try:
        logging.info(f"🔍 Step 5: Verifying files in directory: {run_dir}")

        expected = ["invoice_download.xls", "invoices.zip"]
        missing, found = [], []

        for name in expected:
            p = os.path.join(run_dir, name)
            if os.path.exists(p):
                found.append(f"{name} ({os.path.getsize(p)} bytes)")
                logging.info(f"✅ Found {name}")
            else:
                missing.append(name)
                logging.error(f"❌ Missing file: {name}")

        if missing:
            return False, missing
        return True, found
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

    try:
        with open(invoice_file, "rb") as f:
            header = f.read(32)
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

    # Last resort: show sample
    try:
        with open(invoice_file, "r", encoding="utf-8", errors="ignore") as f:
            sample = f.read(500)
        print(f"📄 File sample:\n{repr(sample)}")
    except Exception as e:
        print(f"⚠️ Could not read file content: {e}")

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

_MOP_ALIASES = [
    "MOP","Mop","ModeOfPayment","Mode Of Payment","Mode_of_Payment",
    "Payment Mode","Payment_Mode","PaymentType","Payment Type"
]

def _try_load_creator_map(run_dir: str) -> dict:
    """
    Load mapping files created by scraper, e.g. inv_created_by.json / *creator*.* / *created_by*.* .
    """
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
    if re.search(r"\bcheque|check|dd|demand\s*draft)\b", s):        return "Cheque"
    if re.search(r"\bcash|petty\s*cash\b", s):                      return "Cash"
    return s.strip().title()

def _derive_payment_method(row) -> str:
    # Prefer explicit MOP-like columns
    for c in _MOP_ALIASES:
        if c in row and str(row.get(c) or "").strip():
            return map_payment_method(row.get(c))
    # Fall back to combined signals
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
    # Try from narration
    narr = str(row.get("Narration",""))
    m = re.search(r"(?:Due\s*Date|Payment\s*Due)\s*[:\-]?\s*([0-9]{1,2}[-/][0-9]{1,2}[-/][0-9]{2,4})", narr, re.I)
    if m:
        return m.group(1).strip()
    return ""

def _due_date_notification(due_date_str: str) -> str:
    if not str(due_date_str).strip():
        return ""
    # try parse with multiple formats
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
    if d < 0:
        return f"Overdue by {-d} day(s)"
    elif d == 0:
        return "Due today"
    elif d <= 7:
        return f"Due in {d} day(s)"
    else:
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
    print("🔍 Running detailed invoice-level validation…")
    try:
        summary_issues, problematic = validate_invoices(df)  # your existing rules
    except Exception as e:
        logger.warning(f"Base validation failed (continuing with detailed only): {e}")
        summary_issues, problematic = [], pd.DataFrame()

    creators_map = _try_load_creator_map(".")  # fallback, overridden later in final build
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
    if detailed_df.empty:
        return {"html_summary": "<p>No invoice data.</p>", "text_summary": "No invoice data.", "statistics": {}}

    total = len(detailed_df)
    passed  = (detailed_df["Validation_Status"] == "✅ PASS").sum()
    warned  = (detailed_df["Validation_Status"] == "⚠️ WARNING").sum()
    failed  = (detailed_df["Validation_Status"] == "❌ FAIL").sum()
    pass_rate = (passed / total * 100) if total else 0

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
    if detailed_df.empty:
        return []
    total = len(detailed_df)
    passed  = (detailed_df["Validation_Status"] == "✅ PASS").sum()
    warned  = (detailed_df["Validation_Status"] == "⚠️ WARNING").sum()
    failed  = (detailed_df["Validation_Status"] == "❌ FAIL").sum()
    return [
        {"Report_Type": "Overall_Summary", "Description": "Total Invoice Count", "Count": total, "Percentage": "100.0%", "Status": "INFO"},
        {"Report_Type": "Overall_Summary", "Description": "Passed Validation",   "Count": int(passed), "Percentage": f"{(passed/total*100):.1f}%", "Status": "PASS"},
        {"Report_Type": "Overall_Summary", "Description": "Warnings",            "Count": int(warned), "Percentage": f"{(warned/total*100):.1f}%", "Status": "WARNING"},
        {"Report_Type": "Overall_Summary", "Description": "Failed Validation",   "Count": int(failed), "Percentage": f"{(failed/total*100):.1f}%", "Status": "FAIL"},
    ]


def build_final_validation_report(src_df: pd.DataFrame,
                                  detailed_df: pd.DataFrame,
                                  run_dir: str,
                                  validation_dt: datetime) -> pd.DataFrame:
    """
    Build the final “Validation Report” by merging:
      - raw/export columns from src_df (to get MOP, Location, Remarks, DueDate, IGST/CGST/SGST/VAT, etc.)
      - validation outputs from detailed_df (Validation_Status, Issues_Found, Issue_Details, Creator)
    Removes duplicate “Invoice Currency” (keeps only 'Invoice_Currency').
    """
    src = src_df.copy()
    det = detailed_df.copy()

    # Create merge key on strongest available column
    key_src = _pick_key(src) or "Row#"
    key_det = _pick_key(det) or "Row#"
    src["_merge_key"] = src.get(key_src, pd.Series(range(1, len(src)+1)))
    det["_merge_key"] = det.get(key_det, pd.Series(range(1, len(det)+1)))

    merged = pd.merge(src, det, how="left", on="_merge_key", suffixes=("","_det"))

    cols = {c.lower(): c for c in merged.columns}
    def col(name: str) -> Optional[str]:
        return cols.get(name.lower())

    # Base / typical columns
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
    c_mop              = next((col(n) for n in _MOP_ALIASES if col(n)), None)

    # Inspected fields
    c_inv_created_by   = col("Inv Created By") or col("Invoice_Creator_Name") or col("Invoice_Creator_Name_det")
    c_inv_currency     = col("Inv Currency") or col("Invoice_Currency") or col("Invoice currency")
    c_inv_entry_date   = col("Inv Entry Date") or c_voucher_dt or c_purchase_inv_dt
    c_inv_mod_date     = col("Inv Mod Date")
    c_due_date_col     = col("DueDate") or col("Due Date")
    c_remarks          = col("Remarks")
    c_location_pref    = col("Location") or col("Branch") or col("State")
    c_scid_hash        = col("SCID#") or col("SCID")

    # Compose key pieces
    invoice_id     = merged[c_invid] if c_invid else (merged[c_voucher_no] if c_voucher_no else pd.Series([""]*len(merged)))
    invoice_number = merged[c_purchase_inv_no] if c_purchase_inv_no else (merged[c_voucher_no] if c_voucher_no else pd.Series([""]*len(merged)))
    invoice_date   = merged[c_purchase_inv_dt] if c_purchase_inv_dt else (merged[c_voucher_dt] if c_voucher_dt else pd.Series([""]*len(merged)))
    entry_date     = merged[c_inv_entry_date] if c_inv_entry_date else invoice_date
    mod_date       = merged[c_inv_mod_date] if c_inv_mod_date else pd.Series([""]*len(merged))
    remarks        = merged[c_remarks] if c_remarks else (merged[c_narration] if c_narration else pd.Series([""]*len(merged)))

    # Creator
    creators_map = _try_load_creator_map(run_dir)
    if c_inv_created_by:
        creator_series = merged[c_inv_created_by].astype(str).apply(lambda s: s.strip() or "System Generated")
    else:
        creator_series = merged.apply(lambda r: _derive_creator(r, creators_map), axis=1)

    # Location
    if c_location_pref:
        location_series = merged[c_location_pref].astype(str).apply(lambda s: s.strip())
    else:
        location_series = merged.apply(_derive_location, axis=1)

    # Method of payment
    if c_mop:
        mop_series = merged[c_mop].apply(map_payment_method)
    else:
        mop_series = merged.apply(_derive_payment_method, axis=1)

    # Account head
    account_series = merged.apply(_derive_account_head, axis=1)

    # Currency – keep only Invoice_Currency
    if c_inv_currency:
        currency_series = merged[c_inv_currency]
    elif c_currency_legacy:
        currency_series = merged[c_currency_legacy]
    else:
        currency_series = pd.Series([""]*len(merged))

    # Validation outputs from detailed
    vstatus_col = col("Validation_Status") or col("Validation_Status_det")
    issues_count_col = col("Issues_Found") or col("Issues_Found_det")
    issues_detail_col = col("Issue_Details") or col("Issue_Details_det")

    validation_status = merged[vstatus_col] if vstatus_col else pd.Series([""]*len(merged))
    issues_found = pd.to_numeric(merged[issues_count_col], errors="coerce").fillna(0).astype(int) if issues_count_col else pd.Series([0]*len(merged))
    issue_details = merged[issues_detail_col].fillna("")

    # Taxes
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
    tax_type = pd.Series([_tax_type_row(i) for i in range(len(merged))])

    # SCID
    if c_scid_hash:
        scid_series = merged[c_scid_hash].astype(str)
    else:
        scid_series = merged.apply(_derive_scid, axis=1)

    # Amount
    amount_series = merged[c_total] if c_total else pd.Series([0]*len(merged))

    # Due date + notification
    if c_due_date_col:
        due_series = merged[c_due_date_col].astype(str).fillna("")
    else:
        due_series = merged.apply(_derive_due_date, axis=1)
    notify_series = due_series.apply(_due_date_notification)

    final = pd.DataFrame({
        "Invoice_ID":             invoice_id,
        "Invoice_Number":         invoice_number,
        "Invoice_Date":           invoice_date,
        "Invoice_Entry_Date":     entry_date,
        "Vendor_Name":            merged[c_party] if c_party else pd.Series([""]*len(merged)),
        "Amount":                 amount_series,
        "Invoice_Creator_Name":   creator_series,
        "Location":               location_series,
        "Invoice_Currency":       currency_series,     # single currency column (no duplicate)
        "Method_of_Payment":      mop_series,
        "Account_Head":           account_series,
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
        # Traceability fields retained but NOT duplicated in main selection
        "Inv Entry Date":         entry_date,
        "Inv Mod Date":           mod_date,
        "Remarks":                remarks,
    })

    # final order (no duplicate Invoice Currency, only Due_Date)
    ordered_cols = [
        "Invoice_ID","Invoice_Number","Invoice_Date","Invoice_Entry_Date",
        "Vendor_Name","Amount","Invoice_Creator_Name","Location",
        "Invoice_Currency","Method_of_Payment","Account_Head","Validation_Status",
        "Issues_Found","Issue_Details","GST_Number","Row_Index","Validation_Date",
        "Tax_Type","Due_Date","Due_Date_Notification",
        "Total_Tax_Calculated","CGST_Amount","SGST_Amount","IGST_Amount",
        "VAT_Amount","TDS_Status","RMS_Invoice_ID","SCID",
        # inspected extras at end (kept, but harmless)
        "Inv Entry Date","Inv Mod Date","Remarks",
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
        print("📧 Email: exact-format attachment disabled by default (SEND_EMAIL=1 to enable)")
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
        run_path = download_cumulative_data(cumulative_start, cumulative_end)
        run_dir, invoice_path = _normalize_run_dir(run_path)
        print(f"✅ Run directory: {run_dir}")
        print(f"✅ Invoice path guess: {invoice_path}")

        # Step 5 - verify files
        ok, details = validate_downloaded_files(run_dir)
        if not ok:
            logging.error(f"❌ File validation failed: {details}")
            logging.error("❌ Aborting: Required files missing")
            return False
        logging.info(f"✅ All required files found: {details}")

        # Step 6 - read export
        if not os.path.exists(invoice_path):
            invoice_path = os.path.join(run_dir, "invoice_download.xls")
        print("📊 Step 6: Read RMS export…")
        src_df = read_invoice_file(invoice_path)
        if src_df is None or src_df.empty:
            print("❌ Empty dataframe after read")
            return False
        print(f"✅ Loaded: {src_df.shape}")

        # Step 7 - filter
        print("🔄 Step 7: Filter to cumulative range…")
        filtered_df = filter_invoices_by_date(src_df, cumulative_start, cumulative_end)
        print(f"📦 Working rows: {len(filtered_df)}")

        # Step 8 - validate
        print("🔎 Step 8: Detailed validation on cumulative…")
        detailed_df, summary_issues, problematic_df = validate_invoices_with_details(filtered_df)

        # Step 9 - email stats (even if we won't send, we compute for logs)
        print("📧 Step 9: Build email summary…")
        email_summary = generate_email_summary_statistics(
            detailed_df, cumulative_start, cumulative_end, batch_start, batch_end, today_str
        )

        # Step 10 - summary sheet rows for the xlsx
        print("📋 Step 10: Build summary sheet data…")
        summary_sheet_rows = generate_detailed_validation_report(detailed_df, today_str)

        # Step 11/12 - Persist snapshot + run window
        print("💾 Step 11: Save snapshot…")
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

        print("📝 Step 12: Record run window…")
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

        # Step 13: Save only the 3 requested files
        print("📑 Step 13: Save reports (3 files only)…")
        os.makedirs("data", exist_ok=True)

        # 13a) Invoice validation report (full detail tabs)
        detailed_report_path = f"data/invoice_validation_detailed_{today_str}.xlsx"
        with pd.ExcelWriter(detailed_report_path, engine="openpyxl") as writer:
            # Final validation sheet will be added after we compute exact-format below
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

        # 13b) Validation result (compact dashboard)
        os.makedirs(f"data/{today_str}", exist_ok=True)
        dashboard_path = f"data/{today_str}/validation_result.xlsx"
        keep_cols = ['Invoice_ID','Invoice_Number','Invoice_Date','Vendor_Name','Amount','Invoice_Creator_Name',
                     'Validation_Status','Issues_Found','Issue_Details','GST_Number']
        dashboard_df = detailed_df[keep_cols].copy()
        dashboard_df["Status_Summary"] = dashboard_df.apply(
            lambda r: f"{r['Validation_Status']} - {r['Issues_Found']} issue(s)"
                      if r['Issues_Found'] > 0 else f"{r['Validation_Status']} - No issues",
            axis=1
        )
        dashboard_df.to_excel(dashboard_path, index=False, engine="openpyxl")
        print(f"📊 Dashboard saved: {dashboard_path}")

        # 13c) Delta (reuse dashboard for now)
        delta_path = f"data/delta_report_{today_str}.xlsx"
        dashboard_df.to_excel(delta_path, index=False, engine="openpyxl")
        print(f"📈 Delta saved: {delta_path}")

        # Step 14: Build exact-format final sheet and append/overwrite appropriately
        print("🧩 Step 14: Build exact-format final sheet…")
        final_df = build_final_validation_report(filtered_df, detailed_df, run_dir, datetime.now())
        # Put it inside the same 'invoice_validation_detailed_*.xlsx' as the first sheet "Validation Report"
        with pd.ExcelWriter(detailed_report_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as xw:
            final_df.to_excel(xw, sheet_name="Validation Report", index=False)
        print(f"✅ Final sheet 'Validation Report' written into: {detailed_report_path}")

        # Step 15: OPTIONAL single email (attachments limited to 3 requested files)
        if SEND_EMAIL:
            print("📮 Step 15: Send email (3 attachments)…")
            stats = email_summary.get("statistics", {})
            html_body = EnhancedEmailSystem().create_professional_html_template(
                {"failed": stats.get("failed_invoices", 0),
                 "warnings": stats.get("warning_invoices", 0),
                 "passed": stats.get("passed_invoices", 0)},
                datetime.now() + timedelta(days=3)
            )
            notifier = EmailNotifier()
            attachments = [detailed_report_path, dashboard_path, delta_path]
            subject = f"Invoice Validation Results - {today_str}"
            sent = notifier.send_validation_report(subject, html_body, attachments=attachments)
            print("📧 Email sent." if sent else "⚠️ Email send returned False")
        else:
            print("✉️ Email sending skipped (SEND_EMAIL not set).")

        # Final summary to console
        print("✅ Detailed cumulative validation workflow completed successfully!\n")
        print("📊 FINAL SUMMARY:")
        print(f"   📦 Current batch: {batch_start} → {batch_end}")
        print(f"   🔄 Cumulative: {cumulative_start} → {cumulative_end}")
        print(f"   🗓️ Days covered: {(datetime.strptime(cumulative_end, '%Y-%m-%d') - datetime.strptime(cumulative_start, '%Y-%m-%d')).days + 1}")
        print(f"   📋 Processed: {len(detailed_df)}")
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
