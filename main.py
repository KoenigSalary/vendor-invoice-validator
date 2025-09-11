Created: 2025-09-03 13:54:41
Version: Production-Ready (Fixed)
"""
from rms_scraper import rms_download
from validator_utils import validate_invoices
# from updater import update_invoice_status        # (unused; keep commented if not used)
# from reporter import save_snapshot_report         # (unused; keep commented if not used)
from dotenv import load_dotenv
from datetime import datetime, timedelta
from pathlib import Path
from email_notifier import EmailNotifier
from invoice_tracker import (
    create_tables,
    save_invoice_snapshot,
    record_run_window,
    get_last_run_date,
    get_first_validation_date,
)
import pandas as pd
import os
import shutil

# ---------- Optional enhanced processor ----------
try:
    from enhanced_processor import enhance_validation_results
    ENHANCED_PROCESSOR_AVAILABLE = True
except ImportError:
    print("⚠️ Enhanced processor not available, using standard validation")
    ENHANCED_PROCESSOR_AVAILABLE = False
    def enhance_validation_results(df, email_summary):
        return {'success': False, 'error': 'Enhanced processor not available'}

# ---------- Env & DB bootstrap ----------
load_dotenv()
create_tables()

# ---------- Config ----------
VALIDATION_INTERVAL_DAYS = 4
VALIDATION_BATCH_DAYS = 4
ACTIVE_VALIDATION_MONTHS = 3
ARCHIVE_FOLDER = "archived_data"
EMAIL_ENABLED = True  # set False to skip email notifications

# ---------- Helpers: field mappers ----------
# ---------- Helpers: field mappers (REPLACE THIS WHOLE BLOCK) ----------
def _first_nonempty(row, cols):
    """Return the first non-empty string value from the given columns in this row."""
    for c in cols:
        if c in row and pd.notna(row[c]):
            v = str(row[c]).strip()
            if v and v.lower() not in ("nan", "none", "null"):
                return v
    return None

def _to_date_safe(v):
    try:
        return pd.to_datetime(v, errors="coerce")
    except Exception:
        return pd.NaT

def _fmt_date_or_default(dt, default="Entry Date Not Available"):
    return dt.strftime("%Y-%m-%d") if pd.notna(dt) else default

def map_account_head(row):
    """Best-effort Account Head mapping from common RMS columns."""
    prefer_cols = [
        "Account_Head", "AccountHead", "GL_Account", "GLAccount",
        "HeadAccount", "Head_Account", "ExpenseHead", "Expense_Head"
    ]
    v = _first_nonempty(row, prefer_cols)
    if v:
        return v
    # RMS export often puts the logical head in PurchaseLEDGER
    if 'PurchaseLEDGER' in row and pd.notna(row['PurchaseLEDGER']) and str(row['PurchaseLEDGER']).strip():
        return str(row['PurchaseLEDGER']).strip()
    return "Unknown"

import re




def map_invoice_entry_date(row):
    # Tries every likely label
    candidates = [
        "Inv Entry Date","Invoice Entry Date","InvEntryDate","InvoiceEntryDate",
        "Entry Date","Entry_Date","EntryDate",
        "Created On","CreatedOn","CreatedDate","Created_Date",
        "Voucher Entry Date","VoucherEntryDate","Voucherdate",   # last-resort
        "PurchaseInvDate"                                       # very last
    ]
    v = _first_nonempty(row, candidates)
    if not v: return "Entry Date Not Available"
    return _fmt_date_or_default(_to_date_safe(v), "Entry Date Not Available")

def map_invoice_modify_date(row):
    candidates = [
        "Inv Mod Date","Invoice Modify Date","Invoice_Modify_Date",
        "Modified On","ModifiedOn","ModifiedDate","Modified_Date",
        "Updated On","UpdatedOn","UpdatedDate","LastUpdated","Last_Changed","ModDate"
    ]
    v = _first_nonempty(row, candidates)
    if not v: return "Modify Date Not Available"
    return _fmt_date_or_default(_to_date_safe(v), "Modify Date Not Available")

def map_invoice_creator_name(row):
    explicit_cols = [
        "Inv Created By","Invoice Created By","Invoice_Created_By",
        "CreatedBy","Created_By","Creator","CreatorName","Creator_Name",
        "EntryBy","Entry_By","InputBy","Input_By",
        "UserName","User_Name","PreparedBy","Prepared_By","MadeBy","Made_By",
        "EnteredBy","Entered_By","Operator","Operator_Name"
    ]
    v = _first_nonempty(row, explicit_cols)
    if v: return v
    # Fallbacks: try to mine Narration/Remarks for "by <name>"
    txt = _first_nonempty(row, ["Narration","Remarks","Description","Notes"])
    if txt:
        m = re.search(r"\bby\s+([A-Za-z][A-Za-z\.\s'-]{2,60})", str(txt), flags=re.I)
        if m:
            guess = m.group(1).strip()
            if len(guess) > 2:
                return guess
    return "Unknown"

_MOP_REGEXES = [
    (re.compile(r"\b(neft|rtgs|imps|wire|bank\s*transfer|swift|tt)\b", re.I), "Bank Transfer"),
    (re.compile(r"\b(credit|debit)\s*card\b|\b(card\s*payment)\b|\bpos\b", re.I), "Credit Card"),
    (re.compile(r"\bupi\b|\bgpay\b|\bgoogle\s*pay\b|\bphonepe\b|\bpaytm\b|vpa@", re.I), "UPI"),
    (re.compile(r"\bcash\b", re.I), "Cash"),
    (re.compile(r"\bcheque|\bcheck\b|\bdd\s*demand\s*draft\b", re.I), "Cheque"),
    (re.compile(r"\bwallet\b", re.I), "Wallet"),
]

def _normalize_mop_text(val):
    if not val: return None
    s = str(val)
    for rx, label in _MOP_REGEXES:
        if rx.search(s):
            return label
    return None

def map_method_of_payment(row):
    # 1) True MOP columns if they exist
    explicit = _first_nonempty(row, [
        "MOP","Mop","ModeOfPayment","Mode_of_Payment","PaymentMode","Payment_Mode",
        "PaymentMethod","Payment_Method","Method_of_Payment"
    ])
    label = _normalize_mop_text(explicit)
    if label: return label

    # 2) Narration/Remarks – strongest real-world signal
    narr = _first_nonempty(row, ["Narration","Remarks","Description","Notes"])
    label = _normalize_mop_text(narr)
    if label: return label

    # 3) Amount logic (keep only as final fallback)
    try:
        party_amt = float(str(row.get("PaytyAmt","")).replace(",","") or 0)
        total_amt = float(str(row.get("Total","")).replace(",","") or 0)
        if total_amt > 0:
            if party_amt == 0: return "Credit Terms"
            if party_amt == total_amt: return "Full Payment"
            if 0 < party_amt < total_amt: return "Partial Payment"
            if party_amt > total_amt: return "Advance Payment"
    except Exception:
        pass

    # 4) Tiny hints from VoucherTypeName
    vt = str(row.get("VoucherTypeName","")).lower()
    if "bank" in vt: return "Bank Transfer"
    if "cash" in vt: return "Cash"
    if ("cheque" in vt) or ("check" in vt): return "Cheque"

    return "Standard Payment Terms"

def map_invoice_currency(row):
    v = _first_nonempty(row, ["Inv Currency","Invoice Currency","InvoiceCurrency","Currency","CurrencyCode","Curr"])
    if not v: return "INR"
    v = str(v).strip().upper()
    if v in {"₹","RUPEE","RS","INR"}: return "INR"
    if v in {"USD","US$","$"}: return "USD"
    if v in {"EUR","€"}: return "EUR"
    if v in {"GBP","£"}: return "GBP"
    return v

# Known short codes you showed (e.g., DELHI 1)
_LOCATION_TOKENS = [
    "DELHI 1","DELHI 2","DELHI 3","NOIDA","GURGAON","MUMBAI","PUNE","BENGALURU","BANGALORE",
    "HYDERABAD","CHENNAI","KOLKATA","JAIPUR","GOA","DUBAI","LONDON","US","USA"
]

def map_invoice_location(row):
    # 1) Obvious columns
    v = _first_nonempty(row, ["Location","Inv Location","Invoice Location","Branch","Office","Site"])
    if v: return v
    # 2) State/City
    v = _first_nonempty(row, ["State","City"])
    if v: return v
    # 3) Scan all text-like fields for well-known tokens (e.g., “DELHI 1”)
    for col in ["Narration","Remarks","Description","Invoice Address","Address","PartyName"]:
        s = str(row.get(col,""))
        for token in _LOCATION_TOKENS:
            if token.lower() in s.lower():
                return token
    return "Unknown"
    
# ---------- Scheduling ----------
def should_run_today():
    """Check if validation should run today based on 4-day interval."""
    try:
        last_run = get_last_run_date()
        if not last_run:
            print("🆕 No previous runs found - running first validation")
            return True
        last_dt = datetime.strptime(last_run, "%Y-%m-%d")
        days_since = (datetime.today() - last_dt).days
        print(f"📅 Last run: {last_run}, Days since: {days_since}")
        if days_since >= VALIDATION_INTERVAL_DAYS:
            print(f"✅ Time to run validation (>= {VALIDATION_INTERVAL_DAYS} days)")
            return True
        print(f"⏳ Too early to run validation (need {VALIDATION_INTERVAL_DAYS - days_since} more days)")
        return False
    except Exception as e:
        print(f"⚠️ Error checking run schedule: {e}, defaulting to run")
        return True

def get_current_batch_dates():
    """Get date range for current 4-day batch (yesterday back 3 days)."""
    end_date = datetime.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=VALIDATION_BATCH_DAYS - 1)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

def get_cumulative_validation_range():
    """Get cumulative range from first validation within last 3 months to current batch."""
    try:
        first = get_first_validation_date()
        if not first:
            return get_current_batch_dates()
        first_dt = datetime.strptime(first, "%Y-%m-%d")
        three_months_ago = datetime.today() - timedelta(days=30 * ACTIVE_VALIDATION_MONTHS)
        start_str = max(first_dt, three_months_ago).strftime("%Y-%m-%d")
        _, end_str = get_current_batch_dates()
        print(f"📅 Cumulative validation range: {start_str} to {end_str}")
        return start_str, end_str
    except Exception as e:
        print(f"⚠️ Error calculating cumulative range: {e}, using current batch")
        return get_current_batch_dates()

# ---------- Archiving ----------
def archive_data_older_than_three_months():
    """Archive validation data older than ACTIVE_VALIDATION_MONTHS."""
    print(f"🗂️ Archiving validation data older than {ACTIVE_VALIDATION_MONTHS} months...")
    try:
        data_dir = "data"
        archive_base = os.path.join(data_dir, ARCHIVE_FOLDER)
        validation_archive = os.path.join(archive_base, "validation_reports")
        snapshot_archive = os.path.join(archive_base, "snapshots")
        daily_data_archive = os.path.join(archive_base, "daily_data")
        for d in [archive_base, validation_archive, snapshot_archive, daily_data_archive]:
            os.makedirs(d, exist_ok=True)

        cutoff_date = datetime.today() - timedelta(days=30 * ACTIVE_VALIDATION_MONTHS)
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        print(f"📅 Archiving data older than: {cutoff_str}")

        archived = 0
        if os.path.exists(data_dir):
            # Move top-level dated reports
            for filename in os.listdir(data_dir):
                try:
                    fp = os.path.join(data_dir, filename)
                    if not os.path.isfile(fp):
                        continue
                    date_extracted = None
                    if filename.startswith("invoice_validation_detailed_") and filename.endswith(".xlsx"):
                        date_extracted = datetime.strptime(filename.replace("invoice_validation_detailed_", "").replace(".xlsx", ""), "%Y-%m-%d")
                    elif filename.startswith("validation_summary_") and filename.endswith(".xlsx"):
                        date_extracted = datetime.strptime(filename.replace("validation_summary_", "").replace(".xlsx", ""), "%Y-%m-%d")
                    elif filename.startswith("delta_report_") and filename.endswith(".xlsx"):
                        date_extracted = datetime.strptime(filename.replace("delta_report_", "").replace(".xlsx", ""), "%Y-%m-%d")
                    if date_extracted and date_extracted < cutoff_date:
                        shutil.move(fp, os.path.join(validation_archive, filename))
                        print(f"📦 Archived report: {filename}")
                        archived += 1
                except ValueError:
                    continue
                except Exception as e:
                    print(f"⚠️ Error archiving file {filename}: {e}")
                    continue

            # Move old daily folders
            for item in os.listdir(data_dir):
                p = os.path.join(data_dir, item)
                if os.path.isdir(p) and item != ARCHIVE_FOLDER:
                    try:
                        folder_dt = datetime.strptime(item, "%Y-%m-%d")
                        if folder_dt < cutoff_date:
                            shutil.move(p, os.path.join(daily_data_archive, item))
                            print(f"📦 Archived daily data folder: {item}")
                            archived += 1
                    except ValueError:
                        continue
                    except Exception as e:
                        print(f"⚠️ Error archiving folder {item}: {e}")

        # DB archival hook
        try:
            from invoice_tracker import archive_validation_records_before_date
            archive_validation_records_before_date(cutoff_str)
            print(f"✅ Database records archived before {cutoff_str}")
        except Exception as e:
            print(f"⚠️ Database archiving failed: {e}")

        print(f"✅ Archiving completed. {archived} items archived to {archive_base}")
        return archived
    except Exception as e:
        print(f"❌ Archiving failed: {e}")
        return 0

# ---------- Download & file checks ----------
def download_cumulative_data(start_str, end_str):
    """Download invoice data for the cumulative validation range."""
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")
    print(f"📥 Downloading cumulative validation data from {start_str} to {end_str}...")
    print(f"📊 Range covers: {(end_date - start_date).days + 1} days")
    return rms_download(start_date, end_date)

def validate_downloaded_files(download_dir):
    """Validate presence & basic integrity of expected files."""
    required_files = ["invoice_download.xls", "invoices.zip"]
    results = {}
    for fname in required_files:
        path = os.path.join(download_dir, fname)
        if os.path.exists(path):
            size = os.path.getsize(path)
            print(f"✅ Found {fname}: {size} bytes")
            results[fname] = "small" if size < 50 else "ok"
            try:
                with open(path, 'rb') as f:
                    header = f.read(20)
                print(f"🔍 {fname} header: {header}")
            except Exception as e:
                print(f"⚠️ Could not read {fname} header: {e}")
        else:
            print(f"❌ Missing file: {fname}")
            results[fname] = "missing"
    return results

# ---------- Column detection ----------
def find_creator_column(df):
    """Find creator column name (case-insensitive fallback)."""
    candidates = [
        'CreatedBy', 'Created_By', 'InvoiceCreatedBy', 'Invoice_Created_By',
        'UserName', 'User_Name', 'CreatorName', 'Creator_Name',
        'EntryBy', 'Entry_By', 'InputBy', 'Input_By',
        'PreparedBy', 'Prepared_By', 'MadeBy', 'Made_By'
    ]
    for c in candidates:
        if c in df.columns: 
            print(f"✅ Found creator column: {c}")
            return c
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            found = lower_map[c.lower()]
            print(f"✅ Found creator column (case-insensitive): {found}")
            return found
    for c in df.columns:
        if any(w in c.lower() for w in ['create', 'by', 'user', 'entry', 'made', 'prepared']):
            print(f"⚠️ Potential creator column found: {c}")
            return c
    print("⚠️ No creator column found, will use Unknown")
    return None
    
# ---------- File reader (TSV-first heuristic) ----------
def read_invoice_file(invoice_file):
    """Robust reader; prefer TSV for RMS 'xls', fallback to real Excel/CSV/HTML."""
    print(f"🔍 Attempting to read file: {invoice_file}")
    if not os.path.exists(invoice_file):
        raise FileNotFoundError(f"Invoice file not found: {invoice_file}")

    p = Path(invoice_file)
    file_ext = p.suffix.lower()
    size = os.path.getsize(invoice_file)
    print(f"📄 File: {p.name}, Extension: {file_ext}, Size: {size} bytes")
    if size < 50:
        raise ValueError(f"File appears too small ({size} bytes)")

    try:
        with open(invoice_file, 'rb') as f:
            header = f.read(50)
        print(f"🔍 File header (first 20 bytes): {header[:20]}")
    except Exception as e:
        print(f"⚠️ Could not read file header: {e}")
        header = b""

    # Heuristics
    is_xlsx = header.startswith(b'PK')                   # XLSX (zip)
    is_old_xls = header.startswith(b'\xD0\xCF\x11\xE0')  # Legacy XLS (OLE)
    looks_like_tsv = (b'\t' in header[:20]) or header[:20].strip().replace(b'_', b'').isalnum()

    # 1) TSV preferred (RMS 'xls' is typically TSV)
    if looks_like_tsv and not is_xlsx and not is_old_xls:
        try:
            df = pd.read_csv(invoice_file, sep='\t', dtype=str, keep_default_na=False)
            print(f"✅ Successfully read as TSV. Shape: {df.shape}")
            print(f"📋 Columns: {list(df.columns)}")
            return df
        except Exception as e:
            print(f"⚠️ TSV read failed; will try Excel/CSV: {e}")

    # 2) Real XLSX
    if is_xlsx:
        try:
            print("📊 Detected XLSX; reading with openpyxl...")
            df = pd.read_excel(invoice_file, engine='openpyxl')
            print(f"✅ XLSX read. Shape: {df.shape}")
            print(f"📋 Columns: {list(df.columns)}")
            return df
        except Exception as e:
            print(f"⚠️ openpyxl failed: {e}")

    # 3) Legacy XLS
    if is_old_xls or file_ext == '.xls':
        try:
            print("📊 Detected legacy XLS; trying xlrd...")
            df = pd.read_excel(invoice_file, engine='xlrd')
            print(f"✅ Legacy XLS read. Shape: {df.shape}")
            print(f"📋 Columns: {list(df.columns)}")
            return df
        except Exception as e:
            print(f"⚠️ xlrd failed: {e}")

    # 4) CSV fallback (probe separators)
    try:
        print("📄 Attempting to read as CSV...")
        for sep in [',', ';', '\t', '|']:
            try:
                probe = pd.read_csv(invoice_file, sep=sep, nrows=5)
                if probe.shape[1] > 1:
                    df = pd.read_csv(invoice_file, sep=sep)
                    print(f"✅ CSV read with sep='{sep}'. Shape: {df.shape}")
                    print(f"📋 Columns: {list(df.columns)}")
                    return df
            except Exception:
                continue
        print("⚠️ CSV reading failed with all separators")
    except Exception as e:
        print(f"⚠️ CSV reading failed: {e}")

    # 5) HTML fallback
    try:
        print("🌐 Attempting to read as HTML...")
        tables = pd.read_html(invoice_file, flavor='lxml')
        if tables:
            df = tables[0]
            print(f"✅ HTML table read. Shape: {df.shape}")
            print(f"📋 Columns: {list(df.columns)}")
            return df
        print("⚠️ No tables found in HTML")
    except Exception as e:
        print(f"⚠️ HTML parsing failed: {e}")

    # 6) Empty/minimal guard
    try:
        with open(invoice_file, 'r', encoding='utf-8', errors='ignore') as f:
            sample = f.read(500)
        print(f"📄 File content sample: {repr(sample[:100])}")
        if 'VoucherN' in sample or len(sample.strip()) < 100:
            print("⚠️ RMS returned empty/minimal data - generating placeholder row")
            df = pd.DataFrame([{
                'InvID': 'EMPTY_001',
                'PurchaseInvNo': 'NO-DATA-001',
                'PurchaseInvDate': datetime.now().strftime('%Y-%m-%d'),
                'PartyName': 'No Data Available',
                'Total': 0.00,
                'GSTNO': '',
                'CreatedBy': 'System',
                'Status': 'Empty Data Period'
            }])
            print(f"✅ Created placeholder data. Shape: {df.shape}")
            return df
    except Exception as e:
        print(f"⚠️ Could not read file content: {e}")

    raise Exception("Could not read invoice file in any supported format.")


def enrich_missing_fields(df):
    """
    Enrich the dataframe with missing invoice fields.
    Ensures all required fields are present and mapped properly.
    """
    # Ensure all required columns exist
    for col in [
        "Invoice_Entry_Date", "Invoice_Modify_Date", "Invoice_Creator_Name",
        "Method_of_Payment", "Invoice_Currency", "Invoice_Location", "Account_Head"
    ]:
        if col not in df.columns:
            df[col] = None

    # Apply mapping functions (these must exist above: map_invoice_entry_date, map_invoice_modify_date, etc.)
    df["Invoice_Entry_Date"]   = df.apply(map_invoice_entry_date, axis=1)
    df["Invoice_Modify_Date"]  = df.apply(map_invoice_modify_date, axis=1)
    df["Invoice_Creator_Name"] = df.apply(map_invoice_creator_name, axis=1)
    df["Method_of_Payment"]    = df.apply(map_method_of_payment, axis=1)
    df["Invoice_Currency"]     = df.apply(map_invoice_currency, axis=1)
    df["Invoice_Location"]     = df.apply(map_invoice_location, axis=1)
    df["Account_Head"]         = df.apply(map_account_head, axis=1)

    return df

# ---------- Filtering ----------
def filter_invoices_by_date(df, start_str, end_str):
    """Filter dataframe by PurchaseInvDate within [start, end]."""
    try:
        if 'PurchaseInvDate' not in df.columns:
            print("⚠️ PurchaseInvDate column not found, returning all data")
            return df
        start_date = datetime.strptime(start_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_str, "%Y-%m-%d")
        df["ParsedInvoiceDate"] = pd.to_datetime(df["PurchaseInvDate"], errors='coerce')
        filtered = df[(df["ParsedInvoiceDate"] >= start_date) & (df["ParsedInvoiceDate"] <= end_date)]
        print(f"📅 Filtered invoices from {start_str} to {end_str}: {len(filtered)} out of {len(df)}")
        return filtered
    except Exception as e:
        print(f"⚠️ Date filtering failed: {e}, returning all data")
        return df

# ---------- Detailed Validation ----------
def validate_invoices_with_details(df):
    print(f"🔍 DEBUG: Starting validation with {len(df)} invoices")
    print(f"🔍 DEBUG: DataFrame columns: {list(df.columns)}")
    print(f"🔍 DEBUG: Sample data from first row: {df.iloc[0].to_dict() if len(df) > 0 else 'No data'}")

    # Test helper functions first (guarded)
    if len(df) > 0:
        test_row = df.iloc[0]
        print("🔍 DEBUG: Testing helper functions on first row:")
        try:
            print(f"   - Entry Date: {map_invoice_entry_date(test_row)}")
        except Exception as e:
            print(f"   - Entry Date ERROR: {e}")
        try:
            print(f"   - Creator: {map_invoice_creator_name(test_row)}")
        except Exception as e:
            print(f"   - Creator ERROR: {e}")
        try:
            print(f"   - Payment: {map_method_of_payment(test_row)}")
        except Exception as e:
            print(f"   - Payment ERROR: {e}")
        try:
            print(f"   - Account: {map_account_head(test_row)}")
        except Exception as e:
            print(f"   - Account ERROR: {e}")

    print("🔍 Running detailed invoice-level validation...")
    try:
        # Run existing summary validator
        summary_issues, problematic_invoices_df = validate_invoices(df)

        # Optional: locate creator column
        _ = find_creator_column(df)

        detailed_results = []
        for idx, row in df.iterrows():
            try:
                invoice_id = row.get('InvID', f'Row_{idx}')
                invoice_number = row.get('PurchaseInvNo', row.get('InvoiceNumber', 'N/A'))
                invoice_date = row.get('PurchaseInvDate', 'N/A')
                vendor = row.get('PartyName', row.get('VendorName', 'N/A'))

                # safe numeric for Amount
                def _to_float_safe(v):
                    try:
                        return float(str(v).replace(',', '').strip() or 0)
                    except Exception:
                        return 0.0
                amount = _to_float_safe(row.get('Total', 0))

                # compute mappers
                invoice_entry_date = map_invoice_entry_date(row)
                invoice_modify_date = map_invoice_modify_date(row)
                creator_name       = map_invoice_creator_name(row)
                method_of_payment  = map_method_of_payment(row)
                account_head       = map_account_head(row)
                invoice_currency   = map_invoice_currency(row)
                invoice_location   = map_invoice_location(row)

                # validation rules
                validation_issues = []
                severity = "✅ PASS"

                if pd.isna(row.get('GSTNO')) or str(row.get('GSTNO')).strip() == '':
                    validation_issues.append("Missing GST Number")
                    severity = "❌ FAIL"

                tot_val = str(row.get('Total', '')).strip()
                if tot_val == '' or tot_val.lower() == 'nan':
                    validation_issues.append("Missing Total Amount")
                    severity = "❌ FAIL"
                elif amount == 0:
                    validation_issues.append("Zero Amount")
                    if severity == "✅ PASS":
                        severity = "⚠️ WARNING"

                detailed_results.append({
                    'Invoice_ID': invoice_id,
                    'Invoice_Number': invoice_number,
                    'Invoice_Date': invoice_date,
                    'Invoice_Entry_Date': invoice_entry_date,
                    'Invoice_Modify_Date': invoice_modify_date,
                    'Invoice_Creator_Name': creator_name,
                    'Method_of_Payment': method_of_payment,
                    'Account_Head': account_head,
                    'Invoice_Currency': invoice_currency,
                    'Invoice_Location': invoice_location,
                    'Vendor_Name': vendor,
                    'Amount': amount,
                    'Validation_Status': severity,
                    'Issues_Found': len(validation_issues),
                    'Issue_Details': " | ".join(validation_issues) if validation_issues else "No issues found",
                    'GST_Number': row.get('GSTNO', ''),
                    'Row_Index': idx,
                    'Validation_Date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
            except Exception as row_error:
                print(f"⚠️ Error processing invoice {idx}: {row_error}")
                detailed_results.append({
                    'Invoice_ID': f'ERROR_{idx}',
                    'Invoice_Number': 'PROCESSING_ERROR',
                    'Invoice_Date': 'N/A',
                    'Invoice_Entry_Date': 'N/A',
                    'Vendor_Name': 'ERROR',
                    'Amount': 0,
                    'Invoice_Creator_Name': 'ERROR',
                    'Method_of_Payment': 'ERROR',
                    'Account_Head': 'ERROR',
                    'Validation_Status': '❌ FAIL',
                    'Issues_Found': 1,
                    'Issue_Details': f"Processing Error: {row_error}",
                    'GST_Number': '',
                    'Row_Index': idx,
                    'Validation_Date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
                continue

        detailed_df = pd.DataFrame(detailed_results)
        print(f"✅ Detailed validation completed: {len(detailed_df)} records processed")
        return detailed_df, summary_issues, problematic_invoices_df

    except Exception as e:
        print(f"❌ Detailed validation failed: {e}")
        import traceback; traceback.print_exc()
        empty = pd.DataFrame({
            'Invoice_ID': [], 'Invoice_Number': [], 'Invoice_Date': [], 'Invoice_Entry_Date': [],
            'Vendor_Name': [], 'Amount': [], 'Invoice_Creator_Name': [], 'Method_of_Payment': [],
            'Account_Head': [], 'Validation_Status': [], 'Issues_Found': [], 'Issue_Details': [],
            'GST_Number': [], 'Row_Index': [], 'Validation_Date': []
        })
        return empty, [], pd.DataFrame()

# ---------- Email summaries & reports ----------
def generate_email_summary_statistics(detailed_df, cumulative_start, cumulative_end, current_batch_start, current_batch_end, today_str):
    print("📧 Generating email summary statistics...")
    try:
        if detailed_df.empty:
            return {
                'html_summary': "No invoice data available for validation.",
                'text_summary': "No invoice data available for validation.",
                'statistics': {}
            }

        total_invoices = len(detailed_df)
        passed = (detailed_df['Validation_Status'] == '✅ PASS').sum()
        warnings = (detailed_df['Validation_Status'] == '⚠️ WARNING').sum()
        failed = (detailed_df['Validation_Status'] == '❌ FAIL').sum()

        pass_rate = (passed / total_invoices * 100) if total_invoices else 0
        issue_rate = ((warnings + failed) / total_invoices * 100) if total_invoices else 0

        # Top issues
        all_issues = []
        for txt in detailed_df['Issue_Details']:
            if txt != "No issues found":
                all_issues.extend(txt.split(' | '))
        issue_counts = {}
        for i in all_issues:
            issue_counts[i] = issue_counts.get(i, 0) + 1
        top_issues = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:5]

        creator_stats = detailed_df['Invoice_Creator_Name'].value_counts()
        unknown_creators = creator_stats.get('Unknown', 0)
        total_creators = len(creator_stats)

        html = f"""
        
          📊 Invoice Validation Summary - {today_str}
          📅 Validation Period
          
            Current Batch: {current_batch_start} to {current_batch_end}
            Cumulative Range: {cumulative_start} to {cumulative_end}
            Total Coverage: {(datetime.strptime(cumulative_end, '%Y-%m-%d') - datetime.strptime(cumulative_start, '%Y-%m-%d')).days + 1} days
          
          Results
          
            ✅ Total Invoices: {total_invoices:,}
            ✅ Passed: {passed:,} ({pass_rate:.1f}%)
            ⚠️ Warnings: {warnings:,}
            ❌ Failed: {failed:,}
          
          🔍 Top Validation Issues
          
        """
        for issue, count in top_issues:
            pct = (count / total_invoices * 100) if total_invoices else 0
            html += f"{issue}: {count:,} invoices ({pct:.1f}%)"
        html += f"""
          
          👤 Invoice Creator Analysis
          
            Total Creators: {total_creators}
            Unknown Creators: {unknown_creators} invoices ({(unknown_creators/total_invoices*100):.1f}%)
          
        
        """

        text = f"""📊 INVOICE VALIDATION SUMMARY - {today_str}

📅 VALIDATION PERIOD:
    • Current Batch: {current_batch_start} to {current_batch_end}
• Cumulative Range: {cumulative_start} to {cumulative_end}
• Total Coverage: {(datetime.strptime(cumulative_end, '%Y-%m-%d') - datetime.strptime(cumulative_start, '%Y-%m-%d')).days + 1} days

📈 RESULTS:
    ✅ Total Invoices: {total_invoices:,}
✅ Passed: {passed:,} ({pass_rate:.1f}%)
⚠️ Warnings: {warnings:,}
❌ Failed: {failed:,}

👤 CREATOR ANALYSIS:
    • Total Creators: {total_creators}
• Unknown Creators: {unknown_creators} invoices ({(unknown_creators/total_invoices*100):.1f}%)

🔍 TOP VALIDATION ISSUES:
    """
        for i, (issue, count) in enumerate(top_issues, 1):
            pct = (count / total_invoices * 100) if total_invoices else 0
            text += f"{i}. {issue}: {count:,} invoices ({pct:.1f}%)\n"
        text += "\nNote: Detailed invoice-level validation report is attached."

        stats = {
            'total_invoices': total_invoices,
            'passed_invoices': passed,
            'warning_invoices': warnings,
            'failed_invoices': failed,
            'pass_rate': pass_rate,
            'issue_rate': issue_rate,
            'top_issues': top_issues,
            'total_creators': total_creators,
            'unknown_creators': unknown_creators,
            'validation_date': today_str,
            'current_batch_start': current_batch_start,
            'current_batch_end': current_batch_end,
            'cumulative_start': cumulative_start,
            'cumulative_end': cumulative_end,
            'total_coverage_days': (datetime.strptime(cumulative_end, '%Y-%m-%d') - datetime.strptime(cumulative_start, '%Y-%m-%d')).days + 1
        }

        print("✅ Email summary statistics generated:")
        print(f"   📊 Total: {total_invoices}, Pass Rate: {pass_rate:.1f}%")
        print(f"   📈 Issues: {len(top_issues)} types identified")
        print(f"   👤 Creators: {total_creators} total, {unknown_creators} unknown")

        return {'html_summary': html, 'text_summary': text, 'statistics': stats}
    except Exception as e:
        print(f"❌ Email summary generation failed: {e}")
        return {'html_summary': f"Error generating summary: {e}", 'text_summary': f"Error generating summary: {e}", 'statistics': {}}

def generate_detailed_validation_report(detailed_df, today_str):
    print("📋 Generating detailed validation report for Excel export...")
    try:
        if detailed_df.empty:
            return []
        total = len(detailed_df)
        passed = (detailed_df['Validation_Status'] == '✅ PASS').sum()
        warnings = (detailed_df['Validation_Status'] == '⚠️ WARNING').sum()
        failed = (detailed_df['Validation_Status'] == '❌ FAIL').sum()
        creator_stats = detailed_df['Invoice_Creator_Name'].value_counts()
        unknown = creator_stats.get('Unknown', 0)

        summary = [
            {'Report_Type': 'Overall_Summary', 'Description': 'Total Invoice Count', 'Count': total, 'Percentage': '100.0%', 'Status': 'INFO'},
            {'Report_Type': 'Overall_Summary', 'Description': 'Passed Validation', 'Count': passed, 'Percentage': f'{(passed/total*100):.1f}%' if total else '0%', 'Status': 'PASS'},
            {'Report_Type': 'Overall_Summary', 'Description': 'Warnings', 'Count': warnings, 'Percentage': f'{(warnings/total*100):.1f}%' if total else '0%', 'Status': 'WARNING'},
            {'Report_Type': 'Overall_Summary', 'Description': 'Failed Validation', 'Count': failed, 'Percentage': f'{(failed/total*100):.1f}%' if total else '0%', 'Status': 'FAIL'},
            {'Report_Type': 'Creator_Analysis', 'Description': 'Total Unique Creators', 'Count': len(creator_stats), 'Percentage': '100.0%', 'Status': 'INFO'},
            {'Report_Type': 'Creator_Analysis', 'Description': 'Unknown/Missing Creators', 'Count': unknown, 'Percentage': f'{(unknown/total*100):.1f}%' if total else '0%', 'Status': 'WARNING' if unknown > 0 else 'PASS'},
        ]
        print(f"✅ Detailed validation report prepared with {len(summary)} summary entries")
        return summary
    except Exception as e:
        print(f"❌ Detailed report generation failed: {e}")
        return []

# ---------- Main workflow ----------
def run_invoice_validation():
    try:
        today = datetime.today()
        today_str = today.strftime("%Y-%m-%d")

        print(f"🚀 Starting ENHANCED cumulative validation workflow for {today_str}")
        print("📧 FEATURE: Enhanced email-ready summary with RMS field analysis")
        print("📋 FEATURE: Complete RMS field mapping (Creator, MOP, Account Head)")
        print("⚙️ Configuration:")
        print(f"   📅 Validation interval: {VALIDATION_INTERVAL_DAYS} days")
        print(f"   📦 Batch size: {VALIDATION_BATCH_DAYS} days")
        print(f"   🗓️ Active window: {ACTIVE_VALIDATION_MONTHS} months")
        print(f"   📁 Archive folder: {ARCHIVE_FOLDER}")

        # Step 1
        print("🔍 Step 1: Checking if validation should run today...")
        if not should_run_today():
            print("⏳ Skipping validation - not yet time for next interval")
            return True

        # Step 2
        print("🗂️ Step 2: Archiving data older than 3 months...")
        try:
            archived_count = archive_data_older_than_three_months()
            print("✅ No old data to archive" if archived_count == 0 else f"✅ Archived {archived_count} old items")
        except Exception as e:
            print(f"⚠️ Archiving failed but continuing: {e}")

        # Step 3
        print("📊 Step 3: Calculating cumulative validation range...")
        try:
            cumulative_start, cumulative_end = get_cumulative_validation_range()
            current_batch_start, current_batch_end = get_current_batch_dates()
            print(f"📅 Current batch: {current_batch_start} to {current_batch_end}")
            print(f"📅 Cumulative range: {cumulative_start} to {cumulative_end}")
        except Exception as e:
            print(f"❌ Failed to calculate date ranges: {e}")
            return False

        # Step 4
        print("📥 Step 4: Downloading cumulative validation data...")
        try:
            invoice_path = download_cumulative_data(cumulative_start, cumulative_end)
            print(f"✅ Cumulative data download completed. Path: {invoice_path}")
        except Exception as e:
            print(f"❌ Cumulative data download failed: {e}")
            return False

        # Step 5 - FIXED: Define variables before use
        download_dir = os.path.join("data", today_str)
        print(f"🔍 Step 5: Verifying files in directory: {download_dir}")
        validation_results = validate_downloaded_files(download_dir)

        # Step 6
        invoice_file = os.path.join(download_dir, "invoice_download.xls")
        if validation_results.get("invoice_download.xls") == "missing":
            print("⚠️ No invoice file downloaded. Skipping validation.")
            return True

        # Step 7
        print("📊 Step 7: Reading cumulative invoice data...")
        try:
            df = read_invoice_file(invoice_file)
            if df is None or df.empty:
                print("⚠️ DataFrame is empty after reading file; nothing to validate.")
                return True
            print(f"✅ Successfully loaded cumulative data. Shape: {df.shape}")
            print(f"📋 Columns: {list(df.columns)}")
        except Exception as e:
            print(f"❌ Failed to read invoice file: {e}")
            return False

        # Step 8
        print("🔄 Step 8: Filtering to cumulative validation range...")
        try:
            filtered_df = filter_invoices_by_date(df, cumulative_start, cumulative_end)
            print(f"📅 Working with {len(filtered_df)} invoices in cumulative range")
            if filtered_df.shape[0] == 0:
                print("⚠️ RMS returned zero rows; skipping validation and email.")
                return True
        except Exception as e:
            print(f"⚠️ Date filtering failed: {e}, using all data")
            filtered_df = df

        # Step 9
        print("🔄 Step 9: Running detailed validation on cumulative data...")
        print("   🔄 This includes:")
        print(f"      📦 Current batch: {current_batch_start} to {current_batch_end}")
        print(f"      🔄 ALL previously validated data from: {cumulative_start}")
        try:
            detailed_df, summary_issues, problematic_invoices_df = validate_invoices_with_details(filtered_df)
            if detailed_df.empty:
                print("⚠️ No detailed validation results generated")
            else:
                print(f"✅ Detailed validation completed on {len(detailed_df)} invoices")
        except Exception as e:
            print(f"❌ Detailed validation failed: {e}")
            return False

        # Step 10
        print("📧 Step 10: Generating email summary statistics...")
        try:
            email_summary = generate_email_summary_statistics(
                detailed_df,
                cumulative_start,
                cumulative_end,
                current_batch_start,
                current_batch_end,
                today_str
            )
        except Exception as e:
            print(f"⚠️ Email summary generation failed: {e}")
            email_summary = {'html_summary': f"Error generating summary: {e}", 'text_summary': f"Error generating summary: {e}", 'statistics': {}}

        # Step 11
        print("📋 Step 11: Generating detailed validation report...")
        try:
            detailed_report = generate_detailed_validation_report(detailed_df, today_str)
        except Exception as e:
            print(f"⚠️ Detailed report generation failed: {e}")
            detailed_report = []

        # Step 12
        print("💾 Step 12: Preparing invoice data for saving...")
        try:
            current_invoices_list = detailed_df.to_dict('records') if not detailed_df.empty else []
            print(f"📋 Prepared {len(current_invoices_list)} detailed invoice records for saving")
        except Exception as e:
            print(f"⚠️ Failed to prepare invoice list: {e}")
            current_invoices_list = []

        # Step 13
        try:
            save_invoice_snapshot(
                current_invoices_list,
                run_date=today_str,
                run_type="enhanced_cumulative_4day",
                batch_start=current_batch_start,
                batch_end=current_batch_end,
                cumulative_start=cumulative_start,
                cumulative_end=cumulative_end
            )
            print("✅ Enhanced validation snapshot saved")
        except Exception as e:
            print(f"⚠️ Failed to save snapshot: {e}")

        # Step 14
        try:
            record_run_window(
                current_batch_start,
                current_batch_end,
                run_type="enhanced_cumulative_4day",
                cumulative_start=cumulative_start,
                cumulative_end=cumulative_end,
                total_days_validated=(datetime.strptime(cumulative_end, "%Y-%m-%d") - datetime.strptime(cumulative_start, "%Y-%m-%d")).days + 1
            )
            
            print("✅ Enhanced cumulative run recorded")
        except Exception as e:
            print(f"⚠️ Failed to record run: {e}")

        # Step 15: Save Excel reports
        try:
            os.makedirs("data", exist_ok=True)
            detailed_report_path = f"data/invoice_validation_detailed_{today_str}.xlsx"

            if not detailed_df.empty:
                with pd.ExcelWriter(detailed_report_path, engine='openpyxl') as writer:
                    detailed_df.to_excel(writer, sheet_name='All_Invoices', index=False)

                    failed_df = detailed_df[detailed_df['Validation_Status'] == '❌ FAIL']
                    if not failed_df.empty:
                        failed_df.to_excel(writer, sheet_name='Failed_Invoices', index=False)

                    warning_df = detailed_df[detailed_df['Validation_Status'] == '⚠️ WARNING']
                    if not warning_df.empty:
                        warning_df.to_excel(writer, sheet_name='Warning_Invoices', index=False)

                    passed_df = detailed_df[detailed_df['Validation_Status'] == '✅ PASS']
                    if not passed_df.empty:
                        passed_df.to_excel(writer, sheet_name='Passed_Invoices', index=False)

                    creator_stats = detailed_df['Invoice_Creator_Name'].value_counts().reset_index()
                    creator_stats.columns = ['Creator_Name', 'Invoice_Count']
                    creator_stats.to_excel(writer, sheet_name='Creator_Analysis', index=False)

                    mop_stats = detailed_df['Method_of_Payment'].value_counts().reset_index()
                    mop_stats.columns = ['Method_of_Payment', 'Invoice_Count']
                    mop_stats.to_excel(writer, sheet_name='MOP_Analysis', index=False)

                    acc_stats = detailed_df['Account_Head'].value_counts().reset_index()
                    acc_stats.columns = ['Account_Head', 'Invoice_Count']
                    acc_stats.to_excel(writer, sheet_name='Account_Head_Analysis', index=False)

                    if detailed_report:
                        pd.DataFrame(detailed_report).to_excel(writer, sheet_name='Summary_Stats', index=False)

                print(f"✅ Enhanced invoice report saved: {detailed_report_path}")

                os.makedirs(f"data/{today_str}", exist_ok=True)
                dashboard_path = f"data/{today_str}/validation_result.xlsx"
                dashboard_cols = ['Invoice_ID', 'Invoice_Number', 'Invoice_Date', 'Invoice_Entry_Date',
                                  'Vendor_Name', 'Amount', 'Invoice_Creator_Name', 'Method_of_Payment',
                                  'Account_Head', 'Validation_Status', 'Issues_Found', 'Issue_Details', 'GST_Number']
                dashboard_df = detailed_df[dashboard_cols].copy()
                dashboard_df['Status_Summary'] = dashboard_df.apply(
                    lambda r: f"{r['Validation_Status']} - {r['Issues_Found']} issues" if r['Issues_Found'] > 0 else f"{r['Validation_Status']} - No issues",
                    axis=1
                )
                dashboard_df.to_excel(dashboard_path, index=False, engine='openpyxl')
                print(f"📋 Enhanced dashboard report created: {dashboard_path}")

                delta_report_path = f"data/delta_report_{today_str}.xlsx"
                dashboard_df.to_excel(delta_report_path, index=False, engine='openpyxl')
                print(f"📋 Enhanced delta report created: {delta_report_path}")

                summary_path = f"data/email_summary_{today_str}.html"
                with open(summary_path, 'w', encoding='utf-8') as f:
                    f.write(email_summary['html_summary'])
                print(f"📧 Email summary saved: {summary_path}")
            else:
                print("⚠️ No detailed validation results - creating empty report")
                pd.DataFrame(columns=[
                    'Invoice_ID','Invoice_Number','Invoice_Date','Invoice_Entry_Date','Vendor_Name','Amount',
                    'Invoice_Creator_Name','Method_of_Payment','Account_Head','Validation_Status','Issues_Found',
                    'Issue_Details','GST_Number','Status_Summary'
                ]).to_excel(detailed_report_path, index=False, engine='openpyxl')
                print(f"✅ Empty enhanced report created: {detailed_report_path}")
        except Exception as e:
            print(f"❌ Failed to save detailed reports: {e}")
            return False

        # Step 16: Enhanced processing
        print("🚀 Step 16: Applying enhanced features...")
        try:
            if ENHANCED_PROCESSOR_AVAILABLE:
                enhancement_result = enhance_validation_results(detailed_df, email_summary)
                if enhancement_result.get('success'):
                    print("✅ Enhancement successful!")
                    enhanced_df = enhancement_result['enhanced_df']
                    summary = enhancement_result['summary']
                    enhanced_report_path = f"data/enhanced_invoice_validation_detailed_{today_str}.xlsx"
                    with pd.ExcelWriter(enhanced_report_path, engine='openpyxl') as writer:
                        enhanced_df.to_excel(writer, sheet_name='Enhanced_All_Invoices', index=False)
                        enh_failed = enhanced_df[enhanced_df['Validation_Status'] == '❌ FAIL']
                        if not enh_failed.empty:
                            enh_failed.to_excel(writer, sheet_name='Enhanced_Failed', index=False)
                        pd.DataFrame([
                            {'Metric': 'Total Invoices', 'Value': summary['total_invoices']},
                            {'Metric': 'Currencies Processed', 'Value': summary['currencies']},
                            {'Metric': 'Global Locations', 'Value': summary['locations']},
                            {'Metric': 'Urgent Due Date Alerts', 'Value': summary['urgent_dues']},
                            {'Metric': 'Tax Calculations Completed', 'Value': summary['tax_calculated']},
                            {'Metric': 'Historical Changes Detected', 'Value': summary['historical_changes']},
                        ]).to_excel(writer, sheet_name='Enhanced_Summary', index=False)
                    print(f"✅ Enhanced report saved: {enhanced_report_path}")

                    if enhancement_result.get('enhanced_email_content'):
                        email_summary['html_summary'] = enhancement_result['enhanced_email_content']
                        email_summary['text_summary'] = enhancement_result['enhanced_email_content']
                        enhanced_email_path = f"data/enhanced_email_summary_{today_str}.html"
                        with open(enhanced_email_path, 'w', encoding='utf-8') as f:
                            f.write(enhancement_result['enhanced_email_content'])
                        print(f"📧 Enhanced email content saved: {enhanced_email_path}")

                    print("🔄 Enhancement Summary:")
                    print(f"   💱 Currencies: {summary['currencies']}")
                    print(f"   🌍 Locations: {summary['locations']}")
                    print(f"   ⏰ Urgent dues: {summary['urgent_dues']}")
                    print(f"   💰 Tax calculated: {summary['tax_calculated']}")
                    print(f"   🔄 Historical changes: {summary['historical_changes']}")
                else:
                    print(f"⚠️ Enhancement failed: {enhancement_result.get('error')}")
            else:
                print("⚠️ Enhanced processor not available, using standard validation")
        except Exception as e:
            print(f"⚠️ Enhancement failed: {e}")

        # Step 17: Email notifications - FIXED INDENTATION
        if EMAIL_ENABLED:
            try:
                from email_notifier import EmailNotifier
                notifier = EmailNotifier()

                # --- Decide whether invoices.zip is valid and attachable ---
                invoice_zip_path = None
                try:
                    if (
                        isinstance(validation_results, dict)
                        and validation_results.get("invoices.zip") == "ok"
                    ):
                        candidate = os.path.join(download_dir, "invoices.zip")
                        if os.path.exists(candidate) and os.path.getsize(candidate) > 0:
                            invoice_zip_path = candidate
                            print(f"📎 Will attach invoice copies: {invoice_zip_path}")
                        else:
                            print("⚠️ invoices.zip path missing or empty; not attaching.")
                    else:
                        print("ℹ️ invoices.zip not marked OK; not attaching.")
                except Exception as _zip_err:
                    print(f"⚠️ Could not evaluate invoices.zip for attachment: {_zip_err}")

                # --- Recipients from environment ---
                ap_team_recipients = [
                    e.strip()
                    for e in os.getenv("AP_TEAM_EMAIL_LIST", "").split(",")
                    if e.strip()
                ]

                if ap_team_recipients:
                    ok = False
                    try:
                        if hasattr(notifier, "send_detailed_validation_report"):
                            # Build kwargs to support newer notifier with optional attachments
                            kwargs = dict(
                                date_str=today_str,
                                recipients=ap_team_recipients,
                                email_summary=email_summary,
                                report_path=(
                                    detailed_report_path if not detailed_df.empty else None
                                ),
                                batch_start=current_batch_start,
                                batch_end=current_batch_end,
                                cumulative_start=cumulative_start,
                                cumulative_end=cumulative_end,
                            )
                            if invoice_zip_path:
                                kwargs["extra_attachments"] = [invoice_zip_path]

                            try:
                                ok = notifier.send_detailed_validation_report(**kwargs)
                            except TypeError:
                                # Notifier doesn't support extra_attachments; retry without it
                                ok = notifier.send_detailed_validation_report(
                                    today_str,
                                    ap_team_recipients,
                                    email_summary,
                                    (
                                        detailed_report_path
                                        if not detailed_df.empty
                                        else None
                                    ),
                                    current_batch_start,
                                    current_batch_end,
                                    cumulative_start,
                                    cumulative_end,
                                )

                            if ok:
                                msg_tail = " (with invoice copies)" if invoice_zip_path else ""
                                print(
                                    f"📧 Detailed validation report sent to AP team{msg_tail}: {', '.join(ap_team_recipients)}"
                                )
                            else:
                                print("❌ Failed to send detailed validation report")
                        else:
                            stats = email_summary.get("statistics", {})
                            total_issues = (
                                stats.get("failed_invoices", 0)
                                + stats.get("warning_invoices", 0)
                            )
                            ok = notifier.send_validation_report(
                                today_str, ap_team_recipients, total_issues
                            )
                            if ok:
                                print(
                                    f"📧 Basic validation report sent to AP team: {', '.join(ap_team_recipients)}"
                                )
                            else:
                                print("❌ Failed to send basic validation report")
                    except Exception as email_error:
                        print(f"⚠️ Enhanced email failed: {email_error}")
                        try:
                            stats = email_summary.get("statistics", {})
                            total_issues = (
                                stats.get("failed_invoices", 0)
                                + stats.get("warning_invoices", 0)
                            )
                            ok = notifier.send_validation_report(
                                today_str, ap_team_recipients, total_issues
                            )
                            if ok:
                                print("📧 Fallback validation report sent to AP team")
                            else:
                                print("❌ Fallback email also failed")
                        except Exception as fallback_error:
                            print(f"❌ All email methods failed: {fallback_error}")
                else:
                    print("⚠️ No AP team email recipients configured in AP_TEAM_EMAIL_LIST")

                print("📧 Email notification workflow completed!")

            except Exception as e:
                print(f"⚠️ Email sending failed: {e}")
                import traceback
                traceback.print_exc()
        else:
            print("📧 Email is disabled by configuration; skipping notifications.")
        
        print("🎉 Enhanced cumulative validation workflow completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Main workflow failed: {e}")
        import traceback
        traceback.print_exc()
        return False

# ---------- Quick self-test (optional) ----------
def test_validation_functions():
    print("🧪 Testing validation functions...")
    test_data = {
        'InvID': ['TEST001'],
        'PurchaseInvNo': ['INV-TEST-001'],
        'PurchaseInvDate': ['2025-09-01'],
        'Voucherdate': ['2025-09-01'],
        'PartyName': ['Test Vendor'],
        'Total': ['1,000.00'],
        'GSTNO': ['12345678901234'],
        'PaytyAmt': ['1,000.00'],
        'PurchaseLEDGER': ['Test Ledger'],
        'Narration': ['Test by Admin'],
        'Currency': ['INR']
    }
    test_df = pd.DataFrame(test_data)
    try:
        detailed_df, summary_issues, problematic_df = validate_invoices_with_details(test_df)
        if len(detailed_df) > 0:
            print("✅ Test PASSED: Validation function works!")
            print(f"   📊 Processed: {len(detailed_df)} records")
            print(f"   📋 Sample result: {detailed_df.iloc[0]['Invoice_Number']}")
            return True
        print("❌ Test FAILED: No records processed")
        return False
    except Exception as e:
        print(f"❌ Test FAILED: {e}")
        return False

# ---------- Entry point ----------
if __name__ == "__main__":
    # Uncomment to run quick test before full workflow
    # test_validation_functions()
    success = run_invoice_validation()
    print(f"🏁 Invoice validation completed with status: {'SUCCESS' if success else 'FAILURE'}")
