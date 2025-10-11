import os
import pandas as pd
from datetime import datetime, timedelta
import glob
import zipfile

COLUMN_MAP_PRIORITIES = {
    "invoice_id":     ["Invoice Id","InvID","InvoiceID","ID"],
    "invoice_number": ["Invoice#","PurchaseInvNo","InvoiceNumber","InvNo","BillNo"],
    "invoice_date":   ["Inv Date","PurchaseInvDate","InvoiceDate","InvDate","BillDate"],
    "vendor_name":    ["Vendor","PartyName","VendorName","SupplierName"],
    "gstin":          ["GSTNo","GSTNO","GSTIN","GST No","GST_No"],
    "amount":         ["Invoice Amount","Total","Amount","GrandTotal","NetAmount"],
    "creator":        ["Inv Created By","Inv Created by","InvCreatedBy","CreatedBy","UserName","EntryBy","PreparedBy"],
    "mop":            ["MOP","Method of Payment","PaymentMethod","PayMode","Payment_Mode","PaymentType"],
    "due_date":       ["DueDate","Due Date","PaymentDue","MaturityDate"],
    "remarks":        ["Remarks","Remark","Notes","Comments","Narration"],
    "state":          ["Location","State","PartyState","Branch"],
    "currency":       ["Inv Currency","InvoiceCurrency","Currency","InvCurrency"],
    "scid":           ["SCID#","SCID","SC Id","SalesContractId","SaleContractID"],
}

def _choose_first(df, names):
    for n in names:
        if n in df.columns: return n
    low = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in low: return low[n.lower()]
    return None

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[:, ~df.columns.duplicated()].copy()
    chosen = {k: _choose_first(df, v) for k, v in COLUMN_MAP_PRIORITIES.items()}
    out = pd.DataFrame(index=df.index)
    out["Invoice_ID"]           = df.get(chosen["invoice_id"], df.index.astype(str))
    out["Invoice_Number"]       = df.get(chosen["invoice_number"], "")
    out["Invoice_Date"]         = pd.to_datetime(df.get(chosen["invoice_date"]), errors="coerce")
    out["Vendor_Name"]          = df.get(chosen["vendor_name"], "")
    out["GST_Number"]           = df.get(chosen["gstin"], "")
    out["Amount"]               = pd.to_numeric(df.get(chosen["amount"]), errors="coerce")
    out["Invoice_Creator_Name"] = df.get(chosen["creator"], "")
    out["Method_of_Payment"]    = df.get(chosen["mop"], "")
    out["Due_Date"]             = pd.to_datetime(df.get(chosen["due_date"]), errors="coerce")
    out["Remarks"]              = df.get(chosen["remarks"], "")
    out["Raw_State"]            = df.get(chosen["state"], "")
    out["Invoice_Currency"]     = df.get(chosen["currency"], "INR")
    out["SCID"]                 = df.get(chosen["scid"], "")

    # Location
    def _state_from_gstin(g):
        s = str(g).strip()
        return s[:2] if len(s) >= 2 else ""
    def _compute_location(row):
        st = str(row["Raw_State"]).strip()
        if st and st.lower() not in ("nan","none","null",""): return st
        sc = _state_from_gstin(row["GST_Number"])
        return sc or ""
    out["Location"] = out.apply(_compute_location, axis=1)

    # Due-date alert
    today = pd.Timestamp("today").normalize()
    dd = out["Due_Date"]
    out["Due_Date_Notification"] = ""
    m = dd.notna()
    out.loc[m, "Due_Date_Notification"] = dd[m].apply(
        lambda d: "OVERDUE" if d.date() < today.date()
        else ("YES" if (d.date() - today.date()).days <= 2 else "NO")
    )

    # Defaults
    out["TDS_Status"] = "Coming Soon"

    # Clean creator
    def _clean(s):
        s = str(s).strip()
        return "" if s.lower() in ("nan","none","null") else s
    out["Invoice_Creator_Name"] = out["Invoice_Creator_Name"].apply(_clean)

    out.replace({pd.NaT: ""}, inplace=True)
    out.fillna("", inplace=True)
    return out

def merge_from_soa(main_df: pd.DataFrame, soa_df: pd.DataFrame) -> pd.DataFrame:
    m = main_df.copy()
    s = normalize_columns(soa_df)
    key = "Invoice_Number"
    if key not in m.columns or key not in s.columns or (m[key] == "").all():
        key = "Invoice_ID"
    fill_cols = ["Invoice_Creator_Name","Method_of_Payment","Due_Date",
                 "Location","Invoice_Currency","SCID","Remarks"]
    avail = [c for c in fill_cols if c in s.columns]
    s_red = s[[key] + avail].drop_duplicates(subset=[key])
    m = m.merge(s_red, on=key, how="left", suffixes=("", "_SOA"))
    for c in avail:
        sc = f"{c}_SOA"
        if sc in m.columns:
            if c == "Due_Date":
                m[c] = pd.to_datetime(m[c], errors="coerce")
                m[sc] = pd.to_datetime(m[sc], errors="coerce")
                m[c] = m[c].where(m[c].notna(), m[sc])
            else:
                m[c] = m[c].where(m[c].astype(str).str.strip() != "", m[sc])
            m.drop(columns=[sc], inplace=True, errors="ignore")
    # recompute alerts
    if "Due_Date" in m.columns:
        today = pd.Timestamp("today").normalize()
        dd = pd.to_datetime(m["Due_Date"], errors="coerce")
        m["Due_Date_Notification"] = ""
        mask = dd.notna()
        m.loc[mask, "Due_Date_Notification"] = dd[mask].apply(
            lambda d: "OVERDUE" if d.date() < today.date()
            else ("YES" if (d.date() - today.date()).days <= 2 else "NO")
        )
    m = m.loc[:, ~m.columns.duplicated()].copy()
    return m

def try_read_file(file_path):
    """Enhanced file reading with better error handling"""
    try:
        # Try as proper Excel
        if file_path.endswith('.xlsx'):
            return pd.read_excel(file_path, engine='openpyxl')

        elif file_path.endswith('.xls'):
            try:
                return pd.read_excel(file_path, engine='xlrd')
            except Exception:
                print(f"⚠️ Not a real Excel: {file_path}, trying as text fallback")

                # Try decoding content
                with open(file_path, 'rb') as f:
                    content = f.read(2048)
                    try:
                        sample = content.decode('utf-8')
                    except UnicodeDecodeError:
                        sample = content.decode('latin1')

                if '\t' in sample:
                    print("🔄 Detected TSV format")
                    return pd.read_csv(file_path, sep='\t', encoding='utf-8', engine='python')
                else:
                    print("🔄 Detected CSV format")
                    return pd.read_csv(file_path, encoding='utf-8', engine='python')

        elif file_path.endswith('.csv'):
            return pd.read_csv(file_path, encoding='utf-8', engine='python')

        elif file_path.endswith('.tsv'):
            return pd.read_csv(file_path, sep='\t', encoding='utf-8', engine='python')

        else:
            raise ValueError("Unsupported file format")

    except Exception as e:
        raise ValueError(f"❌ Failed to read {file_path}: {str(e)}")

def scan_invoice_files(base_folder='data', date_range_days=3):
    """
    Scan invoice files with configurable date range
    """
    print(f"\n🔍 Scanning invoice files in '{base_folder}/' and subfolders...\n")

    today = datetime.today()
    start_date = today - timedelta(days=date_range_days)

    found_files = glob.glob(os.path.join(base_folder, '**/*.*'), recursive=True)
    valid_dataframes = []

    for file in found_files:
        # Skip temporary/incomplete downloads
        if file.endswith(".crdownload"):
            print(f"⏳ Skipping temp download file: {file}")
            continue

        # Rename if it's a likely invoice XLS file but misnamed
        if file.endswith(".xls") and "invoice" in os.path.basename(file).lower() and "download" not in os.path.basename(file).lower():
            target = os.path.join(os.path.dirname(file), "invoice_download.xls")
            try:
                os.rename(file, target)
                print(f"📂 Renamed file to: {target}")
                file = target  # update reference
            except Exception as e:
                print(f"⚠️ Could not rename file {file}: {e}")
                continue

        # Skip non-XLS/XLSX/CSV files
        if not any(file.lower().endswith(ext) for ext in [".xls", ".xlsx", ".csv"]):
            continue

        try:
            df = try_read_file(file)
            print(f"✅ Loaded file: {file} — Rows: {df.shape[0]}, Columns: {df.shape[1]}\n")

            # Check if PurchaseInvDate column exists
            if 'PurchaseInvDate' not in df.columns:
                print(f"⚠️ Skipping {file}: 'PurchaseInvDate' column missing.\n")
                continue

            # Parse dates and filter
            df["ParsedInvoiceDate"] = pd.to_datetime(df["PurchaseInvDate"], errors='coerce')
            filtered_df = df[
                (df["ParsedInvoiceDate"] >= start_date) &
                (df["ParsedInvoiceDate"] <= today)
            ]

            if not filtered_df.empty:
                print(f"✅ Invoices in range {start_date.date()} to {today.date()}: {len(filtered_df)}\n")
                valid_dataframes.append(filtered_df)
            else:
                print(f"⚠️ No invoices found in date range for {file}\n")

        except Exception as e:
            print(f"❌ Error reading {file}: {str(e)}\n")

    if not valid_dataframes:
        print("⚠️ No valid invoice files found with data in the specified date range.")
        return pd.DataFrame()

    combined_df = pd.concat(valid_dataframes, ignore_index=True)
    print(f"📊 Total combined invoices: {len(combined_df)}")
    return combined_df

def validate_invoices(df):
    """
    Enhanced validation function with better error handling and reporting
    """
    if df is None or df.empty:
        print("⚠️ No data provided for validation")
        return ["No data provided"], pd.DataFrame()
    
    print(f"\n✅ Total invoices to validate: {len(df)}")
    issues = []
    rows_with_issues = pd.DataFrame()

    # Define required fields with more comprehensive checks
    required_fields = ['PurchaseInvNo', 'PurchaseInvDate', 'PartyName', 'GSTNO', 'Total']
    optional_but_important = ['VoucherNo', 'OrderNo', 'TaxableValue']

    print("\n🔍 Checking for missing columns and values...")
    
    # Check for required columns
    missing_columns = [field for field in required_fields if field not in df.columns]
    if missing_columns:
        for field in missing_columns:
            issues.append(f"❌ Missing required column: '{field}'")
        print(f"❌ Missing required columns: {missing_columns}")
    else:
        print("✅ All required columns present")

    # Check for missing values in existing required columns
    existing_required_fields = [field for field in required_fields if field in df.columns]
    
    for field in existing_required_fields:
        # Check for null/empty values
        null_mask = df[field].isna()
        empty_mask = df[field].astype(str).str.strip() == ''
        missing_mask = null_mask | empty_mask
        
        missing = df[missing_mask]
        if not missing.empty:
            issues.append(f"❌ {len(missing)} rows missing values in '{field}'")
            rows_with_issues = pd.concat([rows_with_issues, missing])
            print(f"⚠️ Found {len(missing)} rows with missing {field}")

    # Check for duplicate invoice numbers
    if 'PurchaseInvNo' in df.columns:
        # Remove null values before checking duplicates
        non_null_invoices = df[df['PurchaseInvNo'].notna() & (df['PurchaseInvNo'].astype(str).str.strip() != '')]
        
        if not non_null_invoices.empty:
            duplicates = non_null_invoices[non_null_invoices.duplicated('PurchaseInvNo', keep=False)]
            if not duplicates.empty:
                dup_list = duplicates['PurchaseInvNo'].unique().tolist()[:10]  # Show only first 10
                issues.append(f"⚠️ Duplicate invoice numbers found: {len(duplicates)} rows → {dup_list}{'...' if len(duplicates) > 10 else ''}")
                rows_with_issues = pd.concat([rows_with_issues, duplicates])
                print(f"⚠️ Found {len(duplicates)} duplicate invoice numbers")

    # Additional validation checks
    if 'Total' in df.columns:
        # Check for invalid amounts
        try:
            df['Total_numeric'] = pd.to_numeric(df['Total'], errors='coerce')
            invalid_amounts = df[df['Total_numeric'].isna() & df['Total'].notna()]
            if not invalid_amounts.empty:
                issues.append(f"⚠️ {len(invalid_amounts)} rows have invalid amount values")
                rows_with_issues = pd.concat([rows_with_issues, invalid_amounts])
            
            # Check for negative amounts
            negative_amounts = df[df['Total_numeric'] < 0]
            if not negative_amounts.empty:
                issues.append(f"⚠️ {len(negative_amounts)} rows have negative amounts")
                rows_with_issues = pd.concat([rows_with_issues, negative_amounts])
                
        except Exception as e:
            print(f"⚠️ Could not validate amounts: {str(e)}")

    # Check for invalid dates
    if 'PurchaseInvDate' in df.columns:
        try:
            df['ParsedInvoiceDate'] = pd.to_datetime(df['PurchaseInvDate'], errors='coerce')
            invalid_dates = df[df['ParsedInvoiceDate'].isna() & df['PurchaseInvDate'].notna()]
            if not invalid_dates.empty:
                issues.append(f"⚠️ {len(invalid_dates)} rows have invalid dates")
                rows_with_issues = pd.concat([rows_with_issues, invalid_dates])
        except Exception as e:
            print(f"⚠️ Could not validate dates: {str(e)}")

    # Drop exact duplicate rows from issues dataframe
    if not rows_with_issues.empty:
        rows_with_issues = rows_with_issues.drop_duplicates()

    # Print validation summary
    if issues:
        print("\n🚨 Validation Issues Found:")
        for i, issue in enumerate(issues, 1):
            print(f" {i}. {issue}")
    else:
        print("\n✅ All invoices passed validation checks.")

    print(f"\n🧾 Validation Summary:")
    print(f"  - Total invoices: {len(df)}")
    print(f"  - Issues found: {len(issues)}")
    print(f"  - Rows with issues: {len(rows_with_issues)}")
    
    return issues, rows_with_issues

def unzip_files(zip_path, extract_to='data'):
    """Unzip .zip files to the data directory with better error handling"""
    try:
        if not os.path.exists(zip_path):
            raise FileNotFoundError(f"ZIP file not found: {zip_path}")
            
        os.makedirs(extract_to, exist_ok=True)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"✅ Unzipped: {zip_path} to {extract_to}")
        
        # List extracted files
        extracted_files = []
        for root, dirs, files in os.walk(extract_to):
            for file in files:
                extracted_files.append(os.path.join(root, file))
        
        print(f"📁 Extracted {len(extracted_files)} files")
        return extracted_files
        
    except Exception as e:
        print(f"❌ Failed to unzip {zip_path}: {str(e)}")
        return []

def extract_text_from_file(file_path):
    """Enhanced stub for future OCR or text extraction logic"""
    if not os.path.exists(file_path):
        return f"File not found: {file_path}"
    
    file_size = os.path.getsize(file_path)
    return f"Text content of {file_path} (size: {file_size} bytes)"

def match_fields(extracted_text, reference_df, return_row=False):
    """Enhanced stub to simulate field matching from extracted text"""
    if reference_df.empty:
        return ("❌ No reference data", None) if return_row else "❌ No reference data"
    
    dummy_row = reference_df.iloc[0] if not reference_df.empty else None
    confidence_score = 0.85  # Simulated confidence
    
    result = f"✅ Valid (confidence: {confidence_score:.2f})"
    return (result, dummy_row) if return_row else result

def copy_validation_result_for_dashboard():
    """
    Copy validation_result.xlsx of today to delta_report_YYYY-MM-DD.xlsx for dashboard.
    Enhanced with better error handling and file checking.
    """
    from datetime import datetime
    import shutil
    import os

    today_str = datetime.today().strftime("%Y-%m-%d")
    source_file = f"data/{today_str}/validation_result.xlsx"
    target_file = f"data/delta_report_{today_str}.xlsx"

    try:
        if os.path.exists(source_file):
            # Check source file size
            source_size = os.path.getsize(source_file)
            if source_size < 100:
                print(f"⚠️ Warning: {source_file} is very small ({source_size} bytes)")
            
            # Create target directory if needed
            os.makedirs(os.path.dirname(target_file), exist_ok=True)
            
            # Copy file
            shutil.copy2(source_file, target_file)  # copy2 preserves metadata
            
            # Verify copy
            if os.path.exists(target_file):
                target_size = os.path.getsize(target_file)
                print(f"📋 Copied validation_result.xlsx to {target_file} ({target_size} bytes)")
                
                if source_size != target_size:
                    print(f"⚠️ Warning: File sizes don't match (source: {source_size}, target: {target_size})")
            else:
                print(f"❌ Copy failed - target file not found: {target_file}")
                
        else:
            print(f"⚠️ Source file not found: {source_file}. Creating placeholder report...")
            
            # Create a placeholder report
            try:
                import pandas as pd
                placeholder_data = {
                    'Issue_ID': ['No data'],
                    'Issue_Description': ['No validation results available'],
                    'Date_Found': [today_str],
                    'Status': ['Info']
                }
                placeholder_df = pd.DataFrame(placeholder_data)
                placeholder_df.to_excel(target_file, index=False, engine='openpyxl')
                print(f"📋 Created placeholder report: {target_file}")
            except Exception as e:
                print(f"❌ Failed to create placeholder report: {str(e)}")
                
    except Exception as e:
        print(f"❌ Failed to copy dashboard file: {str(e)}")

def get_invoice_summary(df):
    """
    Get summary statistics of invoice data
    """
    if df is None or df.empty:
        return {"error": "No data provided"}
    
    summary = {
        "total_invoices": len(df),
        "columns_count": len(df.columns),
        "columns": list(df.columns)
    }
    
    # Date range if available
    if 'PurchaseInvDate' in df.columns:
        try:
            df['ParsedInvoiceDate'] = pd.to_datetime(df['PurchaseInvDate'], errors='coerce')
            valid_dates = df['ParsedInvoiceDate'].dropna()
            if not valid_dates.empty:
                summary["date_range"] = {
                    "earliest": valid_dates.min().strftime("%Y-%m-%d"),
                    "latest": valid_dates.max().strftime("%Y-%m-%d")
                }
        except:
            pass
    
    # Amount summary if available
    if 'Total' in df.columns:
        try:
            df['Total_numeric'] = pd.to_numeric(df['Total'], errors='coerce')
            valid_amounts = df['Total_numeric'].dropna()
            if not valid_amounts.empty:
                summary["amount_summary"] = {
                    "total_amount": float(valid_amounts.sum()),
                    "average_amount": float(valid_amounts.mean()),
                    "min_amount": float(valid_amounts.min()),
                    "max_amount": float(valid_amounts.max())
                }
        except:
            pass
    
    return summary