import os
import pandas as pd
from datetime import datetime, timedelta
import glob
import zipfile

def try_read_file(file_path):
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
                    return pd.read_csv(file_path, sep='\t', encoding='latin1', engine='python')
                else:
                    print("🔄 Detected CSV format")
                    return pd.read_csv(file_path, encoding='latin1', engine='python')

        elif file_path.endswith('.csv'):
            return pd.read_csv(file_path, encoding='utf-8', engine='python')

        elif file_path.endswith('.tsv'):
            return pd.read_csv(file_path, sep='\t', encoding='utf-8', engine='python')

        else:
            raise ValueError("Unsupported file format")

    except Exception as e:
        raise ValueError(f"❌ Failed to read {file_path}: {str(e)}")

def scan_invoice_files(base_folder='data'):
    print("\n🔍 Scanning invoice files in 'data/' and subfolders...\n")
    
    today = datetime.today()
    start_date = today - timedelta(days=3)

    found_files = glob.glob(os.path.join(base_folder, '**/*.*'), recursive=True)
    valid_dataframes = []

    # Rename unexpected RMS download to invoice_download.xls
    for file in found_files:
        if file.endswith(".xls") and "invoice" in os.path.basename(file).lower() and "download" not in os.path.basename(file).lower():
            target = os.path.join(os.path.dirname(file), "invoice_download.xls")
            os.rename(file, target)
            print(f"📂 Renamed file to: {target}")

        try:
            df = try_read_file(file)
            print(f"✅ Loaded file: {file} — Rows: {df.shape[0]}, Columns: {list(df.columns)}\n")
            
            if 'PurchaseInvDate' not in df.columns:
                print(f"❌ Skipping {file}: 'PurchaseInvDate' column missing.\n")
                continue

            df["ParsedInvoiceDate"] = pd.to_datetime(df["PurchaseInvDate"], errors='coerce')
            filtered_df = df[
                (df["ParsedInvoiceDate"] >= start_date) &
                (df["ParsedInvoiceDate"] <= today)
            ]

            if not filtered_df.empty:
                print(f"✅ Invoices in range {start_date.date()} to {today.date()}: {len(filtered_df)}\n")
                valid_dataframes.append(filtered_df)

        except Exception as e:
            print(f"❌ Error reading {file}: {str(e)}\n")

    if not valid_dataframes:
        print("⚠️ No valid invoice files found.")
        return pd.DataFrame()

    return pd.concat(valid_dataframes, ignore_index=True)

def validate_invoices(df):
    print(f"\n✅ Total invoices to validate: {len(df)}")
    issues = []
    rows_with_issues = pd.DataFrame()

    required_fields = ['PurchaseInvNo', 'PurchaseInvDate', 'PartyName', 'GSTNO', 'Total']

    print("\n🔍 Checking for missing columns and values...")
    for field in required_fields:
        if field not in df.columns:
            issues.append(f"❌ Missing column: '{field}'")
            continue

        missing = df[df[field].isna()]
        if not missing.empty:
            issues.append(f"❌ {len(missing)} rows missing values in '{field}'")
            rows_with_issues = pd.concat([rows_with_issues, missing])

    if 'PurchaseInvNo' in df.columns:
        duplicates = df[df.duplicated('PurchaseInvNo', keep=False)]
        if not duplicates.empty:
            dup_list = duplicates['PurchaseInvNo'].unique().tolist()
            issues.append(f"⚠️ Duplicate invoice numbers found: {len(duplicates)} rows → {dup_list}")
            rows_with_issues = pd.concat([rows_with_issues, duplicates])

    # Drop exact duplicate rows
    rows_with_issues = rows_with_issues.drop_duplicates()

    if issues:
        print("\n🚨 Validation Issues Found:")
        for issue in issues:
            print(" -", issue)
    else:
        print("\n✅ All invoices passed basic validation checks.")

    print(f"\n🧾 Validation Summary: {len(issues)} issue(s) found.")
    return issues, rows_with_issues

def unzip_files(zip_path, extract_to='data'):
    """Unzip .zip files to the data directory"""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    print(f"✅ Unzipped: {zip_path} to {extract_to}")

def extract_text_from_file(file_path):
    """Stub for future OCR or text extraction logic"""
    return f"Text content of {file_path}"

def match_fields(extracted_text, reference_df, return_row=False):
    """Stub to simulate field matching from extracted text"""
    dummy_row = reference_df.iloc[0] if not reference_df.empty else None
    return ("✅ Valid", dummy_row) if return_row else "✅ Valid"

def copy_validation_result_for_dashboard():
    """Copy validation_result.xlsx of today to delta_report_YYYY-MM-DD.xlsx for dashboard."""
    from datetime import datetime
    import shutil
    import os

    today_str = datetime.today().strftime("%Y-%m-%d")
    source_file = f"data/{today_str}/validation_result.xlsx"
    target_file = f"data/delta_report_{today_str}.xlsx"

    if os.path.exists(source_file):
        try:
            shutil.copy(source_file, target_file)
            print(f"📋 Copied validation_result.xlsx to {target_file} for dashboard.")
        except Exception as e:
            print(f"❌ Failed to copy dashboard file: {str(e)}")
    else:
        print("⚠️ validation_result.xlsx not found. Skipping dashboard update.")

