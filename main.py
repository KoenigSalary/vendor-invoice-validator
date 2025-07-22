# main.py

from rms_scraper import rms_download
from validator_utils import validate_invoices
from updater import update_invoice_status
from reporter import save_snapshot_report
from dotenv import load_dotenv
from datetime import datetime, timedelta
from invoice_tracker import (
    create_tables,
    save_invoice_snapshot,
    record_run_window,
    get_all_run_windows
)
import pandas as pd
import os
import shutil
from pathlib import Path

# Load environment variables
load_dotenv()

# === Initialize DB tables if not exists ===
create_tables()

# === Archive old delta reports (> 3 months) ===
def archive_old_reports():
    """Archive delta reports older than 3 months"""
    data_dir = "data"
    archive_dir = os.path.join(data_dir, "archive")
    if not os.path.exists(archive_dir):
        os.makedirs(archive_dir)

    cutoff_date = datetime.today() - timedelta(days=90)
    
    if not os.path.exists(data_dir):
        return
        
    for filename in os.listdir(data_dir):
        if filename.startswith("delta_report_") and filename.endswith(".xls"):
            date_str = filename.replace("delta_report_", "").replace(".xls", "")
            try:
                file_date = datetime.strptime(date_str, "%Y-%m-%d")
                if file_date < cutoff_date:
                    src = os.path.join(data_dir, filename)
                    dst = os.path.join(archive_dir, filename)
                    shutil.move(src, dst)
                    print(f"📦 Archived old report: {filename}")
            except Exception as e:
                print(f"⚠️ Skipping file {filename}: {e}")

def read_invoice_file(invoice_file):
    """
    Robust file reading with multiple format support and proper error handling
    """
    print(f"🔍 Attempting to read file: {invoice_file}")
    
    # Check if file exists
    if not os.path.exists(invoice_file):
        raise FileNotFoundError(f"Invoice file not found: {invoice_file}")
    
    # Get file info
    file_path = Path(invoice_file)
    file_ext = file_path.suffix.lower()
    file_size = os.path.getsize(invoice_file)
    print(f"📄 File: {file_path.name}, Extension: {file_ext}, Size: {file_size} bytes")
    
    # Check if file is too small (likely corrupted or empty)
    if file_size < 50:
        raise ValueError(f"File appears to be too small ({file_size} bytes) - likely corrupted or empty")
    
    # Read file header to detect actual format
    try:
        with open(invoice_file, 'rb') as f:
            header = f.read(50)
        print(f"🔍 File header (first 20 bytes): {header[:20]}")
    except Exception as e:
        print(f"⚠️ Could not read file header: {e}")
        header = b''
    
    df = None
    last_error = None
    
    # Method 1: Try Excel with openpyxl engine (most reliable for .xlsx)
    try:
        print("📊 Attempting to read as Excel with openpyxl engine...")
        df = pd.read_excel(invoice_file, engine='openpyxl')
        print(f"✅ Successfully read Excel file with openpyxl. Shape: {df.shape}")
        print(f"📋 Columns: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"⚠️ openpyxl engine failed: {str(e)}")
        last_error = e
    
    # Method 2: Try Excel with xlrd engine (for older .xls files)
    if file_ext == '.xls':
        try:
            print("📊 Attempting to read as Excel with xlrd engine...")
            df = pd.read_excel(invoice_file, engine='xlrd')
            print(f"✅ Successfully read Excel file with xlrd. Shape: {df.shape}")
            print(f"📋 Columns: {list(df.columns)}")
            return df
        except Exception as e:
            print(f"⚠️ xlrd engine failed: {str(e)}")
            last_error = e
    
    # Method 3: Try reading as CSV with different separators
    try:
        print("📄 Attempting to read as CSV...")
        # Try common separators
        separators = [',', ';', '\t', '|']
        for sep in separators:
            try:
                df_test = pd.read_csv(invoice_file, sep=sep, nrows=5)
                if df_test.shape[1] > 1:  # Multiple columns detected
                    df = pd.read_csv(invoice_file, sep=sep)
                    print(f"✅ Successfully read as CSV with separator '{sep}'. Shape: {df.shape}")
                    print(f"📋 Columns: {list(df.columns)}")
                    return df
            except:
                continue
        print("⚠️ CSV reading failed with all separators")
    except Exception as e:
        print(f"⚠️ CSV reading failed: {str(e)}")
        last_error = e
    
    # Method 4: Try HTML parsing
    try:
        print("🌐 Attempting to read as HTML...")
        tables = pd.read_html(invoice_file, flavor='lxml')
        if tables and len(tables) > 0:
            df = tables[0]  # Get first table
            print(f"✅ Successfully read HTML file. Shape: {df.shape}")
            print(f"📋 Columns: {list(df.columns)}")
            return df
        else:
            print("⚠️ No tables found in HTML")
    except Exception as e:
        print(f"⚠️ HTML parsing failed: {str(e)}")
        last_error = e
    
    # Method 5: Try reading as plain text and show sample
    try:
        print("📝 Attempting to read file content for debugging...")
        with open(invoice_file, 'r', encoding='utf-8', errors='ignore') as f:
            content_sample = f.read(500)  # Read first 500 characters
        print(f"📄 File content sample:\n{repr(content_sample)}")
        
        # Try to detect if it's actually a different format
        if content_sample.strip().startswith('<!DOCTYPE') or content_sample.strip().startswith('<html'):
            print("🔍 File appears to be HTML format")
        elif content_sample.strip().startswith('{') or content_sample.strip().startswith('['):
            print("🔍 File appears to be JSON format")
        elif ',' in content_sample and '\n' in content_sample:
            print("🔍 File appears to be CSV-like format")
            
    except Exception as e:
        print(f"⚠️ Could not read file content: {e}")
    
    # If all methods failed, raise the most relevant error
    if last_error:
        raise Exception(f"Could not read invoice file in any supported format. Last error: {str(last_error)}")
    else:
        raise Exception("Could not read invoice file - unknown format or corrupted file")

def validate_downloaded_files(download_dir):
    """Validate that downloaded files exist and are not corrupted"""
    required_files = ["invoice_download.xls", "invoices.zip"]
    validation_results = {}
    
    for fname in required_files:
        file_path = os.path.join(download_dir, fname)
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            print(f"✅ Found {fname}: {file_size} bytes")
            
            # Basic validation
            if file_size < 50:
                print(f"⚠️ Warning: {fname} seems too small ({file_size} bytes)")
                validation_results[fname] = "small"
            else:
                validation_results[fname] = "ok"
                
            # Check file header for format detection
            try:
                with open(file_path, 'rb') as f:
                    header = f.read(20)
                print(f"🔍 {fname} header: {header}")
            except Exception as e:
                print(f"⚠️ Could not read {fname} header: {e}")
        else:
            print(f"❌ Missing file: {fname}")
            validation_results[fname] = "missing"
    
    return validation_results

def filter_invoices_by_date(df, start_str, end_str):
    """Filter dataframe by date range"""
    try:
        if 'PurchaseInvDate' not in df.columns:
            print("⚠️ PurchaseInvDate column not found, returning all data")
            return df
            
        # Convert dates
        start_date = datetime.strptime(start_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_str, "%Y-%m-%d")
        
        # Parse invoice dates
        df["ParsedInvoiceDate"] = pd.to_datetime(df["PurchaseInvDate"], errors='coerce')
        
        # Filter by date range
        filtered_df = df[
            (df["ParsedInvoiceDate"] >= start_date) &
            (df["ParsedInvoiceDate"] <= end_date)
        ]
        
        print(f"📅 Filtered invoices from {start_str} to {end_str}: {len(filtered_df)} out of {len(df)}")
        return filtered_df
        
    except Exception as e:
        print(f"⚠️ Date filtering failed: {str(e)}, returning all data")
        return df

def run_invoice_validation():
    """Main function to run invoice validation workflow"""
    try:
        today = datetime.today()
        end_date = today - timedelta(days=1)
        start_date = end_date - timedelta(days=3)

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        today_str = today.strftime("%Y-%m-%d")

        print(f"🔍 Validating invoices from {start_str} to {end_str}...")

        # Step 1: Download invoice data from RMS
        print("📥 Starting RMS download...")
        try:
            invoice_path = rms_download(start_date, end_date)
            print(f"✅ RMS download completed. Path returned: {invoice_path}")
        except Exception as e:
            print(f"❌ RMS download failed: {str(e)}")
            return False

        # Step 2: Verify downloaded files in today's folder
        download_dir = os.path.join("data", today_str)
        print(f"🔍 Checking files in directory: {download_dir}")
        
        validation_results = validate_downloaded_files(download_dir)
        
        # Step 3: Determine which file to use for processing
        invoice_file = os.path.join(download_dir, "invoice_download.xls")
        
        # Exit if critical files are missing
        if validation_results.get("invoice_download.xls") == "missing":
            print("❌ No invoice file downloaded. Aborting.")
            return False

        # Step 4: Read and parse invoice file with robust error handling
        try:
            print("📊 Starting invoice file processing...")
            df = read_invoice_file(invoice_file)
            
            if df is None or df.empty:
                print("❌ DataFrame is empty after reading file")
                return False
                
            print(f"✅ Successfully loaded invoice data. Shape: {df.shape}")
            print(f"📋 Columns: {list(df.columns)}")
            
        except Exception as e:
            print(f"❌ Failed to read invoice file: {str(e)}")
            return False

        # Step 5: Filter invoices by date range
        try:
            filtered_df = filter_invoices_by_date(df, start_str, end_str)
            print(f"📅 Working with {len(filtered_df)} invoices in date range")
        except Exception as e:
            print(f"⚠️ Date filtering failed: {str(e)}, using all data")
            filtered_df = df

        # Step 6: Continue with validation workflow
        print("🔄 Starting invoice validation...")
        try:
            # FIXED: Call validate_invoices with only the DataFrame (as per validator_utils.py)
            current_result, current_invoices = validate_invoices(filtered_df)
            
            print(f"✅ Validation completed. Found {len(current_result)} issues and {len(current_invoices)} problematic invoices")
            
        except Exception as e:
            print(f"❌ Validation failed: {str(e)}")
            return False

        # Step 7: Prepare invoice list for saving
        try:
            # Convert the problematic invoices DataFrame to list of dictionaries
            if not current_invoices.empty:
                current_invoices_list = current_invoices.to_dict('records')
            else:
                current_invoices_list = []
                
            print(f"📋 Prepared {len(current_invoices_list)} invoice records for saving")
            
        except Exception as e:
            print(f"⚠️ Failed to prepare invoice list: {str(e)}")
            current_invoices_list = []

        # Step 8: Save validated snapshot
        try:
            save_invoice_snapshot(current_invoices_list, run_date=end_str)
            print("✅ Invoice snapshot saved")
        except Exception as e:
            print(f"⚠️ Failed to save snapshot: {str(e)}")

        # Step 9: Record this run window
        try:
            record_run_window(start_str, end_str)
            print("✅ Run window recorded")
        except Exception as e:
            print(f"⚠️ Failed to record run window: {str(e)}")

        # Step 10: Revalidate all previous windows (simplified approach)
        print("🔁 Preparing cumulative report...")
        try:
            all_windows = get_all_run_windows()
            cumulative_report = []

            # For now, just use current results since revalidation needs refactoring
            print(f"📊 Using current validation results for report")
            
            # Convert current_result (list of issue messages) to report format
            report_entries = []
            for i, issue in enumerate(current_result):
                report_entries.append({
                    'Issue_ID': i + 1,
                    'Issue_Description': issue,
                    'Date_Found': end_str,
                    'Status': 'New'
                })
            
            full_report = report_entries
            print(f"📊 Final report has {len(full_report)} entries")

        except Exception as e:
            print(f"⚠️ Report generation failed: {str(e)}")
            full_report = []

        # Step 11: Save reports
        try:
            os.makedirs("data", exist_ok=True)
            report_path = f"data/delta_report_{today_str}.xlsx"
            
            if full_report:
                pd.DataFrame(full_report).to_excel(report_path, index=False, engine='openpyxl')
                print(f"✅ Delta report generated: {report_path}")

                # Also copy to dashboard path
                os.makedirs(f"data/{today_str}", exist_ok=True)
                dashboard_path = f"data/{today_str}/validation_result.xlsx"
                shutil.copy(report_path, dashboard_path)
                print(f"📋 Copied report for dashboard: {dashboard_path}")
            else:
                print("⚠️ No issues found - creating empty report")
                # Create empty report
                empty_report = pd.DataFrame({
                    'Issue_ID': [],
                    'Issue_Description': [], 
                    'Date_Found': [],
                    'Status': []
                })
                empty_report.to_excel(report_path, index=False, engine='openpyxl')
                print(f"✅ Empty delta report created: {report_path}")

        except Exception as e:
            print(f"❌ Failed to save reports: {str(e)}")
            return False

        # Step 12: Archive old reports
        try:
            archive_old_reports()
            print("🗂️ Old reports archived")
        except Exception as e:
            print(f"⚠️ Failed to archive old reports: {str(e)}")

        # Step 13: Copy validation result for dashboard
        try:
            from validator_utils import copy_validation_result_for_dashboard
            copy_validation_result_for_dashboard()
        except Exception as e:
            print(f"⚠️ Failed to copy dashboard file: {str(e)}")

        # Step 14: Send email notification
        try:
            from email_notifier import EmailNotifier
            team_recipients = os.getenv('TEAM_EMAIL_LIST', '').split(',')
            if team_recipients and team_recipients[0].strip():
                notifier = EmailNotifier()
                issues_count = len(full_report) if 'full_report' in locals() else 0
                notifier.send_validation_report(today_str, team_recipients, issues_count)
                print("📧 Email notification sent successfully!")
            else:
                print("⚠️ No email recipients configured in TEAM_EMAIL_LIST")
        except Exception as e:
            print(f"⚠️ Email sending failed: {str(e)}")

        print("✅ Invoice validation workflow completed successfully!")
        return True

    except Exception as e:
        print(f"❌ Unexpected error in main workflow: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_invoice_validation()
    if not success:
        exit(1)  # Exit with error code for GitHub Actions
    else:
        print("🎉 All done!")
        exit(0)  # Exit successfully

