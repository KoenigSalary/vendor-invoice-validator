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

# === Initialize DB tables if not exists ===
create_tables()

# === Archive old delta reports (> 3 months) ===
def archive_old_reports():
    data_dir = "data"
    archive_dir = os.path.join(data_dir, "archive")
    if not os.path.exists(archive_dir):
        os.makedirs(archive_dir)

    cutoff_date = datetime.today() - timedelta(days=90)
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

# === Main run logic ===
def run_invoice_validation():
    today = datetime.today()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=3)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")

    print(f"🔍 Validating invoices from {start_str} to {end_str}...")

    # ✅ Step 1: Download invoice data from RMS
    invoice_path = rms_download(start_date, end_date)

    # ✅ Step 2: Verify downloaded files in today's folder
    download_dir = os.path.join("data", today_str)
    for fname in ["invoice_download.xls", "invoices.zip"]:
        file_path = os.path.join(download_dir, fname)
        if os.path.exists(file_path):
            print(f"✅ Found {fname} in {download_dir}")
        else:
            print(f"⚠️ {fname} not found in {download_dir}")

    # 🛑 Step 3: Exit if download failed
    invoice_file = os.path.join("data", today_str, "invoice_download.xls")
    if not os.path.exists(invoice_file):
        print("❌ No invoice file downloaded. Aborting.")
        return

    df = pd.read_html(invoice_file)[0]

    # ✅ Step 4: Continue validation
    df = pd.read_excel(invoice_path)

    # Step 5: Validate current window
    current_result, current_invoices = validate_invoices(df, start_str, end_str)

    # Step 6: Save validated snapshot
    save_invoice_snapshot(current_invoices, run_date=end_str)

    # Step 7: Record this run window
    record_run_window(start_str, end_str)

    # Step 8: Revalidate all previous windows
    print("🔁 Revalidating previous invoice windows...")
    all_windows = get_all_run_windows()
    cumulative_report = []

    for window in all_windows:
        window_start, window_end = window
        if window_start == start_str and window_end == end_str:
            continue  # skip current window

        print(f"🔄 Rechecking window {window_start} to {window_end}...")

        # Load existing snapshot if available
        _, past_invoices = validate_invoices(None, window_start, window_end)
        cumulative_report.extend(past_invoices)

    # Combine current + revalidation results
    full_report = current_result + cumulative_report

    # Save report
    os.makedirs("data", exist_ok=True)
    report_path = f"data/delta_report_{today_str}.xls"
    pd.DataFrame(full_report).to_excel(report_path, index=False)
    print(f"✅ Delta report generated: {report_path}")

    # Also copy to dashboard path
    dashboard_path = f"data/{today_str}/validation_result.xlsx"
    shutil.copy(report_path, dashboard_path)
    print(f"📋 Copied report for dashboard: {dashboard_path}")

    # Archive old reports
    archive_old_reports()

if __name__ == "__main__":
    run_invoice_validation()
