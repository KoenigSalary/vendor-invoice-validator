# main.py

from selenium import webdriver
from rms_scraper import login_to_rms, fetch_invoice_rows
from validator import validate_invoice
from updater import update_invoice_status
from reporter import save_snapshot_report
from dateutil.parser import parse
from dotenv import load_dotenv
from datetime import datetime, timedelta
from invoice_tracker import save_invoice_snapshot, record_run_window, get_all_run_windows
import pandas as pd
import os

# === Define main run logic ===
def run_invoice_validation():
    today = datetime.today()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=3)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    print(f"🔍 Validating invoices from {start_str} to {end_str}...")

    # Step 1: Validate current 4-day window
    current_result, current_invoices = validate_invoices(start_str, end_str)

    # Step 2: Save validated snapshot
    save_invoice_snapshot(current_invoices, run_date=end_str)

    # Step 3: Record this run window
    record_run_window(start_str, end_str)

    # Step 4: Revalidate all previous windows
    print("🔁 Revalidating previous invoice windows...")
    all_windows = get_all_run_windows()
    cumulative_report = []

    for window in all_windows:
        window_start, window_end = window
        if window_start == start_str and window_end == end_str:
            continue  # already validated
        print(f"🔄 Rechecking window {window_start} to {window_end}...")
        result, _ = validate_invoices(window_start, window_end)
        cumulative_report.extend(result)

    # Combine current + revalidation results
    full_report = current_result + cumulative_report

    if not os.path.exists("data"):
        os.makedirs("data")

    # Save report
    report_path = f"data/delta_report_{today.strftime('%Y-%m-%d')}.xlsx"
    df = pd.DataFrame(full_report)
    df.to_excel(report_path, index=False)
    print(f"✅ Delta report generated: {report_path}")


if __name__ == "__main__":
    run_invoice_validation()
