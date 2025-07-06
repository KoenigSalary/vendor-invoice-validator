import os
import shutil
from datetime import datetime, timedelta

def archive_old_reports():
    DATA_FOLDER = "data"
    ARCHIVE_FOLDER = os.path.join(DATA_FOLDER, "archive")
    os.makedirs(ARCHIVE_FOLDER, exist_ok=True)

    cutoff_date = datetime.now() - timedelta(days=90)

    for file in os.listdir(DATA_FOLDER):
        file_path = os.path.join(DATA_FOLDER, file)

        # Archive delta reports: delta_report_YYYY-MM-DD.xlsx
        if file.startswith("delta_report_") and file.endswith(".xlsx"):
            date_str = file.replace("delta_report_", "").replace(".xlsx", "")
            try:
                file_date = datetime.strptime(date_str, "%Y-%m-%d")
                if file_date < cutoff_date:
                    shutil.move(file_path, os.path.join(ARCHIVE_FOLDER, file))
                    print(f"📦 Archived delta report: {file}")
            except ValueError:
                print(f"⚠️ Skipped (bad delta date): {file}")

        # Archive bank book reports: BankBookEntryDD-MMM-YYYY.xls
        elif file.startswith("BankBookEntry") and file.endswith(".xls"):
            date_str = file.replace("BankBookEntry", "").replace(".xls", "")
            try:
                file_date = datetime.strptime(date_str, "%d-%b-%Y")
                if file_date < cutoff_date:
                    shutil.move(file_path, os.path.join(ARCHIVE_FOLDER, file))
                    print(f"📦 Archived bank book: {file}")
            except ValueError:
                print(f"⚠️ Skipped (bad bank date): {file}")
