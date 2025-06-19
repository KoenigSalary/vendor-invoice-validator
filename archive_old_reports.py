import os
import shutil
from datetime import datetime, timedelta

def archive_old_reports():
    DATA_FOLDER = "data"
    ARCHIVE_FOLDER = os.path.join(DATA_FOLDER, "archive")
    os.makedirs(ARCHIVE_FOLDER, exist_ok=True)

    cutoff_date = datetime.now() - timedelta(days=90)

    for file in os.listdir(DATA_FOLDER):
        if file.startswith("delta_report_") and file.endswith(".xlsx"):
            date_str = file.replace("delta_report_", "").replace(".xlsx", "")
            try:
                file_date = datetime.strptime(date_str, "%Y-%m-%d")
                if file_date < cutoff_date:
                    shutil.move(os.path.join(DATA_FOLDER, file),
                                os.path.join(ARCHIVE_FOLDER, file))
                    print(f"📦 Archived: {file}")
            except ValueError:
                continue
