import subprocess
import datetime

def should_run_today():
    today = datetime.date.today()
    start_date = datetime.date(2025, 6, 15)  # ✅ First run reference
    delta = (today - start_date).days
    return delta % 4 == 0

if __name__ == "__main__":
    if should_run_today():
        print("✅ Running main.py (4-day cycle trigger)")
        subprocess.run(["python3", "main.py"])
    else:
        print("⏳ Not scheduled today. Will run on next 4th-day cycle.")
