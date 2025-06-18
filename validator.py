import os
import pandas as pd

# === Save current snapshot ===
def save_snapshot(df, start_date, end_date):
    snapshot_path = f"data/snapshot_{start_date}_to_{end_date}.xlsx"
    df.to_excel(snapshot_path, index=False)

# === Load snapshot if exists ===
def load_snapshot(start_date, end_date):
    snapshot_path = f"data/snapshot_{start_date}_to_{end_date}.xlsx"
    if os.path.exists(snapshot_path):
        return pd.read_excel(snapshot_path)
    else:
        return None

# === Get list of all snapshot date ranges ===
def get_all_snapshot_ranges():
    files = os.listdir("data")
    ranges = []
    for f in files:
        if f.startswith("snapshot_") and f.endswith(".xlsx"):
            try:
                part = f.replace("snapshot_", "").replace(".xlsx", "")
                start, end = part.split("_to_")
                ranges.append((start, end))
            except:
                continue
    return sorted(ranges)
