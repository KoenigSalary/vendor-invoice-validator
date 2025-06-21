# run_validator.py
import os
import shutil
import pandas as pd
from datetime import datetime, timedelta
from validator_utils import validate_invoices, save_snapshot, load_snapshot, get_all_snapshot_ranges

# === Date Window ===
from_date = datetime(2025, 6, 18).strftime('%Y-%m-%d')
to_date = datetime(2025, 6, 21).strftime('%Y-%m-%d')
today_str = to_date

# === Run validation ===
results, _ = validate_invoices(from_date, to_date)
df = pd.DataFrame(results)

# === Paths ===
data_folder = "data"
archive_folder = os.path.join(data_folder, "archive")
os.makedirs(archive_folder, exist_ok=True)

# === Save snapshot ===
save_snapshot(df, from_date, to_date)

# === Save delta report ===
delta_path = f"{data_folder}/delta_report_{today_str}.xlsx"
df.to_excel(delta_path, index=False)

# === Archive old delta reports (> 3 months) ===
today = datetime.now()
for filename in os.listdir(data_folder):
    if filename.startswith("delta_report_") and filename.endswith(".xlsx"):
        try:
            file_date = datetime.strptime(filename.replace("delta_report_", "").replace(".xlsx", ""), "%Y-%m-%d")
            if today - file_date > timedelta(days=90):
                shutil.move(os.path.join(data_folder, filename), os.path.join(archive_folder, filename))
        except:
            continue

# === Load and update master log ===
master_log_path = os.path.join(data_folder, "master_invoice_log.xlsx")
if os.path.exists(master_log_path):
    master_df = pd.read_excel(master_log_path)
else:
    master_df = pd.DataFrame(columns=df.columns)

# === Detect modifications and deletions ===
df["Key"] = df["Invoice No"] + "|" + df["GSTIN"]
master_df["Key"] = master_df["Invoice No"] + "|" + master_df["GSTIN"]

# Deleted
deleted_keys = set(master_df["Key"]) - set(df["Key"])
deleted_df = master_df[master_df["Key"].isin(deleted_keys)]
deleted_df["Validation Status"] = "DELETED"

# Modified
modified_keys = []
common_keys = set(master_df["Key"]) & set(df["Key"])
for key in common_keys:
    old_row = master_df[master_df["Key"] == key].iloc[0]
    new_row = df[df["Key"] == key].iloc[0]
    if any(old_row[col] != new_row[col] for col in ["Vendor", "Amount", "Validation Status"]):
        modified_keys.append(key)
        df.loc[df["Key"] == key, "Validation Status"] = "MODIFIED"

# Add deleted to current df
final_df = pd.concat([df, deleted_df], ignore_index=True)

# === Update master log ===
updated_master = pd.concat([master_df[~master_df["Key"].isin(df["Key"])] , df])
updated_master.drop(columns=["Key"], inplace=True)
updated_master.to_excel(master_log_path, index=False)

# === Dashboard Summary ===
total = len(final_df)
valid = (final_df["Validation Status"] == "VALID").sum()
flagged = (final_df["Validation Status"] == "FLAGGED").sum()
changed = (final_df["Validation Status"] == "CHANGED").sum()
modified = (final_df["Validation Status"] == "MODIFIED").sum()
deleted = (final_df["Validation Status"] == "DELETED").sum()

print("\n📋 Vendor Invoice Validation Dashboard")
print(f"✅ Showing Delta Report for {today_str}")
print(f"\n📦 Total Invoices\t{total}")
print(f"✅ Valid\t\t{valid}")
print(f"⚠️ Flagged\t\t{flagged}")
print(f"✏️ Modified\t\t{modified}")
print(f"❌ Deleted\t\t{deleted}")
