import os
import pandas as pd
from datetime import datetime
from snapshot_handler import compare_with_snapshot, save_snapshot
from email_sender import send_email_report

# 1. Get today's folder and fallback logic
today = datetime.today().strftime("%Y-%m-%d")
base_dir = os.path.join("data", today)

def get_latest_data_folder(base="data"):
    folders = [f for f in os.listdir(base) if f.startswith("2025-")]
    folders.sort(reverse=True)
    return os.path.join(base, folders[0]) if folders else None

# 2. Ensure folder exists or fallback
if not os.path.exists(base_dir):
    print(f"❌ Folder not found for today ({today}), trying fallback.")
    fallback_dir = get_latest_data_folder()
    if fallback_dir:
        print(f"🔁 Fallback to latest available folder: {fallback_dir}")
        base_dir = fallback_dir
    else:
        print("❌ No fallback folder found. Exiting.")
        exit(1)

# 3. Define paths
result_path = os.path.join(base_dir, "validation_result.xlsx")
zip_path = os.path.join(base_dir, "invoices.zip")

# 4. Sample dataframe (replace with actual invoice reading logic)
df = pd.DataFrame([{
    "VoucherNo": "V001",
    "VoucherDate": "2025-07-01",
    "PurchaseInvNo": "INV001",
    "PurchaseInvDate": "2025-07-01",
    "PartyName": "ABC Pvt Ltd",
    "GSTNO": "07ABCDE1234F1Z5",
    "VATNumber": "VAT001",
    "TaxableValue": 10000,
    "Currency": "INR",
    "IGST/VATInputLedger": "IGST",
    "IGST/VATInputAmt": 1800,
    "CGSTInputLedger": "CGST",
    "CGSTInputAmt": 900,
    "SGSTInputLedger": "SGST",
    "SGSTInputAmt": 900,
    "Total": 11800,
    "Inv Created By": "user1",
    "InvID": "INV001",
    "Narration": "Purchase goods",
    "Correct": "✅",
    "Flagged": "",
    "Modified Since Last Check": "",
    "Late Upload": ""
}])

# 5. Save result and handle snapshot
df.to_excel(result_path, index=False)
snapshot_dir = os.path.join("snapshots")
os.makedirs(snapshot_dir, exist_ok=True)

delta_report = compare_with_snapshot(df, snapshot_dir, today)
save_snapshot(df, snapshot_dir, today)

# 6. Email the report
send_email_report(result_path, zip_path, delta_report=delta_report)
