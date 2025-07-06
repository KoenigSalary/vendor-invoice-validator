import os
import pandas as pd
from datetime import datetime
from snapshot_handler import compare_with_snapshot, save_snapshot
from email_sender import send_email_report

def validate_and_report():
    today = datetime.today().strftime("%Y-%m-%d")
    base_dir = os.path.join("data", today)
    result_path = os.path.join(base_dir, "validation_result.xlsx")
    zip_path = os.path.join(base_dir, "invoices.zip")

    # Sample minimal DataFrame for illustration (replace with real validation logic)
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

    df.to_excel(result_path, index=False)

    # Save and compare snapshots
    snapshot_dir = os.path.join("snapshots")
    os.makedirs(snapshot_dir, exist_ok=True)
    delta_report = compare_with_snapshot(df, snapshot_dir, today)
    save_snapshot(df, snapshot_dir, today)

    # Email report
    send_email_report(result_path, zip_path, delta_report=delta_report)