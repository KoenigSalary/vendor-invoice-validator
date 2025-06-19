import os
import pandas as pd

# === Validate invoices (Dummy Logic) ===
def validate_invoices(start_date, end_date):
    print(f"🔍 Running validation for {start_date} to {end_date}")
    
    invoices = []
    for i in range(5):
        invoice = {
            "invoice_no": f"INV-{i+1}",
            "invoice_date": start_date,
            "vendor_name": f"Vendor {i+1}",
            "gstin": f"29ABCDE1234F{i+1:02d}Z5",
            "pan": "ABCDE1234F",
            "hsn_code": "9983",
            "taxable_value": 1000 + i * 250,
            "total_amount": 1180 + i * 250,
            "status": "VALID"
        }
        invoices.append(invoice)
    
    results = []
    for invoice in invoices:
        result = {
            "Invoice No": invoice["invoice_no"],
            "Date": invoice["invoice_date"],
            "Vendor": invoice["vendor_name"],
            "GSTIN": invoice["gstin"],
            "Amount": invoice["total_amount"],
            "Validation Status": invoice["status"]
        }
        results.append(result)

    return results, invoices

    df[["Status", "Reason"]] = df.apply(lambda row: pd.Series(validate_row(row)), axis=1)

    # Create report as list of dicts
    report = df.to_dict(orient="records")
    return report, df

# === Save current snapshot ===
def save_snapshot(df, start_date, end_date):
    if not os.path.exists("data"):
        os.makedirs("data")
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
    if not os.path.exists("data"):
        return []
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
