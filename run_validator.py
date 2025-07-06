import os
import zipfile
import pandas as pd
from datetime import datetime
import fitz  # PyMuPDF
from openpyxl import Workbook
from email_sender import send_email_report  # Import your email sending function

# Paths
DOWNLOAD_FOLDER = "data"
TODAY_FOLDER = datetime.today().strftime("%Y-%m-%d")
XLS_PATH = os.path.join(DOWNLOAD_FOLDER, TODAY_FOLDER, "invoice_download.xls")
ZIP_PATH = os.path.join(DOWNLOAD_FOLDER, TODAY_FOLDER, "invoices.zip")
UNZIP_DIR = os.path.join(DOWNLOAD_FOLDER, TODAY_FOLDER, "unzipped")
VALIDATED_DIR = os.path.join(DOWNLOAD_FOLDER, TODAY_FOLDER, "validated_invoices")
RESULT_PATH = os.path.join(DOWNLOAD_FOLDER, TODAY_FOLDER, "validation_result.xlsx")
INV_CREATOR_MAP_PATH = os.path.join(DOWNLOAD_FOLDER, TODAY_FOLDER, "inv_created_by_map.csv")

# Create necessary folders
os.makedirs(UNZIP_DIR, exist_ok=True)
os.makedirs(VALIDATED_DIR, exist_ok=True)

def read_invoice_excel(path):
    try:
        return pd.read_excel(path)
    except:
        try:
            return pd.read_csv(path, sep=None, engine="python")
        except Exception as e:
            print(f"❌ Failed to read invoice file: {e}")
            return None

def extract_text_from_file(file_path):
    try:
        text = ""
        with fitz.open(file_path) as doc:
            for page in doc:
                text += page.get_text()
        return text
    except Exception as e:
        print(f"❌ Failed to extract text from {file_path}: {e}")
        return ""

def match_fields(text, df, return_row=False):
    for _, row in df.iterrows():
        inv_no = str(row.get("PurchaseInvNo", "")).strip()
        if inv_no and inv_no in text:
            return ("✅ VALID", row) if return_row else "✅ VALID"
    return ("❌ Not Matched", None) if return_row else "❌ Not Matched"

def validate_invoices():
    # Step 1: Load invoice data
    if not os.path.exists(XLS_PATH):
        print(f"❌ Missing invoice sheet: {XLS_PATH}")
        return None
    df = read_invoice_excel(XLS_PATH)
    if df is None:
        return None
    print(f"✅ Invoice sheet loaded. Rows: {len(df)}, Columns: {list(df.columns)}")

    # Step 2: Load uploader mapping
    if os.path.exists(INV_CREATOR_MAP_PATH):
        df_map = pd.read_csv(INV_CREATOR_MAP_PATH)
        if "InvID" not in df_map.columns:
            possible_col = [col for col in df_map.columns if "id" in col.lower()]
            if possible_col:
                df_map = df_map.rename(columns={possible_col[0]: "InvID"})
            else:
                print("❌ 'InvID' column missing in inv_created_by_map.csv.")
                df["Inv Created By"] = "Unknown"
                df_map = pd.DataFrame(columns=["InvID", "Inv Created By"])
        df = df.merge(df_map, on="InvID", how="left")
        print(f"✅ 'Inv Created By' mapping loaded from: {INV_CREATOR_MAP_PATH}")
    else:
        print("⚠️ inv_created_by_map.csv not found. 'Inv Created By' will be marked Unknown.")
        df["Inv Created By"] = "Unknown"

    # Step 3: Unzip invoices
    if os.path.exists(ZIP_PATH):
        with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
            zip_ref.extractall(UNZIP_DIR)
        print(f"✅ Unzipped: {ZIP_PATH} to {UNZIP_DIR}")
    else:
        print(f"❌ ZIP file not found: {ZIP_PATH}")
        return None

    # Step 4: Validate invoices
    results = []
    for root, _, files in os.walk(UNZIP_DIR):
        for fname in files:
            if fname.lower().endswith(".pdf"):
                fpath = os.path.join(root, fname)
                text = extract_text_from_file(fpath)
                result, matched_row = match_fields(text, df, return_row=True)
                
                # Ensure matched_row is valid and a pandas.Series
                if isinstance(matched_row, pd.Series):
                    creator = matched_row.get("Inv Created By", "Unknown")
                    voucher_no = matched_row.get("VoucherNo", "")
                    voucher_date = matched_row.get("VoucherDate", "")
                    purchase_inv_no = matched_row.get("PurchaseInvNo", "")
                    purchase_inv_date = matched_row.get("PurchaseInvDate", "")
                    party_name = matched_row.get("PartyName", "")
                    gstno = matched_row.get("GSTNO", "")
                    vat_number = matched_row.get("VATNumber", "")
                    taxable_value = matched_row.get("TaxableValue", "")
                    currency = matched_row.get("Currency", "")
                    igst_ledger = matched_row.get("IGST/VATInputLedger", "")
                    igst_amt = matched_row.get("IGST/VATInputAmt", "")
                    cgst_ledger = matched_row.get("CGSTInputLedger", "")
                    cgst_amt = matched_row.get("CGSTInputAmt", "")
                    sgst_ledger = matched_row.get("SGSTInputLedger", "")
                    sgst_amt = matched_row.get("SGSTInputAmt", "")
                    total = matched_row.get("Total", "")
                    inv_id = matched_row.get("InvID", "")
                    narration = matched_row.get("Narration", "")
                else:
                    creator = "Unknown"
                    voucher_no = voucher_date = purchase_inv_no = purchase_inv_date = party_name = ""
                    gstno = vat_number = taxable_value = currency = igst_ledger = igst_amt = ""
                    cgst_ledger = cgst_amt = sgst_ledger = sgst_amt = total = inv_id = narration = ""

                results.append({
                    "VoucherNo": voucher_no,
                    "VoucherDate": voucher_date,
                    "PurchaseInvNo": purchase_inv_no,
                    "PurchaseInvDate": purchase_inv_date,
                    "PartyName": party_name,
                    "GSTNO": gstno,
                    "VATNumber": vat_number,
                    "TaxableValue": taxable_value,
                    "Currency": currency,
                    "IGST/VATInputLedger": igst_ledger,
                    "IGST/VATInputAmt": igst_amt,
                    "CGSTInputLedger": cgst_ledger,
                    "CGSTInputAmt": cgst_amt,
                    "SGSTInputLedger": sgst_ledger,
                    "SGSTInputAmt": sgst_amt,
                    "Total": total,
                    "Inv Created By": creator,
                    "InvID": inv_id,
                    "Narration": narration,
                    "Correct": "✅" if result == "✅ VALID" else "",
                    "Flagged": "🚩" if result == "❌ Not Matched" else "",
                    "Modified Since Last Check": "",  # Placeholder for future
                    "Late Upload": ""  # Placeholder for future
                })

    result_df = pd.DataFrame(results)
    result_df.to_excel(RESULT_PATH, index=False)
    print(f"✅ Validation complete. Report saved to: {RESULT_PATH}")

    # Delta report handling (optional for snapshot comparison)
    snapshot_dir = os.path.join("snapshots")
    os.makedirs(snapshot_dir, exist_ok=True)
    delta_report = compare_with_snapshot(result_df, snapshot_dir, today)
    save_snapshot(result_df, snapshot_dir, today)

    # Email report
    send_email_report(RESULT_PATH, ZIP_PATH, delta_report=delta_report)

    return RESULT_PATH

# Run script
if __name__ == "__main__":
    validate_invoices()
