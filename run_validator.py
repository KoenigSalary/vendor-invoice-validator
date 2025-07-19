import os
import zipfile
import pandas as pd
from datetime import datetime
import fitz  # PyMuPDF for PDF extraction
from snapshot_handler import compare_with_snapshot, save_snapshot
from email_sender import send_email_report

def get_latest_data_folder(base="data"):
    folders = [f for f in os.listdir(base) if f.startswith("2025-")]
    folders.sort(reverse=True)
    return os.path.join(base, folders[0]) if folders else None

# === Step 1: Set base paths ===
DOWNLOAD_FOLDER = "data"
TODAY_FOLDER = datetime.today().strftime("%Y-%m-%d")
base_dir = os.path.join(DOWNLOAD_FOLDER, TODAY_FOLDER)

if not os.path.exists(base_dir):
    print(f"[INFO] Today's folder '{TODAY_FOLDER}' not found. Looking for latest available data...")
    base_dir = get_latest_data_folder(DOWNLOAD_FOLDER)
    if not base_dir:
        print("[ERROR] No previous data folder found. Aborting validation.")
        exit(1)
    print(f"[INFO] Using fallback folder: {base_dir}")

# === Step 2: Define paths ===
XLS_PATH = os.path.join(base_dir, "invoice_download.xls")
ZIP_PATH = os.path.join(base_dir, "invoices.zip")
UNZIP_DIR = os.path.join(base_dir, "unzipped")
VALIDATED_DIR = os.path.join(base_dir, "validated_invoices")
RESULT_PATH = os.path.join(base_dir, "validation_result.xlsx")
INV_CREATOR_MAP_PATH = os.path.join(base_dir, "inv_created_by_map.csv")

# === Ensure folders ===
os.makedirs(UNZIP_DIR, exist_ok=True)
os.makedirs(VALIDATED_DIR, exist_ok=True)

def read_invoice_excel(path):
    try:
        # Specify the engine explicitly (use openpyxl for .xlsx files)
        return pd.read_excel(path, engine="openpyxl")
    except Exception as e:
        print(f"[ERROR] Failed to read invoice file: {e}")
        return None

def extract_text_from_file(file_path):
    try:
        text = ""
        with fitz.open(file_path) as doc:
            for page in doc:
                text += page.get_text()
        return text
    except Exception as e:
        print(f"[ERROR] Failed to extract text from {file_path}: {e}")
        return ""

def match_fields(text, df, return_row=False):
    for _, row in df.iterrows():
        inv_no = str(row.get("PurchaseInvNo", "")).strip()
        if inv_no and inv_no in text:
            return ("✅ VALID", row) if return_row else "✅ VALID"
    return ("❌ Not Matched", None) if return_row else "❌ Not Matched"

def validate_invoices():
    if not os.path.exists(XLS_PATH):
        print(f"[ERROR] Invoice sheet not found at {XLS_PATH}")
        return None

    df = read_invoice_excel(XLS_PATH)
    if df is None:
        return None

    print(f"[INFO] Invoice sheet loaded: {len(df)} rows.")

    # === Load mapping file (if exists) ===
    if os.path.exists(INV_CREATOR_MAP_PATH):
        df_map = pd.read_csv(INV_CREATOR_MAP_PATH)
        if "InvID" not in df_map.columns:
            possible_col = [col for col in df_map.columns if "id" in col.lower()]
            if possible_col:
                df_map = df_map.rename(columns={possible_col[0]: "InvID"})
            else:
                print("[WARN] 'InvID' column not found in map. Assigning Unknown.")
                df["Inv Created By"] = "Unknown"
                df_map = pd.DataFrame(columns=["InvID", "Inv Created By"])
        df = df.merge(df_map, on="InvID", how="left")
        print(f"[INFO] Uploader mapping loaded from: {INV_CREATOR_MAP_PATH}")
    else:
        print("[WARN] inv_created_by_map.csv not found. Assigning all as Unknown.")
        df["Inv Created By"] = "Unknown"

def is_valid_zip(zip_path):
    """Checks if the file is a valid zip file."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Attempt to list the files in the ZIP to check if it's a valid zip file
            zip_ref.testzip()
        return True
    except zipfile.BadZipFile:
        return False

    # === Step 1: Define paths ===
    ZIP_PATH = os.path.join(base_dir, "invoices.zip")

    # === Unzip invoices ===
    if os.path.exists(ZIP_PATH):
        if is_valid_zip(ZIP_PATH):  # Check if the ZIP file is valid
            with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
                zip_ref.extractall(UNZIP_DIR)
            print(f"[INFO] Invoices unzipped to: {UNZIP_DIR}")
        else:
            print(f"[ERROR] The file at {ZIP_PATH} is not a valid ZIP file.")
            return None  # Exit function if the ZIP file is invalid
    else:
        print(f"[ERROR] Invoices ZIP file not found: {ZIP_PATH}")
        return None

    # === Validate invoices ===
    results = []
    for root, _, files in os.walk(UNZIP_DIR):
        for fname in files:
            if fname.lower().endswith(".pdf"):
                fpath = os.path.join(root, fname)
                text = extract_text_from_file(fpath)
                result, matched_row = match_fields(text, df, return_row=True)
                if matched_row is not None:
                    creator = matched_row.get("Inv Created By", "Unknown")
                    results.append({
                        "VoucherNo": matched_row.get("VoucherNo", ""),
                        "VoucherDate": matched_row.get("VoucherDate", ""),
                        "PurchaseInvNo": matched_row.get("PurchaseInvNo", ""),
                        "PurchaseInvDate": matched_row.get("PurchaseInvDate", ""),
                        "PartyName": matched_row.get("PartyName", ""),
                        "GSTNO": matched_row.get("GSTNO", ""),
                        "VATNumber": matched_row.get("VATNumber", ""),
                        "TaxableValue": matched_row.get("TaxableValue", ""),
                        "Currency": matched_row.get("Currency", ""),
                        "IGST/VATInputLedger": matched_row.get("IGST/VATInputLedger", ""),
                        "IGST/VATInputAmt": matched_row.get("IGST/VATInputAmt", ""),
                        "CGSTInputLedger": matched_row.get("CGSTInputLedger", ""),
                        "CGSTInputAmt": matched_row.get("CGSTInputAmt", ""),
                        "SGSTInputLedger": matched_row.get("SGSTInputLedger", ""),
                        "SGSTInputAmt": matched_row.get("SGSTInputAmt", ""),
                        "Total": matched_row.get("Total", ""),
                        "Inv Created By": creator,
                        "InvID": matched_row.get("InvID", ""),
                        "Narration": matched_row.get("Narration", ""),
                        "Correct": "✅" if result == "✅ VALID" else "",
                        "Flagged": "🚩" if result == "❌ Not Matched" else "",
                        "Modified Since Last Check": "",
                        "Late Upload": ""
                    })
                else:
                    print(f"[WARN] Invoice PDF unmatched: {fname}")

    result_df = pd.DataFrame(results)
    result_df.to_excel(RESULT_PATH, index=False)
    print(f"[INFO] Validation complete. Report saved to: {RESULT_PATH}")

    # === Snapshot & Delta Check ===
    snapshot_dir = os.path.join("snapshots")
    os.makedirs(snapshot_dir, exist_ok=True)
    delta_report = compare_with_snapshot(result_df, snapshot_dir, TODAY_FOLDER)
    save_snapshot(result_df, snapshot_dir, TODAY_FOLDER)

    # === Email the result ===
    send_email_report(RESULT_PATH, ZIP_PATH, delta_report=delta_report)

    return RESULT_PATH

# === Run Automatically if script executed directly ===
if __name__ == "__main__":
    validate_invoices()
