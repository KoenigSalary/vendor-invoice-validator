import os
import pandas as pd
from datetime import datetime, timedelta

DATA_FOLDER = "data"
SNAPSHOT_PREFIX = "snapshot"
DELTA_PREFIX = "delta_report"
ARCHIVE_FOLDER = os.path.join(DATA_FOLDER, "archive")

def validate_invoices(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    # Load invoice data
    invoice_path = os.path.join(DATA_FOLDER, f"invoices_{end_str}.xlsx")
    if not os.path.exists(invoice_path):
        return pd.DataFrame(), []

    df = pd.read_excel(invoice_path)

    # Sample validation (assumes presence of "GST Rate" and "Total Amount")
    df["Validation"] = "VALID"
    df.loc[df["GST Rate"].isnull(), "Validation"] = "FLAGGED"
    df.loc[df["Total Amount"] <= 0, "Validation"] = "FLAGGED"

    return df, [f"Validated {len(df)} invoices from {start_str} to {end_str}"]

def save_snapshot(df, start_date, end_date):
    filename = f"{SNAPSHOT_PREFIX}_{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}.xlsx"
    df.to_excel(os.path.join(DATA_FOLDER, filename), index=False)

def load_snapshot(start_date, end_date):
    filename = f"{SNAPSHOT_PREFIX}_{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}.xlsx"
    path = os.path.join(DATA_FOLDER, filename)
    if os.path.exists(path):
        return pd.read_excel(path)
    return pd.DataFrame()

def generate_delta_report(current_df, previous_df):
    current_ids = set(current_df["Invoice No"])
    previous_ids = set(previous_df["Invoice No"])

    deleted_ids = previous_ids - current_ids
    modified_ids = []

    current_map = current_df.set_index("Invoice No").to_dict("index")
    previous_map = previous_df.set_index("Invoice No").to_dict("index")

    for inv in current_ids & previous_ids:
        if current_map[inv] != previous_map[inv]:
            modified_ids.append(inv)

    current_df["Modified"] = current_df["Invoice No"].apply(lambda x: x in modified_ids)
    current_df["Deleted"] = current_df["Invoice No"].apply(lambda x: x in deleted_ids)

    return current_df, modified_ids, deleted_ids
