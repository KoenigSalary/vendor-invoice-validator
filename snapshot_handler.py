import os
import pandas as pd

def get_snapshot_path(snapshot_dir, date_str):
    return os.path.join(snapshot_dir, f"snapshot_{date_str}.xlsx")

def save_snapshot(df, snapshot_dir, date_str):
    path = get_snapshot_path(snapshot_dir, date_str)
    df.to_excel(path, index=False)

def compare_with_snapshot(current_df, snapshot_dir, today_str):
    # Load the latest snapshot if exists
    files = sorted([f for f in os.listdir(snapshot_dir) if f.endswith(".xlsx")])
    if not files:
        return None
    latest_snapshot_path = os.path.join(snapshot_dir, files[-1])
    old_df = pd.read_excel(latest_snapshot_path)

    current_ids = set(current_df["InvID"])
    old_ids = set(old_df["InvID"])

    deleted = old_ids - current_ids
    added = current_ids - old_ids
    modified = []

    common_ids = old_ids & current_ids
    for inv_id in common_ids:
        old_row = old_df[old_df["InvID"] == inv_id].fillna("").astype(str)
        new_row = current_df[current_df["InvID"] == inv_id].fillna("").astype(str)
        if not old_row.equals(new_row):
            modified.append(inv_id)

    return {
        "deleted": list(deleted),
        "added": list(added),
        "modified": modified
    }