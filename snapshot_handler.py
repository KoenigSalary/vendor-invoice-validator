# snapshot_handler.py

import os
import pandas as pd
from datetime import datetime

def compare_with_snapshot(df, snapshot_dir, today):
    """
    Compares the current dataframe (df) with the snapshot from the previous run
    and generates a delta report.
    """
    snapshot_path = os.path.join(snapshot_dir, f"snapshot_{today}.xlsx")

    # If the snapshot file exists, compare it with the current data
    if os.path.exists(snapshot_path):
        previous_df = pd.read_excel(snapshot_path)
        added = df.loc[~df['InvID'].isin(previous_df['InvID'])]
        modified = df.loc[df['InvID'].isin(previous_df['InvID']) & (df != previous_df).any(axis=1)]
        deleted = previous_df.loc[~previous_df['InvID'].isin(df['InvID'])]

        return {
            "added": added,
            "modified": modified,
            "deleted": deleted
        }
    else:
        # If no snapshot exists, treat everything as added
        return {
            "added": df,
            "modified": pd.DataFrame(),
            "deleted": pd.DataFrame()
        }

def save_snapshot(df, snapshot_dir, today):
    """
    Saves the current dataframe as a snapshot for future comparison.
    """
    os.makedirs(snapshot_dir, exist_ok=True)
    snapshot_path = os.path.join(snapshot_dir, f"snapshot_{today}.xlsx")
    df.to_excel(snapshot_path, index=False)
    print(f"✅ Snapshot saved to: {snapshot_path}")
