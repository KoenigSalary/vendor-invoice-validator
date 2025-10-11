import os
import pandas as pd
from datetime import datetime

# Set data folder
DATA_FOLDER = "data"

# Get sorted list of invoice files
files = sorted([f for f in os.listdir(DATA_FOLDER) if f.startswith("invoices_") and f.endswith(".xlsx")])

if len(files) < 2:
    print("❌ Not enough invoice files to compare (need at least 2).")
    exit()

# Latest two files
prev_file = os.path.join(DATA_FOLDER, files[-2])
curr_file = os.path.join(DATA_FOLDER, files[-1])

# Read both files
df_prev = pd.read_excel(prev_file, dtype=str).fillna("")
df_curr = pd.read_excel(curr_file, dtype=str).fillna("")

# Define unique key columns and fields to compare
key_cols = ['Invoice No', 'Vendor Name', 'Invoice Date']
compare_cols = ['GSTIN', 'PAN', 'HSN Code', 'Taxable Value', 'Total Amount']

# Create key field
df_prev["__key"] = df_prev[key_cols].agg("|".join, axis=1)
df_curr["__key"] = df_curr[key_cols].agg("|".join, axis=1)

# Convert to dicts for lookup
prev_dict = df_prev.set_index("__key").to_dict(orient='index')
curr_dict = df_curr.set_index("__key").to_dict(orient='index')

results = []

# 🔎 Compare current entries
for key, curr_row in curr_dict.items():
    if key not in prev_dict:
        results.append({**curr_row, "Status": "New Upload", "Reason": "Not found in previous file"})
    else:
        prev_row = prev_dict[key]
        diffs = []
        for col in compare_cols:
            if curr_row.get(col) != prev_row.get(col):
                diffs.append(f"{col} changed")
        if diffs:
            results.append({**curr_row, "Status": "Modified", "Reason": ", ".join(diffs)})

# 🔍 Check for deleted entries
for key, prev_row in prev_dict.items():
    if key not in curr_dict:
        results.append({**prev_row, "Status": "Deleted", "Reason": "Missing in current file"})

# Final delta DataFrame
df_delta = pd.DataFrame(results)

# Save delta report
today_str = datetime.now().strftime('%Y-%m-%d')
output_file = os.path.join(DATA_FOLDER, f"delta_report_{today_str}.xlsx")
df_delta.to_excel(output_file, index=False)
print(f"✅ Delta report generated: {output_file}")