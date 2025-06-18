# reporter.py

import os
import pandas as pd
from openpyxl import load_workbook, Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import PatternFill
from openpyxl.worksheet.filters import AutoFilter

def save_snapshot_report(data, start_date, end_date):
    """
    Saves the invoice validation results to a formatted Excel snapshot report
    with filters, frozen headers, conditional formatting, and separate sheets.
    """
    if not data:
        print("⚠ No data to save in report.")
        return

    folder = "snapshots"
    os.makedirs(folder, exist_ok=True)

    filename = f"InvoiceSnapshot_{start_date.date()}_to_{end_date.date()}.xlsx"
    filepath = os.path.join(folder, filename)

    df = pd.DataFrame(data)

    # 🔀 Split by status
    valid_df = df[df["Status"] == "VALID"]
    invalid_df = df[df["Status"] == "INVALID"]
    flagged_df = df[df["Status"] == "FLAGGED"]

    # ✍️ Write to Excel with pandas
    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        valid_df.to_excel(writer, sheet_name="VALID", index=False)
        invalid_df.to_excel(writer, sheet_name="INVALID", index=False)
        flagged_df.to_excel(writer, sheet_name="FLAGGED", index=False)

    # 🎨 Formatting using openpyxl
    wb = load_workbook(filepath)
    color_map = {
        "VALID": "C6EFCE",   # Green
        "INVALID": "FFC7CE", # Red
        "FLAGGED": "FFEB9C"  # Orange
    }

    for sheet_name in ["VALID", "INVALID", "FLAGGED"]:
        if sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            status_fill = PatternFill(start_color=color_map[sheet_name],
                                      end_color=color_map[sheet_name],
                                      fill_type="solid")

            # 📌 Freeze header
            ws.freeze_panes = "A2"

            # 🔍 Add filters
            ws.auto_filter.ref = ws.dimensions

            # 📏 Auto column width + conditional fill
            for col in ws.columns:
                max_length = 0
                col_letter = get_column_letter(col[0].column)
                for cell in col:
                    try:
                        if cell.row > 1:
                            cell.fill = status_fill
                        if cell.value:
                            max_length = max(max_length, len(str(cell.value)))
                    except:
                        pass
                ws.column_dimensions[col_letter].width = max_length + 2

    wb.save(filepath)
    print(f"📊 Snapshot report saved at: {filepath}")
