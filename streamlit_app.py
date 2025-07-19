import streamlit as st
import pandas as pd
import os
import matplotlib.pyplot as plt
from io import BytesIO
from datetime import datetime
from PIL import Image
import base64

# === Page Config ===
st.set_page_config(page_title="Vendor Invoice Validation Dashboard", layout="wide")

# === Colors ===
PRIMARY_COLOR = "#003366"
ACCENT_COLOR = "#0077CC"
ERROR_COLOR = "#FF4B4B"
INFO_COLOR = "#F5F7FA"

# === Centered Logo and Title with Style ===
logo_path = "assets/koenig_logo.png"
if os.path.exists(logo_path):
    buffer = BytesIO()
    Image.open(logo_path).save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode()
    logo_html = f"<img src='data:image/png;base64,{encoded}' width='160' style='margin-bottom: 10px;'/>"
else:
    logo_html = "<p style='color: red;'>Logo not found</p>"

st.markdown(
    f"""
    <div style='
        background-color: {INFO_COLOR};
        padding: 30px 10px 20px;
        border-radius: 15px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
        text-align: center;
        margin-bottom: 25px;
    '>
        {logo_html}
        <h1 style='
            color: {PRIMARY_COLOR};
            font-size: 28px;
            margin-top: 10px;
        '>📋 Vendor Invoice Validation Dashboard</h1>
    </div>
    """,
    unsafe_allow_html=True,
)

# === Load Latest Report ===
DATA_FOLDER = "./data"
os.makedirs(DATA_FOLDER, exist_ok=True)

# List all delta reports
report_files = sorted([
    f for f in os.listdir(DATA_FOLDER)
    if f.startswith("delta_report_") and f.endswith(".xlsx")
])

# ⛔ Fallback if no reports found
if not report_files:
    st.warning("⚠️ No delta reports found in the 'data' folder. Please run the validator first.")
    st.stop()

# Get the latest report
latest_file = report_files[-1]
file_path = os.path.join(DATA_FOLDER, latest_file)

# Read the report data
df = pd.read_excel(file_path)

# === Fill Required Columns if Missing ===
required_cols = [
    "Validation Status", "Vendor", "Amount", "Invoice No", "GSTIN",
    "Modification Reason", "Rate of Product", "SGST", "CGST", "IGST",
    "Upload Date", "Late Upload"
]
for col in required_cols:
    if col not in df.columns:
        df[col] = ""

st.success(f"✅ Showing Delta Report for {latest_file.replace('delta_report_', '').replace('.xlsx', '')}")

# === Filters ===
with st.expander("🔎 Filters"):
    vendor_filter = st.selectbox("Filter by Vendor", options=["All"] + sorted(df["Vendor"].dropna().unique().tolist()))
    status_filter = st.multiselect("Filter by Status", options=sorted(df["Validation Status"].dropna().unique().tolist()), default=sorted(df["Validation Status"].dropna().unique().tolist()))
    date_filter = st.date_input("Filter by Upload Date (optional)", value=None)
    invoice_search = st.text_input("🔍 Search by Invoice No or GSTIN")

filtered_df = df.copy()
if vendor_filter != "All":
    filtered_df = filtered_df[filtered_df["Vendor"] == vendor_filter]
if status_filter:
    filtered_df = filtered_df[filtered_df["Validation Status"].isin(status_filter)]
if date_filter:
    filtered_df = filtered_df[filtered_df["Upload Date"] == pd.to_datetime(date_filter)]
if invoice_search:
    filtered_df = filtered_df[filtered_df["Invoice No"].astype(str).str.contains(invoice_search, case=False, na=False) |
                              filtered_df["GSTIN"].astype(str).str.contains(invoice_search, case=False, na=False)]

# === Metrics Summary (Styled) ===
total = len(df)
valid = (df["Validation Status"].str.upper() == "VALID").sum()
flagged = (df["Validation Status"].str.upper() == "FLAGGED").sum()
changed = (df["Validation Status"].str.upper() == "CHANGED").sum()
modified = (df["Validation Status"].str.upper() == "MODIFIED").sum()
deleted = (df["Validation Status"].str.upper() == "DELETED").sum()

st.markdown("### 📊 Summary Overview")
col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("📦 Total", total)
col2.metric("✅ Valid", valid)
col3.metric("⚠️ Flagged", flagged)
col4.metric("🔁 Changed", changed)
col5.metric("✏️ Modified", modified)
col6.metric("❌ Deleted", deleted)

# === Tabbed View ===
tab1, tab2, tab3 = st.tabs(["📋 All Invoices", "🚩 Flagged", "✏️ Modified"])

with tab1:
    st.subheader("📑 All Validated Invoices")
    st.dataframe(filtered_df, use_container_width=True)

with tab2:
    st.subheader("🚩 Flagged Invoices")
    st.dataframe(filtered_df[filtered_df["Validation Status"].str.upper() == "FLAGGED"], use_container_width=True)

with tab3:
    st.subheader("✏️ Modified Invoices")
    st.dataframe(filtered_df[filtered_df["Validation Status"].str.upper() == "MODIFIED"], use_container_width=True)

# === Chart ===
st.markdown("### 📈 Validation Status Breakdown")
chart_data = filtered_df["Validation Status"].value_counts()
fig, ax = plt.subplots(figsize=(6, 3))
chart_data.plot(kind="bar", ax=ax, color=ACCENT_COLOR, edgecolor='black')
ax.set_title("Validation Status", fontsize=12)
ax.set_xlabel("Status")
ax.set_ylabel("Count")
ax.grid(axis='y', linestyle='--', alpha=0.6)
st.pyplot(fig)

# === Download Filtered Report ===
output = BytesIO()
filtered_df.to_excel(output, index=False, engine='openpyxl')
output.seek(0)

file_name = f"filtered_invoice_report_{datetime.today().strftime('%Y-%m-%d')}.xlsx"

st.download_button(
    label="📥 Download Filtered Report",
    data=output,
    file_name=file_name,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

# === Footer ===
st.markdown(
    "<hr><p style='text-align: center; color: grey;'>© 2025 Koenig Solutions | Vendor Invoice Validator</p>",
    unsafe_allow_html=True
)
