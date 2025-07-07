# streamlit_app.py

import streamlit as st
import pandas as pd
import os
import matplotlib.pyplot as plt
from io import BytesIO
from datetime import datetime
from PIL import Image

# Trigger Streamlit Cloud to rebuild

# Set page configuration
st.set_page_config(page_title="Vendor Invoice Validation Dashboard", layout="wide")

# === Logo and Title ===
col_logo, col_title = st.columns([1, 6])
with col_logo:
    logo_path = "assets/koenig_logo.png"
    if os.path.exists(logo_path):
        logo = Image.open(logo_path)
        st.image(logo, width=250)
    else:
        st.warning("⚠️ Logo not found at assets/koenig_logo.png")

with col_title:
    st.markdown("<h1 style='padding-top: 25px;'>📋 Vendor Invoice Validation Dashboard</h1>", unsafe_allow_html=True)

# === Trigger Validator Script ===
st.markdown("---")
st.subheader("⚙️ Run or Email Validator")

col_run, col_email = st.columns(2)

with col_run:
    if st.button("▶️ Run Validator Now"):
        with st.spinner("Running validation..."):
            result = os.popen("python run_validator.py").read()
        st.success("✅ Validation completed.")
        st.text_area("📋 Output Log", result, height=250)

with col_email:
    if st.button("📧 Email Summary Now"):
        with st.spinner("Sending email..."):
            result = os.popen("python email_report.py").read()
        st.success("✅ Email sent.")
        st.text_area("📋 Email Log", result, height=250)

# === Load Latest Delta Report ===
DATA_FOLDER = "./data"
report_files = sorted([f for f in os.listdir(DATA_FOLDER) if f.startswith("delta_report_") and f.endswith(".xlsx")])

if not report_files:
    st.error("❌ No delta report found. Please run the validator first.")
    st.stop()

latest_file = report_files[-1]
file_path = os.path.join(DATA_FOLDER, latest_file)
df = pd.read_excel(file_path)

# === Format Upload Date ===
if "Upload Date" in df.columns:
    df["Upload Date"] = pd.to_datetime(df["Upload Date"], errors='coerce')

st.success(f"✅ Showing Delta Report for {latest_file.replace('delta_report_', '').replace('.xlsx', '')}")

# === Fill Missing Columns ===
required_cols = [
    "Validation Status", "Vendor", "Amount", "Invoice No", "GSTIN",
    "Modification Reason", "Rate of Product", "SGST", "CGST", "IGST",
    "Upload Date", "Late Upload"
]
for col in required_cols:
    if col not in df.columns:
        df[col] = ""

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

# === Dashboard Metrics ===
total = len(df)
valid = (df["Validation Status"].str.upper() == "VALID").sum()
flagged = (df["Validation Status"].str.upper() == "FLAGGED").sum()
changed = (df["Validation Status"].str.upper() == "CHANGED").sum()
modified = (df["Validation Status"].str.upper() == "MODIFIED").sum()
deleted = (df["Validation Status"].str.upper() == "DELETED").sum()

col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("📦 Total", total)
col2.metric("✅ Valid", valid)
col3.metric("⚠️ Flagged", flagged)
col4.metric("🔁 Changed", changed)
col5.metric("✏️ Modified", modified)
col6.metric("❌ Deleted", deleted)

# === Chart ===
st.subheader("📊 Validation Status Breakdown")
chart_data = filtered_df["Validation Status"].value_counts()
fig, ax = plt.subplots(figsize=(6, 3))
chart_data.plot(kind="bar", ax=ax, color='#3498db', edgecolor='black')
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

# === Table ===
st.subheader("📑 Detailed Invoice Report")
st.dataframe(filtered_df, use_container_width=True)