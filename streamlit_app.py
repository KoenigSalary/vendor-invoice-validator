# streamlit_dashboard.py
import streamlit as st
import pandas as pd
import os
import matplotlib.pyplot as plt
from io import BytesIO
from PIL import Image

# Set page configuration
st.set_page_config(page_title="Invoice Compliance Monitoring System", layout="wide")

# Load and display logo on the top-left
logo = Image.open("assets/koenig_logo.png")
st.image(logo, width=275)  # Adjust width as needed

# Dashboard title
st.title("📋 Invoice Compliance Monitoring System")

DATA_FOLDER = "./data"
report_files = sorted([f for f in os.listdir(DATA_FOLDER) if f.startswith("delta_report_") and f.endswith(".xlsx")])

if not report_files:
    st.error("Invoice report not found. Please run the validator first.")
else:
    latest_file = report_files[-1]
    file_path = os.path.join(DATA_FOLDER, latest_file)
    df = pd.read_excel(file_path)

    st.success(f"✅ Showing Delta Report for {latest_file.replace('delta_report_', '').replace('.xlsx', '')}")

    # Fill missing columns if needed
    for col in [
        "Validation Status", "Vendor", "Amount", "Invoice No", "GSTIN",
        "Modification Reason", "Rate of Product", "SGST", "CGST", "IGST",
        "Upload Date", "Late Upload"
    ]:
        if col not in df.columns:
            df[col] = ""

    # Filters
    with st.expander("🔎 Filters"):
        vendor_filter = st.selectbox("Filter by Vendor", options=["All"] + sorted(df["Vendor"].dropna().unique().tolist()))
        status_filter = st.multiselect("Filter by Status", options=sorted(df["Validation Status"].dropna().unique().tolist()), default=sorted(df["Validation Status"].dropna().unique().tolist()))

    if vendor_filter != "All":
        df = df[df["Vendor"] == vendor_filter]
    if status_filter:
        df = df[df["Validation Status"].isin(status_filter)]

    # Dashboard metrics
    total = len(df)
    valid = df[df["Validation Status"].str.upper() == "VALID"].shape[0]
    flagged = df[df["Validation Status"].str.upper() == "FLAGGED"].shape[0]
    changed = df[df["Validation Status"].str.upper() == "CHANGED"].shape[0]
    modified = df[df["Validation Status"].str.upper() == "MODIFIED"].shape[0]
    deleted = df[df["Validation Status"].str.upper() == "DELETED"].shape[0]

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("📦 Total", total)
    col2.metric("✅ Valid", valid)
    col3.metric("⚠️ Flagged", flagged)
    col4.metric("🔁 Changed", changed)
    col5.metric("✏️ Modified", modified)
    col6.metric("❌ Deleted", deleted)

    # Chart
    st.subheader("📊 Validation Status Breakdown")
    chart_data = df["Validation Status"].value_counts()
    fig, ax = plt.subplots(figsize=(6, 3))
    chart_data.plot(kind="bar", ax=ax, color='skyblue', edgecolor='black')
    ax.set_title("Validation Status", fontsize=12)
    ax.set_xlabel("Status")
    ax.set_ylabel("Count")
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    st.pyplot(fig)

    # Download filtered data
    output = BytesIO()
    df.to_excel(output, index=False, engine='openpyxl')
    output.seek(0)
    st.download_button(
        label="📥 Download Filtered Report",
        data=output,
        file_name="filtered_invoice_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # Table
    st.subheader("📑 Detailed Invoice Report")
    st.dataframe(df, use_container_width=True)
