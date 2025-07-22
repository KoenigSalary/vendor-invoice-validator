# streamlit_app.py (Minimal version - no extra dependencies)

import streamlit as st
import pandas as pd
import os
import matplotlib.pyplot as plt
from io import BytesIO
from datetime import datetime, timedelta
from PIL import Image
import base64
import json

#Page Config
st.set_page_config(
    page_title="Vendor Invoice Validation Dashboard", 
    layout="wide",
    initial_sidebar_state="expanded"
)

#Colors
PRIMARY_COLOR = "#003366"
ACCENT_COLOR = "#0077CC"
SUCCESS_COLOR = "#28a745"
WARNING_COLOR = "#ffc107"
ERROR_COLOR = "#FF4B4B"
INFO_COLOR = "#F5F7FA"

# Set matplotlib style
plt.style.use('default')

Custom CSS
st.markdown(f"""
<style>
    .main-header {{
        background: linear-gradient(90deg, {PRIMARY_COLOR} 0%, {ACCENT_COLOR} 100%);
        padding: 2rem;
        border-radius: 15px;
        margin-bottom: 2rem;
        text-align: center;
        color: white;
    }}
    .metric-card {{
        background-color: {INFO_COLOR};
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid {ACCENT_COLOR};
        margin: 0.5rem 0;
    }}
    .sidebar-info {{
        background-color: {INFO_COLOR};
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1rem;
    }}
</style>
""", unsafe_allow_html=True)

#Functions
@st.cache_data
def load_logo():
    """Load and encode logo image"""
    logo_path = "assets/koenig_logo.png"
    if os.path.exists(logo_path):
        try:
            buffer = BytesIO()
            Image.open(logo_path).save(buffer, format="PNG")
            encoded = base64.b64encode(buffer.getvalue()).decode()
            return f"<img src='data:image/png;base64,{encoded}' width='160' style='margin-bottom: 10px;'/>"
        except Exception as e:
            return f"<p style='color: {ERROR_COLOR};'>Logo error: {str(e)}</p>"
    else:
        return f"<div style='width: 160px; height: 80px; background-color: {INFO_COLOR}; display: flex; align-items: center; justify-content: center; border-radius: 8px; margin-bottom: 10px; color: {PRIMARY_COLOR}; font-weight: bold; font-size: 18px;'>KOENIG</div>"

@st.cache_data
def load_latest_reports():
    """Load and return available reports"""
    DATA_FOLDER = "./data"
    os.makedirs(DATA_FOLDER, exist_ok=True)
    
    delta_reports = []
    
    try:
        # Look for files in main data folder
        for file in os.listdir(DATA_FOLDER):
            if file.startswith("delta_report_") and (file.endswith(".xlsx") or file.endswith(".xls")):
                delta_reports.append(file)
        
        # Also look in dated subfolders
        subfolders = [f for f in os.listdir(DATA_FOLDER) if f.startswith("2025-")]
        for subfolder in sorted(subfolders, reverse=True):
            subfolder_path = os.path.join(DATA_FOLDER, subfolder)
            if os.path.isdir(subfolder_path):
                for file in os.listdir(subfolder_path):
                    if file == "validation_result.xlsx" or file.startswith("delta_report_"):
                        delta_reports.append(os.path.join(subfolder, file))
    except Exception as e:
        st.error(f"Error scanning for reports: {e}")
    
    return sorted(set(delta_reports), reverse=True)

def standardize_dataframe(df):
    """Standardize dataframe column names and add missing columns"""
    
    # Create column mapping for different possible column names
    column_mapping = {
        # Standard mappings from your actual data
        'PurchaseInvNo': 'Invoice_No',
        'PartyName': 'Vendor',
        'Total': 'Amount',
        'GSTNO': 'GSTIN',
        'PurchaseInvDate': 'Invoice_Date',
        'VoucherNo': 'Voucher_No',
        'Voucherdate': 'Voucher_Date',
        'TaxableValue': 'Taxable_Value',
        'CGSTInputAmt': 'CGST',
        'SGSTInputAmt': 'SGST',
        'IGST/VATInputAmt': 'IGST',
        'Narration': 'Description',
        
        # Validation status mappings
        'Issue_Description': 'Issues',
        'Status': 'Validation_Status',
        'Validation_Status': 'Status',
        'Issues_Found': 'Issues',
        'Correct': 'Validation_Flag',
        'Flagged': 'Flag_Status'
    }
    
    # Apply column mapping
    df_standardized = df.copy()
    for old_col, new_col in column_mapping.items():
        if old_col in df_standardized.columns:
            df_standardized = df_standardized.rename(columns={old_col: new_col})
    
    # Required columns for dashboard
    required_columns = [
        'Invoice_No', 'Vendor', 'Amount', 'GSTIN', 'Invoice_Date',
        'Status', 'Issues', 'CGST', 'SGST', 'IGST', 'Description',
        'Voucher_No', 'Taxable_Value', 'Flag_Status'
    ]
    
    # Add missing columns
    for col in required_columns:
        if col not in df_standardized.columns:
            if col == 'Status':
                # Derive status from other columns
                if 'Validation_Flag' in df_standardized.columns:
                    df_standardized[col] = df_standardized['Validation_Flag'].apply(
                        lambda x: 'Valid' if str(x) == '✅' else 'Flagged' if str(x) == '❌' else 'Unknown'
                    )
                elif 'Issue_ID' in df_standardized.columns:
                    # This is likely an issues report
                    df_standardized[col] = 'Flagged'
                else:
                    df_standardized[col] = 'Unknown'
            elif col in ['CGST', 'SGST', 'IGST', 'Amount', 'Taxable_Value']:
                df_standardized[col] = 0
            else:
                df_standardized[col] = ''
    
    # Convert numeric columns
    numeric_columns = ['Amount', 'CGST', 'SGST', 'IGST', 'Taxable_Value']
    for col in numeric_columns:
        if col in df_standardized.columns:
            df_standardized[col] = pd.to_numeric(df_standardized[col], errors='coerce').fillna(0)
    
    # Convert date columns
    date_columns = ['Invoice_Date', 'Date_Found']
    for col in date_columns:
        if col in df_standardized.columns:
            df_standardized[col] = pd.to_datetime(df_standardized[col], errors='coerce')
    
    return df_standardized

def create_summary_metrics(df):
    """Create summary metrics from dataframe"""
    total = len(df)
    
    # Status-based metrics
    status_counts = df['Status'].value_counts()
    valid = status_counts.get('Valid', 0)
    flagged = status_counts.get('Flagged', 0) + status_counts.get('New', 0)
    unknown = status_counts.get('Unknown', 0)
    
    # Amount-based metrics
    total_amount = df['Amount'].sum()
    avg_amount = df['Amount'].mean() if total > 0 else 0
    
    return {
        'total': total,
        'valid': valid,
        'flagged': flagged,
        'unknown': unknown,
        'total_amount': total_amount,
        'avg_amount': avg_amount
    }

#Header
logo_html = load_logo()
st.markdown(
    f"""
    <div class='main-header'>
        {logo_html}
        <h1 style='margin: 0; font-size: 28px;'>📋 Vendor Invoice Validation Dashboard</h1>
        <p style='margin: 0.5rem 0 0 0; opacity: 0.9;'>Real-time invoice validation and monitoring</p>
    </div>
    """,
    unsafe_allow_html=True,
)

#Load Latest Report
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

#Fill Required Columns if Missing
required_cols = [
    "Validation Status", "Vendor", "Amount", "Invoice No", "GSTIN",
    "Modification Reason", "Rate of Product", "SGST", "CGST", "IGST",
    "Upload Date", "Late Upload"
]
for col in required_cols:
    if col not in df.columns:
        df[col] = ""

st.success(f"✅ Showing Delta Report for {latest_file.replace('delta_report_', '').replace('.xlsx', '')}")

#Sidebar
with st.sidebar:
    st.markdown("### 🔧 Dashboard Controls")
    
    # Report selection
    report_files = load_latest_reports()
    
    if not report_files:
        st.error("⚠️ No validation reports found!")
        st.info("Please run the validation process first to generate reports.")
        st.markdown("**Expected locations:**")
        st.markdown("- `./data/delta_report_YYYY-MM-DD.xlsx`")
        st.markdown("- `./data/YYYY-MM-DD/validation_result.xlsx`")
        
        # Create sample data for demo
        if st.button("Create Sample Data for Demo"):
            sample_df = pd.DataFrame({
                'Issue_ID': [1, 2, 3],
                'Issue_Description': ['Missing GSTIN', 'Invalid amount format', 'Duplicate invoice number'],
                'Date_Found': ['2025-07-21', '2025-07-21', '2025-07-21'],
                'Status': ['New', 'New', 'New']
            })
            os.makedirs('./data', exist_ok=True)
            sample_df.to_excel('./data/delta_report_2025-07-21.xlsx', index=False)
            st.success("Sample data created! Please refresh the page.")
            st.rerun()
        
        st.stop()
    
    st.markdown("#### 📊 Select Report")
    selected_report = st.selectbox(
        "Available Reports:",
        report_files,
        format_func=lambda x: f"📄 {os.path.basename(x)}"
    )
    
    # Manual refresh button
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    # Data info
    st.markdown("#### ℹ️ Data Information")
    data_info_container = st.container()

#Load Selected Report
try:
    if selected_report.startswith("data/") or "/" in selected_report:
        file_path = selected_report if selected_report.startswith("./") else f"./{selected_report}"
    else:
        file_path = os.path.join("./data", selected_report)
    
    if not os.path.exists(file_path):
        st.error(f"File not found: {file_path}")
        st.stop()
    
    # Try to read the Excel file
    try:
        df_raw = pd.read_excel(file_path, engine='openpyxl')
    except:
        # Fallback to xlrd for older .xls files
        try:
            df_raw = pd.read_excel(file_path, engine='xlrd')
        except:
            st.error("Could not read the Excel file. Please ensure it's a valid Excel format.")
            st.stop()
    
    df = standardize_dataframe(df_raw)
    
    # Update data info in sidebar
    with data_info_container:
        file_size = os.path.getsize(file_path) / 1024
        st.markdown(f"""
        <div class='sidebar-info'>
            <strong>Report:</strong> {os.path.basename(selected_report)}<br>
            <strong>Records:</strong> {len(df)}<br>
            <strong>Columns:</strong> {len(df.columns)}<br>
            <strong>File Size:</strong> {file_size:.1f} KB
        </div>
        """, unsafe_allow_html=True)
    
    st.success(f"✅ Loaded: {os.path.basename(selected_report)} ({len(df)} records)")
    
except Exception as e:
    st.error(f"❌ Error loading report: {str(e)}")
    st.code(f"File path: {file_path}")
    st.stop()

#Filters
with st.expander("🔎 Advanced Filters", expanded=True):
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        vendor_options = ["All"] + sorted([str(v) for v in df["Vendor"].dropna().unique() if str(v).strip() and str(v) != 'nan'])
        vendor_filter = st.selectbox("🏢 Vendor", vendor_options)
    
    with col2:
        status_options = sorted([str(s) for s in df["Status"].dropna().unique() if str(s).strip() and str(s) != 'nan'])
        status_filter = st.multiselect("📊 Status", status_options, default=status_options)
    
    with col3:
        if df["Amount"].max() > df["Amount"].min():
            amount_range = st.slider(
                "💰 Amount Range",
                min_value=float(df["Amount"].min()),
                max_value=float(df["Amount"].max()),
                value=(float(df["Amount"].min()), float(df["Amount"].max())),
                format="₹%.0f"
            )
        else:
            amount_range = (df["Amount"].min(), df["Amount"].max())
            st.write("💰 Amount: All records")
    
    with col4:
        search_term = st.text_input("🔍 Search Invoice/GSTIN/Vendor")

# Apply filters
filtered_df = df.copy()

if vendor_filter != "All":
    filtered_df = filtered_df[filtered_df["Vendor"].astype(str) == vendor_filter]

if status_filter:
    filtered_df = filtered_df[filtered_df["Status"].isin(status_filter)]

filtered_df = filtered_df[
    (filtered_df["Amount"] >= amount_range[0]) & 
    (filtered_df["Amount"] <= amount_range[1])
]

if search_term:
    mask = (
        filtered_df["Invoice_No"].astype(str).str.contains(search_term, case=False, na=False) |
        filtered_df["GSTIN"].astype(str).str.contains(search_term, case=False, na=False) |
        filtered_df["Vendor"].astype(str).str.contains(search_term, case=False, na=False) |
        filtered_df["Issues"].astype(str).str.contains(search_term, case=False, na=False)
    )
    filtered_df = filtered_df[mask]

#Summary Metrics
metrics = create_summary_metrics(filtered_df)

st.markdown("### 📊 Summary Overview")
col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    st.metric(
        label="📦 Total Records",
        value=metrics['total'],
        delta=f"{metrics['total'] - len(df)} filtered" if metrics['total'] != len(df) else None
    )

with col2:
    st.metric(
        label="✅ Valid",
        value=metrics['valid'],
        delta=f"{(metrics['valid']/metrics['total']*100):.1f}%" if metrics['total'] > 0 else "0%"
    )

with col3:
    st.metric(
        label="⚠️ Issues",
        value=metrics['flagged'],
        delta=f"{(metrics['flagged']/metrics['total']*100):.1f}%" if metrics['total'] > 0 else "0%"
    )

with col4:
    st.metric(
        label="💰 Total Amount",
        value=f"₹{metrics['total_amount']:,.0f}",
        delta=f"Avg: ₹{metrics['avg_amount']:,.0f}"
    )

with col5:
    vendors_count = filtered_df["Vendor"].nunique()
    st.metric(
        label="🏢 Unique Vendors",
        value=vendors_count
    )

with col6:
    success_rate = (metrics['valid'] / metrics['total'] * 100) if metrics['total'] > 0 else 0
    st.metric(
        label="📈 Success Rate",
        value=f"{success_rate:.1f}%"
    )

#Simple Charts
st.markdown("### 📈 Quick Analytics")

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.markdown("#### Validation Status Distribution")
    status_counts = filtered_df['Status'].value_counts()
    
    if not status_counts.empty and len(status_counts) > 0:
        fig, ax = plt.subplots(figsize=(8, 6))
        colors = ['#28a745', '#FF4B4B', '#ffc107', '#17a2b8']  # Green, Red, Yellow, Blue
        wedges, texts, autotexts = ax.pie(
            status_counts.values, 
            labels=status_counts.index,
            autopct='%1.1f%%',
            colors=colors[:len(status_counts)],
            startangle=90
        )
        ax.set_title('Validation Status Distribution')
        st.pyplot(fig)
    else:
        st.info("No status data to display")

with chart_col2:
    st.markdown("#### Top 10 Vendors by Amount")
    if len(filtered_df) > 0 and filtered_df["Amount"].sum() > 0:
        vendor_amounts = filtered_df.groupby('Vendor')['Amount'].sum().sort_values(ascending=True).tail(10)
        
        if not vendor_amounts.empty:
            fig, ax = plt.subplots(figsize=(8, 6))
            bars = ax.barh(range(len(vendor_amounts)), vendor_amounts.values, color=ACCENT_COLOR)
            ax.set_yticks(range(len(vendor_amounts)))
            ax.set_yticklabels([str(v)[:20] + '...' if len(str(v)) > 20 else str(v) for v in vendor_amounts.index])
            ax.set_xlabel('Total Amount (₹)')
            ax.set_title('Top 10 Vendors by Invoice Amount')
            
            # Add value labels on bars
            for i, bar in enumerate(bars):
                width = bar.get_width()
                ax.text(width + width*0.01, bar.get_y() + bar.get_height()/2, 
                       f'₹{width:,.0f}', ha='left', va='center', fontsize=8)
            
            plt.tight_layout()
            st.pyplot(fig)
        else:
            st.info("No vendor amount data to display")
    else:
        st.info("No amount data available")

#Data Tables
st.markdown("### 📋 Detailed Views")

tab1, tab2, tab3 = st.tabs(["📑 All Records", "⚠️ Issues Found", "📊 Summary"])

with tab1:
    st.markdown("#### All Validation Records")
    
    # Smart column selection
    available_columns = [col for col in df.columns if not df[col].isna().all()]
    
    # Priority columns that should be shown if available
    priority_columns = ['Invoice_No', 'Vendor', 'Amount', 'Status', 'Issues', 'GSTIN']
    default_columns = [col for col in priority_columns if col in available_columns][:6]
    
    display_columns = st.multiselect(
        "Select columns to display:",
        options=available_columns,
        default=default_columns
    )
    
    if display_columns:
        display_df = filtered_df[display_columns].copy()
        
        # Format amount column
        if 'Amount' in display_df.columns:
            display_df['Amount'] = display_df['Amount'].apply(
                lambda x: f"₹{x:,.0f}" if pd.notna(x) and x != 0 else ""
            )
        
        st.dataframe(display_df, use_container_width=True, height=400)
        st.markdown(f"**Showing {len(display_df)} of {len(df)} total records**")
    else:
        st.warning("Please select at least one column to display.")

with tab2:
    st.markdown("#### Issues & Problems Found")
    
    # Find records with issues
    issues_df = filtered_df[
        (filtered_df["Status"].isin(["Flagged", "New"])) |
        (~filtered_df["Issues"].astype(str).isin(['', 'nan', 'None']))
    ].copy()
    
    if not issues_df.empty:
        st.write(f"Found **{len(issues_df)}** records with issues:")
        
        # Show key columns for issues
        issue_columns = ['Invoice_No', 'Vendor', 'Amount', 'Status', 'Issues']
        available_issue_columns = [col for col in issue_columns if col in issues_df.columns]
        
        if available_issue_columns:
            display_issues = issues_df[available_issue_columns].copy()
            if 'Amount' in display_issues.columns:
                display_issues['Amount'] = display_issues['Amount'].apply(
                    lambda x: f"₹{x:,.0f}" if pd.notna(x) and x != 0 else ""
                )
            st.dataframe(display_issues, use_container_width=True, height=400)
        else:
            st.dataframe(issues_df, use_container_width=True, height=400)
        
        # Issues summary
        if 'Issues' in issues_df.columns:
            issue_text = issues_df['Issues'].astype(str)
            non_empty_issues = issue_text[
                ~issue_text.isin(['', 'nan', 'None']) & 
                (issue_text.str.len() > 1)
            ]
            
            if not non_empty_issues.empty:
                st.markdown("**Most Common Issues:**")
                issue_counts = non_empty_issues.value_counts().head(5)
                for issue, count in issue_counts.items():
                    st.markdown(f"- **{count}x**: {issue}")
    else:
        st.success("🎉 No issues found in the current selection!")

with tab3:
    st.markdown("#### Summary Statistics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Data Overview:**")
        summary_stats = {
            "Total Records": len(filtered_df),
            "Unique Vendors": filtered_df["Vendor"].nunique(),
            "Unique Invoices": filtered_df["Invoice_No"].nunique(),
        }
        
        for key, value in summary_stats.items():
            st.markdown(f"- **{key}**: {value}")
    
    with col2:
        st.markdown("**Amount Statistics:**")
        if filtered_df["Amount"].sum() > 0:
            amount_stats = {
                "Total Amount": f"₹{filtered_df['Amount'].sum():,.0f}",
                "Average Amount": f"₹{filtered_df['Amount'].mean():,.0f}",
                "Median Amount": f"₹{filtered_df['Amount'].median():,.0f}",
                "Max Amount": f"₹{filtered_df['Amount'].max():,.0f}"
            }
            
            for key, value in amount_stats.items():
                st.markdown(f"- **{key}**: {value}")
        else:
            st.info("No amount data available")

#Export Section
st.markdown("### 📥 Export Data")

col1, col2 = st.columns(2)

with col1:
    # Excel export
    output_excel = BytesIO()
    try:
        filtered_df.to_excel(output_excel, index=False, engine='openpyxl')
        output_excel.seek(0)
        
        st.download_button(
            label="📊 Download Excel Report",
            data=output_excel,
            file_name=f"invoice_validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        st.error(f"Excel export error: {e}")

with col2:
    # CSV export
    try:
        csv_data = filtered_df.to_csv(index=False)
        st.download_button(
            label="📄 Download CSV Data",
            data=csv_data,
            file_name=f"invoice_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    except Exception as e:
        st.error(f"CSV export error: {e}")

#Footer
st.markdown("---")
st.markdown(
    f"""
    <div style='text-align: center; color: grey; padding: 1rem;'>
        <p>© 2025 Koenig Solutions | Vendor Invoice Validation Dashboard</p>
        <p>Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 
        Report: {os.path.basename(selected_report)} | 
        Records: {len(filtered_df)}/{len(df)}</p>
    </div>
    """,
    unsafe_allow_html=True
)
