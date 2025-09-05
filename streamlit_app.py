import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import re
import json
from datetime import datetime, timedelta
import io
import base64
# REMOVED: import emoji.fix.py  # This was causing ModuleNotFoundError
import unicodedata
import warnings
warnings.filterwarnings('ignore')

# Configure Streamlit page
st.set_page_config(
    page_title="Invoice Validation Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""

""", unsafe_allow_html=True)

# Emoji replacement function to handle Unicode issues
def clean_emoji_text(text):
    """
    Replace problematic Unicode characters and emojis with ASCII alternatives
    """
    if not isinstance(text, str):
        return text
    
    # Common emoji replacements
    emoji_replacements = {
        '✅': '[PASS]',
        '❌': '[FAIL]',
        '⚠️': '[WARNING]',
        '📊': '[CHART]',
        '💰': '[MONEY]',
        '📈': '[GROWTH]',
        '📉': '[DECLINE]',
        '🔍': '[SEARCH]',
        '📄': '[DOCUMENT]',
        '✨': '[STAR]',
        '🎯': '[TARGET]'
    }
    
    for emoji, replacement in emoji_replacements.items():
        text = text.replace(emoji, replacement)
    
    # Remove any remaining non-ASCII characters
    try:
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    except:
        pass
    
    return text

# File upload and processing functions
def load_excel_file(uploaded_file):
    """
    Load and process Excel files with multiple sheet support
    """
    try:
        # Try to read as Excel first
        excel_file = pd.ExcelFile(uploaded_file)
        sheets = excel_file.sheet_names
        
        if len(sheets) == 1:
            df = pd.read_excel(uploaded_file, sheet_name=0)
        else:
            # Let user choose sheet
            selected_sheet = st.selectbox("Select Excel Sheet:", sheets)
            df = pd.read_excel(uploaded_file, sheet_name=selected_sheet)
        
        # Clean text columns
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).apply(clean_emoji_text)
        
        return df, None
        
    except Exception as e:
        return None, f"Error loading Excel file: {str(e)}"

def process_validation_data(df):
    """
    Process and analyze validation data
    """
    try:
        # Standard column mappings for different report formats
        standard_columns = [
            'Invoice Number', 'Vendor Code', 'Vendor Name', 
            'Invoice Date', 'Invoice Amount', 'Status',
            'Validation Result', 'Error Details'
        ]
        
        enhanced_columns = [
            'Invoice Number', 'Vendor Code', 'Vendor Name',
            'Invoice Date', 'Invoice Amount', 'Currency',
            'Payment Terms', 'Due Date', 'Status',
            'Validation Result', 'Pass Rate', 'Error Count'
        ]
        
        # Detect report format
        if len(df.columns) >= 25:
            report_type = "Enhanced Report (31+ fields)"
        else:
            report_type = "Standard Report (18 fields)"
        
        # Process status column
        status_col = None
        for col in df.columns:
            if 'status' in col.lower() or 'result' in col.lower():
                status_col = col
                break
        
        # Calculate metrics
        total_invoices = len(df)
        
        if status_col:
            # Clean status values
            df[status_col] = df[status_col].astype(str).apply(clean_emoji_text)
            passed_invoices = len(df[df[status_col].str.contains('PASS', case=False, na=False)])
            failed_invoices = len(df[df[status_col].str.contains('FAIL', case=False, na=False)])
        else:
            passed_invoices = 0
            failed_invoices = 0
        
        pass_rate = (passed_invoices / total_invoices * 100) if total_invoices > 0 else 0
        
        # Calculate total amount
        amount_col = None
        for col in df.columns:
            if 'amount' in col.lower() or 'value' in col.lower():
                amount_col = col
                break
        
        total_amount = 0
        if amount_col:
            try:
                # Convert to numeric, handling various formats
                df[amount_col] = pd.to_numeric(df[amount_col].astype(str).str.replace('[^0-9.-]', '', regex=True), errors='coerce')
                total_amount = df[amount_col].sum()
            except:
                total_amount = 0
        
        metrics = {
            'report_type': report_type,
            'total_invoices': total_invoices,
            'passed_invoices': passed_invoices,
            'failed_invoices': failed_invoices,
            'pass_rate': pass_rate,
            'total_amount': total_amount,
            'status_column': status_col,
            'amount_column': amount_col
        }
        
        return metrics, None
        
    except Exception as e:
        return None, f"Error processing data: {str(e)}"

# Visualization functions
def create_status_chart(df, status_col):
    """
    Create status distribution chart
    """
    if status_col and status_col in df.columns:
        status_counts = df[status_col].value_counts()
        
        fig = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            title="Invoice Validation Status Distribution",
            color_discrete_map={
                '[PASS]': '#10b981',
                '[FAIL]': '#ef4444',
                'PASS': '#10b981',
                'FAIL': '#ef4444'
            }
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        return fig
    return None

def create_amount_chart(df, amount_col, status_col):
    """
    Create amount analysis chart
    """
    if amount_col and amount_col in df.columns:
        if status_col and status_col in df.columns:
            # Amount by status
            amount_by_status = df.groupby(status_col)[amount_col].sum().reset_index()
            
            fig = px.bar(
                amount_by_status,
                x=status_col,
                y=amount_col,
                title="Total Amount by Validation Status",
                color=status_col,
                color_discrete_map={
                    '[PASS]': '#10b981',
                    '[FAIL]': '#ef4444',
                    'PASS': '#10b981',
                    'FAIL': '#ef4444'
                }
            )
            fig.update_layout(showlegend=False)
            return fig
        else:
            # Amount distribution histogram
            fig = px.histogram(
                df,
                x=amount_col,
                title="Invoice Amount Distribution",
                nbins=20
            )
            return fig
    return None

# Main dashboard function
def main():
    # Header
    st.markdown("""
    

        

            📊 Invoice Validation Dashboard
        

        

            Analyze validation results and monitor invoice processing performance
        


    

    """, unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.markdown("### 📋 File Upload")
        uploaded_file = st.file_uploader(
            "Upload validation results",
            type=['xlsx', 'xls', 'csv'],
            help="Upload your invoice validation Excel report"
        )
        
        st.markdown("""
        

            
📁 Supported Formats:

            

                
Enhanced Report (31+ fields)

                
Standard Report (18 fields)

                
CSV exports

            

            
🔧 Features:

            

                
Automatic emoji handling

                
Multi-sheet Excel support

                
Real-time metrics

                
Interactive visualizations

            

        

        """, unsafe_allow_html=True)
    
    if uploaded_file is not None:
        # Load and process file
        with st.spinner("Loading and processing file..."):
            df, load_error = load_excel_file(uploaded_file)
            
            if load_error:
                st.error(load_error)
                return
            
            metrics, process_error = process_validation_data(df)
            
            if process_error:
                st.error(process_error)
                return
        
        # Display metrics
        st.markdown("## 📈 Key Metrics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Invoices",
                f"{metrics['total_invoices']:,}",
                help=f"Report Type: {metrics['report_type']}"
            )
        
        with col2:
            st.metric(
                "Pass Rate",
                f"{metrics['pass_rate']:.1f}%",
                delta=f"{metrics['passed_invoices']} passed"
            )
        
        with col3:
            st.metric(
                "Failed Invoices",
                f"{metrics['failed_invoices']:,}",
                delta=f"{metrics['failed_invoices']} failed",
                delta_color="inverse"
            )
        
        with col4:
            if metrics['total_amount'] > 0:
                st.metric(
                    "Total Amount",
                    f"₹{metrics['total_amount']:,.0f}"
                )
            else:
                st.metric("Total Amount", "N/A")
        
        # Visualizations
        st.markdown("## 📊 Data Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            status_chart = create_status_chart(df, metrics['status_column'])
            if status_chart:
                st.plotly_chart(status_chart, use_container_width=True)
            else:
                st.info("Status chart not available - no status column detected")
        
        with col2:
            amount_chart = create_amount_chart(df, metrics['amount_column'], metrics['status_column'])
            if amount_chart:
                st.plotly_chart(amount_chart, use_container_width=True)
            else:
                st.info("Amount chart not available - no amount column detected")
        
        # Data table
        st.markdown("## 📋 Data Preview")
        
        # Filter options
        col1, col2 = st.columns(2)
        
        with col1:
            if metrics['status_column']:
                status_filter = st.multiselect(
                    "Filter by Status",
                    options=df[metrics['status_column']].unique(),
                    default=df[metrics['status_column']].unique()
                )
                df_filtered = df[df[metrics['status_column']].isin(status_filter)]
            else:
                df_filtered = df
        
        with col2:
            max_rows = st.slider("Rows to display", 10, 1000, 100)
        
        # Display filtered data
        st.dataframe(
            df_filtered.head(max_rows),
            use_container_width=True,
            height=400
        )
        
        # Download section
        st.markdown("## 💾 Export Data")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # CSV download
            csv = df_filtered.to_csv(index=False)
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name="validation_results_filtered.csv",
                mime="text/csv"
            )
        
        with col2:
            # Excel download
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df_filtered.to_excel(writer, sheet_name='Validation Results', index=False)
            
            st.download_button(
                label="📥 Download as Excel",
                data=buffer.getvalue(),
                file_name="validation_results_filtered.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        # Summary statistics
        st.markdown("## 📊 Summary Statistics")
        
        if metrics['amount_column']:
            amount_stats = df[metrics['amount_column']].describe()
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Average Amount", f"₹{amount_stats['mean']:,.0f}")
            
            with col2:
                st.metric("Median Amount", f"₹{amount_stats['50%']:,.0f}")
            
            with col3:
                st.metric("Max Amount", f"₹{amount_stats['max']:,.0f}")
    
    else:
        # Welcome message
        st.markdown("""
        ## 👋 Welcome to the Invoice Validation Dashboard
        
        Upload your validation results file to get started with the analysis.
        
        ### 🚀 Features:
        - **Multi-format support**: Excel (xlsx, xls) and CSV files
        - **Automatic emoji handling**: Converts Unicode characters to ASCII
        - **Interactive visualizations**: Charts and graphs for data analysis
        - **Real-time metrics**: Key performance indicators
        - **Data filtering**: Filter and export results
        - **Enhanced reporting**: Support for both standard and enhanced report formats
        
        ### 📁 File Requirements:
        - Excel files with invoice validation data
        - Columns should include invoice details, amounts, and validation status
        - Files up to 200MB are supported
        
        **Note**: This dashboard automatically handles emoji and Unicode character issues that were causing errors in the previous version.
        """)

# Error handling for the main execution
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"An unexpected error occurred: {str(e)}")
        st.markdown("""
        ### 🔧 Troubleshooting:
        1. Ensure your file is a valid Excel or CSV format
        2. Check that the file contains the expected columns
        3. Verify the file is not corrupted
        4. Try refreshing the page and re-uploading
        
        If the problem persists, please check the file format and content.
        """)
            
