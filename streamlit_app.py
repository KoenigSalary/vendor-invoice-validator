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

    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #667eea;
    }
    .info-box {
        background: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .success-text { color: #28a745; font-weight: bold; }
    .danger-text { color: #dc3545; font-weight: bold; }
    .warning-text { color: #ffc107; font-weight: bold; }

""", unsafe_allow_html=True)

# Emoji replacement function to handle Unicode issues
def clean_emoji_text(text):
    """
    Replace problematic Unicode characters and emojis with ASCII alternatives
    """
    if not isinstance(text, str):
        return str(text) if text is not None else ""
    
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
        '🎯': '[TARGET]',
        '🚀': '[ROCKET]',
        '💡': '[IDEA]',
        '🔧': '[TOOL]',
        '📋': '[CLIPBOARD]',
        '💾': '[SAVE]',
        '📥': '[DOWNLOAD]',
        '📤': '[UPLOAD]'
    }
    
    # Apply emoji replacements
    for emoji, replacement in emoji_replacements.items():
        text = text.replace(emoji, replacement)
    
    # Remove any remaining non-ASCII characters safely
    try:
        # First try to normalize Unicode
        text = unicodedata.normalize('NFKD', text)
        # Then encode to ASCII, ignoring errors
        text = text.encode('ascii', 'ignore').decode('ascii')
    except Exception as e:
        # If normalization fails, just remove non-ASCII chars
        text = ''.join(char for char in text if ord(char) < 128)
    
    return text

# Enhanced file loading with better error handling
def load_excel_file(uploaded_file):
    """
    Load and process Excel files with comprehensive error handling
    """
    try:
        # Reset file pointer
        uploaded_file.seek(0)
        
        # Try to read as Excel first
        excel_file = pd.ExcelFile(uploaded_file)
        sheets = excel_file.sheet_names
        
        st.info(f"Detected {len(sheets)} sheet(s): {', '.join(sheets)}")
        
        if len(sheets) == 1:
            df = pd.read_excel(uploaded_file, sheet_name=0)
            st.success(f"Loaded sheet: {sheets[0]}")
        else:
            # Let user choose sheet
            selected_sheet = st.selectbox(
                "Select Excel Sheet:", 
                sheets,
                help="Choose which sheet contains your validation data"
            )
            df = pd.read_excel(uploaded_file, sheet_name=selected_sheet)
            st.success(f"Loaded sheet: {selected_sheet}")
        
        # Validate dataframe
        if df.empty:
            return None, "The selected sheet is empty"
        
        # Clean text columns with enhanced error handling
        text_columns = df.select_dtypes(include=['object']).columns
        for col in text_columns:
            try:
                df[col] = df[col].astype(str).apply(clean_emoji_text)
            except Exception as e:
                st.warning(f"Could not clean column '{col}': {str(e)}")
                # Continue with original data for this column
        
        st.success(f"Successfully loaded {len(df)} rows and {len(df.columns)} columns")
        return df, None
        
    except Exception as e:
        error_msg = f"Error loading Excel file: {str(e)}"
        
        # Try CSV as fallback
        try:
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file)
            
            # Clean text columns
            text_columns = df.select_dtypes(include=['object']).columns
            for col in text_columns:
                df[col] = df[col].astype(str).apply(clean_emoji_text)
            
            st.warning("Excel loading failed, successfully loaded as CSV")
            return df, None
            
        except Exception as csv_error:
            return None, f"{error_msg}. CSV fallback also failed: {str(csv_error)}"

def detect_column_mappings(df):
    """
    Intelligently detect column mappings from dataframe
    """
    column_mappings = {
        'status': None,
        'amount': None,
        'invoice_number': None,
        'vendor': None,
        'date': None,
        'error_details': None
    }
    
    columns_lower = [col.lower() for col in df.columns]
    
    # Status column detection
    status_keywords = ['status', 'result', 'validation', 'pass', 'fail']
    for i, col_lower in enumerate(columns_lower):
        if any(keyword in col_lower for keyword in status_keywords):
            column_mappings['status'] = df.columns[i]
            break
    
    # Amount column detection
    amount_keywords = ['amount', 'value', 'total', 'sum', 'price']
    for i, col_lower in enumerate(columns_lower):
        if any(keyword in col_lower for keyword in amount_keywords):
            column_mappings['amount'] = df.columns[i]
            break
    
    # Invoice number detection
    invoice_keywords = ['invoice', 'inv', 'number', 'id']
    for i, col_lower in enumerate(columns_lower):
        if any(all(kw in col_lower for kw in combo) for combo in [['invoice', 'number'], ['inv', 'no']]) or col_lower == 'invoice_number':
            column_mappings['invoice_number'] = df.columns[i]
            break
    
    # Vendor detection
    vendor_keywords = ['vendor', 'supplier', 'company']
    for i, col_lower in enumerate(columns_lower):
        if any(keyword in col_lower for keyword in vendor_keywords):
            column_mappings['vendor'] = df.columns[i]
            break
    
    # Date detection
    date_keywords = ['date', 'created', 'timestamp']
    for i, col_lower in enumerate(columns_lower):
        if any(keyword in col_lower for keyword in date_keywords):
            column_mappings['date'] = df.columns[i]
            break
    
    # Error details detection
    error_keywords = ['error', 'details', 'issues', 'problems']
    for i, col_lower in enumerate(columns_lower):
        if any(keyword in col_lower for keyword in error_keywords):
            column_mappings['error_details'] = df.columns[i]
            break
    
    return column_mappings

def process_validation_data(df):
    """
    Process and analyze validation data with enhanced error handling
    """
    try:
        # Detect column mappings
        mappings = detect_column_mappings(df)
        
        # Report type detection
        column_count = len(df.columns)
        if column_count >= 25:
            report_type = f"Enhanced Report ({column_count} fields)"
        elif column_count >= 15:
            report_type = f"Standard Report ({column_count} fields)"
        else:
            report_type = f"Basic Report ({column_count} fields)"
        
        # Calculate basic metrics
        total_invoices = len(df)
        
        # Process status column
        passed_invoices = 0
        failed_invoices = 0
        warning_invoices = 0
        
        if mappings['status']:
            status_col = mappings['status']
            df[status_col] = df[status_col].astype(str).apply(clean_emoji_text)
            
            # Count different statuses
            status_values = df[status_col].str.upper()
            passed_invoices = len(df[status_values.str.contains('PASS', case=False, na=False)])
            failed_invoices = len(df[status_values.str.contains('FAIL', case=False, na=False)])
            warning_invoices = len(df[status_values.str.contains('WARN', case=False, na=False)])
            
            # Adjust counts to ensure they sum correctly
            accounted = passed_invoices + failed_invoices + warning_invoices
            if accounted < total_invoices:
                # Assume remaining are failed if not explicitly categorized
                failed_invoices += (total_invoices - accounted)
        else:
            # No status column found - estimate based on other indicators
            st.warning("No status column detected - metrics may be incomplete")
        
        # Calculate pass rate
        pass_rate = (passed_invoices / total_invoices * 100) if total_invoices > 0 else 0
        
        # Process amount column
        total_amount = 0
        passed_amount = 0
        failed_amount = 0
        
        if mappings['amount']:
            amount_col = mappings['amount']
            try:
                # Clean and convert amount column
                df[amount_col] = pd.to_numeric(
                    df[amount_col].astype(str).str.replace(r'[^\d.-]', '', regex=True), 
                    errors='coerce'
                )
                df[amount_col] = df[amount_col].fillna(0)
                
                total_amount = df[amount_col].sum()
                
                # Calculate amounts by status if possible
                if mappings['status']:
                    status_col = mappings['status']
                    status_values = df[status_col].str.upper()
                    
                    passed_mask = status_values.str.contains('PASS', case=False, na=False)
                    failed_mask = status_values.str.contains('FAIL', case=False, na=False)
                    
                    passed_amount = df[passed_mask][amount_col].sum()
                    failed_amount = df[failed_mask][amount_col].sum()
                
            except Exception as e:
                st.warning(f"Could not process amount data: {str(e)}")
                total_amount = 0
        
        # Compile metrics
        metrics = {
            'report_type': report_type,
            'total_invoices': total_invoices,
            'passed_invoices': passed_invoices,
            'failed_invoices': failed_invoices,
            'warning_invoices': warning_invoices,
            'pass_rate': pass_rate,
            'total_amount': total_amount,
            'passed_amount': passed_amount,
            'failed_amount': failed_amount,
            'column_mappings': mappings
        }
        
        return metrics, None
        
    except Exception as e:
        return None, f"Error processing data: {str(e)}"

# Enhanced visualization functions
def create_status_chart(df, status_col):
    """
    Create enhanced status distribution chart
    """
    if not status_col or status_col not in df.columns:
        return None
    
    try:
        # Clean and process status values
        df[status_col] = df[status_col].astype(str).apply(clean_emoji_text)
        status_counts = df[status_col].value_counts()
        
        # Define colors
        color_map = {}
        for status in status_counts.index:
            status_upper = status.upper()
            if 'PASS' in status_upper:
                color_map[status] = '#10b981'  # Green
            elif 'FAIL' in status_upper:
                color_map[status] = '#ef4444'  # Red
            elif 'WARN' in status_upper:
                color_map[status] = '#f59e0b'  # Yellow
            else:
                color_map[status] = '#6b7280'  # Gray
        
        # Create pie chart
        fig = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            title="Invoice Validation Status Distribution",
            color=status_counts.index,
            color_discrete_map=color_map
        )
        
        fig.update_traces(
            textposition='inside', 
            textinfo='percent+label',
            hovertemplate='%{label}Count: %{value}Percentage: %{percent}'
        )
        
        fig.update_layout(
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.2),
            height=400
        )
        
        return fig
        
    except Exception as e:
        st.error(f"Error creating status chart: {str(e)}")
        return None

def create_amount_chart(df, amount_col, status_col):
    """
    Create enhanced amount analysis chart
    """
    if not amount_col or amount_col not in df.columns:
        return None
    
    try:
        # Clean amount data
        df[amount_col] = pd.to_numeric(
            df[amount_col].astype(str).str.replace(r'[^\d.-]', '', regex=True), 
            errors='coerce'
        )
        df[amount_col] = df[amount_col].fillna(0)
        
        if status_col and status_col in df.columns:
            # Amount by status
            df[status_col] = df[status_col].astype(str).apply(clean_emoji_text)
            amount_by_status = df.groupby(status_col)[amount_col].sum().reset_index()
            
            # Define colors
            colors = []
            for status in amount_by_status[status_col]:
                status_upper = status.upper()
                if 'PASS' in status_upper:
                    colors.append('#10b981')
                elif 'FAIL' in status_upper:
                    colors.append('#ef4444')
                elif 'WARN' in status_upper:
                    colors.append('#f59e0b')
                else:
                    colors.append('#6b7280')
            
            fig = px.bar(
                amount_by_status,
                x=status_col,
                y=amount_col,
                title="Total Amount by Validation Status",
                color=status_col,
                color_discrete_sequence=colors
            )
            
            fig.update_layout(
                showlegend=False,
                xaxis_title="Status",
                yaxis_title="Amount (₹)",
                height=400
            )
            
            # Format y-axis to show currency
            fig.update_yaxis(tickformat=",.0f")
            
        else:
            # Amount distribution histogram
            fig = px.histogram(
                df,
                x=amount_col,
                title="Invoice Amount Distribution",
                nbins=20,
                color_discrete_sequence=['#667eea']
            )
            
            fig.update_layout(
                xaxis_title="Amount (₹)",
                yaxis_title="Count",
                height=400
            )
        
        return fig
        
    except Exception as e:
        st.error(f"Error creating amount chart: {str(e)}")
        return None

def create_trend_chart(df, date_col, status_col):
    """
    Create trend analysis chart over time
    """
    if not date_col or date_col not in df.columns:
        return None
    
    try:
        # Convert date column
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df = df.dropna(subset=[date_col])
        
        if len(df) == 0:
            return None
        
        # Group by date and status
        if status_col and status_col in df.columns:
            df[status_col] = df[status_col].astype(str).apply(clean_emoji_text)
            
            # Create daily summary
            daily_summary = df.groupby([df[date_col].dt.date, status_col]).size().reset_index(name='count')
            daily_summary['date'] = pd.to_datetime(daily_summary[date_col])
            
            fig = px.line(
                daily_summary,
                x='date',
                y='count',
                color=status_col,
                title="Invoice Processing Trend Over Time",
                markers=True
            )
            
            fig.update_layout(
                xaxis_title="Date",
                yaxis_title="Number of Invoices",
                height=400
            )
            
        else:
            # Simple count over time
            daily_count = df.groupby(df[date_col].dt.date).size().reset_index(name='count')
            daily_count['date'] = pd.to_datetime(daily_count[date_col])
            
            fig = px.line(
                daily_count,
                x='date',
                y='count',
                title="Invoice Volume Over Time",
                markers=True,
                color_discrete_sequence=['#667eea']
            )
            
            fig.update_layout(
                xaxis_title="Date",
                yaxis_title="Number of Invoices",
                height=400
            )
        
        return fig
        
    except Exception as e:
        st.error(f"Error creating trend chart: {str(e)}")
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
            help="Upload your invoice validation Excel report generated by main.py"
        )
        
        st.markdown("""
        
            📁 Supported Formats:
            
                Enhanced Report (25+ fields)
                Standard Report (15-24 fields)
                Basic Report (< 15 fields)
                CSV exports
            
            
            🔧 Features:
            
                Automatic emoji handling
                Multi-sheet Excel support
                Real-time metrics
                Interactive visualizations
                Advanced filtering
                Export capabilities
            
        
        """, unsafe_allow_html=True)
    
    if uploaded_file is not None:
        # Load and process file
        with st.spinner("🔄 Loading and processing file..."):
            df, load_error = load_excel_file(uploaded_file)
            
            if load_error:
                st.error(f"❌ {load_error}")
                return
            
            metrics, process_error = process_validation_data(df)
            
            if process_error:
                st.error(f"❌ {process_error}")
                return
        
        # Display metrics
        st.markdown("## 📈 Key Metrics")
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                "Total Invoices",
                f"{metrics['total_invoices']:,}",
                help=f"Report Type: {metrics['report_type']}"
            )
        
        with col2:
            delta_color = "normal" if metrics['pass_rate'] >= 80 else "inverse"
            st.metric(
                "Pass Rate",
                f"{metrics['pass_rate']:.1f}%",
                delta=f"{metrics['passed_invoices']} passed",
                delta_color=delta_color
            )
        
        with col3:
            st.metric(
                "Failed Invoices",
                f"{metrics['failed_invoices']:,}",
                delta=f"{metrics['failed_invoices']} failed",
                delta_color="inverse"
            )
        
        with col4:
            if metrics['warning_invoices'] > 0:
                st.metric(
                    "Warnings",
                    f"{metrics['warning_invoices']:,}",
                    delta=f"{metrics['warning_invoices']} warnings",
                    delta_color="off"
                )
            else:
                st.metric("Warnings", "0")
        
        with col5:
            if metrics['total_amount'] > 0:
                st.metric(
                    "Total Amount",
                    f"₹{metrics['total_amount']:,.0f}",
                    help="Total value of all invoices"
                )
            else:
                st.metric("Total Amount", "N/A")
        
        # Additional financial metrics if available
        if metrics['passed_amount'] > 0 or metrics['failed_amount'] > 0:
            st.markdown("### 💰 Financial Breakdown")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if metrics['passed_amount'] > 0:
                    st.metric(
                        "Passed Amount",
                        f"₹{metrics['passed_amount']:,.0f}",
                        delta="Ready for payment"
                    )
            
            with col2:
                if metrics['failed_amount'] > 0:
                    st.metric(
                        "Failed Amount",
                        f"₹{metrics['failed_amount']:,.0f}",
                        delta="Requires attention",
                        delta_color="inverse"
                    )
            
            with col3:
                if metrics['total_amount'] > 0:
                    success_rate = (metrics['passed_amount'] / metrics['total_amount']) * 100
                    st.metric(
                        "Financial Success Rate",
                        f"{success_rate:.1f}%"
                    )
        
        # Visualizations
        st.markdown("## 📊 Data Analysis")
        
        # Create visualization tabs
        tab1, tab2, tab3 = st.tabs(["📊 Status Analysis", "💰 Amount Analysis", "📈 Trend Analysis"])
        
        with tab1:
            col1, col2 = st.columns(2)
            
            with col1:
                status_chart = create_status_chart(df, metrics['column_mappings']['status'])
                if status_chart:
                    st.plotly_chart(status_chart, use_container_width=True)
                else:
                    st.info("📊 Status chart not available - no status column detected")
            
            with col2:
                # Status summary table
                if metrics['column_mappings']['status']:
                    status_col = metrics['column_mappings']['status']
                    df[status_col] = df[status_col].astype(str).apply(clean_emoji_text)
                    status_summary = df[status_col].value_counts().reset_index()
                    status_summary.columns = ['Status', 'Count']
                    status_summary['Percentage'] = (status_summary['Count'] / len(df) * 100).round(1)
                    
                    st.markdown("#### Status Summary")
                    st.dataframe(status_summary, use_container_width=True)
        
        with tab2:
            col1, col2 = st.columns(2)
            
            with col1:
                amount_chart = create_amount_chart(
                    df, 
                    metrics['column_mappings']['amount'], 
                    metrics['column_mappings']['status']
                )
                if amount_chart:
                    st.plotly_chart(amount_chart, use_container_width=True)
                else:
                    st.info("💰 Amount chart not available - no amount column detected")
            
            with col2:
                # Amount statistics
                if metrics['column_mappings']['amount']:
                    amount_col = metrics['column_mappings']['amount']
                    amount_stats = df[amount_col].describe()
                    
                    st.markdown("#### Amount Statistics")
                    stats_df = pd.DataFrame({
                        'Statistic': ['Mean', 'Median', 'Std Dev', 'Min', 'Max'],
                        'Value': [
                            f"₹{amount_stats['mean']:,.0f}",
                            f"₹{amount_stats['50%']:,.0f}",
                            f"₹{amount_stats['std']:,.0f}",
                            f"₹{amount_stats['min']:,.0f}",
                            f"₹{amount_stats['max']:,.0f}"
                        ]
                    })
                    st.dataframe(stats_df, use_container_width=True)
        
        with tab3:
            trend_chart = create_trend_chart(
                df, 
                metrics['column_mappings']['date'], 
                metrics['column_mappings']['status']
            )
            if trend_chart:
                st.plotly_chart(trend_chart, use_container_width=True)
            else:
                st.info("📈 Trend chart not available - no date column detected")
        
        # Data table with advanced filtering
        st.markdown("## 📋 Data Explorer")
        
        # Filter controls
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if metrics['column_mappings']['status']:
                status_filter = st.multiselect(
                    "Filter by Status",
                    options=df[metrics['column_mappings']['status']].unique(),
                    default=df[metrics['column_mappings']['status']].unique()
                )
                df_filtered = df[df[metrics['column_mappings']['status']].isin(status_filter)]
            else:
                df_filtered = df
        
        with col2:
            if metrics['column_mappings']['vendor']:
                vendor_options = df[metrics['column_mappings']['vendor']].unique()
                if len(vendor_options) <= 20:  # Only show if manageable number
                    vendor_filter = st.multiselect(
                        "Filter by Vendor",
                        options=vendor_options,
                        default=vendor_options
                    )
                    df_filtered = df_filtered[df_filtered[metrics['column_mappings']['vendor']].isin(vendor_filter)]
        
        with col3:
            max_rows = st.slider("Rows to display", 10, min(1000, len(df_filtered)), 100)
        
        # Search functionality
        search_term = st.text_input("🔍 Search in data (searches all text columns)")
        if search_term:
            text_columns = df_filtered.select_dtypes(include=['object']).columns
            search_mask = df_filtered[text_columns].astype(str).apply(
                lambda x: x.str.contains(search_term, case=False, na=False)
            ).any(axis=1)
            df_filtered = df_filtered[search_mask]
        
        # Display filtered data with info
        st.info(f"Showing {min(max_rows, len(df_filtered))} of {len(df_filtered)} filtered rows (Total: {len(df)} rows)")
        
        st.dataframe(
            df_filtered.head(max_rows),
            use_container_width=True,
            height=400
        )
        
        # Download section
        st.markdown("## 💾 Export Data")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # CSV download
            csv = df_filtered.to_csv(index=False)
            st.download_button(
                label="📥 Download Filtered Data (CSV)",
                data=csv,
                file_name=f"validation_results_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        with col2:
            # Excel download
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df_filtered.to_excel(writer, sheet_name='Validation Results', index=False)
                
                # Add summary sheet
                summary_data = {
                    'Metric': ['Total Invoices', 'Passed', 'Failed', 'Warnings', 'Pass Rate %', 'Total Amount'],
                    'Value': [
                        metrics['total_invoices'],
                        metrics['passed_invoices'],
                        metrics['failed_invoices'],
                        metrics['warning_invoices'],
                        f"{metrics['pass_rate']:.1f}%",
                        f"₹{metrics['total_amount']:,.0f}" if metrics['total_amount'] > 0 else "N/A"
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            st.download_button(
                label="📥 Download with Summary (Excel)",
                data=buffer.getvalue(),
                file_name=f"validation_results_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        with col3:
            # JSON download for API integration
            json_data = {
                'metadata': {
                    'export_timestamp': datetime.now().isoformat(),
                    'total_records': len(df_filtered),
                    'report_type': metrics['report_type']
                },
                'summary': metrics,
                'data': df_filtered.to_dict('records')
            }
            
            st.download_button(
                label="📥 Download JSON (API)",
                data=json.dumps(json_data, indent=2, default=str),
                file_name=f"validation_results_api_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
    
    else:
        # Welcome message with enhanced instructions
        st.markdown("""
        ## 👋 Welcome to the Invoice Validation Dashboard
        
        This dashboard analyzes the Excel reports generated by your **main.py** invoice validation system.
        
        ### 🚀 Quick Start Guide:
        
        1. **Run your corrected main.py** to process RMS data and generate validation reports
        2. **Upload the Excel report** file (e.g., `invoice_validation_report_20241206_143052.xlsx`)
        3. **Explore your data** with interactive charts and metrics
        4. **Filter and export** results as needed
        
        ### 📁 What files can you upload?
        
        - **Excel files (.xlsx, .xls)** generated by your main.py script
        - **CSV exports** from your validation system
        - Files with invoice validation data including status, amounts, and vendor information
        
        ### 🔧 Key Features:
        
        - **📊 Interactive Visualizations**: Status distribution, amount analysis, trend charts
        - **🎯 Real-time Metrics**: Pass rates, total amounts, processing statistics
        - **🔍 Advanced Filtering**: Search, filter by status/vendor, customizable views
        - **💾 Multiple Export Formats**: CSV, Excel with summaries, JSON for APIs
        - **🌐 Unicode Support**: Handles emojis and special characters automatically
        - **📱 Responsive Design**: Works on desktop and mobile devices
        
        ### 📋 Expected Data Format:
        
        Your Excel file should contain columns like:
        - Invoice Number, Vendor Code, Vendor Name
        - Invoice Date, Invoice Amount, Currency
        - **Validation Status** (PASS/FAIL/WARNING)
        - Error Details, Payment Terms, etc.
        
        ### 💡 Pro Tips:
        
        - **Large files**: The dashboard can handle files with thousands of records
        - **Multi-sheet Excel**: You can select which sheet to analyze
        - **Real-time updates**: Upload new reports anytime to see updated metrics
        - **Error handling**: The system gracefully handles missing columns and data issues
        
        ---
        
        **🎯 Ready to start?** Upload your invoice validation report using the file uploader in the sidebar!
        """)
        
        # Sample data structure display
        with st.expander("📋 Sample Data Structure"):
            sample_data = {
                'Invoice Number': ['INV-001', 'INV-002', 'INV-003'],
                'Vendor Name': ['ABC Corp', 'XYZ Ltd', 'DEF Inc'],
                'Invoice Amount': [15000, 25000, 8500],
                'Validation Status': ['[PASS]', '[FAIL]', '[PASS]'],
                'Error Details': ['None', 'Missing PO Number', 'None']
            }
            st.dataframe(pd.DataFrame(sample_data))

# Error handling for the main execution
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"⚠️ An unexpected error occurred: {str(e)}")
        
        with st.expander("🔧 Error Details & Troubleshooting"):
            st.code(str(e))
            st.markdown("""
            ### 🔧 Troubleshooting Steps:
            1. **File Format**: Ensure your file is a valid Excel (.xlsx, .xls) or CSV format
            2. **File Size**: Check that the file is not corrupted and under 200MB
            3. **Data Content**: Verify the file contains the expected columns with validation data
            4. **Browser**: Try refreshing the page and re-uploading
            5. **File Path**: Make sure there are no special characters in the filename
            
            ### 📧 Still having issues?
            If the problem persists:
            - Check your main.py is generating the Excel reports correctly
            - Verify the Excel file opens properly in Excel/LibreOffice
            - Contact support with the error details above
            """)
            
        # Show system info for debugging
        st.markdown("### 🔍 System Information")
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"Streamlit Version: {st.__version__}")
        with col2:
            st.info(f"Python Version: {pd.__version__}")
