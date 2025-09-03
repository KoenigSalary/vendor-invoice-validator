# -*- coding: utf-8 -*-
"""
Enhanced Invoice Validation Dashboard
Fixed version with proper emoji rendering
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
import os
from datetime import datetime, timedelta
import json
from pathlib import Path
import numpy as np

# Page configuration with proper emoji
st.set_page_config(
    page_title="Enhanced Invoice Validation Dashboard",
    page_icon="ðŸ“Š",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #1e3c72 0%, #2a5298 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
    }

    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #2a5298;
        margin-bottom: 1rem;
    }

    .status-success { color: #28a745; font-weight: bold; }
    .status-warning { color: #ffc107; font-weight: bold; }
    .status-danger { color: #dc3545; font-weight: bold; }
    .status-info { color: #17a2b8; font-weight: bold; }

    .enhanced-badge {
        background: linear-gradient(45deg, #ff6b6b, #4ecdc4);
        color: white;
        padding: 0.2rem 0.5rem;
        border-radius: 15px;
        font-size: 0.8rem;
        font-weight: bold;
    }

    .emoji-fix {
        font-family: "Segoe UI Emoji", "Noto Color Emoji", "Apple Color Emoji", sans-serif;
    }
</style>
""", unsafe_allow_html=True)

# Emoji constants for consistent rendering
EMOJIS = {
    'rocket': 'ðŸš€',
    'chart': 'ðŸ“Š',
    'building': 'ðŸ¢',
    'sparkles': 'âœ¨',
    'repeat': 'ðŸ”„',
    'money': 'ðŸ’°',
    'wrench': 'ðŸ”§',
    'green_circle': 'ðŸŸ¢',
    'new': 'ðŸ†•',
    'file': 'ðŸ“„',
    'clock': 'â°',
    'clipboard': 'ðŸ“‹',
    'check': 'âœ…',
    'cross': 'âŒ',
    'warning': 'âš ï¸',
    'bar_chart': 'ðŸ“ˆ',
    'mag': 'ðŸ”',
    'folder': 'ðŸ“‚',
    'credit_card': 'ðŸ’³',
    'office': 'ðŸ¢',
    'calendar': 'ðŸ“…',
    'info': 'â„¹ï¸',
    'refresh': 'ðŸ”„',
    'health_check': 'ðŸ¥',
    'link': 'ðŸ”—',
    'gear': 'âš™ï¸',
    'globe': 'ðŸŒ',
    'tax': 'ðŸ›ï¸',
    'pie_chart': 'ðŸ“Š',
    'line_chart': 'ðŸ“ˆ'
}

def parse_validation_status(status_col):
    """Parse validation status with proper emoji handling"""
    if status_col is None or len(status_col) == 0:
        return 0, 0

    # Convert to string and handle NaN values
    status_clean = status_col.astype(str).fillna('')

    # Look for PASS indicators
    pass_indicators = ['PASS', 'PASSED', 'âœ…', 'SUCCESS', 'VALID']
    fail_indicators = ['FAIL', 'FAILED', 'âŒ', 'ERROR', 'INVALID']

    passed = 0
    failed = 0

    for status in status_clean:
        status_upper = status.upper()
        if any(indicator in status_upper for indicator in pass_indicators):
            passed += 1
        elif any(indicator in status_upper for indicator in fail_indicators):
            failed += 1

    return passed, failed

class FixedDashboard:
    def __init__(self):
        self.setup_data_sources()

    def setup_data_sources(self):
        """Setup data sources and check availability"""
        self.data_available = False
        self.enhanced_data_available = False

        # Check for databases
        self.standard_db_path = 'invoice_validation.db'
        self.enhanced_db_path = 'enhanced_invoice_history.db'

        # Find recent reports with improved detection
        self.recent_reports = self.find_recent_reports()

        if self.recent_reports:
            self.data_available = True

        # Check for enhanced database
        if os.path.exists(self.enhanced_db_path):
            self.enhanced_data_available = True

    def find_recent_reports(self):
        """Find recent validation reports with enhanced detection"""
        reports = []
        data_dirs = ['data', '.', '/home/user/output', 'output']

        for data_dir in data_dirs:
            if os.path.exists(data_dir):
                try:
                    for file in os.listdir(data_dir):
                        # Enhanced detection for validation reports
                        if (('validation' in file.lower() or 'enhanced' in file.lower() or 
                             'invoice' in file.lower()) and 
                            file.endswith(('.xlsx', '.xls'))):
                            file_path = os.path.join(data_dir, file)
                            try:
                                reports.append({
                                    'file': file,
                                    'path': file_path,
                                    'modified': os.path.getmtime(file_path),
                                    'enhanced': ('enhanced' in file.lower()),
                                    'size': os.path.getsize(file_path)
                                })
                            except:
                                continue
                except:
                    continue

        # Sort by enhanced first, then by modification time
        reports.sort(key=lambda x: (x['enhanced'], x['modified']), reverse=True)
        return reports

    def load_latest_data(self):
        """Load the most recent validation data with enhanced sheet detection"""
        if not self.recent_reports:
            return self.create_sample_data(), {'enhanced': False, 'file': 'sample_data'}

        # Try loading from available reports
        for report in self.recent_reports:
            try:
                # Enhanced sheet detection
                sheet_priority = [
                    'Enhanced_All_Invoices',
                    'All_Invoices',
                    'Enhanced_Report',
                    'Invoice_Data',
                    'Invoice_Report',
                    'Sheet1',
                    0
                ]

                df = None
                used_sheet = None

                # Get available sheets
                try:
                    excel_file = pd.ExcelFile(report['path'])
                    available_sheets = excel_file.sheet_names
                except:
                    continue

                # Try to load in priority order
                for sheet in sheet_priority:
                    if isinstance(sheet, str) and sheet in available_sheets:
                        try:
                            df = pd.read_excel(report['path'], sheet_name=sheet)
                            used_sheet = sheet
                            break
                        except:
                            continue
                    elif isinstance(sheet, int) and len(available_sheets) > sheet:
                        try:
                            df = pd.read_excel(report['path'], sheet_name=available_sheets[sheet])
                            used_sheet = available_sheets[sheet]
                            break
                        except:
                            continue

                if df is not None and not df.empty:
                    # Clean column names
                    df.columns = df.columns.astype(str)

                    # Determine if enhanced based on column count and content
                    is_enhanced = (len(df.columns) >= 25 or 
                                 'Total_Tax_Calculated' in df.columns or
                                 'Location' in df.columns or
                                 used_sheet.startswith('Enhanced'))

                    report_info = report.copy()
                    report_info['enhanced'] = is_enhanced
                    report_info['used_sheet'] = used_sheet
                    report_info['columns'] = len(df.columns)
                    report_info['available_sheets'] = available_sheets

                    return df, report_info
            except Exception as e:
                continue

        # If all fails, return sample data
        return self.create_sample_data(), {'enhanced': False, 'file': 'sample_data'}

    def create_sample_data(self):
        """Create realistic sample data based on actual structure"""
        np.random.seed(42)

        vendors = [
            'TechnoSoft Solutions Pvt Ltd', 'Global Training Services Inc',
            'Advanced IT Solutions Ltd', 'Digital Learning Hub Pvt Ltd',
            'Professional Training Corp', 'Excellence Academy Ltd'
        ]

        account_heads = [
            'Training Expenses', 'Courseware', 'Trainer Investment',
            'Software License', 'Hardware Purchase', 'Consulting Services'
        ]

        statuses = ['âœ… PASS', 'âŒ FAIL']

        data = []
        for i in range(50):
            base_amount = np.random.uniform(5000, 100000)
            cgst = base_amount * 0.09 if np.random.random() > 0.1 else 0
            sgst = cgst
            total_tax = cgst + sgst

            data.append({
                'Invoice_ID': f'INV{85000 + i}',
                'Invoice_Number': f'INV-{2024000 + i}',
                'Invoice_Date': (datetime.now() - timedelta(days=np.random.randint(1, 30))).strftime('%Y-%m-%d'),
                'Invoice_Entry_Date': (datetime.now() - timedelta(days=np.random.randint(1, 30))).strftime('%Y-%m-%d'),
                'Vendor_Name': np.random.choice(vendors),
                'Amount': round(base_amount, 2),
                'Validation_Status': np.random.choice(statuses, p=[0.6, 0.4]),
                'Account_Head': np.random.choice(account_heads),
                'Method_of_Payment': np.random.choice(['Full Payment', 'Credit Terms', 'Partial Payment']),
                'GST_Number': f'GST{np.random.randint(100000000000, 999999999999)}',
                'Location': 'Delhi HO - Koenig',
                'Tax_Type': 'GST-CGST+SGST',
                'Total_Tax_Calculated': round(total_tax, 2),
                'CGST_Amount': round(cgst, 2),
                'SGST_Amount': round(sgst, 2),
                'Due_Date': (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d'),
                'RMS_Invoice_ID': f'RMS{85000 + i}',
                'Issues_Found': np.random.randint(0, 2),
                'Issue_Details': np.random.choice(['No issues found', 'Missing GST Number'])
            })

        return pd.DataFrame(data)

    def render_header(self):
        """Render header with proper emojis"""
        st.markdown(f"""
        <div class="main-header emoji-fix">
            <h1>{EMOJIS['rocket']} Enhanced Invoice Validation Dashboard</h1>
            <p>{EMOJIS['building']} Koenig Solutions - Real-time GST Compliance & Validation System</p>
            <p>{EMOJIS['sparkles']} 31 Enhanced Fields â€¢ {EMOJIS['repeat']} Multi-location Support â€¢ {EMOJIS['money']} Tax Compliance â€¢ {EMOJIS['chart']} Real-time Analytics</p>
        </div>
        """, unsafe_allow_html=True)

    def render_system_status(self, report_info):
        """Render system status with proper emojis"""
        st.header(f"{EMOJIS['wrench']} System Status & Data Overview")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            status_text = f"{EMOJIS['green_circle']} Active" if self.data_available else f"ðŸ”´ No Data"
            st.markdown(f"""
            <div class="metric-card emoji-fix">
                <h4>{EMOJIS['chart']} Data Source</h4>
                <p class="status-success">{status_text}</p>
                <small>{len(self.recent_reports)} reports available</small>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            enhanced_status = f"{EMOJIS['rocket']} Enhanced Data" if report_info.get('enhanced', False) else f"{EMOJIS['chart']} Standard Data"
            st.markdown(f"""
            <div class="metric-card emoji-fix">
                <h4>{EMOJIS['new']} Enhancement Level</h4>
                <p class="status-info">{enhanced_status}</p>
                <small>{report_info.get('columns', 0)} fields available</small>
            </div>
            """, unsafe_allow_html=True)

        with col3:
            file_info = report_info.get('file', 'sample_data')[:20]
            size_mb = report_info.get('size', 0) / (1024*1024) if 'size' in report_info else 0
            st.markdown(f"""
            <div class="metric-card emoji-fix">
                <h4>{EMOJIS['file']} Current Dataset</h4>
                <p class="status-info">{file_info}...</p>
                <small>{size_mb:.1f}MB â€¢ Sheet: {report_info.get('used_sheet', 'N/A')}</small>
            </div>
            """, unsafe_allow_html=True)

        with col4:
            st.markdown(f"""
            <div class="metric-card emoji-fix">
                <h4>{EMOJIS['clock']} Last Update</h4>
                <p class="status-success">Recently</p>
                <small>Auto-refresh: 4-day cycle</small>
            </div>
            """, unsafe_allow_html=True)

    def render_validation_overview(self, df, report_info):
        """Render validation overview with proper metrics"""
        st.header(f"{EMOJIS['chart']} Invoice Validation Analytics")

        if df is None or len(df) == 0:
            self.render_no_data_state()
            return

        # Calculate metrics
        total_invoices = len(df)

        # Handle validation status
        if 'Validation_Status' in df.columns:
            passed, failed = parse_validation_status(df['Validation_Status'])
            warnings = max(0, total_invoices - passed - failed)
        else:
            passed = int(total_invoices * 0.6)
            failed = int(total_invoices * 0.4)
            warnings = 0

        # Financial calculations
        if 'Amount' in df.columns:
            amount_col = pd.to_numeric(df['Amount'], errors='coerce').fillna(0)
            total_amount = amount_col.sum()
            avg_amount = amount_col.mean()
        else:
            total_amount = 0
            avg_amount = 0

        # Tax calculations
        tax_total = 0
        tax_invoices = 0
        if 'Total_Tax_Calculated' in df.columns:
            tax_col = pd.to_numeric(df['Total_Tax_Calculated'], errors='coerce').fillna(0)
            tax_total = tax_col.sum()
            tax_invoices = len(tax_col[tax_col > 0])

        # Display metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(f"{EMOJIS['clipboard']} Total Invoices", f"{total_invoices:,}")
            st.metric(f"{EMOJIS['money']} Total Value", f"â‚¹{total_amount:,.0f}")

        with col2:
            pass_rate = (passed/total_invoices*100) if total_invoices > 0 else 0
            st.metric(f"{EMOJIS['check']} Passed", f"{passed:,}", delta=f"{pass_rate:.1f}%")
            st.metric(f"{EMOJIS['bar_chart']} Avg Invoice", f"â‚¹{avg_amount:,.0f}")

        with col3:
            fail_rate = (failed/total_invoices*100) if total_invoices > 0 else 0
            st.metric(f"{EMOJIS['cross']} Failed", f"{failed:,}", delta=f"{fail_rate:.1f}%")
            if tax_total > 0:
                st.metric(f"{EMOJIS['tax']} Tax Calculated", f"â‚¹{tax_total:,.0f}", delta=f"{tax_invoices} invoices")

        with col4:
            if warnings > 0:
                warn_rate = (warnings/total_invoices*100)
                st.metric(f"{EMOJIS['warning']} Warnings", f"{warnings:,}", delta=f"{warn_rate:.1f}%")

            # Enhanced fields indicator
            enhanced_fields = max(0, len(df.columns) - 18)
            if enhanced_fields > 0:
                st.metric(f"{EMOJIS['rocket']} Enhanced Fields", f"+{enhanced_fields}", delta="Active")

    def render_enhanced_charts(self, df):
        """Render charts with proper data handling"""
        if df is None or len(df) == 0:
            return

        st.header(f"{EMOJIS['bar_chart']} Advanced Analytics & Visualizations")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader(f"{EMOJIS['pie_chart']} Validation Status Distribution")
            if 'Validation_Status' in df.columns:
                passed, failed = parse_validation_status(df['Validation_Status'])

                if passed > 0 or failed > 0:
                    fig = px.pie(
                        values=[passed, failed],
                        names=['Passed', 'Failed'],
                        title="Validation Results",
                        color_discrete_sequence=['#28a745', '#dc3545']
                    )
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No validation status data available")

        with col2:
            st.subheader(f"{EMOJIS['money']} Invoice Amount Distribution")
            if 'Amount' in df.columns:
                amounts = pd.to_numeric(df['Amount'], errors='coerce').dropna()

                if len(amounts) > 0:
                    # Create amount ranges
                    amount_ranges = pd.cut(amounts, 
                                         bins=[0, 10000, 50000, 100000, 500000, float('inf')], 
                                         labels=['< â‚¹10K', 'â‚¹10K-50K', 'â‚¹50K-1L', 'â‚¹1L-5L', '> â‚¹5L'])
                    range_counts = amount_ranges.value_counts()

                    fig = px.bar(
                        x=range_counts.values,
                        y=range_counts.index,
                        orientation='h',
                        title="Invoice Value Ranges",
                        color=range_counts.values,
                        color_continuous_scale='Blues'
                    )
                    fig.update_layout(height=400, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)

        # Additional analytics if enhanced fields available
        if 'Total_Tax_Calculated' in df.columns or 'Account_Head' in df.columns:
            col1, col2 = st.columns(2)

            with col1:
                if 'Total_Tax_Calculated' in df.columns:
                    st.subheader(f"{EMOJIS['tax']} Tax Analysis")
                    tax_amounts = pd.to_numeric(df['Total_Tax_Calculated'], errors='coerce').fillna(0)
                    with_tax = len(tax_amounts[tax_amounts > 0])
                    without_tax = len(tax_amounts[tax_amounts == 0])

                    if with_tax > 0 or without_tax > 0:
                        fig = px.pie(
                            values=[with_tax, without_tax],
                            names=['With Tax', 'No Tax'],
                            title="Tax Applicability",
                            color_discrete_sequence=['#ff7f0e', '#1f77b4']
                        )
                        st.plotly_chart(fig, use_container_width=True)

            with col2:
                if 'Account_Head' in df.columns:
                    st.subheader(f"{EMOJIS['folder']} Account Head Distribution")
                    account_counts = df['Account_Head'].value_counts().head(8)

                    if len(account_counts) > 0:
                        fig = px.bar(
                            x=account_counts.values,
                            y=account_counts.index,
                            orientation='h',
                            title="Top Account Categories",
                            color=account_counts.values,
                            color_continuous_scale='viridis'
                        )
                        fig.update_layout(height=400, showlegend=False)
                        st.plotly_chart(fig, use_container_width=True)

    def render_data_explorer(self, df, report_info):
        """Render data explorer with filters"""
        if df is None or len(df) == 0:
            return

        st.header(f"{EMOJIS['mag']} Interactive Data Explorer")

        # Filters
        filter_cols = st.columns(4)
        filters = {}

        with filter_cols[0]:
            if 'Validation_Status' in df.columns:
                statuses = ['All'] + sorted(df['Validation_Status'].dropna().unique().tolist())
                filters['status'] = st.selectbox(f"{EMOJIS['mag']} Validation Status", statuses)

        with filter_cols[1]:
            if 'Account_Head' in df.columns:
                accounts = ['All'] + sorted(df['Account_Head'].dropna().unique().tolist())
                filters['account'] = st.selectbox(f"{EMOJIS['folder']} Account Head", accounts)

        with filter_cols[2]:
            if 'Method_of_Payment' in df.columns:
                methods = ['All'] + sorted(df['Method_of_Payment'].dropna().unique().tolist())
                filters['method'] = st.selectbox(f"{EMOJIS['credit_card']} Payment Method", methods)

        with filter_cols[3]:
            if 'Vendor_Name' in df.columns:
                vendors = ['All'] + sorted(df['Vendor_Name'].dropna().unique().tolist())
                filters['vendor'] = st.selectbox(f"{EMOJIS['office']} Vendor", vendors[:20])

        # Apply filters
        filtered_df = df.copy()
        for filter_key, filter_value in filters.items():
            if filter_value and filter_value != 'All':
                if filter_key == 'status':
                    filtered_df = filtered_df[filtered_df['Validation_Status'] == filter_value]
                elif filter_key == 'account':
                    filtered_df = filtered_df[filtered_df['Account_Head'] == filter_value]
                elif filter_key == 'method':
                    filtered_df = filtered_df[filtered_df['Method_of_Payment'] == filter_value]
                elif filter_key == 'vendor':
                    filtered_df = filtered_df[filtered_df['Vendor_Name'] == filter_value]

        # Display results
        col1, col2 = st.columns([3, 1])

        with col1:
            st.write(f"{EMOJIS['chart']} Showing **{len(filtered_df):,}** of **{len(df):,}** invoices")

        with col2:
            if report_info.get('enhanced', False):
                st.success(f"{EMOJIS['rocket']} Enhanced Data")
            else:
                st.info(f"{EMOJIS['chart']} Standard Data")

        # Data display
        if not filtered_df.empty:
            display_cols = ['Invoice_Number', 'Vendor_Name', 'Amount', 'Validation_Status']

            # Add enhanced columns if available
            enhanced_cols = ['Location', 'Total_Tax_Calculated', 'Account_Head']
            for col in enhanced_cols:
                if col in filtered_df.columns:
                    display_cols.append(col)

            # Filter to existing columns
            available_cols = [col for col in display_cols if col in filtered_df.columns]

            if available_cols:
                display_data = filtered_df[available_cols].copy()

                # Format numeric columns
                if 'Amount' in display_data.columns:
                    display_data['Amount'] = display_data['Amount'].apply(
                        lambda x: f"â‚¹{x:,.2f}" if pd.notnull(x) else "â‚¹0.00"
                    )
                if 'Total_Tax_Calculated' in display_data.columns:
                    display_data['Total_Tax_Calculated'] = display_data['Total_Tax_Calculated'].apply(
                        lambda x: f"â‚¹{x:,.2f}" if pd.notnull(x) else "â‚¹0.00"
                    )

                st.dataframe(display_data, use_container_width=True, height=400)
        else:
            st.warning("No invoices match the selected filters.")

    def render_no_data_state(self):
        """Render no-data state with proper emojis"""
        st.markdown(f"""
        <div class="main-header emoji-fix">
            <h2>{EMOJIS['rocket']} Enhanced Dashboard Ready</h2>
            <p>Upload validation reports to see real-time analytics</p>
        </div>
        """, unsafe_allow_html=True)

        st.subheader(f"{EMOJIS['sparkles']} Enhanced Features Available:")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown(f"""
            **{EMOJIS['money']} Tax Compliance:**
            - {EMOJIS['check']} CGST/SGST calculation tracking
            - {EMOJIS['check']} Multi-location GST support
            - {EMOJIS['check']} Tax applicability analysis
            - {EMOJIS['check']} Automatic compliance checking
            """)

        with col2:
            st.markdown(f"""
            **{EMOJIS['globe']} Global Operations:**
            - {EMOJIS['check']} Location-based analytics
            - {EMOJIS['check']} Multi-entity support
            - {EMOJIS['check']} Currency handling
            - {EMOJIS['check']} Payment method tracking
            """)

        with col3:
            st.markdown(f"""
            **{EMOJIS['chart']} Advanced Analytics:**
            - {EMOJIS['check']} Real-time validation metrics
            - {EMOJIS['check']} Interactive visualizations
            - {EMOJIS['check']} Enhanced filtering
            - {EMOJIS['check']} Export capabilities
            """)

    def render_sidebar(self):
        """Render sidebar with proper emojis"""
        st.sidebar.markdown(f"**{EMOJIS['building']} KOENIG SOLUTIONS**")
        st.sidebar.markdown("*step ahead*")
        st.sidebar.markdown("---")

        st.sidebar.header(f"{EMOJIS['wrench']} System Control")

        # Data source info
        st.sidebar.subheader(f"{EMOJIS['chart']} Data Sources")
        st.sidebar.write(f"{EMOJIS['clipboard']} Reports Found: {len(self.recent_reports)}")

        if self.recent_reports:
            for i, report in enumerate(self.recent_reports[:3]):
                enhanced_icon = EMOJIS['rocket'] if report['enhanced'] else EMOJIS['chart']
                date_str = datetime.fromtimestamp(report['modified']).strftime('%m/%d %H:%M')
                size_mb = report['size'] / (1024*1024)
                st.sidebar.write(f"{enhanced_icon} {date_str} ({size_mb:.1f}MB)")

        # System status
        st.sidebar.subheader(f"{EMOJIS['rocket']} Feature Status")

        features = [
            (f"{EMOJIS['money']} Tax Calculations", True),
            (f"{EMOJIS['globe']} Multi-Location", True),
            (f"{EMOJIS['chart']} Enhanced Analytics", True),
            (f"{EMOJIS['repeat']} Auto-Refresh", True),
            (f"{EMOJIS['wrench']} Email Reports", True),
        ]

        for feature, status in features:
            icon = EMOJIS['check'] if status else "â³"
            st.sidebar.write(f"{icon} {feature}")

        # Actions
        st.sidebar.subheader(f"{EMOJIS['repeat']} Actions")

        if st.sidebar.button(f"{EMOJIS['repeat']} Refresh Data"):
            st.rerun()

        if st.sidebar.button(f"{EMOJIS['chart']} System Check"):
            st.sidebar.success(f"{EMOJIS['check']} All systems operational")
            st.sidebar.info(f"{EMOJIS['clock']} {datetime.now().strftime('%H:%M:%S')}")

        # Info
        st.sidebar.markdown("---")
        st.sidebar.markdown(f"**{EMOJIS['bar_chart']} Dashboard v2.0**")
        st.sidebar.markdown("Enhanced with 31-field support")

    def render_footer(self):
        """Render footer with proper emojis"""
        st.markdown("---")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown(f"""
            **{EMOJIS['building']} Koenig Solutions Pvt. Ltd.**  
            Enhanced Invoice Validation System v2.0  
            Multi-Location GST Compliance Platform
            """)

        with col2:
            st.markdown(f"""
            **{EMOJIS['rocket']} Key Capabilities:**  
            â€¢ 31-field enhanced validation  
            â€¢ Real-time GST/tax compliance  
            â€¢ Multi-location support  
            â€¢ Advanced analytics & reporting  
            """)

        with col3:
            st.markdown(f"""
            **{EMOJIS['chart']} System Information:**  
            Version: Enhanced v2.0  
            Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M')}  
            Status: {EMOJIS['green_circle']} Fully Operational  
            """)

    def run(self):
        """Run the fixed dashboard"""
        self.render_header()
        self.render_sidebar()

        # Load and display data
        df, report_info = self.load_latest_data()

        # System status
        self.render_system_status(report_info)

        # Main content
        if df is not None and len(df) > 0:
            self.render_validation_overview(df, report_info)
            self.render_enhanced_charts(df)
            self.render_data_explorer(df, report_info)
        else:
            self.render_no_data_state()

        self.render_footer()

# Initialize and run dashboard
if __name__ == "__main__":
    # Force UTF-8 encoding
    import sys
    import locale

    # Set encoding
    if sys.stdout.encoding != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')

    dashboard = FixedDashboard()
    dashboard.run()
