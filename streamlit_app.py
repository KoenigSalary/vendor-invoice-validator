"""
Enhanced Invoice Validation Dashboard with GitHub Integration
Connects to real validation data from GitHub Actions workflow
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import sys
import json
import requests
import emoji.fix.py
from datetime import datetime, timedelta
from io import BytesIO
import base64
import numpy as np

# Force UTF-8 encoding to fix emoji issues
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

# Set environment variables for proper encoding
os.environ['PYTHONIOENCODING'] = 'utf-8'

# Page configuration with proper emojis
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

    .connection-status {
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: bold;
        display: inline-block;
        margin: 0.2rem;
    }

    .status-connected { background: #d4edda; color: #155724; }
    .status-fallback { background: #fff3cd; color: #856404; }
    .status-error { background: #f8d7da; color: #721c24; }

    .enhanced-badge {
        background: linear-gradient(45deg, #ff6b6b, #4ecdc4);
        color: white;
        padding: 0.2rem 0.5rem;
        border-radius: 15px;
        font-size: 0.8rem;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

class GitHubIntegratedDashboard:
    def __init__(self):
        self.github_repo = "KoenigSalary/vendor-invoice-validator"  # Update with actual repo
        self.data_folder = "streamlit_data"
        self.connection_status = "checking"
        self.data_source = "unknown"
        self.setup_data_sources()

    def setup_data_sources(self):
        """Setup and test multiple data sources"""
        self.data_sources = {
            'github_repo': self.check_github_repo_access(),
            'local_files': self.check_local_files(),
            'uploaded_files': False,  # Will be set when user uploads
            'sample_data': True
        }

        # Determine primary data source
        if self.data_sources['github_repo']:
            self.data_source = "github_repo"
            self.connection_status = "connected"
        elif self.data_sources['local_files']:
            self.data_source = "local_files"
            self.connection_status = "local"
        else:
            self.data_source = "sample_data"
            self.connection_status = "fallback"

    def check_github_repo_access(self):
        """Check if GitHub repository data is accessible"""
        try:
            # Try to access the metadata file
            url = f"https://raw.githubusercontent.com/{self.github_repo}/main/{self.data_folder}/validation_metadata.json"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                metadata = response.json()
                return True
        except:
            pass
        return False

    def check_local_files(self):
        """Check for local validation files"""
        local_paths = ['streamlit_data', 'data', '.']
        for path in local_paths:
            if os.path.exists(path):
                files = [f for f in os.listdir(path) if f.endswith(('.xlsx', '.xls'))]
                if files:
                    return True
        return False

    def load_github_data(self):
        """Load validation data from GitHub repository"""
        try:
            # Get metadata first
            metadata_url = f"https://raw.githubusercontent.com/{self.github_repo}/main/{self.data_folder}/validation_metadata.json"
            response = requests.get(metadata_url)

            if response.status_code == 200:
                metadata = response.json()

                # Try to load the enhanced validation file first
                for file_info in metadata.get('files_available', []):
                    filename = file_info['name']
                    if 'enhanced' in filename.lower() and filename.endswith('.xlsx'):
                        file_url = f"https://raw.githubusercontent.com/{self.github_repo}/main/{self.data_folder}/{filename}"
                        return self.download_and_parse_excel(file_url), metadata

                # Fallback to any xlsx file
                for file_info in metadata.get('files_available', []):
                    filename = file_info['name']
                    if filename.endswith('.xlsx'):
                        file_url = f"https://raw.githubusercontent.com/{self.github_repo}/main/{self.data_folder}/{filename}"
                        return self.download_and_parse_excel(file_url), metadata
        except Exception as e:
            st.error(f"GitHub access error: {str(e)}")

        return None, {}

    def download_and_parse_excel(self, url):
        """Download and parse Excel file from URL"""
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                excel_data = BytesIO(response.content)

                # Try different sheet names
                excel_file = pd.ExcelFile(excel_data)
                sheet_names = excel_file.sheet_names

                for sheet_name in ['Enhanced_All_Invoices', 'All_Invoices', sheet_names[0]]:
                    try:
                        if sheet_name in sheet_names:
                            df = pd.read_excel(excel_data, sheet_name=sheet_name)
                            if not df.empty:
                                return df
                    except:
                        continue
        except Exception as e:
            st.error(f"Excel parsing error: {str(e)}")
        return None

    def load_local_data(self):
        """Load data from local files"""
        local_paths = ['streamlit_data', 'data']

        for path in local_paths:
            if os.path.exists(path):
                files = [f for f in os.listdir(path) 
                        if f.endswith(('.xlsx', '.xls')) and 'validation' in f.lower()]

                if files:
                    # Sort by modification time, get newest
                    files.sort(key=lambda x: os.path.getmtime(os.path.join(path, x)), reverse=True)

                    try:
                        file_path = os.path.join(path, files[0])
                        df = pd.read_excel(file_path)
                        metadata = {
                            'last_updated': datetime.now().isoformat(),
                            'source_file': files[0],
                            'total_files': len(files)
                        }
                        return df, metadata
                    except Exception as e:
                        continue

        return None, {}

    def create_sample_data(self):
        """Create realistic sample data"""
        np.random.seed(42)

        vendors = [
            'TechnoSoft Solutions Pvt Ltd', 'Global Training Services Inc', 
            'Advanced IT Solutions Ltd', 'Digital Learning Hub Pvt Ltd'
        ]

        data = []
        for i in range(150):
            base_amount = np.random.uniform(5000, 150000)
            has_tax = np.random.random() > 0.15

            data.append({
                'Invoice_Number': f'INV-2024-{1000 + i}',
                'Vendor_Name': np.random.choice(vendors),
                'Amount': round(base_amount, 2),
                'Validation_Status': np.random.choice(['âœ… PASS', 'âŒ FAIL'], p=[0.65, 0.35]),
                'Account_Head': np.random.choice(['Training', 'Infrastructure', 'Software']),
                'GST_Number': f'22AAAAA0000A1Z{i % 10}' if np.random.random() > 0.1 else '',
                'Invoice_Date': (datetime.now() - timedelta(days=np.random.randint(1, 60))).strftime('%Y-%m-%d'),
                'Total_Tax_Calculated': round(base_amount * 0.18, 2) if has_tax else 0,
            })

        return pd.DataFrame(data)

    def load_data(self):
        """Load data from the best available source"""
        if self.data_source == "github_repo":
            df, metadata = self.load_github_data()
            if df is not None:
                return df, metadata, "ðŸ”— GitHub Repository"

        if self.data_source == "local_files":
            df, metadata = self.load_local_data()
            if df is not None:
                return df, metadata, "ðŸ“ Local Files"

        # Fallback to sample data
        df = self.create_sample_data()
        metadata = {'source': 'sample', 'total_records': len(df)}
        return df, metadata, "ðŸ”¬ Sample Data"

    def render_connection_status(self):
        """Render connection status indicator"""
        st.subheader("ðŸ”— Data Connection Status")

        col1, col2, col3 = st.columns(3)

        with col1:
            github_status = "ðŸŸ¢ Connected" if self.data_sources['github_repo'] else "ðŸ”´ Not Available"
            st.markdown(f"""
            <div class="connection-status {'status-connected' if self.data_sources['github_repo'] else 'status-error'}">
                GitHub Repo: {github_status}
            </div>
            """, unsafe_allow_html=True)

        with col2:
            local_status = "ðŸŸ¡ Available" if self.data_sources['local_files'] else "ðŸ”´ Not Found"
            st.markdown(f"""
            <div class="connection-status {'status-fallback' if self.data_sources['local_files'] else 'status-error'}">
                Local Files: {local_status}
            </div>
            """, unsafe_allow_html=True)

        with col3:
            current_source = self.data_source.replace('_', ' ').title()
            st.markdown(f"""
            <div class="connection-status status-connected">
                Active Source: {current_source}
            </div>
            """, unsafe_allow_html=True)

    def render_header(self):
        """Render dashboard header"""
        st.markdown("""
        <div class="main-header">
            <h1>ðŸš€ Enhanced Invoice Validation Dashboard</h1>
            <p>ðŸ¢ Koenig Solutions - Real-time GST Compliance & Validation System</p>
            <p>ðŸ”— GitHub Actions Integration â€¢ ðŸ“Š Live Data â€¢ ðŸ’° Tax Compliance</p>
        </div>
        """, unsafe_allow_html=True)

    def render_metrics(self, df, source_info):
        """Render key metrics"""
        if df is None or len(df) == 0:
            return

        st.header(f"ðŸ“Š Invoice Analytics - {source_info}")

        # Calculate metrics
        total_invoices = len(df)

        # Parse validation status
        if 'Validation_Status' in df.columns:
            passed = len(df[df['Validation_Status'].str.contains('PASS|âœ…', case=False, na=False)])
            failed = len(df[df['Validation_Status'].str.contains('FAIL|âŒ', case=False, na=False)])
        else:
            passed = int(total_invoices * 0.65)
            failed = total_invoices - passed

        # Financial calculations
        try:
            amount_col = pd.to_numeric(df['Amount'], errors='coerce').fillna(0)
            total_amount = amount_col.sum()
            avg_amount = amount_col.mean()
        except:
            total_amount = 0
            avg_amount = 0

        # Display metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("ðŸ“‹ Total Invoices", f"{total_invoices:,}")

        with col2:
            pass_rate = (passed/total_invoices*100) if total_invoices > 0 else 0
            st.metric("âœ… Pass Rate", f"{pass_rate:.1f}%", delta=f"{passed} passed")

        with col3:
            fail_rate = (failed/total_invoices*100) if total_invoices > 0 else 0
            st.metric("âŒ Failed", f"{failed:,}", delta=f"{fail_rate:.1f}%")

        with col4:
            st.metric("ðŸ’° Total Value", f"â‚¹{total_amount:,.0f}")

    def render_charts(self, df):
        """Render visualization charts"""
        if df is None or len(df) == 0:
            return

        st.header("ðŸ“ˆ Visual Analytics")

        col1, col2 = st.columns(2)

        with col1:
            # Validation Status Chart
            if 'Validation_Status' in df.columns:
                status_clean = df['Validation_Status'].str.replace('âœ… ', '').str.replace('âŒ ', '')
                status_counts = status_clean.value_counts()

                colors = ['#28a745' if 'PASS' in str(idx).upper() else '#dc3545' 
                         for idx in status_counts.index]

                fig = px.pie(
                    values=status_counts.values,
                    names=status_counts.index,
                    title="ðŸ“Š Validation Results",
                    color_discrete_sequence=colors
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Amount Distribution
            try:
                amounts = pd.to_numeric(df['Amount'], errors='coerce').dropna()
                if not amounts.empty:
                    fig = px.histogram(
                        x=amounts,
                        nbins=20,
                        title="ðŸ’° Invoice Amount Distribution"
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
            except:
                st.info("Amount data not available for visualization")

    def render_data_table(self, df):
        """Render data explorer table"""
        if df is None or len(df) == 0:
            return

        st.header("ðŸ” Data Explorer")

        # Filters
        col1, col2, col3 = st.columns(3)

        with col1:
            if 'Validation_Status' in df.columns:
                statuses = ['All'] + list(df['Validation_Status'].dropna().unique())
                selected_status = st.selectbox("Filter by Status", statuses)
            else:
                selected_status = 'All'

        with col2:
            if 'Account_Head' in df.columns:
                accounts = ['All'] + list(df['Account_Head'].dropna().unique())
                selected_account = st.selectbox("Filter by Account", accounts[:20])
            else:
                selected_account = 'All'

        with col3:
            if 'Vendor_Name' in df.columns:
                vendors = ['All'] + list(df['Vendor_Name'].dropna().unique())
                selected_vendor = st.selectbox("Filter by Vendor", vendors[:20])
            else:
                selected_vendor = 'All'

        # Apply filters
        filtered_df = df.copy()

        if selected_status != 'All' and 'Validation_Status' in df.columns:
            filtered_df = filtered_df[filtered_df['Validation_Status'] == selected_status]
        if selected_account != 'All' and 'Account_Head' in df.columns:
            filtered_df = filtered_df[filtered_df['Account_Head'] == selected_account]
        if selected_vendor != 'All' and 'Vendor_Name' in df.columns:
            filtered_df = filtered_df[filtered_df['Vendor_Name'] == selected_vendor]

        st.write(f"Showing {len(filtered_df):,} of {len(df):,} invoices")

        # Display table
        display_cols = ['Invoice_Number', 'Vendor_Name', 'Amount', 'Validation_Status']
        available_cols = [col for col in display_cols if col in filtered_df.columns]

        if available_cols:
            st.dataframe(filtered_df[available_cols], use_container_width=True, height=400)

    def render_file_upload(self):
        """Render file upload section"""
        st.sidebar.header("ðŸ“¤ Upload Validation Report")

        uploaded_file = st.sidebar.file_uploader(
            "Upload Excel validation report",
            type=['xlsx', 'xls'],
            help="Upload your validation report to override current data"
        )

        if uploaded_file is not None:
            try:
                df = pd.read_excel(uploaded_file)
                st.sidebar.success(f"âœ… File uploaded: {len(df)} records")
                self.data_sources['uploaded_files'] = True
                self.data_source = "uploaded_files"
                return df, {'source': 'uploaded', 'filename': uploaded_file.name}
            except Exception as e:
                st.sidebar.error(f"Error reading file: {str(e)}")

        return None, {}

    def run(self):
        """Main dashboard execution"""
        # Header
        self.render_header()

        # Connection Status
        self.render_connection_status()

        # Check for file upload
        uploaded_df, uploaded_metadata = self.render_file_upload()

        if uploaded_df is not None:
            df, metadata, source_info = uploaded_df, uploaded_metadata, "ðŸ“¤ Uploaded File"
        else:
            # Load data from primary source
            df, metadata, source_info = self.load_data()

        # Render dashboard sections
        if df is not None and len(df) > 0:
            self.render_metrics(df, source_info)
            self.render_charts(df)
            self.render_data_table(df)

            # Sidebar info
            st.sidebar.header("â„¹ï¸ Data Information")
            st.sidebar.json(metadata)
        else:
            st.error("No data available. Please check your data sources or upload a file.")

        # Refresh button
        if st.sidebar.button("ðŸ”„ Refresh Data"):
            st.rerun()

# Initialize and run dashboard
if __name__ == "__main__":
    try:
        dashboard = GitHubIntegratedDashboard()
        dashboard.run()
    except Exception as e:
        st.error(f"Application Error: {str(e)}")
        st.info("Please contact support if this error persists.")
