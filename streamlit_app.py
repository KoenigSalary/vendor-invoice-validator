import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime
import sqlite3

# Page configuration
st.set_page_config(
    page_title="Koenig Invoice Validator",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

class InvoiceDashboard:
    def __init__(self):
        self.csv_path = self.find_invoice_file()
        self.data = self.load_data()
    
    def find_invoice_file(self):
        """Find the actual invoice CSV file"""
        # Common file names for invoice data
        possible_files = [
            'invoices.csv',
            'invoice_data.csv',
            'enhanced_invoices.csv',
            'vendor_invoices.csv',
            'processed_invoices.csv',
            'data/invoices.csv',
            'data/invoice_data.csv',
            'output/invoices.csv'
        ]
        
        for file_path in possible_files:
            if os.path.exists(file_path):
                return file_path
        
        # Check for any CSV files in current directory
        for file in os.listdir('.'):
            if file.endswith('.csv') and 'invoice' in file.lower():
                return file
        
        return None

    def load_data(self):
        """Load invoice data without showing sample data message"""
        if self.csv_path and os.path.exists(self.csv_path):
            try:
                data = pd.read_csv(self.csv_path)
                return data
            except Exception as e:
                st.error(f"Error loading {self.csv_path}: {str(e)}")
        
        # Create minimal structure for empty state
        return pd.DataFrame(columns=[
            'Invoice_Number', 'Vendor_Name', 'Amount', 
            'Location', 'Status', 'Date'
        ])

    def render_sidebar(self):
        """Render sidebar with logo and controls"""
        # Logo at top
        logo_path = "assets/koenig-logo.png"
        if os.path.exists(logo_path):
            st.sidebar.image(logo_path, width=180)
        else:
            st.sidebar.markdown("**KOENIG**")
            st.sidebar.markdown("*step forward*")
        
        st.sidebar.markdown("---")
        
        # Dashboard Controls
        st.sidebar.header("📊 Dashboard Controls")
        
        if st.sidebar.button("🔄 Refresh Dashboard"):
            st.rerun()
        
        if st.sidebar.button("📥 Reload Data"):
            self.csv_path = self.find_invoice_file()
            self.data = self.load_data()
            st.rerun()
        
        st.sidebar.markdown("---")
        
        # System Info
        st.sidebar.header("⚙️ System Info")
        
        if self.csv_path:
            st.sidebar.success(f"✅ Data source: {os.path.basename(self.csv_path)}")
            st.sidebar.write(f"Records: {len(self.data)}")
        else:
            st.sidebar.warning("⚠️ No invoice data file found")
        
        st.sidebar.write(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        st.sidebar.write("Version: 2.1.0 Enhanced")
        st.sidebar.write("Status: ✅ Operational")

    def render_stats_cards(self):
        """Render statistics cards"""
        if len(self.data) == 0:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Invoices", "0")
            with col2:
                st.metric("✅ Passed", "0")
            with col3:
                st.metric("❌ Failed", "0")
            return
        
        # Calculate stats from actual data
        total = len(self.data)
        
        # Find status column
        status_col = None
        for col in ['Status', 'Validation_Status', 'status', 'validation_status']:
            if col in self.data.columns:
                status_col = col
                break
        
        if status_col:
            passed = len(self.data[self.data[status_col].str.contains('Pass', case=False, na=False)])
            failed = len(self.data[self.data[status_col].str.contains('Fail', case=False, na=False)])
            warnings = total - passed - failed
        else:
            passed = 0
            warnings = 0  
            failed = total
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Invoices", total)
        
        with col2:
            st.metric("✅ Passed", passed, delta=f"{(passed/total*100):.1f}%" if total > 0 else "0%")
        
        with col3:
            st.metric("⚠️ Warnings", warnings, delta=f"{(warnings/total*100):.1f}%" if total > 0 else "0%")
        
        with col4:
            st.metric("❌ Failed", failed, delta=f"{(failed/total*100):.1f}%" if total > 0 else "0%")

    def render_charts(self):
        """Render charts"""
        if len(self.data) == 0:
            st.info("📋 No invoice data available for visualization")
            st.markdown("""
            To get started:
            1. Add your invoice CSV file to the project directory
            2. Make sure it contains columns like: Invoice_Number, Vendor_Name, Amount, Location, Status
            3. Click 'Reload Data' in the sidebar
            """)
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Validation Status Distribution")
            
            # Create status distribution
            status_col = None
            for col in ['Status', 'Validation_Status', 'status']:
                if col in self.data.columns:
                    status_col = col
                    break
            
            if status_col:
                status_counts = self.data[status_col].value_counts()
                fig_pie = px.pie(values=status_counts.values, names=status_counts.index)
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("Status column not found in data")
        
        with col2:
            st.subheader("🏢 Invoice Amount by Location")
            
            if 'Location' in self.data.columns and 'Amount' in self.data.columns:
                location_amounts = self.data.groupby('Location')['Amount'].sum().reset_index()
                fig_bar = px.bar(location_amounts, x='Location', y='Amount')
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.info("Location or Amount column not found in data")

    def render_data_table(self):
        """Render data table"""
        st.subheader("📋 Invoice Data")
        
        if len(self.data) == 0:
            st.info("No data to display")
        else:
            st.dataframe(self.data, use_container_width=True)

    def run(self):
        """Main dashboard runner"""
        # Render sidebar
        self.render_sidebar()
        
        # Main header
        st.markdown("# Enhanced Invoice Validation System")
        st.markdown("**Multi-location Tax Calculations • Historical Tracking • Advanced Analytics**")
        st.markdown("---")
        
        # Tabs
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📈 Analytics", "📋 Data Table", "⚙️ System"])
        
        with tab1:
            self.render_stats_cards()
            st.markdown("---")
            self.render_charts()
        
        with tab2:
            st.subheader("📈 Advanced Analytics")
            self.render_charts()
        
        with tab3:
            self.render_data_table()
        
        with tab4:
            st.subheader("⚙️ System Information")
            
            col1, col2 = st.columns(2)
            with col1:
                st.info(f"**Data Source:** {self.csv_path if self.csv_path else 'None'}")
                st.info(f"**Total Records:** {len(self.data)}")
            
            with col2:
                st.info(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                st.info("**Status:** ✅ Operational")
            
            if len(self.data) > 0:
                st.subheader("Data Schema")
                st.write(self.data.dtypes)

# Initialize and run dashboard
if __name__ == "__main__":
    dashboard = InvoiceDashboard()
    dashboard.run()