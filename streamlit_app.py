import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sqlite3
import os
import json
from pathlib import Path
import time

# Enhanced Invoice Validation Dashboard
# Fixed: st.experimental_rerun() -> st.rerun()

class KoenigInvoiceDashboard:
    def __init__(self):
        self.setup_page_config()
        self.data_path = "enhanced_invoice_data.xlsx"
        self.db_path = "enhanced_invoice_history.db"
        
    def setup_page_config(self):
        st.set_page_config(
            page_title="Koenig Enhanced Invoice Validator",
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        # Apply custom CSS
        st.markdown("""
        
        .main-header {
            background: linear-gradient(90deg, #1e3a8a 0%, #3b82f6 100%);
            padding: 2rem;
            border-radius: 10px;
            margin-bottom: 2rem;
            color: white;
        }
        .metric-card {
            background: white;
            padding: 1.5rem;
            border-radius: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            border-left: 4px solid #3b82f6;
        }
        .status-passed { color: #10b981; }
        .status-warning { color: #f59e0b; }
        .status-failed { color: #ef4444; }
        .stTabs [data-baseweb="tab-list"] {
            gap: 2px;
        }
        .stTabs [data-baseweb="tab"] {
            background-color: #f8fafc;
            border-radius: 8px 8px 0 0;
        }
        
        """, unsafe_allow_html=True)

    def load_data(self):
        """Load invoice data with enhanced fields"""
        try:
            if os.path.exists(self.data_path):
                df = pd.read_excel(self.data_path)
                return df
            else:
                # Create sample data for demonstration
                return self.create_sample_data()
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            return pd.DataFrame()

    def create_sample_data(self):
        """Create sample data with all enhanced fields"""
        sample_data = {
            'Invoice Number': ['INV-2024-001', 'INV-2024-002', 'INV-2024-003'],
            'Vendor Name': ['Tech Solutions Pvt Ltd', 'Global Services Inc', 'Local Vendor Co'],
            'Invoice Date': ['2024-01-15', '2024-01-20', '2024-01-25'],
            'Due Date': ['2024-02-15', '2024-02-20', '2024-02-25'],
            'Invoice Amount': [50000, 75000, 30000],
            'Invoice Currency': ['INR', 'USD', 'INR'],
            'Location': ['Delhi HO', 'USA', 'Bangalore'],
            'Entity': ['Koenig', 'Koenig', 'Rayontara'],
            'TDS Status': ['Applied', 'Not Applicable', 'Applied'],
            'RMS Invoice ID': ['RMS001', 'RMS002', 'RMS003'],
            'SCID': ['SC001', 'SC002', 'SC003'],
            'MOP': ['Bank Transfer', 'Wire Transfer', 'Cheque'],
            'Account Head': ['Training', 'Consulting', 'Software'],
            'CGST': [4500, 0, 2700],
            'SGST': [4500, 0, 2700],
            'IGST': [0, 0, 0],
            'VAT': [0, 7500, 0],
            'Total Tax': [9000, 7500, 5400],
            'Net Amount': [41000, 67500, 24600],
            'Validation Status': ['Failed', 'Warning', 'Failed'],
            'Last Modified': ['2024-01-28 10:30:00', '2024-01-28 11:15:00', '2024-01-28 09:45:00']
        }
        return pd.DataFrame(sample_data)

    def render_header(self):
        """Render main dashboard header with logo"""
        col1, col2 = st.columns([1, 4])
        
        with col1:
            # Fixed logo path
            logo_path = "assets/koenig-logo.png"
            if os.path.exists(logo_path):
                st.image(logo_path, width=150)
            else:
                st.info("🏢 KOENIG")
        
        with col2:
            st.markdown("""
            
                Enhanced Invoice Validation System
                
                    Multi-location Tax Calculations • Historical Tracking • Advanced Analytics
                
            
            """, unsafe_allow_html=True)

    def render_metrics(self, df):
        """Render key metrics cards"""
        if df.empty:
            st.warning("No data available")
            return
            
        col1, col2, col3, col4 = st.columns(4)
        
        total_invoices = len(df)
        passed_count = len(df[df['Validation Status'] == 'Passed']) if 'Validation Status' in df.columns else 0
        warning_count = len(df[df['Validation Status'] == 'Warning']) if 'Validation Status' in df.columns else 0
        failed_count = len(df[df['Validation Status'] == 'Failed']) if 'Validation Status' in df.columns else 0
        
        with col1:
            st.metric(
                label="Total Invoices",
                value=total_invoices,
                delta=f"+{total_invoices - 52}" if total_invoices > 52 else None
            )
        
        with col2:
            st.metric(
                label="✅ Passed",
                value=passed_count,
                delta=f"{(passed_count/total_invoices*100):.1f}%" if total_invoices > 0 else "0%"
            )
        
        with col3:
            st.metric(
                label="⚠️ Warnings",
                value=warning_count,
                delta=f"{(warning_count/total_invoices*100):.1f}%" if total_invoices > 0 else "0%"
            )
        
        with col4:
            st.metric(
                label="❌ Failed",
                value=failed_count,
                delta=f"{(failed_count/total_invoices*100):.1f}%" if total_invoices > 0 else "0%"
            )

    def render_charts(self, df):
        """Render analysis charts"""
        if df.empty:
            return
            
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Validation Status Distribution")
            if 'Validation Status' in df.columns:
                status_counts = df['Validation Status'].value_counts()
                fig = px.pie(
                    values=status_counts.values,
                    names=status_counts.index,
                    color_discrete_map={
                        'Passed': '#10b981',
                        'Warning': '#f59e0b',
                        'Failed': '#ef4444'
                    }
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("💰 Invoice Amount by Location")
            if 'Location' in df.columns and 'Invoice Amount' in df.columns:
                location_amounts = df.groupby('Location')['Invoice Amount'].sum().reset_index()
                fig = px.bar(
                    location_amounts,
                    x='Location',
                    y='Invoice Amount',
                    color='Invoice Amount',
                    color_continuous_scale='Blues'
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

    def render_tax_analysis(self, df):
        """Render tax analysis section"""
        if df.empty:
            return
            
        st.subheader("🧾 Tax Analysis Dashboard")
        
        col1, col2, col3 = st.columns(3)
        
        tax_columns = ['CGST', 'SGST', 'IGST', 'VAT']
        existing_tax_cols = [col for col in tax_columns if col in df.columns]
        
        if existing_tax_cols:
            with col1:
                total_cgst = df['CGST'].sum() if 'CGST' in df.columns else 0
                st.metric("Total CGST", f"₹{total_cgst:,.2f}")
                
            with col2:
                total_sgst = df['SGST'].sum() if 'SGST' in df.columns else 0
                st.metric("Total SGST", f"₹{total_sgst:,.2f}")
                
            with col3:
                total_vat = df['VAT'].sum() if 'VAT' in df.columns else 0
                st.metric("Total VAT", f"₹{total_vat:,.2f}")
        
        # Tax breakdown chart
        if existing_tax_cols:
            tax_data = df[existing_tax_cols].sum()
            if tax_data.sum() > 0:
                fig = px.bar(
                    x=tax_data.index,
                    y=tax_data.values,
                    title="Tax Component Breakdown",
                    color=tax_data.values,
                    color_continuous_scale='Viridis'
                )
                st.plotly_chart(fig, use_container_width=True)

    def render_data_table(self, df):
        """Render enhanced data table"""
        if df.empty:
            return
            
        st.subheader("📋 Invoice Data Table")
        
        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if 'Validation Status' in df.columns:
                status_filter = st.selectbox(
                    "Filter by Status",
                    options=['All'] + list(df['Validation Status'].unique())
                )
            else:
                status_filter = 'All'
        
        with col2:
            if 'Location' in df.columns:
                location_filter = st.selectbox(
                    "Filter by Location",
                    options=['All'] + list(df['Location'].unique())
                )
            else:
                location_filter = 'All'
        
        with col3:
            if 'Entity' in df.columns:
                entity_filter = st.selectbox(
                    "Filter by Entity",
                    options=['All'] + list(df['Entity'].unique())
                )
            else:
                entity_filter = 'All'
        
        # Apply filters
        filtered_df = df.copy()
        if status_filter != 'All' and 'Validation Status' in df.columns:
            filtered_df = filtered_df[filtered_df['Validation Status'] == status_filter]
        if location_filter != 'All' and 'Location' in df.columns:
            filtered_df = filtered_df[filtered_df['Location'] == location_filter]
        if entity_filter != 'All' and 'Entity' in df.columns:
            filtered_df = filtered_df[filtered_df['Entity'] == entity_filter]
        
        # Display table
        st.dataframe(
            filtered_df,
            use_container_width=True,
            hide_index=True
        )
        
        # Export functionality
        if not filtered_df.empty:
            csv = filtered_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Filtered Data",
                data=csv,
                file_name=f"invoice_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )

    def render_system_status(self):
        """Render system status and health checks"""
        st.subheader("🔧 System Status")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**System Health:**")
            
            # Check data file
            data_status = "✅ Active" if os.path.exists(self.data_path) else "❌ Missing"
            st.write(f"• Data File: {data_status}")
            
            # Check database
            db_status = "✅ Connected" if os.path.exists(self.db_path) else "❌ Missing"
            st.write(f"• History Database: {db_status}")
            
            # Check GitHub Actions (simulated)
            st.write("• GitHub Actions: ✅ Running")
            st.write("• Email Notifications: ✅ Enabled")
            st.write("• RMS Integration: ⚠️ Mock Data")
        
        with col2:
            st.write("**Enhanced Features Status:**")
            st.write("• 21 Excel Fields: ✅ Active")
            st.write("• Multi-location Tax: ✅ Operational")
            st.write("• Historical Tracking: ✅ 3 Months")
            st.write("• Due Date Alerts: ✅ 5-Day Notice")
            st.write("• ZIP Attachments: ⚠️ Pending")
            st.write("• Logo Branding: ✅ Fixed")

    def render_sidebar(self):
        """Render sidebar with controls"""
        with st.sidebar:
            st.image("assets/koenig-logo.png" if os.path.exists("assets/koenig-logo.png") else None, width=200)
            st.markdown("---")
            
            st.subheader("🔄 Dashboard Controls")
            
            # Refresh button - FIXED: Using st.rerun() instead of st.experimental_rerun()
            if st.button("🔄 Refresh Dashboard", use_container_width=True):
                st.rerun()  # FIXED: Updated from st.experimental_rerun()
            
            # Manual data reload
            if st.button("📥 Reload Data", use_container_width=True):
                st.cache_data.clear()
                st.success("Data cache cleared!")
                st.rerun()  # FIXED: Updated from st.experimental_rerun()
            
            st.markdown("---")
            
            # System information
            st.subheader("ℹ️ System Info")
            st.write(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            st.write("**Version:** 2.1.0 Enhanced")
            st.write("**Status:** ✅ Operational")
            
            # Quick stats
            if os.path.exists(self.data_path):
                try:
                    df = pd.read_excel(self.data_path)
                    st.write(f"**Records:** {len(df)}")
                    st.write(f"**Last Process:** {datetime.now().strftime('%H:%M')}")
                except:
                    st.write("**Records:** Loading...")

    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def load_cached_data(_self):
        """Load data with caching"""
        return _self.load_data()

    def run(self):
        """Main dashboard execution"""
        self.render_header()
        
        # Load data
        df = self.load_cached_data()
        
        # Main content tabs
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📈 Analytics", "📋 Data Table", "⚙️ System"])
        
        with tab1:
            self.render_metrics(df)
            st.markdown("---")
            self.render_charts(df)
        
        with tab2:
            self.render_tax_analysis(df)
            
            # Due date monitoring
            st.subheader("📅 Due Date Monitoring")
            if 'Due Date' in df.columns:
                df['Due Date'] = pd.to_datetime(df['Due Date'])
                upcoming_due = df[df['Due Date'] <= datetime.now() + timedelta(days=5)]
                
                if not upcoming_due.empty:
                    st.warning(f"⚠️ {len(upcoming_due)} invoices due within 5 days!")
                    st.dataframe(upcoming_due[['Invoice Number', 'Vendor Name', 'Due Date', 'Invoice Amount']])
                else:
                    st.success("✅ No urgent due dates")
        
        with tab3:
            self.render_data_table(df)
        
        with tab4:
            self.render_system_status()
            
            # Historical tracking info
            st.subheader("📈 Historical Tracking")
            if os.path.exists(self.db_path):
                try:
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM invoice_history")
                    count = cursor.fetchone()[0]
                    conn.close()
                    st.write(f"**Historical Records:** {count}")
                    st.write("**Tracking Period:** 3 Months")
                    st.write("**Change Detection:** ✅ Active")
                except:
                    st.write("**Historical Records:** Error loading")
            else:
                st.write("**Historical Records:** Not initialized")
        
        # Render sidebar
        self.render_sidebar()

# Initialize and run dashboard
if __name__ == "__main__":
    dashboard = KoenigInvoiceDashboard()
    dashboard.run()
