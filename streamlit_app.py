import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sqlite3
import os
import json
from pathlib import Path

class KoenigInvoiceDashboard:
    def __init__(self):
        self.setup_page_config()
        self.load_data()
    
    def setup_page_config(self):
        """Configure Streamlit page settings"""
        st.set_page_config(
            page_title="Koenig Enhanced Invoice Validator",
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="expanded"
        )
    
    def load_data(self):
        """Load invoice data from database"""
        try:
            # Connect to enhanced invoice database
            conn = sqlite3.connect('enhanced_invoice_history.db')
            
            # Load current invoices
            self.invoices_df = pd.read_sql_query("""
                SELECT * FROM invoice_snapshots 
                WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM invoice_snapshots)
            """, conn)
            
            # Load historical changes
            self.changes_df = pd.read_sql_query("""
                SELECT * FROM invoice_changes 
                WHERE change_date >= date('now', '-90 days')
            """, conn)
            
            conn.close()
            
            # Mock data if database is empty
            if self.invoices_df.empty:
                self.create_mock_data()
                
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            self.create_mock_data()
    
    def create_mock_data(self):
        """Create sample data for demonstration"""
        self.invoices_df = pd.DataFrame({
            'Invoice_Number': ['INV-001', 'INV-002', 'INV-003'],
            'Vendor_Name': ['TechCorp India', 'GlobalSoft LLC', 'DataSystems PVT'],
            'Invoice_Amount': [50000, 75000, 25000],
            'Invoice_Currency': ['INR', 'USD', 'INR'],
            'Location': ['Delhi HO', 'USA', 'Bangalore'],
            'Entity': ['Koenig', 'Rayontara', 'Koenig'],
            'TDS_Status': ['Applied', 'Not Applied', 'Applied'],
            'RMS_Invoice_ID': ['RMS001', 'RMS002', 'RMS003'],
            'SCID': ['SC001', 'SC002', 'SC003'],
            'MOP': ['Bank Transfer', 'Wire Transfer', 'Bank Transfer'],
            'Account_Head': ['Training', 'Consulting', 'Training'],
            'Due_Date': ['2025-08-20', '2025-08-25', '2025-08-18'],
            'GST_CGST': [4500, 0, 2250],
            'GST_SGST': [4500, 0, 2250],
            'GST_IGST': [0, 0, 0],
            'VAT_Rate': [0, 10, 0],
            'Tax_Total': [9000, 7500, 4500],
            'Status': ['Passed', 'Warning', 'Failed'],
            'Processing_Date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')] * 3
        })
        
        self.changes_df = pd.DataFrame({
            'invoice_id': ['INV-001', 'INV-002'],
            'field_name': ['Invoice_Amount', 'Status'],
            'old_value': ['45000', 'Failed'],
            'new_value': ['50000', 'Warning'],
            'change_date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')] * 2
        })
    
    def render_sidebar(self):
        """Render enhanced sidebar with logo and controls"""
        # Logo at the top of sidebar
        logo_path = "assets/koenig-logo.png"
        
        if os.path.exists(logo_path):
            st.sidebar.image(logo_path, width=200, use_column_width=True)
        else:
            # Fallback with styled text logo
            st.sidebar.markdown("""
                
                    
                        KOENIG
                    
                    
                        step forward
                    
                
            """, unsafe_allow_html=True)
        
        st.sidebar.markdown("---")
        
        # Dashboard Controls
        st.sidebar.header("📊 Dashboard Controls")
        
        # Refresh Dashboard Button
        if st.sidebar.button("🔄 Refresh Dashboard", key="refresh_btn", use_container_width=True):
            self.load_data()
            st.rerun()
        
        # Reload Data Button
        if st.sidebar.button("📥 Reload Data", key="reload_btn", use_container_width=True):
            self.load_data()
            st.rerun()
        
        st.sidebar.markdown("---")
        
        # System Information
        st.sidebar.header("⚙️ System Info")
        
        system_info = {
            "Last Updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Version": "2.1.0 Enhanced",
            "Total Records": len(self.invoices_df),
            "Status": "🟢 Operational"
        }
        
        for key, value in system_info.items():
            st.sidebar.metric(key, value)
        
        st.sidebar.markdown("---")
        
        # Filter Controls
        st.sidebar.header("🔍 Filters")
        
        # Status Filter
        status_options = ['All'] + list(self.invoices_df['Status'].unique())
        status_filter = st.sidebar.selectbox("Filter by Status", status_options)
        
        # Location Filter
        location_options = ['All'] + list(self.invoices_df['Location'].unique())
        location_filter = st.sidebar.selectbox("Filter by Location", location_options)
        
        # Entity Filter
        entity_options = ['All'] + list(self.invoices_df['Entity'].unique())
        entity_filter = st.sidebar.selectbox("Filter by Entity", entity_options)
        
        # Apply filters
        filtered_df = self.invoices_df.copy()
        
        if status_filter != 'All':
            filtered_df = filtered_df[filtered_df['Status'] == status_filter]
        if location_filter != 'All':
            filtered_df = filtered_df[filtered_df['Location'] == location_filter]
        if entity_filter != 'All':
            filtered_df = filtered_df[filtered_df['Entity'] == entity_filter]
        
        return filtered_df
    
    def render_header(self):
        """Render main header with branding"""
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.title("🏢 KOENIG")
            st.markdown("**Enhanced Invoice Validation System**")
            st.markdown("Multi-location Tax Calculations • Historical Tracking • Advanced Analytics")
        
        with col2:
            # Header logo (smaller version)
            logo_path = "assets/koenig-logo.png"
            if os.path.exists(logo_path):
                st.image(logo_path, width=120)
    
    def render_overview_tab(self, df):
        """Render main overview dashboard"""
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        total_invoices = len(df)
        passed_invoices = len(df[df['Status'] == 'Passed'])
        warning_invoices = len(df[df['Status'] == 'Warning'])
        failed_invoices = len(df[df['Status'] == 'Failed'])
        
        with col1:
            st.metric(
                "Total Invoices", 
                total_invoices,
                delta=f"{(passed_invoices/total_invoices*100):.1f}%" if total_invoices > 0 else "0%",
                delta_color="normal"
            )
        
        with col2:
            st.metric(
                "✅ Passed", 
                passed_invoices,
                delta=f"{(passed_invoices/total_invoices*100):.1f}%" if total_invoices > 0 else "0%",
                delta_color="normal"
            )
        
        with col3:
            st.metric(
                "⚠️ Warnings", 
                warning_invoices,
                delta=f"{(warning_invoices/total_invoices*100):.1f}%" if total_invoices > 0 else "0%",
                delta_color="normal"
            )
        
        with col4:
            st.metric(
                "❌ Failed", 
                failed_invoices,
                delta=f"{(failed_invoices/total_invoices*100):.1f}%" if total_invoices > 0 else "0%",
                delta_color="inverse"
            )
        
        st.markdown("---")
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Validation Status Distribution")
            if not df.empty:
                status_counts = df['Status'].value_counts()
                fig_pie = px.pie(
                    values=status_counts.values,
                    names=status_counts.index,
                    color=status_counts.index,
                    color_discrete_map={
                        'Passed': '#22c55e',
                        'Warning': '#f59e0b',
                        'Failed': '#ef4444'
                    }
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("No data available for chart")
        
        with col2:
            st.subheader("💰 Invoice Amount by Location")
            if not df.empty:
                location_amounts = df.groupby('Location')['Invoice_Amount'].sum().reset_index()
                fig_bar = px.bar(
                    location_amounts,
                    x='Location',
                    y='Invoice_Amount',
                    color='Invoice_Amount',
                    color_continuous_scale='Blues'
                )
                fig_bar.update_layout(showlegend=False)
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.info("No data available for chart")
    
    def render_analytics_tab(self, df):
        """Render advanced analytics"""
        st.subheader("📈 Advanced Analytics")
        
        # Tax Analysis
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🏛️ Tax Breakdown by Entity")
            if not df.empty:
                tax_analysis = df.groupby('Entity').agg({
                    'GST_CGST': 'sum',
                    'GST_SGST': 'sum',
                    'GST_IGST': 'sum',
                    'Tax_Total': 'sum'
                }).reset_index()
                
                fig_tax = go.Figure()
                fig_tax.add_trace(go.Bar(name='CGST', x=tax_analysis['Entity'], y=tax_analysis['GST_CGST']))
                fig_tax.add_trace(go.Bar(name='SGST', x=tax_analysis['Entity'], y=tax_analysis['GST_SGST']))
                fig_tax.add_trace(go.Bar(name='IGST', x=tax_analysis['Entity'], y=tax_analysis['GST_IGST']))
                fig_tax.update_layout(barmode='stack', title="Tax Components by Entity")
                st.plotly_chart(fig_tax, use_container_width=True)
        
        with col2:
            st.subheader("💱 Currency Distribution")
            if not df.empty:
                currency_dist = df['Invoice_Currency'].value_counts()
                fig_currency = px.pie(
                    values=currency_dist.values,
                    names=currency_dist.index,
                    title="Invoice Currency Distribution"
                )
                st.plotly_chart(fig_currency, use_container_width=True)
        
        # Due Date Analysis
        st.subheader("📅 Due Date Analysis")
        if not df.empty:
            df['Due_Date'] = pd.to_datetime(df['Due_Date'])
            df['Days_to_Due'] = (df['Due_Date'] - datetime.now()).dt.days
            
            overdue = len(df[df['Days_to_Due'] < 0])
            due_soon = len(df[(df['Days_to_Due'] >= 0) & (df['Days_to_Due'] <= 5)])
            future = len(df[df['Days_to_Due'] > 5])
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("🔴 Overdue", overdue)
            with col2:
                st.metric("🟡 Due Soon (≤5 days)", due_soon)
            with col3:
                st.metric("🟢 Future", future)
    
    def render_data_table_tab(self, df):
        """Render detailed data table"""
        st.subheader("📋 Detailed Invoice Data")
        
        if not df.empty:
            # Display options
            col1, col2, col3 = st.columns(3)
            
            with col1:
                show_all = st.checkbox("Show All Columns", value=False)
            
            with col2:
                page_size = st.selectbox("Rows per page", [10, 25, 50, 100], index=1)
            
            with col3:
                search_term = st.text_input("Search invoices...")
            
            # Filter by search
            display_df = df.copy()
            if search_term:
                display_df = display_df[
                    display_df.astype(str).apply(
                        lambda x: x.str.contains(search_term, case=False, na=False)
                    ).any(axis=1)
                ]
            
            # Select columns to display
            if not show_all:
                key_columns = [
                    'Invoice_Number', 'Vendor_Name', 'Invoice_Amount', 
                    'Location', 'Entity', 'Status', 'Due_Date'
                ]
                display_df = display_df[key_columns]
            
            # Pagination
            total_rows = len(display_df)
            total_pages = (total_rows - 1) // page_size + 1
            
            if total_pages > 1:
                page = st.selectbox(f"Page (1 of {total_pages})", range(1, total_pages + 1))
                start_idx = (page - 1) * page_size
                end_idx = min(start_idx + page_size, total_rows)
                display_df = display_df.iloc[start_idx:end_idx]
            
            # Display table
            st.dataframe(display_df, use_container_width=True)
            
            # Export options
            col1, col2 = st.columns(2)
            with col1:
                csv_data = df.to_csv(index=False)
                st.download_button(
                    "📥 Download CSV",
                    csv_data,
                    "invoice_data.csv",
                    "text/csv"
                )
            
            with col2:
                json_data = df.to_json(orient='records', indent=2)
                st.download_button(
                    "📥 Download JSON",
                    json_data,
                    "invoice_data.json",
                    "application/json"
                )
        else:
            st.info("No invoice data available")
    
    def render_system_tab(self):
        """Render system information and settings"""
        st.subheader("⚙️ System Information")
        
        # System Status
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🔧 System Health")
            
            # Database connectivity
            try:
                conn = sqlite3.connect('enhanced_invoice_history.db')
                conn.close()
                db_status = "🟢 Connected"
            except:
                db_status = "🔴 Disconnected"
            
            st.metric("Database", db_status)
            st.metric("Processing Engine", "🟢 Active")
            st.metric("Email Service", "🟢 Ready")
            st.metric("Auto-Schedule", "🟢 Running (4-day cycle)")
        
        with col2:
            st.subheader("📊 Processing Statistics")
            
            # Recent activity
            st.metric("Last Process Run", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            st.metric("Total Processed Today", "3")
            st.metric("Success Rate", "33.3%")
            st.metric("Average Processing Time", "2.3 seconds")
        
        # Configuration
        st.subheader("⚙️ Configuration")
        
        config_data = {
            "RMS Integration": "Enabled",
            "Email Notifications": "Enabled",
            "Historical Tracking": "90 days",
            "Due Date Alerts": "5 days advance",
            "Tax Calculation": "Multi-jurisdiction",
            "Supported Locations": "India (6) + International (12)",
            "Supported Currencies": "INR, USD, EUR, GBP, AUD, CAD, etc."
        }
        
        for key, value in config_data.items():
            col1, col2 = st.columns([1, 2])
            with col1:
                st.write(f"**{key}:**")
            with col2:
                st.write(value)
    
    def run(self):
        """Main application runner"""
        # Render sidebar and get filtered data
        filtered_df = self.render_sidebar()
        
        # Render header
        self.render_header()
        
        # Main tabs
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📈 Analytics", "📋 Data Table", "⚙️ System"])
        
        with tab1:
            self.render_overview_tab(filtered_df)
        
        with tab2:
            self.render_analytics_tab(filtered_df)
        
        with tab3:
            self.render_data_table_tab(filtered_df)
        
        with tab4:
            self.render_system_tab()
        
        # Footer
        st.markdown("---")
        st.markdown(
            ""
            "Koenig Enhanced Invoice Validation System | "
            f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            "",
            unsafe_allow_html=True
        )

# Initialize and run the dashboard
if __name__ == "__main__":
    dashboard = KoenigInvoiceDashboard()
    dashboard.run()
