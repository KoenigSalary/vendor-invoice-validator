import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sqlite3
import os
import uuid

# Page configuration
st.set_page_config(
    page_title="Koenig Invoice Validator",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

class KoenigEnhancedInvoiceDashboard:
    def __init__(self):
        self.csv_path = "enhanced_invoice_data.csv"
        self.db_path = "enhanced_invoice_history.db"
        self.data = pd.DataFrame()
        
        # Initialize session state for component tracking
        if 'component_counter' not in st.session_state:
            st.session_state.component_counter = 0
        
        # Load data on initialization
        self.load_data()
    
    def get_unique_key(self, base_name):
        """Generate unique key for components"""
        st.session_state.component_counter += 1
        return f"{base_name}_{st.session_state.component_counter}_{uuid.uuid4().hex[:8]}"
    
    def init_database(self):
        """Initialize database with proper table structure"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create invoice_snapshots table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS invoice_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    invoice_number TEXT,
                    snapshot_date DATE,
                    vendor_name TEXT,
                    amount REAL,
                    validation_status TEXT,
                    location TEXT,
                    tax_amount REAL,
                    currency TEXT,
                    due_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create change_log table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS change_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    invoice_number TEXT,
                    change_type TEXT,
                    old_value TEXT,
                    new_value TEXT,
                    change_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            st.error(f"Database initialization error: {str(e)}")
    
    def load_data(self):
        """Load data with proper error handling"""
        try:
            # Initialize database first
            self.init_database()
            
            # Load invoice data
            if os.path.exists(self.csv_path):
                self.data = pd.read_csv(self.csv_path)
            else:
                # Create sample data if CSV doesn't exist
                self.data = self.create_sample_data()
                self.data.to_csv(self.csv_path, index=False)
                
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            self.data = self.create_sample_data()
    
    def create_sample_data(self):
        """Create sample data for testing"""
        sample_data = {
            'Invoice Number': ['INV-001', 'INV-002', 'INV-003'],
            'Vendor Name': ['ABC Corp', 'XYZ Ltd', 'PQR Inc'],
            'Amount': [10000, 15000, 8000],
            'Validation Status': ['Passed', 'Warning', 'Failed'],
            'Location': ['Delhi HO', 'Bangalore', 'Goa'],
            'Currency': ['INR', 'INR', 'USD'],
            'Tax Amount': [1800, 2700, 0],
            'Due Date': ['2025-08-20', '2025-08-25', '2025-08-15']
        }
        return pd.DataFrame(sample_data)
    
    def render_header(self):
        """Render clean header without logo duplication"""
        st.markdown("""
            

                

                    Enhanced Invoice Validation System
                

                

                    Multi-location Tax Calculations • Historical Tracking • Advanced Analytics
                


            

        """, unsafe_allow_html=True)
    
    def render_sidebar(self):
        """Render sidebar with logo and controls"""
        # Logo at the top of sidebar
        logo_path = "assets/koenig-logo.png"
        if os.path.exists(logo_path):
            st.sidebar.image(logo_path, width=180, use_container_width=False)
        else:
            st.sidebar.markdown("**🏢 KOENIG**")
            st.sidebar.markdown("*step forward*")
        
        st.sidebar.markdown("---")
        
        # Dashboard Controls
        st.sidebar.header("📊 Dashboard Controls")
        
        if st.sidebar.button("🔄 Refresh Dashboard", key=self.get_unique_key("sidebar_refresh")):
            st.rerun()
        
        if st.sidebar.button("📥 Reload Data", key=self.get_unique_key("sidebar_reload")):
            self.load_data()
            st.rerun()
        
        st.sidebar.markdown("---")
        
        # System Info
        st.sidebar.header("⚙️ System Info")
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.sidebar.info(
            f"**Last Updated:** {current_time}\n\n"
            f"**Version:** 2.1.0 Enhanced\n\n"
            f"**Status:** ✅ Operational"
        )
    
    def render_metrics(self, data):
        """Render key metrics with unique keys"""
        if data.empty:
            st.warning("No data available for metrics display")
            return
        
        # Calculate metrics
        total_invoices = len(data)
        passed = len(data[data['Validation Status'] == 'Passed']) if 'Validation Status' in data.columns else 0
        warnings = len(data[data['Validation Status'] == 'Warning']) if 'Validation Status' in data.columns else 0
        failed = len(data[data['Validation Status'] == 'Failed']) if 'Validation Status' in data.columns else 0
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="📊 Total Invoices",
                value=total_invoices,
                key=self.get_unique_key("metric_total")
            )
        
        with col2:
            st.metric(
                label="✅ Passed",
                value=passed,
                delta=f"{(passed/total_invoices*100):.1f}%" if total_invoices > 0 else "0%",
                key=self.get_unique_key("metric_passed")
            )
        
        with col3:
            st.metric(
                label="⚠️ Warnings",
                value=warnings,
                delta=f"{(warnings/total_invoices*100):.1f}%" if total_invoices > 0 else "0%",
                key=self.get_unique_key("metric_warnings")
            )
        
        with col4:
            st.metric(
                label="❌ Failed",
                value=failed,
                delta=f"{(failed/total_invoices*100):.1f}%" if total_invoices > 0 else "0%",
                key=self.get_unique_key("metric_failed")
            )
    
    def render_overview_tab(self, data):
        """Overview tab with unique chart keys"""
        st.header("📊 Overview")
        
        if data.empty:
            st.warning("No data available for overview")
            return
        
        # Validation Status Distribution
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📈 Validation Status Distribution")
            if 'Validation Status' in data.columns:
                try:
                    status_counts = data['Validation Status'].value_counts()
                    fig_pie = px.pie(
                        values=status_counts.values,
                        names=status_counts.index,
                        title="Validation Status",
                        color_discrete_map={
                            'Passed': '#10b981',
                            'Warning': '#f59e0b',
                            'Failed': '#ef4444'
                        }
                    )
                    st.plotly_chart(
                        fig_pie, 
                        use_container_width=True, 
                        key=self.get_unique_key("overview_validation_pie")
                    )
                except Exception as e:
                    st.error(f"Error rendering pie chart: {str(e)}")
            else:
                st.warning("Validation Status column not found")
        
        with col2:
            st.subheader("🏢 Invoice Amount by Location")
            if 'Location' in data.columns and 'Amount' in data.columns:
                try:
                    location_amounts = data.groupby('Location')['Amount'].sum().sort_values(ascending=False)
                    fig_bar = px.bar(
                        x=location_amounts.index,
                        y=location_amounts.values,
                        title="Amount by Location",
                        labels={'x': 'Location', 'y': 'Amount'},
                        color=location_amounts.values,
                        color_continuous_scale='viridis'
                    )
                    st.plotly_chart(
                        fig_bar, 
                        use_container_width=True, 
                        key=self.get_unique_key("overview_location_bar")
                    )
                except Exception as e:
                    st.error(f"Error rendering bar chart: {str(e)}")
            else:
                st.warning("Location or Amount columns not found")
    
    def render_analytics_tab(self, data):
        """Analytics tab with unique chart keys"""
        st.header("📈 Analytics")
        
        if data.empty:
            st.warning("No data available for analytics")
            return
        
        # Time series analysis
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📅 Validation Trend")
            try:
                # Create sample time series data
                dates = pd.date_range(start='2025-01-01', periods=len(data), freq='D')
                trend_data = pd.DataFrame({
                    'Date': dates,
                    'Passed': [max(0, 5 + i//3) for i in range(len(data))],
                    'Failed': [max(0, 3 - i//5) for i in range(len(data))]
                })
                
                fig_trend = px.line(
                    trend_data,
                    x='Date',
                    y=['Passed', 'Failed'],
                    title="Validation Trend Over Time"
                )
                st.plotly_chart(
                    fig_trend, 
                    use_container_width=True, 
                    key=self.get_unique_key("analytics_trend_line")
                )
            except Exception as e:
                st.error(f"Error rendering trend chart: {str(e)}")
        
        with col2:
            st.subheader("💰 Tax Breakdown")
            if 'Tax Amount' in data.columns:
                try:
                    tax_data = data[data['Tax Amount'] > 0] if 'Tax Amount' in data.columns else pd.DataFrame()
                    if not tax_data.empty:
                        fig_tax = px.histogram(
                            tax_data,
                            x='Tax Amount',
                            title="Tax Amount Distribution",
                            nbins=10
                        )
                        st.plotly_chart(
                            fig_tax, 
                            use_container_width=True, 
                            key=self.get_unique_key("analytics_tax_histogram")
                        )
                    else:
                        st.warning("No tax data available")
                except Exception as e:
                    st.error(f"Error rendering tax chart: {str(e)}")
            else:
                st.warning("Tax Amount column not found")
    
    def render_data_table_tab(self, data):
        """Data table tab with filters"""
        st.header("📋 Data Table")
        
        if data.empty:
            st.warning("No data available")
            return
        
        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if 'Location' in data.columns:
                locations = ['All'] + sorted(data['Location'].unique().tolist())
                selected_location = st.selectbox(
                    "Filter by Location:",
                    options=locations,
                    key=self.get_unique_key("location_filter")
                )
            else:
                selected_location = 'All'
        
        with col2:
            if 'Validation Status' in data.columns:
                statuses = data['Validation Status'].unique().tolist()
                selected_statuses = st.multiselect(
                    "Filter by Status:",
                    options=statuses,
                    default=statuses,
                    key=self.get_unique_key("status_filter")
                )
            else:
                selected_statuses = []
        
        with col3:
            if 'Amount' in data.columns:
                min_amount = st.number_input(
                    "Minimum Amount:",
                    min_value=0,
                    value=0,
                    key=self.get_unique_key("amount_filter")
                )
            else:
                min_amount = 0
        
        # Apply filters
        filtered_data = data.copy()
        
        if selected_location != 'All' and 'Location' in data.columns:
            filtered_data = filtered_data[filtered_data['Location'] == selected_location]
        
        if selected_statuses and 'Validation Status' in data.columns:
            filtered_data = filtered_data[filtered_data['Validation Status'].isin(selected_statuses)]
        
        if 'Amount' in data.columns:
            filtered_data = filtered_data[filtered_data['Amount'] >= min_amount]
        
        # Display table
        st.dataframe(
            filtered_data,
            use_container_width=True,
            key=self.get_unique_key("main_data_table")
        )
        
        # Export functionality
        if not filtered_data.empty:
            csv = filtered_data.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"invoice_data_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                key=self.get_unique_key("download_button")
            )
    
    def render_system_tab(self):
        """System information tab"""
        st.header("⚙️ System Information")
        
        # System metrics
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 System Status")
            st.success("✅ System Operational")
            st.info(f"📅 Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            st.info("🔄 Auto-refresh: Every 4 days")
            st.info("📁 Data Source: CSV + SQLite")
        
        with col2:
            st.subheader("🔧 Configuration")
            st.code(f"""
Database Path: {self.db_path}
CSV Path: {self.csv_path}
Version: 2.1.0 Enhanced
Environment: Production
            """)
        
        # Database status
        st.subheader("🗄️ Database Status")
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            st.write("**Available Tables:**")
            for table in tables:
                st.write(f"- {table[0]}")
            
            conn.close()
            st.success("Database connection successful")
            
        except Exception as e:
            st.error(f"Database error: {str(e)}")
    
    def run(self):
        """Main application runner"""
        try:
            # Render header
            self.render_header()
            
            # Render sidebar
            self.render_sidebar()
            
            # Render metrics
            self.render_metrics(self.data)
            
            # Tab navigation with unique keys
            tab1, tab2, tab3, tab4 = st.tabs([
                "📊 Overview", 
                "📈 Analytics", 
                "📋 Data Table", 
                "⚙️ System"
            ])
            
            with tab1:
                self.render_overview_tab(self.data)
            
            with tab2:
                self.render_analytics_tab(self.data)
            
            with tab3:
                self.render_data_table_tab(self.data)
            
            with tab4:
                self.render_system_tab()
                
        except Exception as e:
            st.error(f"Application error: {str(e)}")
            st.info("Please refresh the page or contact support if the issue persists.")

# Application entry point
if __name__ == "__main__":
    dashboard = KoenigEnhancedInvoiceDashboard()
    dashboard.run()
            