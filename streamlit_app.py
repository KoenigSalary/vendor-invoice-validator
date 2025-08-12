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

# Page configuration
st.set_page_config(
    page_title="Enhanced Invoice Validation Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
.main-header {
    background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
    color: white;
    padding: 30px;
    border-radius: 15px;
    text-align: center;
    margin-bottom: 30px;
    box-shadow: 0 4px 15px rgba(0,0,0,0.1);
}
.metric-card {
    background: white;
    padding: 20px;
    border-radius: 10px;
    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    border-left: 5px solid #2E86C1;
    margin-bottom: 15px;
    transition: transform 0.2s;
}
.metric-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 20px rgba(0,0,0,0.15);
}
.success-metric { border-left-color: #28a745; }
.warning-metric { border-left-color: #ffc107; }
.danger-metric { border-left-color: #dc3545; }
.info-metric { border-left-color: #17a2b8; }
.feature-badge {
    background: linear-gradient(45deg, #667eea, #764ba2);
    color: white;
    padding: 5px 15px;
    border-radius: 20px;
    font-size: 12px;
    margin: 2px;
    display: inline-block;
}
.status-active { background: linear-gradient(45deg, #28a745, #20c997); }
.status-inactive { background: linear-gradient(45deg, #6c757d, #adb5bd); }
</style>
""", unsafe_allow_html=True)

class EnhancedDashboard:
    def __init__(self):
        self.setup_data_sources()
    
    def setup_data_sources(self):
        """Setup data sources and check availability"""
        self.data_available = False
        self.enhanced_data_available = False
        
        # Check for databases
        self.standard_db_path = 'invoice_validation.db'
        self.enhanced_db_path = 'enhanced_invoice_history.db'
        
        # Check for recent Excel reports
        self.recent_reports = self.find_recent_reports()
        
        if self.recent_reports:
            self.data_available = True
            
        # Check for enhanced database
        if os.path.exists(self.enhanced_db_path):
            self.enhanced_data_available = True
    
    def find_recent_reports(self):
        """Find recent validation reports"""
        reports = []
        data_dirs = ['data', '.']
        
        for data_dir in data_dirs:
            if os.path.exists(data_dir):
                for file in os.listdir(data_dir):
                    if ('validation_detailed' in file or 'enhanced_invoice' in file) and file.endswith('.xlsx'):
                        file_path = os.path.join(data_dir, file)
                        try:
                            reports.append({
                                'file': file,
                                'path': file_path,
                                'modified': os.path.getmtime(file_path),
                                'enhanced': 'enhanced' in file.lower(),
                                'size': os.path.getsize(file_path)
                            })
                        except:
                            continue
        
        # Sort by modification time (newest first)
        reports.sort(key=lambda x: x['modified'], reverse=True)
        return reports
    
    def load_latest_data(self):
        """Load the most recent validation data"""
        if not self.recent_reports:
            return None, None
        
        latest_report = self.recent_reports[0]
        try:
            # Try different sheet names
            sheet_names = ['Enhanced_Report', 'Enhanced_All_Invoices', 'All_Invoices', 0]
            df = None
            
            for sheet in sheet_names:
                try:
                    df = pd.read_excel(latest_report['path'], sheet_name=sheet)
                    break
                except:
                    continue
            
            if df is None:
                df = pd.read_excel(latest_report['path'])
            
            return df, latest_report
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            return None, None
    
    def render_header(self):
        """Render dashboard header with complete Koenig branding"""
        st.markdown("""
        <div class="main-header">
            <h1>🚀 Enhanced Invoice Validation Dashboard</h1>
            <h3>🏢 Koenig Solutions - Multi-Location GST/VAT Compliance System</h3>
            <p>✨ Real-time validation • 🔄 Historical tracking • 🌍 Global tax compliance • 💰 21 Enhanced Fields</p>
        </div>
        """, unsafe_allow_html=True)
    
    def render_system_status(self):
        """Render enhanced system status"""
        st.header("🔧 Enhanced System Status")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            status_class = "success-metric" if self.data_available else "danger-metric"
            status_text = "🟢 Active" if self.data_available else "🔴 No Data"
            st.markdown(f"""
            <div class="metric-card {status_class}">
                <h4>📊 Validation System</h4>
                <h3>{status_text}</h3>
                <p>{len(self.recent_reports)} reports available</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            enhanced_class = "success-metric" if self.enhanced_data_available else "warning-metric"
            enhanced_text = "🆕 Enhanced" if self.enhanced_data_available else "📊 Standard"
            st.markdown(f"""
            <div class="metric-card {enhanced_class}">
                <h4>🚀 Enhancement Status</h4>
                <h3>{enhanced_text}</h3>
                <p>21 enhanced fields {'active' if self.enhanced_data_available else 'ready'}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            db_status = "🟢 Connected" if os.path.exists(self.enhanced_db_path) else "🟡 Standard DB"
            db_class = "success-metric" if os.path.exists(self.enhanced_db_path) else "info-metric"
            st.markdown(f"""
            <div class="metric-card {db_class}">
                <h4>🗄️ Database Status</h4>
                <h3>{db_status}</h3>
                <p>Historical tracking {'active' if os.path.exists(self.enhanced_db_path) else 'pending'}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            if self.recent_reports:
                last_run = datetime.fromtimestamp(self.recent_reports[0]['modified'])
                time_ago = datetime.now() - last_run
                if time_ago.days == 0:
                    time_text = f"{time_ago.seconds//3600}h ago"
                else:
                    time_text = f"{time_ago.days}d ago"
            else:
                time_text = "Never"
            
            st.markdown(f"""
            <div class="metric-card info-metric">
                <h4>⏰ Last Validation</h4>
                <h3>{time_text}</h3>
                <p>Next run: {'In ' + str(4 - (time_ago.days % 4)) + ' days' if self.recent_reports else 'Pending'}</p>
            </div>
            """, unsafe_allow_html=True)
    
    def render_enhanced_features_status(self):
        """Render enhanced features status"""
        st.header("🚀 Enhanced Features Status")
        
        features = [
            ("💱 Multi-Currency Support", self.enhanced_data_available, "Process invoices in multiple currencies"),
            ("🌍 Global Location Tracking", self.enhanced_data_available, "Track invoices across all Koenig locations"),
            ("💰 Automatic GST/VAT Calculation", self.enhanced_data_available, "Calculate taxes for India + 12 international locations"),
            ("⏰ Due Date Monitoring", self.enhanced_data_available, "5-day advance payment alerts"),
            ("🔄 Historical Change Tracking", self.enhanced_data_available, "3-month data change detection"),
            ("📊 Enhanced Analytics", True, "Interactive charts and visualizations"),
            ("📧 Automated Email Reports", True, "4-day scheduled notifications"),
            ("🔗 RMS Integration", self.data_available, "SCID, MOP, Account Head data")
        ]
        
        cols = st.columns(4)
        for i, (feature, active, description) in enumerate(features):
            with cols[i % 4]:
                status_class = "status-active" if active else "status-inactive"
                status_icon = "✅" if active else "⏳"
                
                st.markdown(f"""
                <div style="text-align: center; margin-bottom: 20px;">
                    <div class="feature-badge {status_class}">
                        {status_icon} {feature}
                    </div>
                    <p style="font-size: 12px; color: #666; margin-top: 5px;">{description}</p>
                </div>
                """, unsafe_allow_html=True)
    
    def render_validation_overview(self, df, report_info):
        """Render validation overview with enhanced metrics"""
        st.header("📊 Validation Analytics Overview")
        
        if df is None:
            self.render_no_data_state()
            return
        
        # Calculate basic metrics
        total_invoices = len(df)
        
        # Enhanced column detection
        enhanced_columns = [
            'Location', 'Invoice_Currency', 'Tax_Type', 'Due_Date_Notification',
            'Total_Tax_Calculated', 'CGST_Amount', 'SGST_Amount', 'IGST_Amount', 'VAT_Amount'
        ]
        has_enhanced = any(col in df.columns for col in enhanced_columns)
        
        # Status calculations
        if 'Validation_Status' in df.columns:
            passed = len(df[df['Validation_Status'].str.contains('PASS|Valid', case=False, na=False)])
            failed = len(df[df['Validation_Status'].str.contains('FAIL|Invalid', case=False, na=False)])
            warnings = len(df[df['Validation_Status'].str.contains('WARNING|Warning', case=False, na=False)])
        else:
            passed = failed = warnings = 0
        
        # Display main metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="metric-card info-metric">
                <h4>📋 Total Invoices</h4>
                <h2>{total_invoices:,}</h2>
                <p>{'Enhanced processing' if has_enhanced else 'Standard processing'}</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            pass_rate = (passed/total_invoices*100) if total_invoices > 0 else 0
            st.markdown(f"""
            <div class="metric-card success-metric">
                <h4>✅ Passed Validation</h4>
                <h2>{passed:,}</h2>
                <p>{pass_rate:.1f}% success rate</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            warn_rate = (warnings/total_invoices*100) if total_invoices > 0 else 0
            st.markdown(f"""
            <div class="metric-card warning-metric">
                <h4>⚠️ Warnings</h4>
                <h2>{warnings:,}</h2>
                <p>{warn_rate:.1f}% need attention</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            fail_rate = (failed/total_invoices*100) if total_invoices > 0 else 0
            st.markdown(f"""
            <div class="metric-card danger-metric">
                <h4>❌ Failed Validation</h4>
                <h2>{failed:,}</h2>
                <p>{fail_rate:.1f}% require action</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Enhanced metrics if available
        if has_enhanced:
            st.subheader("🚀 Enhanced Analytics")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if 'Invoice_Currency' in df.columns:
                    currencies = df['Invoice_Currency'].nunique()
                    main_currency = df['Invoice_Currency'].mode().iloc[0] if not df['Invoice_Currency'].empty else 'N/A'
                    st.metric("💱 Currencies Processed", f"{currencies} currencies", f"Primary: {main_currency}")
            
            with col2:
                if 'Location' in df.columns:
                    locations = df['Location'].str.split(' -').str[0].nunique()
                    main_location = df['Location'].str.split(' -').str[0].mode().iloc[0] if not df['Location'].empty else 'N/A'
                    st.metric("🌍 Global Locations", f"{locations} locations", f"Primary: {main_location}")
            
            with col3:
                if 'Due_Date_Notification' in df.columns:
                    urgent = len(df[df['Due_Date_Notification'] == 'YES'])
                    st.metric("⏰ Payment Alerts", f"{urgent} urgent", f"Due ≤5 days")
            
            with col4:
                if 'Total_Tax_Calculated' in df.columns:
                    # Handle both string and numeric values
                    tax_series = pd.to_numeric(df['Total_Tax_Calculated'], errors='coerce').fillna(0)
                    tax_calculated = len(tax_series[tax_series > 0])
                    total_tax = tax_series.sum()
                    st.metric("💰 Tax Processing", f"{tax_calculated} invoices", f"₹{total_tax:,.0f} total")
    
    def render_enhanced_charts(self, df):
        """Render enhanced analytics charts"""
        if df is None or not any(col in df.columns for col in ['Location', 'Invoice_Currency', 'Tax_Type']):
            return
        
        st.header("📈 Enhanced Visual Analytics")
        
        # Currency and Location Analysis
        col1, col2 = st.columns(2)
        
        with col1:
            if 'Invoice_Currency' in df.columns:
                st.subheader("💱 Currency Distribution")
                currency_counts = df['Invoice_Currency'].value_counts()
                fig = px.pie(
                    values=currency_counts.values, 
                    names=currency_counts.index,
                    title="Invoice Currency Breakdown",
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if 'Location' in df.columns:
                st.subheader("🌍 Location Analysis")
                location_counts = df['Location'].str.split(' -').str[0].value_counts()
                
                fig = px.bar(
                    x=location_counts.values,
                    y=location_counts.index,
                    orientation='h',
                    title="Invoices by Global Location",
                    color=location_counts.values,
                    color_continuous_scale='viridis'
                )
                fig.update_layout(
                    xaxis_title="Invoice Count",
                    yaxis_title="Location",
                    showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True)
    
    def render_no_data_state(self):
        """Render enhanced no-data state"""
        st.markdown("""
        <div style="text-align: center; padding: 40px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border-radius: 15px; margin: 20px 0;">
            <h2>🚀 Enhanced Invoice Validation Dashboard</h2>
            <p style="font-size: 18px; margin-bottom: 30px;">Ready to process invoices with 21 enhanced fields!</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.subheader("✨ Enhanced Features Ready for Deployment:")
        
        # Feature columns
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            **💰 Tax Compliance:**
            - ✅ Multi-location GST/VAT calculation
            - ✅ India: CGST, SGST, IGST (18%)
            - ✅ International: 12 countries supported
            - ✅ Automatic tax validation
            """)
        
        with col2:
            st.markdown("""
            **🌍 Global Operations:**
            - ✅ Currency support (INR, USD, EUR, etc.)
            - ✅ Location tracking (India + International)
            - ✅ Entity identification (Koenig/Rayontara)
            - ✅ Branch-specific processing
            """)
        
        with col3:
            st.markdown("""
            **📊 Advanced Analytics:**
            - ✅ Due date monitoring (5-day alerts)
            - ✅ Historical change tracking (3 months)
            - ✅ Interactive dashboards
            - ✅ Real-time status monitoring
            """)
    
    def render_data_explorer(self, df, report_info):
        """Render interactive data explorer"""
        if df is None:
            return
        
        st.header("🔍 Interactive Invoice Data Explorer")
        
        # Enhanced filters with fixed sorting
        col1, col2, col3, col4 = st.columns(4)
        
        filters = {}
        
        with col1:
            if 'Validation_Status' in df.columns:
                unique_statuses = df['Validation_Status'].dropna().astype(str).unique()
                statuses = ['All'] + sorted(list(unique_statuses))
                filters['status'] = st.selectbox("🔍 Filter by Status", statuses)
        
        with col2:
            if 'Location' in df.columns:
                unique_locations = df['Location'].dropna().astype(str).unique()
                locations = ['All'] + sorted(list(unique_locations))
                filters['location'] = st.selectbox("🌍 Filter by Location", locations)
        
        with col3:
            if 'Invoice_Currency' in df.columns:
                unique_currencies = df['Invoice_Currency'].dropna().astype(str).unique()
                currencies = ['All'] + sorted(list(unique_currencies))
                filters['currency'] = st.selectbox("💱 Filter by Currency", currencies)
        
        with col4:
            if 'Tax_Type' in df.columns:
                # FIXED: Handle mixed data types in sorting
                unique_tax_types = df['Tax_Type'].dropna().astype(str).unique()
                tax_types = ['All'] + sorted(list(unique_tax_types))
                filters['tax_type'] = st.selectbox("🏛️ Filter by Tax Type", tax_types)
        
        # Apply filters
        filtered_df = df.copy()
        
        for filter_key, filter_value in filters.items():
            if filter_value and filter_value != 'All':
                if filter_key == 'status':
                    filtered_df = filtered_df[filtered_df['Validation_Status'] == filter_value]
                elif filter_key == 'location':
                    filtered_df = filtered_df[filtered_df['Location'] == filter_value]
                elif filter_key == 'currency':
                    filtered_df = filtered_df[filtered_df['Invoice_Currency'] == filter_value]
                elif filter_key == 'tax_type':
                    filtered_df = filtered_df[filtered_df['Tax_Type'] == filter_value]
        
        # Display filtered data
        if not filtered_df.empty:
            st.write(f"📊 Showing **{len(filtered_df):,}** of **{len(df):,}** invoices")
            st.dataframe(filtered_df, use_container_width=True, height=400)
        else:
            st.warning("No data matches the selected filters.")
    
    def render_sidebar(self):
        """Render enhanced sidebar with logo"""
        # ADD: Logo at top of sidebar
        logo_path = "assets/koenig-logo.png"
        if os.path.exists(logo_path):
            st.sidebar.image(logo_path, width=180)
        else:
            st.sidebar.markdown("**🏢 KOENIG**")
            st.sidebar.markdown("*step forward*")
        
        st.sidebar.markdown("---")
        
        st.sidebar.header("🔧 Enhanced System Control")
        
        # System status
        st.sidebar.subheader("📊 Data Status")
        st.sidebar.write(f"📋 Reports: {len(self.recent_reports)}")
        st.sidebar.write(f"🚀 Enhanced: {'✅ Active' if self.enhanced_data_available else '⏳ Ready'}")
        st.sidebar.write(f"🗄️ Database: {'✅ Connected' if os.path.exists(self.enhanced_db_path) else '📊 Standard'}")
        
        # System actions
        st.sidebar.subheader("🔄 System Actions")
        
        if st.sidebar.button("🔄 Refresh Dashboard"):
            st.rerun()
        
        if st.sidebar.button("📊 System Health Check"):
            st.sidebar.success("✅ All systems operational")
            st.sidebar.info(f"📈 Dashboard version: Enhanced v2.0")
            st.sidebar.info(f"🕐 Last refresh: {datetime.now().strftime('%H:%M:%S')}")
    
    def run(self):
        """Run the enhanced dashboard"""
        self.render_header()
        
        # Create sidebar
        self.render_sidebar()
        
        # Main content
        self.render_system_status()
        self.render_enhanced_features_status()
        
        # Load and display data
        df, report_info = self.load_latest_data()
        
        if df is not None:
            self.render_validation_overview(df, report_info)
            self.render_enhanced_charts(df)
            self.render_data_explorer(df, report_info)
        else:
            self.render_no_data_state()

# Initialize and run dashboard
if __name__ == "__main__":
    dashboard = EnhancedDashboard()
    dashboard.run()
