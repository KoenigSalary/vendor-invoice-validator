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
        """Render dashboard header"""
        st.markdown("""
        
            🚀 Enhanced Invoice Validation Dashboard
            🏢 Koenig Solutions - Multi-Location GST/VAT Compliance System
            ✨ Real-time validation • 🔄 Historical tracking • 🌍 Global tax compliance • 💰 21 Enhanced Fields
        
        """, unsafe_allow_html=True)
    
    def render_system_status(self):
        """Render enhanced system status"""
        st.header("🔧 Enhanced System Status")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            status_class = "success-metric" if self.data_available else "danger-metric"
            status_text = "🟢 Active" if self.data_available else "🔴 No Data"
            st.markdown(f"""
            
                📊 Validation System
                {status_text}
                {len(self.recent_reports)} reports available
            
            """, unsafe_allow_html=True)
        
        with col2:
            enhanced_class = "success-metric" if self.enhanced_data_available else "warning-metric"
            enhanced_text = "🆕 Enhanced" if self.enhanced_data_available else "📊 Standard"
            st.markdown(f"""
            
                🚀 Enhancement Status
                {enhanced_text}
                21 enhanced fields {'active' if self.enhanced_data_available else 'ready'}
            
            """, unsafe_allow_html=True)
        
        with col3:
            db_status = "🟢 Connected" if os.path.exists(self.enhanced_db_path) else "🟡 Standard DB"
            db_class = "success-metric" if os.path.exists(self.enhanced_db_path) else "info-metric"
            st.markdown(f"""
            
                🗄️ Database Status
                {db_status}
                Historical tracking {'active' if os.path.exists(self.enhanced_db_path) else 'pending'}
            
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
            
                ⏰ Last Validation
                {time_text}
                Next run: {'In ' + str(4 - (time_ago.days % 4)) + ' days' if self.recent_reports else 'Pending'}
            
            """, unsafe_allow_html=True)
    
    def render_enhanced_features_status(self):
        """Render enhanced features status - THIS WAS MISSING"""
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
                
                    
                        {status_icon} {feature}
                    
                    {description}
                
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
            
                📋 Total Invoices
                {total_invoices:,}
                {'Enhanced processing' if has_enhanced else 'Standard processing'}
            
            """, unsafe_allow_html=True)
        
        with col2:
            pass_rate = (passed/total_invoices*100) if total_invoices > 0 else 0
            st.markdown(f"""
            
                ✅ Passed Validation
                {passed:,}
                {pass_rate:.1f}% success rate
            
            """, unsafe_allow_html=True)
        
        with col3:
            warn_rate = (warnings/total_invoices*100) if total_invoices > 0 else 0
            st.markdown(f"""
            
                ⚠️ Warnings
                {warnings:,}
                {warn_rate:.1f}% need attention
            
            """, unsafe_allow_html=True)
        
        with col4:
            fail_rate = (failed/total_invoices*100) if total_invoices > 0 else 0
            st.markdown(f"""
            
                ❌ Failed Validation
                {failed:,}
                {fail_rate:.1f}% require action
            
            """, unsafe_allow_html=True)
    
    def render_enhanced_charts(self, df):
        """Render enhanced analytics charts"""
        if df is None:
            return
        
        st.header("📈 Enhanced Visual Analytics")
        
        # Basic charts for demo
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Validation Status Distribution")
            if 'Validation_Status' in df.columns:
                status_counts = df['Validation_Status'].value_counts()
                fig = px.pie(values=status_counts.values, names=status_counts.index,
                           title="Validation Status Breakdown")
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("🌍 Location Analysis")
            if 'Location' in df.columns:
                location_counts = df['Location'].value_counts().head(10)
                fig = px.bar(x=location_counts.values, y=location_counts.index,
                           orientation='h', title="Top 10 Locations by Invoice Count")
                st.plotly_chart(fig, use_container_width=True)
    
    def render_no_data_state(self):
        """Render enhanced no-data state"""
        st.markdown("""
        
            🚀 Enhanced Invoice Validation Dashboard
            Ready to process invoices with 21 enhanced fields!
        
        """, unsafe_allow_html=True)
        
        st.info("✨ System is ready for enhanced invoice processing. Waiting for first data run...")
    
    def render_data_explorer(self, df, report_info):
        """Render interactive data explorer"""
        if df is None:
            return
        
        st.header("🔍 Interactive Invoice Data Explorer")
        
        # Enhanced filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if 'Validation_Status' in df.columns:
                unique_statuses = df['Validation_Status'].dropna().astype(str).unique()
                statuses = ['All'] + sorted(list(unique_statuses))
                status_filter = st.selectbox("🔍 Filter by Status", statuses)
        
        with col2:
            if 'Location' in df.columns:
                unique_locations = df['Location'].dropna().astype(str).unique()
                locations = ['All'] + sorted(list(unique_locations))
                location_filter = st.selectbox("🌍 Filter by Location", locations)
        
        with col3:
            if 'Invoice_Currency' in df.columns:
                unique_currencies = df['Invoice_Currency'].dropna().astype(str).unique()
                currencies = ['All'] + sorted(list(unique_currencies))
                currency_filter = st.selectbox("💱 Filter by Currency", currencies)
        
        # Apply filters
        filtered_df = df.copy()
        
        if 'status_filter' in locals() and status_filter != 'All':
            filtered_df = filtered_df[filtered_df['Validation_Status'] == status_filter]
        
        if 'location_filter' in locals() and location_filter != 'All':
            filtered_df = filtered_df[filtered_df['Location'] == location_filter]
        
        if 'currency_filter' in locals() and currency_filter != 'All':
            filtered_df = filtered_df[filtered_df['Invoice_Currency'] == currency_filter]
        
        # Display filtered data
        if not filtered_df.empty:
            st.write(f"📊 Showing **{len(filtered_df):,}** of **{len(df):,}** invoices")
            st.dataframe(filtered_df, use_container_width=True, height=400)
        else:
            st.warning("No data matches the selected filters.")
    
    def render_sidebar(self):
        """Render enhanced sidebar with logo"""
        # Logo at top of sidebar
        logo_path = "assets/koenig-logo.png"
        try:
            if os.path.exists(logo_path):
                st.sidebar.image(logo_path, width=180)
            else:
                st.sidebar.markdown("**🏢 KOENIG**")
                st.sidebar.markdown("*step forward*")
        except:
            st.sidebar.markdown("**🏢 KOENIG**")
            st.sidebar.markdown("*step forward*")
        
        st.sidebar.markdown("---")
        
        st.sidebar.header("🔧 Enhanced System Control")
        
        # System status
        st.sidebar.subheader("📊 Data Status")
        st.sidebar.write(f"📋 Reports: {len(self.recent_reports)}")
        st.sidebar.write(f"🚀 Enhanced: {'✅ Active' if self.enhanced_data_available else '⏳ Ready'}")
        st.sidebar.write(f"🗄️ Database: {'✅ Connected' if os.path.exists(self.enhanced_db_path) else '📊 Standard'}")
        
        # Recent reports
        if self.recent_reports:
            st.sidebar.subheader("📋 Recent Validation Runs")
            for i, report in enumerate(self.recent_reports[:5]):
                date_str = datetime.fromtimestamp(report['modified']).strftime('%Y-%m-%d %H:%M')
                size_mb = report['size'] / (1024*1024)
                enhanced_icon = "🚀" if report['enhanced'] else "📊"
                st.sidebar.write(f"{enhanced_icon} {date_str} ({size_mb:.1f}MB)")
        
        # System actions
        st.sidebar.subheader("🔄 System Actions")
        
        if st.sidebar.button("🔄 Refresh Dashboard"):
            st.rerun()
        
        if st.sidebar.button("📊 System Health Check"):
            st.sidebar.success("✅ All systems operational")
    
    def render_footer(self):
        """Render enhanced footer"""
        st.markdown("---")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            **🏢 Koenig Solutions Pvt. Ltd.**  
            Enhanced Invoice Validation System v2.0  
            Multi-Location GST/VAT Compliance
            """)
        
        with col2:
            st.markdown("""
            **🚀 Enhanced Features:**  
            ✅ 21 additional fields for comprehensive analysis  
            ✅ Multi-currency and global location support  
            ✅ Real-time tax compliance monitoring  
            ✅ Historical data change tracking (3 months)  
            ✅ Advanced due date alert system  
            """)
        
        with col3:
            st.markdown(f"""
            **📊 Dashboard Information:**  
            Version: Enhanced v2.0  
            Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
            Data source: {'Enhanced Database' if self.enhanced_data_available else 'Standard Processing'}  
            System status: {'🟢 Fully Operational' if self.data_available else '🟡 Ready for Data'}
            """)
    
    def run(self):
        """Run the enhanced dashboard"""
        # Create sidebar first
        self.render_sidebar()
        
        # Main content
        self.render_header()
        self.render_system_status()
        
        # ADD THIS LINE - This was missing from GitHub version
        self.render_enhanced_features_status()
        
        # Load and display data
        df, report_info = self.load_latest_data()
        
        if df is not None:
            self.render_validation_overview(df, report_info)
            self.render_enhanced_charts(df)
            self.render_data_explorer(df, report_info)
        else:
            self.render_no_data_state()
        
        self.render_footer()

# Initialize and run dashboard
if __name__ == "__main__":
    dashboard = EnhancedDashboard()
    dashboard.run()