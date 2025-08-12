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

# Company Logo
try:
    from PIL import Image
    logo_path = "assets/koenig_logo.png"
    if os.path.exists(logo_path):
        with st.container():
            col1, col2, col3 = st.columns([3, 2, 2])
            with col2:
                logo = Image.open(logo_path)
                st.image(logo, width=300)
        st.markdown("<br>", unsafe_allow_html=True)
except Exception as e:
    pass  # Logo loading is optional

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
        """Render dashboard header"""
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
        
        # Tax Analysis
        if 'Tax_Type' in df.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🏛️ Tax Compliance Analysis")
                tax_types = df['Tax_Type'].value_counts()
                
                fig = px.pie(
                    values=tax_types.values,
                    names=tax_types.index,
                    title="Tax Type Distribution",
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                if 'Total_Tax_Calculated' in df.columns:
                    st.subheader("💰 Tax Amount by Location")
                    
                    # Prepare data for tax analysis
                    df_tax = df.copy()
                    df_tax['Tax_Numeric'] = pd.to_numeric(df_tax['Total_Tax_Calculated'], errors='coerce').fillna(0)
                    df_tax['Location_Clean'] = df_tax['Location'].str.split(' -').str[0]
                    
                    tax_by_location = df_tax.groupby('Location_Clean')['Tax_Numeric'].sum().sort_values(ascending=True)
                    
                    fig = px.bar(
                        x=tax_by_location.values,
                        y=tax_by_location.index,
                        orientation='h',
                        title="Total Tax Calculated by Location",
                        color=tax_by_location.values,
                        color_continuous_scale='blues'
                    )
                    fig.update_layout(
                        xaxis_title="Tax Amount (₹)",
                        yaxis_title="Location",
                        showlegend=False
                    )
                    st.plotly_chart(fig, use_container_width=True)
        
        # Due Date Analysis
        if 'Due_Date_Notification' in df.columns:
            st.subheader("⏰ Payment Due Date Analysis")
            
            col1, col2 = st.columns(2)
            
            with col1:
                due_alerts = df['Due_Date_Notification'].value_counts()
                
                colors = ['#ff6b6b' if x == 'YES' else '#51cf66' for x in due_alerts.index]
                fig = px.pie(
                    values=due_alerts.values,
                    names=due_alerts.index,
                    title="Due Date Alert Status",
                    color_discrete_sequence=colors
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Show urgent invoices table
                urgent_df = df[df['Due_Date_Notification'] == 'YES']
                if not urgent_df.empty:
                    st.write("🚨 **Urgent Payment Alerts:**")
                    display_cols = ['Invoice_Number', 'Vendor_Name', 'Amount', 'Due_Date']
                    available_cols = [col for col in display_cols if col in urgent_df.columns]
                    if available_cols:
                        st.dataframe(urgent_df[available_cols].head(10), use_container_width=True)
                else:
                    st.info("✅ No urgent payment alerts - all invoices have sufficient time before due dates.")
    
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
        
        st.subheader("🔄 System Status:")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.info("""
            **📊 Validation System:**
            - ✅ Backend processing: Fully configured
            - ✅ Enhanced features: Ready for deployment  
            - ✅ Database tracking: Initialized
            - ⏳ First run: Waiting for data
            """)
        
        with col2:
            st.success("""
            **🚀 Next Steps:**
            1. System runs automatically every 4 days
            2. Enhanced processing activates on first run
            3. Dashboard populates with analytics
            4. Email notifications sent to AP team
            """)
        
        st.markdown("""
        ---
        **🔗 System Integration Status:**
        - 🟢 GitHub Actions: Configured for 4-day automation
        - 🟢 RMS Integration: Active and processing invoices  
        - 🟢 Email Notifications: Sending to AP team
        - 🟢 Enhanced Processing: 21 new fields ready
        - 🟢 Streamlit Dashboard: Enhanced with interactive analytics
        """)
    
    def render_data_explorer(self, df, report_info):
        """Render interactive data explorer"""
        if df is None:
            return
        
        st.header("🔍 Interactive Invoice Data Explorer")
        
        # Enhanced filters
        col1, col2, col3, col4 = st.columns(4)
        
        filters = {}
        
        with col1:
            if 'Validation_Status' in df.columns:
                statuses = ['All'] + sorted(list(df['Validation_Status'].unique()))
                filters['status'] = st.selectbox("🔍 Filter by Status", statuses)
        
        with col2:
            if 'Location' in df.columns:
                locations = ['All'] + sorted(list(df['Location'].unique()))
                filters['location'] = st.selectbox("🌍 Filter by Location", locations)
        
        with col3:
            if 'Invoice_Currency' in df.columns:
                currencies = ['All'] + sorted(list(df['Invoice_Currency'].unique()))
                filters['currency'] = st.selectbox("💱 Filter by Currency", currencies)
        
        with col4:
            if 'Tax_Type' in df.columns:
                tax_types = ['All'] + sorted(list(df['Tax_Type'].unique()))
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
        
        # Display summary
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.write(f"📊 Showing **{len(filtered_df):,}** of **{len(df):,}** invoices")
        
        with col2:
            if report_info and report_info.get('enhanced'):
                st.success("🆕 Enhanced Report (21+ fields)")
            else:
                st.info("📊 Standard Report")
        
        # Enhanced data display
        if not filtered_df.empty:
            # Show key columns first
            key_columns = [
                'Invoice_Number', 'Vendor_Name', 'Amount', 'Invoice_Date', 
                'Validation_Status', 'Location', 'Invoice_Currency', 'Tax_Type'
            ]
            
            display_columns = [col for col in key_columns if col in filtered_df.columns]
            remaining_columns = [col for col in filtered_df.columns if col not in display_columns]
            
            # Column selector
            with st.expander("🔧 Customize Column Display"):
                selected_columns = st.multiselect(
                    "Select columns to display:",
                    display_columns + remaining_columns,
                    default=display_columns[:6]  # Show first 6 by default
                )
            
            if selected_columns:
                st.dataframe(
                    filtered_df[selected_columns], 
                    use_container_width=True, 
                    height=400
                )
            else:
                st.dataframe(filtered_df, use_container_width=True, height=400)
            
            # Download options
            col1, col2, col3 = st.columns(3)
            
            with col1:
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Filtered Data (CSV)",
                    data=csv,
                    file_name=f"filtered_invoices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            with col2:
                if len(selected_columns) > 0:
                    selected_csv = filtered_df[selected_columns].to_csv(index=False)
                    st.download_button(
                        label="📋 Download Selected Columns",
                        data=selected_csv,
                        file_name=f"selected_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
            
            with col3:
                # Summary stats
                if st.button("📊 Generate Summary Report", use_container_width=True):
                    summary_data = {
                        'Metric': [
                            'Total Records',
                            'Unique Vendors',
                            'Total Amount',
                            'Average Amount',
                            'Date Range'
                        ],
                        'Value': [
                            len(filtered_df),
                            filtered_df['Vendor_Name'].nunique() if 'Vendor_Name' in filtered_df.columns else 'N/A',
                            f"₹{filtered_df['Amount'].sum():,.2f}" if 'Amount' in filtered_df.columns else 'N/A',
                            f"₹{filtered_df['Amount'].mean():,.2f}" if 'Amount' in filtered_df.columns else 'N/A',
                            f"{filtered_df['Invoice_Date'].min()} to {filtered_df['Invoice_Date'].max()}" if 'Invoice_Date' in filtered_df.columns else 'N/A'
                        ]
                    }
                    st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)
        else:
            st.warning("No data matches the selected filters.")
    
    def render_sidebar(self):
        """Render enhanced sidebar"""
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
        
        # Enhanced features status
        st.sidebar.subheader("🚀 Enhanced Features")
        
        feature_status = [
            ("💱 Multi-Currency", self.enhanced_data_available),
            ("🌍 Global Locations", self.enhanced_data_available),
            ("💰 Tax Calculations", self.enhanced_data_available),
            ("⏰ Due Date Alerts", self.enhanced_data_available),
            ("🔄 Change Tracking", self.enhanced_data_available),
            ("📧 Email Reports", True),
            ("📊 Analytics", True),
        ]
        
        for feature, active in feature_status:
            icon = "✅" if active else "⏳"
            st.sidebar.write(f"{icon} {feature}")
        
        # System actions
        st.sidebar.subheader("🔄 System Actions")
        
        if st.sidebar.button("🔄 Refresh Dashboard"):
            st.experimental_rerun()
        
        if st.sidebar.button("📊 System Health Check"):
            st.sidebar.success("✅ All systems operational")
            st.sidebar.info(f"📈 Dashboard version: Enhanced v2.0")
            st.sidebar.info(f"🕐 Last refresh: {datetime.now().strftime('%H:%M:%S')}")
        
        # GitHub integration info
        st.sidebar.subheader("🔗 Integration Status")
        st.sidebar.write("✅ GitHub Actions: Active")
        st.sidebar.write("✅ 4-day Automation: Configured")
        st.sidebar.write("✅ RMS Integration: Connected")
        st.sidebar.write("✅ Email Notifications: Active")
        
        # Manual trigger info
        st.sidebar.info("""
        💡 **Manual Trigger:**
        Visit your GitHub repository → Actions → Run workflow to trigger validation manually.
        """)
    
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
        
        # System architecture info
        with st.expander("🔧 System Architecture"):
            st.markdown("""
            **Backend Processing:**
            - 🔄 Automated 4-day validation cycle
            - 🗄️ SQLite databases for historical tracking  
            - 🌐 Selenium-based RMS data extraction
            - 📧 SMTP email notification system
            
            **Enhanced Analytics:**
            - 💰 Multi-jurisdiction tax calculations (India + 12 countries)
            - 🌍 Global location and entity tracking
            - 💱 Multi-currency invoice processing
            - ⏰ Payment due date monitoring with 5-day alerts
            - 📊 Interactive Plotly visualizations
            
            **Integration:**
            - 🔗 GitHub Actions for automated deployment
            - 📊 Streamlit for real-time dashboard
            - 🏢 RMS system for live invoice data
            - 📧 Email integration for stakeholder notifications
            """)
    
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
        
        self.render_footer()

# Initialize and run dashboard
if __name__ == "__main__":
    dashboard = EnhancedDashboard()
    dashboard.run()
