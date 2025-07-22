# Create a completely clean streamlit_app.py
import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta

# Page config
st.set_page_config(
    page_title="Invoice Validator Dashboard",
    page_icon="📊",
    layout="wide"
)
# Company Logo Section - Very Top
try:
    from PIL import Image
    import os
    
    logo_path = "assets/koenig_logo.png"
    
    if os.path.exists(logo_path):
        # Custom CSS for logo container
        st.markdown("""
        <style>
            .logo-container {
                display: flex;
                justify-content: center;
                align-items: center;
                padding: 20px 0 30px 0;
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
                border-radius: 15px;
                margin-bottom: 30px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
        </style>
        """, unsafe_allow_html=True)
        
        # Logo container
        st.markdown('<div class="logo-container">', unsafe_allow_html=True)
        
        # Display logo centered
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            logo = Image.open(logo_path)
            st.image(logo, use_column_width=True)
            
        st.markdown('</div>', unsafe_allow_html=True)
        
    else:
        # Text fallback
        st.error("⚠️ Logo file not found at assets/koenig_logo.png")
        
except Exception as e:
    st.error(f"⚠️ Error loading logo: {str(e)}")

# Custom CSS
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #2E86C1 0%, #F39C12 100%);
        color: white;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
        margin-bottom: 30px;
    }
    
    .metric-card {
        background: white;
        padding: 20px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #2E86C1;
        margin-bottom: 10px;
    }
    
    .success-message {
        background-color: #d4edda;
        border-color: #c3e6cb;
        color: #155724;
        padding: 15px;
        border-radius: 5px;
        border: 1px solid;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# Main header
st.markdown("""
<div class="main-header">
    <h1>📊 Invoice Validation Dashboard</h1>
    <p>Automated GST Compliance & Data Validation System</p>
</div>
""", unsafe_allow_html=True)

# Check for data availability
data_dir = "data"
has_data = os.path.exists(data_dir) and any(f.endswith('.xlsx') for f in os.listdir(data_dir) if os.path.isfile(os.path.join(data_dir, f)))

if not has_data:
    # Demo mode - no data available
    st.markdown("""
    <div class="success-message">
        🎉 <strong>Invoice Validation Dashboard Successfully Deployed!</strong><br>
        📊 This dashboard displays validation reports when data is available.
    </div>
    """, unsafe_allow_html=True)
    
    # Features overview
    st.subheader("🔧 System Features")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **📋 Validation Features:**
        - ✅ GST Number compliance checking
        - ✅ Missing invoice data detection
        - ✅ Duplicate invoice identification  
        - ✅ Negative amount validation
        - ✅ Data completeness verification
        """)
        
    with col2:
        st.markdown("""
        **🤖 Automation Features:**
        - ✅ Automated RMS data scraping
        - ✅ Email notifications to stakeholders
        - ✅ Scheduled validation every 4 days
        - ✅ Excel report generation
        - ✅ Data archival and cleanup
        """)
    
    # Status indicators
    st.subheader("📊 System Status")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("System Status", "🟢 Active", "Ready")
    
    with col2:
        st.metric("Last Validation", "Pending", "Awaiting data")
    
    with col3:
        st.metric("Reports Generated", "0", "No data yet")
    
    with col4:
        st.metric("Issues Found", "0", "No validation run")
    
    # Instructions
    st.subheader("🚀 Getting Started")
    st.info("""
    **To see validation reports:**
    1. Run the validator locally with: `python main.py`
    2. The system will process invoices and generate reports
    3. Reports will appear in this dashboard automatically
    
    **Automated scheduling:**
    - The system runs every 4 days at 6:00 PM
    - Email notifications sent to stakeholders
    - All validation results archived
    """)
    
    # Contact information
    st.subheader("📧 Support")
    st.write("For technical support or questions, contact the development team.")
    
else:
    # Data mode - show actual reports
    st.success("📊 Validation reports found! Displaying data...")
    
    try:
        # Look for the latest report
        report_files = [f for f in os.listdir(data_dir) if f.endswith('.xlsx')]
        
        if report_files:
            latest_report = max(report_files, key=lambda x: os.path.getctime(os.path.join(data_dir, x)))
            
            st.subheader(f"📋 Latest Report: {latest_report}")
            
            # Try to read and display the report
            df = pd.read_excel(os.path.join(data_dir, latest_report))
            
            # Display summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Invoices", len(df))
            
            with col2:
                st.metric("Issues Found", len(df) if len(df) > 0 else 0)
            
            with col3:
                st.metric("Report Date", datetime.now().strftime("%Y-%m-%d"))
            
            with col4:
                st.metric("Status", "✅ Complete")
            
            # Display the data
            st.subheader("📊 Validation Results")
            st.dataframe(df, use_container_width=True)
            
        else:
            st.warning("No Excel reports found in the data directory.")
            
    except Exception as e:
        st.error(f"Error loading reports: {str(e)}")

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; padding: 20px;">
    🏢 Koenig Solutions - Invoice Validation System<br>
    Built with Streamlit • Automated with Python • Deployed on Cloud
</div>
""", unsafe_allow_html=True)
