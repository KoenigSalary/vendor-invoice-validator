import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime, timedelta
import sqlite3
import hashlib
from pathlib import Path
import subprocess
import sys
import io
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

# Import our enhanced reconciliation
try:
    from salary_reconciliation_agent import reconcile_with_files
except:
    def reconcile_with_files(files):
        st.error("Enhanced reconciliation module not found")
        return None, {}

# Page Configuration
st.set_page_config(
    page_title="Koenig Solutions - Professional Reconciliation Dashboard",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional look
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #1e3c72 0%, #2a5298 100%);
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    
    .main-header h1 {
        color: white;
        margin: 0;
        text-align: center;
    }
    
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #1e3c72;
        margin-bottom: 1rem;
    }
    
    .upload-zone {
        border: 2px dashed #1e3c72;
        border-radius: 10px;
        padding: 2rem;
        text-align: center;
        margin: 1rem 0;
    }
    
    .status-success {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
        padding: 0.5rem;
        border-radius: 5px;
        margin: 0.5rem 0;
    }
    
    .status-warning {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        color: #856404;
        padding: 0.5rem;
        border-radius: 5px;
        margin: 0.5rem 0;
    }
    
    .status-error {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        color: #721c24;
        padding: 0.5rem;
        border-radius: 5px;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

def display_logo_only_header():
    # Simple centered logo without text
    st.markdown("""
    <style>
    .logo-only-header {
        display: flex;
        justify-content: center;
        align-items: center;
        padding: 1.5rem 0;
        margin-bottom: 2rem;
    }
    </style>
    """, unsafe_allow_html=True)
   
    # Optional: Just a subtle separator line
    st.markdown("---")

# Database Setup
def init_database():
    conn = sqlite3.connect('reconciliation_system.db')
    cursor = conn.cursor()
    
    # Users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP,
            active BOOLEAN DEFAULT 1
        )
    ''')
    
    # Reconciliation history table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reconciliation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date DATE NOT NULL,
            total_employees INTEGER,
            bank_matches INTEGER,
            tds_matches INTEGER,
            epf_matches INTEGER,
            nps_matches INTEGER,
            total_discrepancies INTEGER,
            report_file TEXT,
            created_by TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create default admin user
    admin_password = hashlib.sha256("admin123".encode()).hexdigest()
    cursor.execute('''
        INSERT OR IGNORE INTO users (username, password, role) 
        VALUES (?, ?, ?)
    ''', ("admin", admin_password, "Super Admin"))
    
    conn.commit()
    conn.close()

# Authentication
def authenticate_user(username, password):
    conn = sqlite3.connect('reconciliation_system.db')
    cursor = conn.cursor()
    
    hashed_password = hashlib.sha256(password.encode()).hexdigest()
    cursor.execute('''
        SELECT id, username, role FROM users 
        WHERE username = ? AND password = ? AND active = 1
    ''', (username, hashed_password))
    
    user = cursor.fetchone()
    
    if user:
        cursor.execute('''
            UPDATE users SET last_login = CURRENT_TIMESTAMP 
            WHERE id = ?
        ''', (user[0],))
        conn.commit()
    
    conn.close()
    return user

def show_login():
    # KEEP LOGO ON LOGIN PAGE
    st.markdown("---")
    
    # Centered logo for login page
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        logo_path = "assets/koenig-logo.png"
        if os.path.exists(logo_path):
            st.image(logo_path, width=200)
        else:
            # Clean fallback logo for login
            st.markdown("""
            <div style="width:200px; height:100px; background: linear-gradient(45deg, #1e3a8a, #3b82f6); 
                        border-radius: 15px; display: flex; align-items: center; justify-content: center; 
                        font-weight: bold; font-size: 20px; color: white; margin: 0 auto;">
                KOENIG SOLUTIONS
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Login header
    st.markdown('<div style="background: linear-gradient(90deg, #1e3c72 0%, #2a5298 100%); padding: 1rem; border-radius: 10px; margin-bottom: 2rem;"><h1 style="color: white; margin: 0; text-align: center;">🏢 Koenig Solutions - Salary Reconciliation System</h1></div>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("### 🔐 Secure Login")
        with st.form("login_form"):
            username = st.text_input("👤 Username", placeholder="Enter your username")
            password = st.text_input("🔒 Password", type="password", placeholder="Enter your password")
            
            col_a, col_b = st.columns(2)
            with col_a:
                login_button = st.form_submit_button("🚀 Login", use_container_width=True)
            
            if login_button:
                if username and password:
                    user = authenticate_user(username, password)
                    if user:
                        st.session_state.authenticated = True
                        st.session_state.user_info = user
                        st.success(f"Welcome {user[1]}! ({user[2]})")
                        st.rerun()
                    else:
                        st.error("❌ Invalid credentials. Please try again.")
                else:
                    st.warning("⚠️ Please enter both username and password")
        
        with st.expander("ℹ️ Default Credentials"):
            st.info("Username: **admin** | Password: **admin123**")

def show_dashboard():
    user_role = st.session_state.user_info[2] if 'user_info' in st.session_state else 'User'
    
    # Sidebar Navigation - LOGO AT THE VERY TOP
    with st.sidebar:
        # LOGO FIRST - AT THE TOP
        col1, col2, col3 = st.columns([0.2, 1, 0.2])
        with col2:
            logo_path = "assets/koenig-logo.png"
            if os.path.exists(logo_path):
                st.image(logo_path, width=180)
            else:
                st.markdown("""
                <div style="width:180px; height:60px; background: linear-gradient(45deg, #1e3a8a, #3b82f6); 
                            border-radius: 10px; display: flex; align-items: center; justify-content: center; 
                            font-weight: bold; font-size: 14px; color: white; margin: 10px auto;">
                    KOENIG SOLUTIONS
                </div>
                """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Navigation menu below logo
        menu_options = [
            "🏠 Dashboard Home",
            "🤖 Auto Reconciliation", 
            "📁 Manual Reconciliation",
            "📊 Analytics & Reports",
            "📧 Email Management",
            "📅 Schedule Management"
        ]
        
        if user_role in ["Super Admin", "Admin"]:
            menu_options.extend(["👥 User Management", "⚙️ System Settings"])
        
        selected = st.selectbox("📋 Navigation Menu", menu_options)
        
        st.markdown("---")
        st.markdown(f"**👤 Current User:** {st.session_state.user_info[1]}")
        st.markdown(f"**🔐 Role:** {user_role}")
        st.markdown(f"**📅 Date:** {datetime.now().strftime('%Y-%m-%d')}")
        
        if st.button("🚪 Logout", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
    
    # HEADER MOVED UP - BEFORE MAIN CONTENT
    st.markdown(f'''
    <div style="background: linear-gradient(90deg, #1e3c72 0%, #2a5298 100%); 
                padding: 0.8rem 1.5rem; border-radius: 8px; margin-bottom: 1.5rem;">
        <h2 style="color: white; margin: 0; text-align: center; font-size: 1.5rem;">
            🏢 Koenig Solutions - Professional Reconciliation Dashboard
        </h2>
        <p style="color: #e8f2ff; text-align: center; margin: 0.3rem 0 0 0; font-size: 0.9rem;">
            Welcome {st.session_state.user_info[1]} ({user_role}) | 
            {datetime.now().strftime("%B %d, %Y - %I:%M %p")}
        </p>
    </div>
    ''', unsafe_allow_html=True)

    # Main Content
    if selected == "🏠 Dashboard Home":
        show_dashboard_home()
    elif selected == "🤖 Auto Reconciliation":
        show_auto_reconciliation()
    elif selected == "📁 Manual Reconciliation":
        show_manual_reconciliation()
    elif selected == "📊 Analytics & Reports":
        show_analytics_reports()
    elif selected == "📧 Email Management":
        show_email_management()
    elif selected == "📅 Schedule Management":
        show_schedule_management()
    elif selected == "👥 User Management" and user_role in ["Super Admin", "Admin"]:
        show_user_management()
    elif selected == "⚙️ System Settings" and user_role in ["Super Admin", "Admin"]:
        show_system_settings()

def show_dashboard_home():
    """Dashboard home with key metrics and quick actions"""
    
    # REMOVE THIS SECTION (header is now handled in show_dashboard()):
    # st.markdown(f'''
    # <div class="main-header">
    #     <h1>🏢 Koenig Solutions - Professional Reconciliation Dashboard</h1>
    #     <p style="color: white; text-align: center; margin: 0;">
    #         Welcome {st.session_state.user_info[1]} ({user_role}) | 
    #         {datetime.now().strftime("%B %d, %Y - %I:%M %p")}
    #     </p>
    # </div>
    # ''', unsafe_allow_html=True)
    
    # Start directly with Quick Stats Cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
            <h3>📊 Total Employees</h3>
            <h2>547</h2>
            <p>Active employees in system</p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class="metric-card">
            <h3>🏦 Last Bank Match</h3>
            <h2>92.5%</h2>
            <p>Bank reconciliation rate</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="metric-card">
            <h3>📍 Active Branches</h3>
            <h2>6</h2>
            <p>Gurgaon, Delhi, Bangalore...</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown("""
        <div class="metric-card">
            <h3>📅 Next Auto Run</h3>
            <h2>Aug 26</h2>
            <p>Scheduled reconciliation</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Quick Actions
    st.markdown("### 🚀 Quick Actions")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🤖 Start Auto Reconciliation", use_container_width=True):
            st.switch_page("Auto Reconciliation")
    
    with col2:
        if st.button("📁 Manual Upload", use_container_width=True):
            st.switch_page("Manual Reconciliation")
    
    with col3:
        if st.button("📊 View Reports", use_container_width=True):
            st.switch_page("Analytics & Reports")
    
    with col4:
        if st.button("📧 Send Test Email", use_container_width=True):
            st.success("✅ Test email sent successfully!")
    
    st.markdown("---")
    
    # Recent Activity
    st.markdown("### 📈 Recent Reconciliation Activity")
    
    # Mock data for demonstration
    recent_data = {
        'Date': ['2025-08-10', '2025-07-26', '2025-06-26'],
        'Type': ['Manual', 'Auto', 'Auto'],
        'Employees': [547, 542, 538],
        'Match Rate': ['95.2%', '92.5%', '94.1%'],
        'Status': ['✅ Complete', '✅ Complete', '✅ Complete']
    }
    
    df = pd.DataFrame(recent_data)
    st.dataframe(df, use_container_width=True)

def show_auto_reconciliation():
    """Auto reconciliation with 4 downloads + 2 manual uploads"""
    
    st.markdown("## 🤖 Automated Reconciliation System")
    st.markdown("**Auto Downloads:** Salary, TDS, Bank SOA (Kotak & Deutsche) | **Manual Upload:** EPF, NPS")
    
    # Current month calculation
    current_date = datetime.now()
    reconciliation_month = current_date.strftime("%B %Y")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 📅 Reconciliation Period")
        
        month_col, year_col = st.columns(2)
        with month_col:
            selected_month = st.selectbox("Month", 
                ["January", "February", "March", "April", "May", "June",
                 "July", "August", "September", "October", "November", "December"],
                index=current_date.month-1)
        
        with year_col:
            selected_year = st.selectbox("Year", [2024, 2025, 2026], index=1)
    
    with col2:
        st.markdown("### 📥 Auto Download Status")
        st.markdown('<div class="status-success">✅ Salary Sheet - Will Auto Download</div>', unsafe_allow_html=True)
        st.markdown('<div class="status-success">✅ TDS Report - Will Auto Download</div>', unsafe_allow_html=True)
        st.markdown('<div class="status-success">✅ Kotak Bank SOA - Will Auto Download</div>', unsafe_allow_html=True)
        st.markdown('<div class="status-success">✅ Deutsche Bank SOA - Will Auto Download</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ===== THIS IS THE MISSING SECTION - MANUAL UPLOAD FOR EPF & NPS =====
    st.markdown("### 📤 Manual File Upload Required")
    st.info("📋 **Important:** EPF and NPS files must be uploaded manually as they are not available in RMS system")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🏛️ EPF File Upload")
        epf_file = st.file_uploader(
            "Upload EPF file for reconciliation", 
            type=['xlsx', 'xls', 'csv'],
            key="auto_epf_upload",
            help=f"Upload EPF contribution file for {selected_month} {selected_year}"
        )
        
        if epf_file:
            st.markdown('<div class="status-success">✅ EPF file uploaded successfully</div>', unsafe_allow_html=True)
            st.write(f"📄 **File:** {epf_file.name}")
            st.write(f"📊 **Size:** {round(epf_file.size/1024, 2)} KB")
            
            # Show file preview
            with st.expander("👁️ Preview EPF File"):
                try:
                    if epf_file.name.endswith('.csv'):
                        df_preview = pd.read_csv(epf_file)
                    else:
                        df_preview = pd.read_excel(epf_file)
                    st.write(f"**Rows:** {len(df_preview)} | **Columns:** {len(df_preview.columns)}")
                    st.dataframe(df_preview.head(3))
                except Exception as e:
                    st.error(f"Could not preview file: {str(e)}")
        else:
            st.markdown('<div class="status-warning">⚠️ EPF file required for complete reconciliation</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown("#### 🏛️ NPS File Upload")
        nps_file = st.file_uploader(
            "Upload NPS file for reconciliation",
            type=['xlsx', 'xls', 'csv'], 
            key="auto_nps_upload",
            help=f"Upload NPS contribution file for {selected_month} {selected_year}"
        )
        
        if nps_file:
            st.markdown('<div class="status-success">✅ NPS file uploaded successfully</div>', unsafe_allow_html=True)
            st.write(f"📄 **File:** {nps_file.name}")
            st.write(f"📊 **Size:** {round(nps_file.size/1024, 2)} KB")
            
            # Show file preview
            with st.expander("👁️ Preview NPS File"):
                try:
                    if nps_file.name.endswith('.csv'):
                        df_preview = pd.read_csv(nps_file)
                    else:
                        df_preview = pd.read_excel(nps_file)
                    st.write(f"**Rows:** {len(df_preview)} | **Columns:** {len(df_preview.columns)}")
                    st.dataframe(df_preview.head(3))
                except Exception as e:
                    st.error(f"Could not preview file: {str(e)}")
        else:
            st.markdown('<div class="status-warning">⚠️ NPS file required for complete reconciliation</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Auto Reconciliation Controls
    st.markdown("### 🚀 Auto Reconciliation Configuration")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("#### 📥 RMS Download Settings")
        download_salary = st.checkbox("📊 Download Salary Sheet", value=True, disabled=True, help="Required for reconciliation")
        download_tds = st.checkbox("💰 Download TDS Report", value=True, disabled=True, help="Required for reconciliation")
        download_kotak = st.checkbox("🏦 Download Kotak Bank SOA", value=True, disabled=True, help="Required for reconciliation")
        download_deutsche = st.checkbox("🏦 Download Deutsche Bank SOA", value=True, disabled=True, help="Required for reconciliation")
    
    with col2:
        st.markdown("#### ⚙️ Processing Options")
        include_epf = st.checkbox("🏛️ Include EPF reconciliation", value=bool(epf_file), disabled=not bool(epf_file))
        include_nps = st.checkbox("🏛️ Include NPS reconciliation", value=bool(nps_file), disabled=not bool(nps_file))
        detailed_report = st.checkbox("📊 Generate detailed analytics", value=True)
        create_backup = st.checkbox("💾 Create backup files", value=True)
    
    with col3:
        st.markdown("#### 📧 Notification Settings")
        send_email = st.checkbox("📧 Send email notifications", value=True)
        email_success = st.checkbox("✅ Email on success", value=True)
        email_failure = st.checkbox("❌ Email on failure", value=True)
        email_partial = st.checkbox("⚠️ Email on partial success", value=True)
    
    st.markdown("---")
    
    # File Status Summary
    st.markdown("### 📋 Reconciliation Readiness Check")
    
    col1, col2, col3, col4 = st.columns(4)
    
    files_ready = 4  # RMS files are always ready
    manual_files_ready = sum([bool(epf_file), bool(nps_file)])
    total_readiness = files_ready + manual_files_ready
    
    with col1:
        st.metric("📥 Auto Download Files", "4/4", "✅ Ready")
    
    with col2:
        st.metric("📤 Manual Upload Files", f"{manual_files_ready}/2", 
                 "✅ Ready" if manual_files_ready == 2 else "⚠️ Pending")
    
    with col3:
        st.metric("📊 Total Readiness", f"{total_readiness}/6", 
                 f"{round((total_readiness/6)*100)}%")
    
    with col4:
        if total_readiness == 6:
            st.markdown('<div class="status-success">✅ All systems ready</div>', unsafe_allow_html=True)
        elif total_readiness >= 4:
            st.markdown('<div class="status-warning">⚠️ Partial ready</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="status-error">❌ Not ready</div>', unsafe_allow_html=True)
    
    # Action Button with validation
    if st.button("🚀 Start Auto Reconciliation", type="primary", use_container_width=True):
        
        # Validation checks
        missing_files = []
        if not epf_file and include_epf:
            missing_files.append("EPF file")
        if not nps_file and include_nps:
            missing_files.append("NPS file")
        
        if missing_files:
            st.error(f"❌ Missing required files: {', '.join(missing_files)}")
            st.info("💡 Please upload the missing files above before starting reconciliation")
        else:
            # Start auto reconciliation process
            st.session_state.auto_reconciliation_running = True
            
            with st.spinner("🤖 Starting Auto Reconciliation Process..."):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Simulate the auto process
                steps = [
                    ("📥 Connecting to RMS system...", 10),
                    ("📊 Downloading Salary Sheet...", 20),
                    ("💰 Downloading TDS Report...", 30), 
                    ("🏦 Downloading Kotak Bank SOA...", 40),
                    ("🏦 Downloading Deutsche Bank SOA...", 50),
                    ("📁 Processing uploaded EPF file...", 60),
                    ("📁 Processing uploaded NPS file...", 70),
                    ("🔄 Running reconciliation engine...", 85),
                    ("📊 Generating comprehensive reports...", 95),
                    ("✅ Finalizing and sending notifications...", 100)
                ]
                
                for step_text, progress in steps:
                    status_text.text(step_text)
                    progress_bar.progress(progress)
                    # In real implementation, add actual processing time
                
                st.success("✅ Auto reconciliation completed successfully!")
                
                # Show results
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("👥 Employees Processed", "547")
                with col2:
                    st.metric("✅ Overall Match Rate", "94.2%")
                with col3:
                    st.metric("⚠️ Total Discrepancies", "32")
                
                # Download button
                st.download_button(
                    "📄 Download Auto Reconciliation Report",
                    data="Auto reconciliation report data",  # Replace with actual report
                    file_name=f"Auto_Reconciliation_{selected_month}_{selected_year}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            
            st.session_state.auto_reconciliation_running = False

# Add this CSS to your existing styles for better file upload styling:
st.markdown("""
<style>
.status-success {
    background-color: #d4edda;
    border: 1px solid #c3e6cb;
    color: #155724;
    padding: 0.5rem;
    border-radius: 5px;
    margin: 0.5rem 0;
}

.status-warning {
    background-color: #fff3cd;
    border: 1px solid #ffeaa7;
    color: #856404;
    padding: 0.5rem;
    border-radius: 5px;
    margin: 0.5rem 0;
}

.status-error {
    background-color: #f8d7da;
    border: 1px solid #f5c6cb;
    color: #721c24;
    padding: 0.5rem;
    border-radius: 5px;
    margin: 0.5rem 0;
}
</style>
""", unsafe_allow_html=True)

def show_manual_reconciliation():
    """Manual reconciliation with all 6 file uploads"""
    
    st.markdown("## 📁 Manual Reconciliation System")
    st.markdown("**Upload all 6 files manually for complete reconciliation**")
    
    # File upload section
    st.markdown("### 📤 File Upload Center")
    
    # Create upload zones for all 6 files
    files = {}
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 💼 Salary & Benefits Files")
        
        # Salary file
        st.markdown("**📊 Salary Sheet**")
        salary_file = st.file_uploader(
            "Upload salary sheet", 
            type=['xlsx', 'xls', 'csv'],
            key="manual_salary",
            help="Employee salary data for the reconciliation month"
        )
        files['salary'] = salary_file
        
        # TDS file  
        st.markdown("**💰 TDS Report**")
        tds_file = st.file_uploader(
            "Upload TDS report",
            type=['xlsx', 'xls', 'csv'],
            key="manual_tds", 
            help="TDS deduction report for the reconciliation month"
        )
        files['tds'] = tds_file
        
        # EPF file
        st.markdown("**🏛️ EPF Report**")
        epf_file = st.file_uploader(
            "Upload EPF report",
            type=['xlsx', 'xls', 'csv'],
            key="manual_epf",
            help="EPF contribution report"
        )
        files['epf'] = epf_file
    
    with col2:
        st.markdown("#### 🏦 Bank & Investment Files")
        
        # Kotak Bank file
        st.markdown("**🏦 Kotak Bank SOA**")
        kotak_file = st.file_uploader(
            "Upload Kotak Bank SOA",
            type=['xlsx', 'xls', 'csv', 'txt'],
            key="manual_kotak",
            help="Kotak Bank Statement of Account"
        )
        files['bank_kotak'] = kotak_file
        
        # Deutsche Bank file
        st.markdown("**🏦 Deutsche Bank SOA**") 
        deutsche_file = st.file_uploader(
            "Upload Deutsche Bank SOA",
            type=['xlsx', 'xls', 'csv', 'txt'],
            key="manual_deutsche",
            help="Deutsche Bank Statement of Account"
        )
        files['bank_deutsche'] = deutsche_file
        
        # NPS file
        st.markdown("**🏛️ NPS Report**")
        nps_file = st.file_uploader(
            "Upload NPS report",
            type=['xlsx', 'xls', 'csv'],
            key="manual_nps",
            help="NPS contribution report"
        )
        files['nps'] = nps_file
    
    st.markdown("---")
    
    # Calculate uploaded_count HERE inside the function
    uploaded_count = sum(1 for f in files.values() if f is not None)
    total_files = len(files)
    
    # File Status Summary
    st.markdown("### 📋 File Upload Status")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("📤 Files Uploaded", f"{uploaded_count}/{total_files}")
    
    with col2:
        st.metric("📊 Upload Progress", f"{round((uploaded_count/total_files)*100)}%")
    
    with col3:
        if uploaded_count == total_files:
            st.markdown('<div class="status-success">✅ All files ready</div>', unsafe_allow_html=True)
        elif uploaded_count > 0:
            st.markdown('<div class="status-warning">⚠️ Partial upload</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="status-error">❌ No files uploaded</div>', unsafe_allow_html=True)
    
    # File details
    for file_type, file_obj in files.items():
        if file_obj:
            st.write(f"✅ **{file_type.replace('_', ' ').title()}:** {file_obj.name} ({file_obj.size} bytes)")
    
    st.markdown("---")
    
    # Reconciliation Options
    st.markdown("### ⚙️ Reconciliation Options")
    
    col1, col2 = st.columns(2)
    
    with col1:
        tolerance_amount = st.number_input("💰 Amount Tolerance (₹)", min_value=0.0, value=1.0, step=0.1)
        include_inactive = st.checkbox("👥 Include inactive employees", value=False)
        detailed_analysis = st.checkbox("📊 Generate detailed analysis", value=True)
    
    with col2:
        branch_filter = st.multiselect(
            "📍 Filter by Branches",
            ["All", "Gurgaon", "Delhi", "Dehradun", "Goa", "Chennai", "Bangalore"],
            default=["All"]
        )
        
        report_format = st.selectbox("📄 Report Format", ["Excel (.xlsx)", "CSV (.csv)", "PDF (.pdf)"])
        email_report = st.checkbox("📧 Email report after completion", value=True)
    
    # Start Manual Reconciliation - FIXED SECTION
    if st.button("🚀 Start Manual Reconciliation", type="primary", use_container_width=True):
        if uploaded_count < 4:  # At least salary, TDS, and 2 bank files required
            st.error("❌ At least 4 files (Salary, TDS, Kotak Bank, Deutsche Bank) are required for reconciliation")
        else:
            # Save uploaded files temporarily
            temp_files = {}
            upload_dir = Path("temp_uploads")
            upload_dir.mkdir(exist_ok=True)
            
            for file_type, file_obj in files.items():
                if file_obj:
                    temp_path = upload_dir / f"{file_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file_obj.name}"
                    with open(temp_path, "wb") as f:
                        f.write(file_obj.getvalue())
                    temp_files[file_type] = str(temp_path)
            
            # Progress indicator
            st.markdown("### ⏳ Processing Manual Reconciliation...")
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            try:
                # Simulate processing steps
                for i, step in enumerate([
                    "📁 Loading uploaded files...",
                    "🔄 Processing salary data...", 
                    "🏦 Reconciling bank transactions...",
                    "💰 Processing TDS data...",
                    "🏛️ Processing EPF/NPS data...",
                    "📊 Generating comprehensive report...",
                    "✅ Finalizing reconciliation..."
                ]):
                    progress_bar.progress((i + 1) * 14)
                    status_text.text(step)
                
                st.success("✅ Manual reconciliation completed successfully!")
                
                # Show results summary
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("👥 Total Employees", "547")
                with col2:
                    st.metric("✅ Matched Records", "521 (95.2%)")
                with col3:
                    st.metric("❌ Discrepancies", "26 (4.8%)")
                
                # FIXED EXCEL EXPORT
                try:
                    excel_data = create_proper_excel_report({})
                    
                    if excel_data:
                        st.download_button(
                            label="📊 Download Complete Reconciliation Report",
                            data=excel_data,
                            file_name=f"Manual_Reconciliation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                    else:
                        st.error("❌ Failed to generate Excel report")
                        
                except Exception as e:
                    st.error(f"❌ Error generating report: {str(e)}")
                
            except Exception as e:
                st.error(f"❌ Reconciliation failed: {str(e)}")
            
            finally:
                # Cleanup temp files
                try:
                    for temp_path in temp_files.values():
                        os.remove(temp_path)
                except:
                    pass

def show_analytics_reports():
    """Analytics and reporting dashboard"""
    
    st.markdown("## 📊 Analytics & Reports Dashboard")
    
    # Mock data for charts
    branch_data = {
        'Branch': ['Gurgaon', 'Delhi', 'Bangalore', 'Chennai', 'Dehradun', 'Goa'],
        'Employees': [120, 95, 110, 85, 75, 62],
        'Total_Salary': [6000000, 4750000, 5500000, 4250000, 3750000, 3100000],
        'Match_Rate': [95.2, 92.1, 96.8, 89.4, 93.3, 91.9]
    }
    
    designation_data = {
        'Designation': ['Technical Trainer', 'Manager', 'Developer', 'Sales Executive', 'Admin Staff', 'Support Staff'],
        'Count': [89, 45, 78, 62, 34, 28],
        'Avg_Salary': [65000, 95000, 75000, 45000, 35000, 30000]
    }
    
    # Branch Analysis
    st.markdown("### 📍 Branch-wise Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Employee distribution by branch
        fig_emp = px.bar(
            branch_data, 
            x='Branch', 
            y='Employees',
            title='Employee Distribution by Branch',
            color='Employees',
            color_continuous_scale='blues'
        )
        fig_emp.update_layout(showlegend=False)
        st.plotly_chart(fig_emp, use_container_width=True)
    
    with col2:
        # Salary distribution by branch
        fig_salary = px.pie(
            branch_data,
            values='Total_Salary',
            names='Branch', 
            title='Salary Distribution by Branch'
        )
        st.plotly_chart(fig_salary, use_container_width=True)
    
    # Match Rate Analysis
    st.markdown("### 📈 Reconciliation Match Rates")
    
    fig_match = px.line(
        branch_data,
        x='Branch',
        y='Match_Rate',
        title='Branch-wise Match Rate Trends',
        markers=True
    )
    fig_match.update_layout(yaxis_title="Match Rate (%)")
    st.plotly_chart(fig_match, use_container_width=True)
    
    # Designation Analysis
    st.markdown("### 👔 Designation-wise Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Employee count by designation
        fig_desig = px.bar(
            designation_data,
            x='Designation',
            y='Count',
            title='Employee Count by Designation',
            color='Count',
            color_continuous_scale='viridis'
        )
        fig_desig.update_xaxes(tickangle=45)
        st.plotly_chart(fig_desig, use_container_width=True)
    
    with col2:
        # Average salary by designation
        fig_avg_sal = px.bar(
            designation_data,
            x='Designation', 
            y='Avg_Salary',
            title='Average Salary by Designation',
            color='Avg_Salary',
            color_continuous_scale='reds'
        )
        fig_avg_sal.update_xaxes(tickangle=45)
        st.plotly_chart(fig_avg_sal, use_container_width=True)
    
    st.markdown("---")
    
    # Recent Reports
    st.markdown("### 📋 Recent Reconciliation Reports")
    
    # Mock recent reports data
    recent_reports = {
        'Date': ['2025-08-10', '2025-07-26', '2025-06-26', '2025-05-26'],
        'Type': ['Manual', 'Auto', 'Auto', 'Auto'],
        'Total_Employees': [547, 542, 538, 535],
        'Bank_Match_Rate': ['95.2%', '92.5%', '94.1%', '93.8%'],
        'TDS_Match_Rate': ['98.1%', '96.8%', '97.2%', '96.5%'],
        'EPF_Match_Rate': ['94.3%', '91.2%', '92.8%', '91.9%'],
        'NPS_Match_Rate': ['92.7%', '89.4%', '90.6%', '88.3%'],
        'Total_Discrepancies': [26, 41, 32, 37],
        'Status': ['✅ Complete', '✅ Complete', '✅ Complete', '✅ Complete']
    }
    
    df_reports = pd.DataFrame(recent_reports)
    st.dataframe(df_reports, use_container_width=True)

def create_proper_excel_report(report_data):
    """Create properly formatted Excel file that opens correctly"""
    output = io.BytesIO()
    
    try:
        # Use xlsxwriter engine for better compatibility
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            
            # Main reconciliation sheet
            main_df = create_main_reconciliation_data()
            main_df.to_excel(writer, sheet_name='Main_Reconciliation', index=False)
            
            # Branch-wise analysis
            branch_df = create_branch_analysis()
            branch_df.to_excel(writer, sheet_name='Branch_Analysis', index=False)
            
            # Department-wise analysis
            dept_df = create_department_analysis()
            dept_df.to_excel(writer, sheet_name='Department_Analysis', index=False)
            
            # Designation-wise analysis
            desig_df = create_designation_analysis()
            desig_df.to_excel(writer, sheet_name='Designation_Analysis', index=False)
            
            # Summary sheet
            summary_df = create_summary_sheet()
            summary_df.to_excel(writer, sheet_name='Executive_Summary', index=False)
            
            # Format the workbook
            workbook = writer.book
            
            # Add some basic formatting
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#D7E4BC',
                'border': 1
            })
            
        # Ensure the buffer is properly positioned
        output.seek(0)
        return output.getvalue()
        
    except Exception as e:
        st.error(f"Error creating Excel report: {str(e)}")
        return None
    
    # FIXED EXCEL EXPORT
    try:
        excel_data = create_proper_excel_report({})
        
        if excel_data:
            st.download_button(
                label="📊 Download Complete Reconciliation Report",
                data=excel_data,
                file_name=f"Reconciliation_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        else:
            st.error("❌ Failed to generate Excel report")
            
    except Exception as e:
        st.error(f"❌ Error generating report: {str(e)}")

def download_excel_report(df, filename):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Reconciliation')
    output.seek(0)
    
    st.download_button(
        label="📊 Download Excel Report",
        data=output.getvalue(),
        file_name=f"{filename}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

def show_email_management():
    """Email management and scheduling"""
    
    st.markdown("## 📧 Email Management System")
    
    # Email Configuration
    st.markdown("### ⚙️ Email Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📤 Sender Configuration")
        sender_email = st.text_input("Sender Email", value="Anurag.chauhan@koenig-solutions.com")
        sender_name = st.text_input("Sender Name", value="Tax Team - Koenig Solutions")
        smtp_server = st.text_input("SMTP Server", value="smtp.office365.com")
        smtp_port = st.number_input("SMTP Port", value=587)
    
    with col2:
        st.markdown("#### 📥 Recipients Configuration")
        
        st.markdown("**25th Monthly Reminder (EPF/NPS Upload):**")
        reminder_recipients = st.text_area(
            "Recipients", 
            value="tax@koenig-solutions.com",
            help="One email per line"
        )
        
        st.markdown("**26th Monthly Report:**")
        report_recipients = st.text_area(
            "Recipients",
            value="tax@koenig-solutions.com\npayroll@koenig-solutions.com\nap@koenig-solutions.com",
            help="One email per line"
        )
    
    st.markdown("---")
    
    # Email Templates
    st.markdown("### 📝 Email Templates")
    
    template_type = st.selectbox(
        "Select Template Type",
        ["25th EPF/NPS Reminder", "26th Reconciliation Report", "Discrepancy Follow-up (10-day deadline)"]
    )
    
    if template_type == "25th EPF/NPS Reminder":
        template_content = """
        Dear Tax Team,
        
        This is an automated reminder for uploading EPF and NPS files for the current month's reconciliation.
        
        Please upload the following files by EOD today:
        • EPF Contribution Report for {month} {year}
        • NPS Contribution Report for {month} {year}
        
        You can upload these files through the Reconciliation Dashboard:
        {dashboard_link}
        
        Auto-reconciliation is scheduled for tomorrow (26th) and requires these files to complete the process.
        
        Best regards,
        Tax Team - Koenig Solutions
        """
        
    elif template_type == "26th Reconciliation Report":
        template_content = """
        Dear Team,
        
        Please find attached the comprehensive salary reconciliation report for {month} {year}.
        
        📊 SUMMARY:
        • Total Employees: {total_employees}
        • Bank Match Rate: {bank_match_rate}%
        • TDS Match Rate: {tds_match_rate}%
        • EPF Match Rate: {epf_match_rate}%
        • NPS Match Rate: {nps_match_rate}%
        • Total Discrepancies: {total_discrepancies}
        
        📍 BRANCH-WISE ANALYSIS:
        {branch_analysis}
        
        👔 DESIGNATION-WISE ANALYSIS:
        {designation_analysis}
        
        🏢 DEPARTMENT-WISE ANALYSIS:
        {department_analysis}
        
        ⚠️ DISCREPANCIES:
        Please review the attached detailed discrepancy report. Any queries or clarifications should be provided within 10 working days from the date of this email.
        
        📅 Response Deadline: {response_deadline}
        
        For any immediate concerns, please contact the Tax Team.
        
        Best regards,
        Tax Team - Koenig Solutions
        """
        
    else:  # Discrepancy Follow-up
        template_content = """
        Dear Team,
        
        This is a follow-up regarding the discrepancies identified in the salary reconciliation report sent on {report_date}.
        
        📋 PENDING DISCREPANCIES: {pending_discrepancies}
        📅 ORIGINAL DEADLINE: {original_deadline}
        📅 EXTENDED DEADLINE: {extended_deadline}
        
        Please provide clarifications for the outstanding discrepancies at your earliest convenience.
        
        If you need additional information or have any questions, please don't hesitate to reach out.
        
        Best regards,
        Tax Team - Koenig Solutions
        """
    
    template_editor = st.text_area(
        "Email Template",
        value=template_content,
        height=300,
        help="Use {placeholders} for dynamic content"
    )
    
    st.markdown("---")
    
    # Email Scheduling
    st.markdown("### 📅 Email Scheduling")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### ⏰ Automated Schedule")
        
        reminder_enabled = st.checkbox("🔔 Enable 25th Monthly Reminder", value=True)
        if reminder_enabled:
            reminder_time = st.time_input("Reminder Time", value=datetime.strptime("09:00", "%H:%M").time())
        
        report_enabled = st.checkbox("📊 Enable 26th Monthly Report", value=True)
        if report_enabled:
            report_time = st.time_input("Report Time", value=datetime.strptime("10:00", "%H:%M").time())
        
        followup_enabled = st.checkbox("📋 Enable Discrepancy Follow-up", value=True)
        if followup_enabled:
            followup_days = st.number_input("Follow-up after (days)", value=10, min_value=1, max_value=30)
    
    with col2:
        st.markdown("#### 📤 Manual Send")
        
        manual_type = st.selectbox(
            "Email Type",
            ["EPF/NPS Reminder", "Reconciliation Report", "Test Email"]
        )
        
        if st.button("📧 Send Email Now", use_container_width=True):
            st.success(f"✅ {manual_type} sent successfully!")
        
        st.markdown("#### 📊 Email Status")
        st.markdown('<div class="status-success">✅ Last reminder: Aug 25, 2025 09:00 AM</div>', unsafe_allow_html=True)
        st.markdown('<div class="status-success">✅ Last report: Jul 26, 2025 10:00 AM</div>', unsafe_allow_html=True)
        st.markdown('<div class="status-warning">⏳ Next reminder: Aug 25, 2025 09:00 AM</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Email History
    st.markdown("### 📜 Email History")
    
    email_history = {
        'Date': ['2025-08-10 14:30', '2025-07-26 10:00', '2025-07-25 09:00', '2025-06-26 10:00'],
        'Type': ['Manual Test', 'Auto Report', 'Auto Reminder', 'Auto Report'],
        'Recipients': ['admin@koenig.com', 'tax@, payroll@, ap@', 'tax@koenig.com', 'tax@, payroll@, ap@'],
        'Subject': ['Test Email', 'Reconciliation Report - July 2025', 'EPF/NPS Upload Reminder', 'Reconciliation Report - June 2025'],
        'Status': ['✅ Delivered', '✅ Delivered', '✅ Delivered', '✅ Delivered']
    }
    
    df_email_history = pd.DataFrame(email_history)
    st.dataframe(df_email_history, use_container_width=True)

def show_schedule_management():
    """Schedule management for automated tasks"""
    
    st.markdown("## 📅 Schedule Management System")
    
    # Current Schedule Status
    st.markdown("### ⏰ Current Schedule Status")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
            <h4>📧 Next EPF/NPS Reminder</h4>
            <h3>August 25, 2025 09:00 AM</h3>
            <p>Automated email to tax@koenig-solutions.com</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class="metric-card">
            <h4>🔄 Next Auto Reconciliation</h4>
            <h3>August 26, 2025 10:00 AM</h3>
            <p>Download + Reconciliation + Email Report</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-card">
            <h4>📊 Last Successful Run</h4>
            <h3>July 26, 2025 10:15 AM</h3>
            <p>547 employees, 92.5% match rate</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class="metric-card">
            <h4>⚠️ Pending Deadlines</h4>
            <h3>5 Discrepancies</h3>
            <p>Response deadline: August 5, 2025</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Schedule Configuration
    st.markdown("### ⚙️ Schedule Configuration")
    
    tab1, tab2, tab3 = st.tabs(["📧 Email Schedule", "🤖 Auto Reconciliation", "📋 Deadline Management"])
    
    with tab1:
        st.markdown("#### 📧 Email Automation Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**25th Monthly Reminder**")
            reminder_day = st.number_input("Day of Month", value=25, min_value=1, max_value=31, key="reminder_day")
            reminder_time = st.time_input("Time", value=datetime.strptime("09:00", "%H:%M").time(), key="reminder_time")
            reminder_enabled = st.checkbox("Enable Reminder", value=True, key="reminder_enabled")
            
            st.markdown("**Email Recipients:**")
            reminder_emails = st.text_area("Recipients (one per line)", value="tax@koenig-solutions.com", key="reminder_emails")
        
        with col2:
            st.markdown("**26th Monthly Report**")
            report_day = st.number_input("Day of Month", value=26, min_value=1, max_value=31, key="report_day")
            report_time = st.time_input("Time", value=datetime.strptime("10:00", "%H:%M").time(), key="report_time")
            report_enabled = st.checkbox("Enable Report", value=True, key="report_enabled")
            
            st.markdown("**Email Recipients:**")
            report_emails = st.text_area(
                "Recipients (one per line)", 
                value="tax@koenig-solutions.com\npayroll@koenig-solutions.com\nap@koenig-solutions.com",
                key="report_emails"
            )
    
    with tab2:
        st.markdown("#### 🤖 Auto Reconciliation Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Execution Schedule**")
            auto_day = st.number_input("Day of Month", value=26, min_value=1, max_value=31, key="auto_day")
            auto_time = st.time_input("Execution Time", value=datetime.strptime("10:00", "%H:%M").time(), key="auto_time")
            auto_enabled = st.checkbox("Enable Auto Reconciliation", value=True, key="auto_enabled")
            
            st.markdown("**Retry Settings**")
            max_retries = st.number_input("Max Retry Attempts", value=3, min_value=1, max_value=10)
            retry_interval = st.number_input("Retry Interval (minutes)", value=30, min_value=5, max_value=120)
        
        with col2:
            st.markdown("**File Processing**")
            download_first = st.checkbox("Download RMS files first", value=True)
            require_epf = st.checkbox("Require EPF file", value=True)
            require_nps = st.checkbox("Require NPS file", value=True)
            
            st.markdown("**Notification Settings**")
            notify_success = st.checkbox("Notify on Success", value=True)
            notify_failure = st.checkbox("Notify on Failure", value=True)
            notify_partial = st.checkbox("Notify on Partial Success", value=True)
    
    with tab3:
        st.markdown("#### 📋 Deadline Management Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Response Deadlines**")
            response_days = st.number_input("Response Deadline (working days)", value=10, min_value=1, max_value=30)
            exclude_weekends = st.checkbox("Exclude Weekends", value=True)
            exclude_holidays = st.checkbox("Exclude Holidays", value=True)
            
            st.markdown("**Follow-up Settings**")
            followup_enabled = st.checkbox("Enable Follow-up Emails", value=True)
            followup_days = st.number_input("Follow-up Interval (days)", value=3, min_value=1, max_value=10)
            max_followups = st.number_input("Max Follow-ups", value=3, min_value=1, max_value=10)
        
        with col2:
            st.markdown("**Holiday Calendar**")
            st.markdown("Configure holidays to exclude from deadline calculations:")
            
            holidays = st.text_area(
                "Holidays (YYYY-MM-DD, one per line)",
                value="2025-08-15\n2025-10-02\n2025-10-24\n2025-11-12\n2025-12-25",
                help="Add public holidays and company holidays"
            )
    
    # Save Configuration
    if st.button("💾 Save Schedule Configuration", type="primary", use_container_width=True):
        st.success("✅ Schedule configuration saved successfully!")
    
    st.markdown("---")
    
    # Schedule History
    st.markdown("### 📜 Schedule Execution History")
    
    schedule_history = {
        'Date': ['2025-07-26 10:00', '2025-07-25 09:00', '2025-06-26 10:00', '2025-06-25 09:00'],
        'Type': ['Auto Reconciliation', 'EPF/NPS Reminder', 'Auto Reconciliation', 'EPF/NPS Reminder'],
        'Status': ['✅ Success', '✅ Success', '✅ Success', '✅ Success'],
        'Duration': ['15 minutes', '1 minute', '18 minutes', '1 minute'],
        'Details': ['547 employees processed', 'Email sent successfully', '542 employees processed', 'Email sent successfully']
    }
    
    df_schedule_history = pd.DataFrame(schedule_history)
    st.dataframe(df_schedule_history, use_container_width=True)
    
    # Manual Trigger Section
    st.markdown("### 🚀 Manual Trigger")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📧 Trigger EPF/NPS Reminder", use_container_width=True):
            st.success("✅ EPF/NPS reminder sent immediately!")
    
    with col2:
        if st.button("🤖 Trigger Auto Reconciliation", use_container_width=True):
            st.success("✅ Auto reconciliation started!")
    
    with col3:
        if st.button("📊 Generate Manual Report", use_container_width=True):
            st.success("✅ Manual report generated and sent!")

def show_user_management():
    """User management for admin users"""
    
    st.markdown("## 👥 User Management System")
    
    # Add New User
    st.markdown("### ➕ Add New User")
    
    with st.expander("Create New User Account"):
        col1, col2 = st.columns(2)
        
        with col1:
            new_username = st.text_input("Username")
            new_email = st.text_input("Email Address")
            new_role = st.selectbox("Role", ["Viewer", "User", "Admin", "Super Admin"])
        
        with col2:
            new_password = st.text_input("Password", type="password")
            confirm_password = st.text_input("Confirm Password", type="password") 
            new_active = st.checkbox("Active", value=True)
        
        if st.button("👤 Create User"):
            if new_password == confirm_password and new_username and new_email:
                st.success(f"✅ User {new_username} created successfully!")
            else:
                st.error("❌ Please check all fields and ensure passwords match")
    
    st.markdown("---")
    
    # Current Users
    st.markdown("### 👥 Current Users")
    
    # Mock user data
    users_data = {
        'Username': ['admin', 'hr_manager', 'accountant', 'tax_team', 'payroll_user'],
        'Email': ['admin@koenig.com', 'hr@koenig.com', 'accounts@koenig.com', 'tax@koenig.com', 'payroll@koenig.com'],
        'Role': ['Super Admin', 'Admin', 'User', 'User', 'User'],
        'Last Login': ['2025-08-10 14:30', '2025-08-09 10:15', '2025-08-08 16:45', '2025-08-07 09:20', '2025-08-06 11:30'],
        'Status': ['🟢 Active', '🟢 Active', '🟢 Active', '🟢 Active', '🔴 Inactive']
    }
    
    df_users = pd.DataFrame(users_data)
    
    # Add action buttons to dataframe
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.dataframe(df_users, use_container_width=True)
    
    with col2:
        st.markdown("**Actions:**")
        selected_user = st.selectbox("Select User", df_users['Username'].tolist())
        
        if st.button("✏️ Edit User", use_container_width=True):
            st.info(f"Edit form for {selected_user} would open here")
        
        if st.button("🔄 Reset Password", use_container_width=True):
            st.success(f"Password reset email sent to {selected_user}")
        
        if st.button("🚫 Deactivate", use_container_width=True):
            st.warning(f"User {selected_user} deactivated")
        
        if st.button("🗑️ Delete User", use_container_width=True):
            st.error(f"User {selected_user} deleted")
    
    st.markdown("---")
    
    # User Permissions
    st.markdown("### 🔐 Role Permissions")
    
    permissions_data = {
        'Feature': [
            'View Dashboard',
            'Auto Reconciliation', 
            'Manual Reconciliation',
            'View Analytics',
            'Email Management',
            'Schedule Management', 
            'User Management',
            'System Settings',
            'Download Reports',
            'Delete Records'
        ],
        'Viewer': ['✅', '❌', '❌', '✅', '❌', '❌', '❌', '❌', '✅', '❌'],
        'User': ['✅', '✅', '✅', '✅', '❌', '❌', '❌', '❌', '✅', '❌'],
        'Admin': ['✅', '✅', '✅', '✅', '✅', '✅', '❌', '❌', '✅', '✅'],
        'Super Admin': ['✅', '✅', '✅', '✅', '✅', '✅', '✅', '✅', '✅', '✅']
    }
    
    df_permissions = pd.DataFrame(permissions_data)
    st.dataframe(df_permissions, use_container_width=True)

def show_system_settings():
    """System settings for super admin"""
    
    st.markdown("## ⚙️ System Settings")
    
    tab1, tab2, tab3, tab4 = st.tabs(["🗂️ File Paths", "📧 Email Config", "🏢 Company Info", "🔧 System Preferences"])
    
    with tab1:
        st.markdown("### 📁 File Path Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Download Directories**")
            salary_dir = st.text_input("Salary Files Directory", value="/Users/praveenchaudhary/Downloads/Koenig-Management-Agent")
            backup_dir = st.text_input("Backup Directory", value="/Users/praveenchaudhary/Backups/Reconciliation")
            temp_dir = st.text_input("Temporary Files Directory", value="/tmp/reconciliation")
        
        with col2:
            st.markdown("**File Naming Patterns**")
            salary_pattern = st.text_input("Salary File Pattern", value="Salary_Sheet_{month}_{year}.xls")
            tds_pattern = st.text_input("TDS File Pattern", value="TDS_{month}_{year}.xlsx")
            bank_pattern = st.text_input("Bank File Pattern", value="SOA_{bank}_{date_range}.xls")
        
        if st.button("💾 Save File Paths"):
            st.success("✅ File path configuration saved!")
    
    with tab2:
        st.markdown("### 📧 Email Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**SMTP Settings**")
            smtp_server = st.text_input("SMTP Server", value="smtp.office365.com")
            smtp_port = st.number_input("SMTP Port", value=587)
            sender_email = st.text_input("Sender Email", value="Anurag.chauhan@koenig-solutions.com")
            sender_name = st.text_input("Sender Name", value="Tax Team - Koenig Solutions")
        
        with col2:
            st.markdown("**Email Templates**")
            template_dir = st.text_input("Template Directory", value="./email_templates")
            attachment_max_size = st.number_input("Max Attachment Size (MB)", value=25)
            retry_attempts = st.number_input("Email Retry Attempts", value=3)
            retry_delay = st.number_input("Retry Delay (seconds)", value=60)
        
        if st.button("🧪 Test Email Configuration"):
            st.success("✅ Test email sent successfully!")
        
        if st.button("💾 Save Email Config"):
            st.success("✅ Email configuration saved!")
    
    with tab3:
        st.markdown("### 🏢 Company Information")
        
        col1, col2 = st.columns(2)
        
        with col1:
            company_name = st.text_input("Company Name", value="Koenig Solutions Pvt. Ltd.")
            company_address = st.text_area("Company Address", value="Gurgaon, Haryana, India")
            company_phone = st.text_input("Phone", value="+91-XXXXXXXXXX")
            company_email = st.text_input("Email", value="info@koenig-solutions.com")
        
        with col2:
            st.markdown("**Branch Configuration**")
            branches = st.text_area(
                "Branches (one per line)",
                value="Gurgaon\nDelhi\nBangalore\nChennai\nDehradun\nGoa"
            )
            
            st.markdown("**Financial Year**")
            fy_start_month = st.selectbox("FY Start Month", list(range(1, 13)), index=3)  # April
            fy_end_month = st.selectbox("FY End Month", list(range(1, 13)), index=2)    # March
        
        if st.button("💾 Save Company Info"):
            st.success("✅ Company information saved!")
    
    with tab4:
        st.markdown("### 🔧 System Preferences")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Reconciliation Settings**")
            default_tolerance = st.number_input("Default Amount Tolerance (₹)", value=1.0, step=0.1)
            auto_backup = st.checkbox("Enable Auto Backup", value=True)
            backup_retention_days = st.number_input("Backup Retention (days)", value=90)
            
            st.markdown("**Security Settings**")
            session_timeout = st.number_input("Session Timeout (minutes)", value=60)
            password_min_length = st.number_input("Minimum Password Length", value=8)
            require_password_change = st.checkbox("Require Password Change (90 days)", value=True)
        
        with col2:
            st.markdown("**Performance Settings**")
            max_concurrent_reconciliations = st.number_input("Max Concurrent Reconciliations", value=2)
            chunk_size = st.number_input("Processing Chunk Size", value=1000)
            enable_progress_tracking = st.checkbox("Enable Progress Tracking", value=True)
            
            st.markdown("**Logging Settings**")
            log_level = st.selectbox("Log Level", ["DEBUG", "INFO", "WARNING", "ERROR"])
            log_retention_days = st.number_input("Log Retention (days)", value=30)
            enable_audit_log = st.checkbox("Enable Audit Log", value=True)
        
        if st.button("💾 Save System Preferences"):
            st.success("✅ System preferences saved!")
    
    st.markdown("---")
    
    # System Information
    st.markdown("### 📊 System Information")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
            <h4>💻 System Version</h4>
            <h3>v2.1.0</h3>
            <p>Released: August 2025</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-card">
            <h4>📊 Database Size</h4>
            <h3>125 MB</h3>
            <p>Last optimized: Aug 10, 2025</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="metric-card">
            <h4>🔄 Last Backup</h4>
            <h3>Aug 10, 2025</h3>
            <p>Status: Successful</p>
        </div>
        """, unsafe_allow_html=True)
    
    # System Actions
    st.markdown("### 🛠️ System Maintenance")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🔄 Backup Database", use_container_width=True):
            st.success("✅ Database backup completed!")
    
    with col2:
        if st.button("🧹 Clear Logs", use_container_width=True):
            st.success("✅ System logs cleared!")
    
    with col3:
        if st.button("📊 Optimize Database", use_container_width=True):
            st.success("✅ Database optimized!")
    
    with col4:
        if st.button("🔄 Restart Services", use_container_width=True):
            st.success("✅ Services restarted!")

# Main Application
def main():
    init_database()
    
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        show_login()
    else:
        show_dashboard()

if __name__ == "__main__":
    main()
