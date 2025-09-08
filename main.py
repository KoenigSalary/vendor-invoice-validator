"""
FnF (Final and Full) Settlement Dashboard
A comprehensive Streamlit application for employee final settlement calculations
"""

import streamlit as st
import pandas as pd
import numpy as np
import json
import hashlib
import smtplib
import os
from datetime import datetime, date, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import gspread
from google.auth import default
import io
import base64
from pathlib import Path
import logging
from typing import Dict, Any, Optional, Tuple

# Configure page
st.set_page_config(
    page_title="FnF Settlement Dashboard",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better UI
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        margin-bottom: 2rem;
        color: #1f77b4;
    }
    .step-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #2e8b57;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .warning-box {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        color: #856404;
        padding: 1rem;
        border-radius: 0.25rem;
        margin: 1rem 0;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
        padding: 1rem;
        border-radius: 0.25rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'user_role' not in st.session_state:
    st.session_state.user_role = None
if 'username' not in st.session_state:
    st.session_state.username = None
if 'employee_data' not in st.session_state:
    st.session_state.employee_data = {}
if 'fnf_results' not in st.session_state:
    st.session_state.fnf_results = {}

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/user/output/fnf_audit.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# ==================== AUTHENTICATION SYSTEM ====================

class UserManager:
    """User management system for FnF Dashboard"""

    def __init__(self):
        self.users_file = '/home/user/output/users.json'
        self.tax_team_emails = ['tax.team@company.com', 'hr.manager@company.com']
        self._initialize_users()

    def _initialize_users(self):
        """Initialize default users if file doesn't exist"""
        if not os.path.exists(self.users_file):
            default_users = {
                'admin': {
                    'password_hash': self._hash_password('admin123'),
                    'role': 'admin',
                    'email': 'admin@company.com',
                    'created_date': datetime.now().isoformat()
                },
                'tax_team': {
                    'password_hash': self._hash_password('tax2024'),
                    'role': 'tax_team',
                    'email': 'tax.team@company.com',
                    'created_date': datetime.now().isoformat()
                },
                'hr_user': {
                    'password_hash': self._hash_password('hr2024'),
                    'role': 'hr',
                    'email': 'hr@company.com',
                    'created_date': datetime.now().isoformat()
                }
            }
            self._save_users(default_users)
            logger.info("Initialized default users")

    def _hash_password(self, password: str) -> str:
        """Hash password using SHA-256"""
        return hashlib.sha256(password.encode()).hexdigest()

    def _load_users(self) -> Dict[str, Any]:
        """Load users from JSON file"""
        try:
            with open(self.users_file, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save_users(self, users: Dict[str, Any]):
        """Save users to JSON file"""
        with open(self.users_file, 'w') as f:
            json.dump(users, f, indent=2)

    def authenticate(self, username: str, password: str) -> Tuple[bool, Optional[str]]:
        """Authenticate user and return role"""
        users = self._load_users()

        if username in users:
            password_hash = self._hash_password(password)
            if users[username]['password_hash'] == password_hash:
                logger.info(f"Successful login: {username}")
                return True, users[username]['role']
            else:
                logger.warning(f"Failed login attempt: {username}")
                return False, None

        logger.warning(f"Unknown user login attempt: {username}")
        return False, None

    def change_password(self, username: str, old_password: str, new_password: str) -> bool:
        """Change user password and notify tax team"""
        users = self._load_users()

        if username not in users:
            return False

        # Verify old password
        old_password_hash = self._hash_password(old_password)
        if users[username]['password_hash'] != old_password_hash:
            return False

        # Update password
        users[username]['password_hash'] = self._hash_password(new_password)
        users[username]['password_changed'] = datetime.now().isoformat()
        self._save_users(users)

        # Notify tax team
        self._notify_tax_team_password_change(username, new_password)
        logger.info(f"Password changed for user: {username}")

        return True

    def _notify_tax_team_password_change(self, username: str, new_password: str):
        """Send email notification to tax team about password change"""
        try:
            subject = f"FnF System - Password Changed for {username}"
            body = f"""
            Password has been changed for FnF Settlement System.

            Username: {username}
            New Password: {new_password}
            Changed On: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

            Please update your records accordingly.

            Regards,
            FnF Settlement System
            """

            # In a production environment, you would configure SMTP settings
            logger.info(f"Password change notification sent for user: {username}")
            # self._send_email(self.tax_team_emails, subject, body)

        except Exception as e:
            logger.error(f"Failed to send password change notification: {str(e)}")

def show_login_page():
    """Display login page"""
    st.markdown('<h1 class="main-header">🔐 FnF Settlement Dashboard</h1>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.markdown("### Please Login to Continue")

        with st.form("login_form"):
            username = st.text_input("Username", placeholder="Enter your username")
            password = st.text_input("Password", type="password", placeholder="Enter your password")
            login_button = st.form_submit_button("Login", use_container_width=True)

            if login_button:
                if username and password:
                    user_manager = UserManager()
                    success, role = user_manager.authenticate(username, password)

                    if success:
                        st.session_state.authenticated = True
                        st.session_state.username = username
                        st.session_state.user_role = role
                        st.success("Login successful!")
                        st.rerun()
                    else:
                        st.error("Invalid username or password")
                else:
                    st.error("Please enter both username and password")

        st.markdown("---")
        st.info("""
        **Default Login Credentials:**
        - Admin: admin / admin123
        - Tax Team: tax_team / tax2024
        - HR User: hr_user / hr2024
        """)

def show_change_password():
    """Show change password interface"""
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Account Settings")

    if st.sidebar.button("Change Password"):
        with st.sidebar.form("change_password_form"):
            st.markdown("#### Change Password")
            old_password = st.text_input("Current Password", type="password")
            new_password = st.text_input("New Password", type="password")
            confirm_password = st.text_input("Confirm New Password", type="password")

            if st.form_submit_button("Update Password"):
                if not all([old_password, new_password, confirm_password]):
                    st.error("All fields are required")
                elif new_password != confirm_password:
                    st.error("New passwords don't match")
                elif len(new_password) < 6:
                    st.error("Password must be at least 6 characters long")
                else:
                    user_manager = UserManager()
                    if user_manager.change_password(st.session_state.username, old_password, new_password):
                        st.success("Password updated successfully!")
                        st.info("Tax team has been notified of the password change.")
                    else:
                        st.error("Current password is incorrect")

def logout():
    """Logout function"""
    for key in ['authenticated', 'username', 'user_role', 'employee_data', 'fnf_results']:
        if key in st.session_state:
            del st.session_state[key]
    st.success("Logged out successfully!")
    st.rerun()



# ==================== INVESTMENT DEDUCTIONS SYSTEM ====================

def investment_deductions_input():
    """Investment and deduction options with proper tax regime handling"""
    st.subheader("💼 Investment & Deductions")

    # Get basic salary for calculations
    basic_salary = st.session_state.employee_data.get('basic_salary', 0)

    # Tax regime selection
    tax_regime = st.radio(
        "Select Tax Regime:",
        ["Old Tax Regime", "New Tax Regime"],
        help="New regime has limited deductions but lower tax rates"
    )

    if tax_regime == "Old Tax Regime":
        return _old_regime_deductions(basic_salary)
    else:
        return _new_regime_deductions(basic_salary)

def _old_regime_deductions(basic_salary: float) -> Dict[str, Any]:
    """Handle deductions for Old Tax Regime"""
    st.info("📊 Old Tax Regime - Multiple deductions available")

    # Section 80C Investments
    st.markdown("### 📊 Section 80C Investments (Max ₹1,50,000)")
    col1, col2 = st.columns(2)

    with col1:
        ppf = st.number_input("PPF (₹)", min_value=0.0, max_value=150000.0, value=0.0, step=5000.0, key="ppf_old")
        epf_employee = st.number_input("EPF Employee Contribution (₹)", min_value=0.0, value=0.0, step=1000.0, key="epf_emp_old")
        elss = st.number_input("ELSS Mutual Funds (₹)", min_value=0.0, value=0.0, step=5000.0, key="elss_old")
        life_insurance = st.number_input("Life Insurance Premium (₹)", min_value=0.0, value=0.0, step=2000.0, key="life_ins_old")

    with col2:
        fd_5year = st.number_input("5-Year Fixed Deposit (₹)", min_value=0.0, value=0.0, step=5000.0, key="fd_old")
        nsc = st.number_input("NSC (₹)", min_value=0.0, value=0.0, step=5000.0, key="nsc_old")
        sukanya_samriddhi = st.number_input("Sukanya Samriddhi (₹)", min_value=0.0, value=0.0, step=5000.0, key="sukanya_old")
        tuition_fees = st.number_input("Children Tuition Fees (₹)", min_value=0.0, value=0.0, step=5000.0, key="tuition_old")

    total_80c = ppf + epf_employee + elss + life_insurance + fd_5year + nsc + sukanya_samriddhi + tuition_fees
    eligible_80c = min(total_80c, 150000)

    if total_80c > 150000:
        st.warning(f"⚠️ Total 80C (₹{total_80c:,.0f}) exceeds limit. Eligible: ₹{eligible_80c:,.0f}")
    else:
        st.success(f"✅ Section 80C: ₹{eligible_80c:,.0f}")

    # Employer Contributions (Separate from 80C)
    st.markdown("### 💼 Employer Contributions (Exempt)")
    col1, col2 = st.columns(2)

    with col1:
        epf_employer = st.number_input("EPF Employer Contribution (₹)", 
                                      min_value=0.0, value=0.0, step=1000.0,
                                      help="Exempt up to ₹1.5L annually", key="epf_emp_contr_old")

    with col2:
        # Validate EPF employer contribution limit
        max_epf_employer = min(epf_employer, 150000)
        if epf_employer > 150000:
            st.warning(f"⚠️ EPF employer contribution limited to ₹1.5L: ₹{max_epf_employer:,.0f}")
        else:
            st.info(f"EPF Employer Contribution: ₹{max_epf_employer:,.0f}")

    # Health Insurance with age-based limits
    st.markdown("### 🏥 Section 80D (Health Insurance)")
    col1, col2 = st.columns(2)

    with col1:
        employee_age = st.number_input("Employee Age", min_value=18, max_value=100, value=30, key="emp_age_old")
        self_limit = 50000 if employee_age >= 60 else 25000
        health_insurance_self = st.number_input(f"Self & Family (₹) [Max: ₹{self_limit:,}]", 
                                              min_value=0.0, max_value=float(self_limit), 
                                              value=0.0, step=1000.0, key="health_self_old")

    with col2:
        parents_age = st.number_input("Parents Age (0 if not applicable)", min_value=0, max_value=100, value=0, key="parents_age_old")
        parents_limit = 50000 if parents_age >= 60 else (25000 if parents_age > 0 else 0)
        health_insurance_parents = st.number_input(f"Parents (₹) [Max: ₹{parents_limit:,}]", 
                                                 min_value=0.0, max_value=float(parents_limit), 
                                                 value=0.0, step=1000.0, key="health_parents_old")

    total_80d = health_insurance_self + health_insurance_parents

    # Other Deductions
    st.markdown("### 🏦 Other Deductions")
    col1, col2 = st.columns(2)

    with col1:
        section_80dd = st.number_input("80DD - Disability (₹)", min_value=0.0, max_value=125000.0, value=0.0, step=5000.0, key="80dd_old")
        section_80ddb = st.number_input("80DDB - Medical Treatment (₹)", min_value=0.0, max_value=100000.0, value=0.0, step=5000.0, key="80ddb_old")
        home_loan_interest = st.number_input("Home Loan Interest (₹)", min_value=0.0, max_value=200000.0, value=0.0, step=5000.0, key="home_loan_old")

    with col2:
        education_loan_interest = st.number_input("Education Loan Interest (₹)", min_value=0.0, value=0.0, step=2000.0, key="edu_loan_old")
        nps_80ccd_1b = st.number_input("NPS 80CCD(1B) (₹)", min_value=0.0, max_value=50000.0, value=0.0, step=5000.0, key="nps_1b_old")
        nps_80ccd_2 = st.number_input("NPS 80CCD(2) Employer (₹)", min_value=0.0, value=0.0, step=5000.0, key="nps_2_old")

    # Validate 80CCD(2) limit
    max_80ccd_2 = min(nps_80ccd_2, basic_salary * 0.1) if basic_salary > 0 else nps_80ccd_2
    if nps_80ccd_2 > basic_salary * 0.1 and basic_salary > 0:
        st.warning(f"⚠️ 80CCD(2) limited to 10% of basic salary: ₹{max_80ccd_2:,.0f}")

    # Exempt Allowances
    st.markdown("### 🚗 Exempt Allowances (Old Regime) - FY 2024-25")
    col1, col2 = st.columns(2)

    with col1:
        conveyance_allowance = st.number_input("Conveyance Allowance (₹)", min_value=0.0, max_value=21600.0, value=0.0, step=1000.0, key="conv_old")
        helper_allowance = st.number_input("Helper Allowance (₹)", min_value=0.0, max_value=25200.0, value=0.0, step=500.0, key="helper_old")
        lta = st.number_input("LTA (₹)", min_value=0.0, value=0.0, step=5000.0, key="lta_old")

    with col2:
        tel_broadband = st.number_input("Mobile & Internet (₹)", min_value=0.0, value=0.0, step=500.0, key="mobile_old")
        ld_allowance = st.number_input("L&D Allowance (₹)", min_value=0.0, value=0.0, step=1000.0, key="ld_old")
        hra_exemption = st.number_input("HRA Exemption (₹)", min_value=0.0, value=0.0, step=2000.0, key="hra_old")

    # Calculate totals
    total_other_deductions = (section_80dd + section_80ddb + home_loan_interest + 
                            education_loan_interest + nps_80ccd_1b + max_80ccd_2)
    total_exempt_allowances = (conveyance_allowance + helper_allowance + lta + 
                             tel_broadband + ld_allowance + hra_exemption)

    total_deductions_old_regime = eligible_80c + total_80d + total_other_deductions
    total_exempt_amount = max_epf_employer + total_exempt_allowances

    # Summary
    st.markdown("### 📋 Investment & Deduction Summary (Old Regime)")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Section 80C", f"₹{eligible_80c:,.0f}")
        st.metric("Section 80D", f"₹{total_80d:,.0f}")

    with col2:
        st.metric("Other Deductions", f"₹{total_other_deductions:,.0f}")
        st.metric("Exempt Amounts", f"₹{total_exempt_amount:,.0f}")

    with col3:
        st.metric("Total Tax Deductions", f"₹{total_deductions_old_regime:,.0f}")
        st.info("*For Old Tax Regime Only*")

    return {
        'tax_regime': 'old',
        '80c_total': eligible_80c,
        '80d_total': total_80d,
        'other_deductions': total_other_deductions,
        'epf_employer_exempt': max_epf_employer,
        'exempt_allowances': total_exempt_allowances,
        'total_deductions': total_deductions_old_regime,
        'total_exempt_amount': total_exempt_amount,
        'breakdown': {
            'ppf': ppf, 'epf_employee': epf_employee, 'epf_employer': max_epf_employer,
            'elss': elss, 'life_insurance': life_insurance,
            'fd_5year': fd_5year, 'nsc': nsc, 'sukanya_samriddhi': sukanya_samriddhi, 
            'tuition_fees': tuition_fees, 'health_insurance_self': health_insurance_self, 
            'health_insurance_parents': health_insurance_parents, 'section_80dd': section_80dd, 
            'section_80ddb': section_80ddb, 'home_loan_interest': home_loan_interest, 
            'education_loan_interest': education_loan_interest, 'nps_80ccd_1b': nps_80ccd_1b, 
            'nps_80ccd_2': max_80ccd_2, 'conveyance_allowance': conveyance_allowance, 
            'helper_allowance': helper_allowance, 'lta': lta, 'tel_broadband': tel_broadband, 
            'ld_allowance': ld_allowance, 'hra_exemption': hra_exemption
        }
    }

def _new_regime_deductions(basic_salary: float) -> Dict[str, Any]:
    """Handle deductions for New Tax Regime - Only 80CCD(2) allowed"""
    st.warning("⚠️ New Tax Regime - Limited deductions available but lower tax rates")

    st.markdown("### 🏦 Allowed Deductions (New Regime)")
    st.info("Under New Tax Regime, only employer NPS contribution [80CCD(2)] is allowed as deduction")

    col1, col2 = st.columns(2)

    with col1:
        nps_80ccd_2 = st.number_input("NPS 80CCD(2) Employer Contribution (₹)", 
                                     min_value=0.0, value=0.0, step=5000.0,
                                     help="Only deduction allowed in new regime", key="nps_2_new")

    with col2:
        # Validate 80CCD(2) limit (10% of basic salary)
        max_80ccd_2 = min(nps_80ccd_2, basic_salary * 0.1) if basic_salary > 0 else nps_80ccd_2
        if nps_80ccd_2 > basic_salary * 0.1 and basic_salary > 0:
            st.warning(f"⚠️ Limited to 10% of basic salary: ₹{max_80ccd_2:,.0f}")
        else:
            st.success(f"✅ Eligible 80CCD(2): ₹{max_80ccd_2:,.0f}")

    # No other allowances are exempt in new regime
    st.markdown("### 🚫 No Exempt Allowances in New Regime")
    st.info("""
    In New Tax Regime:
    - No HRA exemption
    - No LTA exemption
    - No special allowances
    - All salary components are taxable
    """)

    # Summary
    st.markdown("### 📋 Deduction Summary (New Regime)")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Deductions", f"₹{max_80ccd_2:,.0f}")

    with col2:
        st.metric("Exempt Allowances", "₹0")

    with col3:
        st.info("Lower tax rates apply")

    return {
        'tax_regime': 'new',
        'total_deductions': max_80ccd_2,
        'new_regime_deduction': max_80ccd_2,
        'exempt_allowances': 0,
        'breakdown': {
            'nps_80ccd_2': max_80ccd_2
        }
    }



# ==================== GRATUITY & TAX CALCULATIONS ====================

def calculate_gratuity(basic_salary: float, years_of_service: int, months_of_service: int = 0) -> Dict[str, float]:
    """
    Calculate gratuity based on Payment of Gratuity Act, 1972
    Formula: (Basic Salary × Years of Service × 15) / 26
    """
    try:
        if years_of_service < 5:
            return {
                'total_gratuity': 0.0,
                'exempt_gratuity': 0.0,
                'taxable_gratuity': 0.0,
                'eligibility_status': 'Not eligible - Less than 5 years of service'
            }

        # Calculate total service in days
        total_service_years = years_of_service + (months_of_service / 12)

        # Gratuity calculation: (Basic × Years × 15) / 26
        # For each completed year: (Basic × 15) / 26
        gratuity = (basic_salary * total_service_years * 15) / 26

        # Gratuity exemption limit (currently ₹20 lakhs as per Section 10(10))
        exemption_limit = 2000000
        exempt_gratuity = min(gratuity, exemption_limit)
        taxable_gratuity = max(0, gratuity - exemption_limit)

        return {
            'total_gratuity': round(gratuity, 2),
            'exempt_gratuity': round(exempt_gratuity, 2),
            'taxable_gratuity': round(taxable_gratuity, 2),
            'eligibility_status': f'Eligible - {total_service_years:.1f} years of service'
        }

    except Exception as e:
        logger.error(f"Gratuity calculation error: {str(e)}")
        return {
            'total_gratuity': 0.0,
            'exempt_gratuity': 0.0,
            'taxable_gratuity': 0.0,
            'eligibility_status': f'Calculation error: {str(e)}'
        }

def calculate_tds_old_regime(annual_income: float, deductions_data: Dict[str, Any]) -> Dict[str, float]:
    """Calculate TDS under Old Tax Regime (AY 2024-25)"""
    try:
        if annual_income is None or annual_income <= 0:
            raise ValueError("Annual income should be a positive number")

        # Standard deduction
        standard_deduction = 50000

        # Total deductions from investment data
        total_deductions = deductions_data.get('total_deductions', 0)
        exempt_allowances = deductions_data.get('total_exempt_amount', 0)

        # Calculate taxable income
        # Income - Standard Deduction - Investment Deductions - Exempt Allowances
        taxable_income = annual_income - standard_deduction - total_deductions - exempt_allowances

        # Ensure taxable income is not negative
        taxable_income = max(0, taxable_income)

        # Old regime tax slabs (AY 2024-25)
        tax = 0
        if taxable_income <= 250000:
            tax = 0
        elif taxable_income <= 500000:
            tax = (taxable_income - 250000) * 0.05
        elif taxable_income <= 1000000:
            tax = 12500 + (taxable_income - 500000) * 0.20
        else:
            tax = 112500 + (taxable_income - 1000000) * 0.30

        # Add 4% cess
        tax_with_cess = tax * 1.04

        return {
            'taxable_income': round(taxable_income, 2),
            'tax_before_cess': round(tax, 2),
            'cess_amount': round(tax * 0.04, 2),
            'total_tax': round(tax_with_cess, 2),
            'effective_rate': round((tax_with_cess / annual_income * 100), 2) if annual_income > 0 else 0,
            'standard_deduction': standard_deduction,
            'total_deductions': total_deductions,
            'exempt_allowances': exempt_allowances
        }

    except Exception as e:
        logger.error(f"Old regime tax calculation error: {str(e)}")
        return {
            'taxable_income': 0, 'tax_before_cess': 0, 'cess_amount': 0, 
            'total_tax': 0, 'effective_rate': 0, 'standard_deduction': 0,
            'total_deductions': 0, 'exempt_allowances': 0
        }

def calculate_tds_new_regime(annual_income: float, deductions_data: Dict[str, Any]) -> Dict[str, float]:
    """Calculate TDS under New Tax Regime (AY 2024-25)"""
    try:
        if annual_income is None or annual_income <= 0:
            raise ValueError("Annual income should be a positive number")

        # Only 80CCD(2) deduction allowed in new regime
        allowed_deduction = deductions_data.get('new_regime_deduction', 0)

        # Calculate taxable income (No standard deduction in new regime)
        taxable_income = annual_income - allowed_deduction

        # Ensure taxable income is not negative
        taxable_income = max(0, taxable_income)

        # New regime tax slabs (AY 2024-25) with rebate
        tax = 0
        if taxable_income <= 300000:
            tax = 0
        elif taxable_income <= 600000:
            tax = (taxable_income - 300000) * 0.05
        elif taxable_income <= 900000:
            tax = 15000 + (taxable_income - 600000) * 0.10
        elif taxable_income <= 1200000:
            tax = 45000 + (taxable_income - 900000) * 0.15
        elif taxable_income <= 1500000:
            tax = 90000 + (taxable_income - 1200000) * 0.20
        else:
            tax = 150000 + (taxable_income - 1500000) * 0.30

        # Rebate under section 87A (if total income <= 700000)
        if annual_income <= 700000:
            rebate = min(tax, 25000)
            tax = max(0, tax - rebate)
        else:
            rebate = 0

        # Add 4% cess
        tax_with_cess = tax * 1.04

        return {
            'taxable_income': round(taxable_income, 2),
            'tax_before_cess': round(tax, 2),
            'rebate_87a': round(rebate, 2),
            'cess_amount': round(tax * 0.04, 2),
            'total_tax': round(tax_with_cess, 2),
            'effective_rate': round((tax_with_cess / annual_income * 100), 2) if annual_income > 0 else 0,
            'allowed_deduction': allowed_deduction
        }

    except Exception as e:
        logger.error(f"New regime tax calculation error: {str(e)}")
        return {
            'taxable_income': 0, 'tax_before_cess': 0, 'rebate_87a': 0,
            'cess_amount': 0, 'total_tax': 0, 'effective_rate': 0, 'allowed_deduction': 0
        }

def calculate_leave_encashment(basic_salary: float, leave_balance: int, per_day_salary: float = None) -> float:
    """Calculate leave encashment amount"""
    try:
        if per_day_salary is None:
            # Calculate per day salary (Basic / 30)
            per_day_salary = basic_salary / 30

        leave_encashment = leave_balance * per_day_salary
        return round(leave_encashment, 2)

    except Exception as e:
        logger.error(f"Leave encashment calculation error: {str(e)}")
        return 0.0

def calculate_notice_pay(basic_salary: float, notice_period_days: int) -> float:
    """Calculate notice period payment"""
    try:
        per_day_salary = basic_salary / 30
        notice_pay = notice_period_days * per_day_salary
        return round(notice_pay, 2)

    except Exception as e:
        logger.error(f"Notice pay calculation error: {str(e)}")
        return 0.0

def compare_tax_regimes(annual_income: float, old_regime_deductions: Dict[str, Any], new_regime_deductions: Dict[str, Any]) -> Dict[str, Any]:
    """Compare tax liability under both regimes"""
    old_regime_tax = calculate_tds_old_regime(annual_income, old_regime_deductions)
    new_regime_tax = calculate_tds_new_regime(annual_income, new_regime_deductions)

    savings = old_regime_tax['total_tax'] - new_regime_tax['total_tax']
    better_regime = "New Regime" if new_regime_tax['total_tax'] < old_regime_tax['total_tax'] else "Old Regime"

    return {
        'old_regime': old_regime_tax,
        'new_regime': new_regime_tax,
        'tax_savings': round(savings, 2),
        'better_regime': better_regime,
        'savings_percentage': round((abs(savings) / max(old_regime_tax['total_tax'], new_regime_tax['total_tax']) * 100), 2) if max(old_regime_tax['total_tax'], new_regime_tax['total_tax']) > 0 else 0
    }



# ==================== EMAIL NOTIFICATION SYSTEM ====================

class EmailManager:
    """Email notification system for FnF settlements"""

    def __init__(self):
        # Email configuration (to be set in production)
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        self.sender_email = "hr@company.com"  # To be configured
        self.sender_password = ""  # To be set securely

    def send_fnf_email(self, employee_data: Dict[str, Any], fnf_results: Dict[str, Any]) -> bool:
        """Send FnF calculation details to candidate"""
        try:
            recipient_email = employee_data.get('email', '')
            if not recipient_email:
                logger.warning("No email address provided for employee")
                return False

            subject = f"F&F Settlement Details - {employee_data.get('name', 'Employee')}"

            # Create HTML email body
            html_body = self._create_fnf_email_template(employee_data, fnf_results)

            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.sender_email
            msg['To'] = recipient_email

            # Add HTML content
            html_part = MIMEText(html_body, 'html')
            msg.attach(html_part)

            # In production, configure SMTP settings properly
            logger.info(f"FnF email prepared for: {recipient_email}")

            # For demo purposes, save email content to file
            self._save_email_to_file(employee_data, html_body)

            return True

        except Exception as e:
            logger.error(f"Failed to send FnF email: {str(e)}")
            return False

    def _create_fnf_email_template(self, employee_data: Dict[str, Any], fnf_results: Dict[str, Any]) -> str:
        """Create HTML email template for FnF settlement"""

        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>F&F Settlement Details</title>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 800px; margin: 0 auto; padding: 20px; }}
                .header {{ background-color: #1f77b4; color: white; padding: 20px; text-align: center; }}
                .section {{ margin: 20px 0; padding: 15px; background-color: #f8f9fa; border-radius: 5px; }}
                .table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
                .table th, .table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                .table th {{ background-color: #e9ecef; font-weight: bold; }}
                .amount {{ font-weight: bold; color: #28a745; }}
                .deduction {{ color: #dc3545; }}
                .footer {{ margin-top: 30px; padding: 15px; background-color: #e9ecef; text-align: center; font-size: 12px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Final & Full Settlement Details</h1>
                    <p>Settlement Date: {datetime.now().strftime('%B %d, %Y')}</p>
                </div>

                <div class="section">
                    <h2>Employee Information</h2>
                    <table class="table">
                        <tr><td><strong>Employee Name</strong></td><td>{employee_data.get('name', 'N/A')}</td></tr>
                        <tr><td><strong>Employee ID</strong></td><td>{employee_data.get('emp_id', 'N/A')}</td></tr>
                        <tr><td><strong>Department</strong></td><td>{employee_data.get('department', 'N/A')}</td></tr>
                        <tr><td><strong>Date of Joining</strong></td><td>{employee_data.get('date_of_joining', 'N/A')}</td></tr>
                        <tr><td><strong>Last Working Day</strong></td><td>{employee_data.get('last_working_day', 'N/A')}</td></tr>
                    </table>
                </div>

                <div class="section">
                    <h2>Settlement Breakdown</h2>
                    <table class="table">
                        <tr><th>Component</th><th>Amount (₹)</th></tr>
                        <tr><td>Basic Salary (Pro-rata)</td><td class="amount">{fnf_results.get('basic_salary_prorata', 0):,.2f}</td></tr>
                        <tr><td>Other Allowances</td><td class="amount">{fnf_results.get('allowances', 0):,.2f}</td></tr>
                        <tr><td>Gratuity</td><td class="amount">{fnf_results.get('gratuity', 0):,.2f}</td></tr>
                        <tr><td>Leave Encashment</td><td class="amount">{fnf_results.get('leave_encashment', 0):,.2f}</td></tr>
                        <tr><td>Notice Period Recovery</td><td class="deduction">-{fnf_results.get('notice_recovery', 0):,.2f}</td></tr>
                        <tr><td>Other Recoveries</td><td class="deduction">-{fnf_results.get('other_recoveries', 0):,.2f}</td></tr>
                        <tr style="border-top: 2px solid #333;"><td><strong>Gross Settlement</strong></td><td class="amount"><strong>{fnf_results.get('gross_settlement', 0):,.2f}</strong></td></tr>
                        <tr><td>TDS Deducted</td><td class="deduction">-{fnf_results.get('tds_amount', 0):,.2f}</td></tr>
                        <tr style="border-top: 2px solid #333;"><td><strong>Net Payable Amount</strong></td><td class="amount"><strong>{fnf_results.get('net_payable', 0):,.2f}</strong></td></tr>
                    </table>
                </div>

                <div class="section">
                    <h2>Tax Details</h2>
                    <table class="table">
                        <tr><td><strong>Tax Regime</strong></td><td>{fnf_results.get('tax_regime', 'N/A')}</td></tr>
                        <tr><td><strong>Taxable Income</strong></td><td>₹{fnf_results.get('taxable_income', 0):,.2f}</td></tr>
                        <tr><td><strong>Total Tax Liability</strong></td><td>₹{fnf_results.get('total_tax', 0):,.2f}</td></tr>
                        <tr><td><strong>Effective Tax Rate</strong></td><td>{fnf_results.get('effective_rate', 0):.2f}%</td></tr>
                    </table>
                </div>

                <div class="section">
                    <h2>Important Notes</h2>
                    <ul>
                        <li>This settlement is final and includes all dues up to your last working day.</li>
                        <li>TDS has been deducted as per Income Tax regulations.</li>
                        <li>Form 16 will be issued separately for tax filing purposes.</li>
                        <li>Please retain this document for your records.</li>
                        <li>For any queries, please contact HR department within 30 days.</li>
                    </ul>
                </div>

                <div class="footer">
                    <p>This is a system-generated document. For queries, contact HR Department.</p>
                    <p>Company Name | HR Department | Email: hr@company.com</p>
                </div>
            </div>
        </body>
        </html>
        """

    def _save_email_to_file(self, employee_data: Dict[str, Any], email_content: str):
        """Save email content to file for demo purposes"""
        try:
            filename = f"/home/user/output/fnf_email_{employee_data.get('emp_id', 'unknown')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(email_content)
            logger.info(f"Email content saved to: {filename}")
        except Exception as e:
            logger.error(f"Failed to save email content: {str(e)}")

    def send_password_change_notification(self, username: str, new_password: str) -> bool:
        """Send password change notification to tax team"""
        try:
            tax_team_emails = ['tax.team@company.com', 'hr.manager@company.com']

            subject = f"FnF System - Password Changed for {username}"
            body = f"""
            Password Change Notification

            Username: {username}
            New Password: {new_password}
            Changed On: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

            Please update your records accordingly.

            Regards,
            FnF Settlement System
            """

            # In production, implement actual email sending
            logger.info(f"Password change notification prepared for: {username}")

            return True

        except Exception as e:
            logger.error(f"Failed to send password change notification: {str(e)}")
            return False



# ==================== DATA PERSISTENCE & EXPORT ====================

class DataManager:
    """Data persistence and export functionality"""

    def __init__(self):
        self.submissions_file = '/home/user/output/fnf_submissions.json'
        self.reports_dir = '/home/user/output/reports'
        self._ensure_directories()

    def _ensure_directories(self):
        """Create necessary directories"""
        os.makedirs(self.reports_dir, exist_ok=True)

    def save_fnf_submission(self, employee_data: Dict[str, Any], fnf_results: Dict[str, Any], 
                           deductions_data: Dict[str, Any]) -> str:
        """Save FnF submission data"""
        try:
            submission_id = f"FNF_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{employee_data.get('emp_id', 'unknown')}"

            submission_data = {
                'submission_id': submission_id,
                'timestamp': datetime.now().isoformat(),
                'processed_by': st.session_state.get('username', 'unknown'),
                'employee_data': employee_data,
                'deductions_data': deductions_data,
                'fnf_results': fnf_results,
                'status': 'completed'
            }

            # Load existing submissions
            submissions = self._load_submissions()

            # Add new submission
            submissions.append(submission_data)

            # Save updated submissions
            with open(self.submissions_file, 'w') as f:
                json.dump(submissions, f, indent=2, default=str)

            logger.info(f"FnF submission saved: {submission_id}")
            return submission_id

        except Exception as e:
            logger.error(f"Failed to save FnF submission: {str(e)}")
            return ""

    def _load_submissions(self) -> list:
        """Load existing submissions"""
        try:
            if os.path.exists(self.submissions_file):
                with open(self.submissions_file, 'r') as f:
                    return json.load(f)
            return []
        except Exception as e:
            logger.error(f"Failed to load submissions: {str(e)}")
            return []

    def get_submissions_summary(self) -> pd.DataFrame:
        """Get summary of all submissions"""
        try:
            submissions = self._load_submissions()

            if not submissions:
                return pd.DataFrame()

            summary_data = []
            for sub in submissions:
                summary_data.append({
                    'Submission ID': sub.get('submission_id', ''),
                    'Employee Name': sub.get('employee_data', {}).get('name', ''),
                    'Employee ID': sub.get('employee_data', {}).get('emp_id', ''),
                    'Department': sub.get('employee_data', {}).get('department', ''),
                    'Net Payable': sub.get('fnf_results', {}).get('net_payable', 0),
                    'Tax Regime': sub.get('deductions_data', {}).get('tax_regime', ''),
                    'Processed By': sub.get('processed_by', ''),
                    'Date': sub.get('timestamp', '')[:10] if sub.get('timestamp') else ''
                })

            return pd.DataFrame(summary_data)

        except Exception as e:
            logger.error(f"Failed to get submissions summary: {str(e)}")
            return pd.DataFrame()

    def export_to_excel(self, submission_id: str = None) -> str:
        """Export FnF data to Excel"""
        try:
            submissions = self._load_submissions()

            if submission_id:
                # Export specific submission
                submission = next((s for s in submissions if s.get('submission_id') == submission_id), None)
                if not submission:
                    return ""

                filename = f"{self.reports_dir}/fnf_report_{submission_id}.xlsx"
                self._create_individual_excel_report(submission, filename)
            else:
                # Export all submissions summary
                filename = f"{self.reports_dir}/fnf_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                self._create_summary_excel_report(submissions, filename)

            return filename

        except Exception as e:
            logger.error(f"Failed to export to Excel: {str(e)}")
            return ""

    def _create_individual_excel_report(self, submission: Dict[str, Any], filename: str):
        """Create individual Excel report"""
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Employee Info
            emp_data = pd.DataFrame([submission.get('employee_data', {})])
            emp_data.to_excel(writer, sheet_name='Employee Info', index=False)

            # FnF Results
            fnf_data = pd.DataFrame([submission.get('fnf_results', {})])
            fnf_data.to_excel(writer, sheet_name='FnF Calculation', index=False)

            # Deductions
            ded_data = pd.DataFrame([submission.get('deductions_data', {})])
            ded_data.to_excel(writer, sheet_name='Deductions', index=False)

    def _create_summary_excel_report(self, submissions: list, filename: str):
        """Create summary Excel report"""
        summary_df = self.get_submissions_summary()

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            summary_df.to_excel(writer, sheet_name='FnF Summary', index=False)

            # Detailed data for each submission
            for i, sub in enumerate(submissions[:10]):  # Limit to 10 for performance
                sheet_name = f"Details_{i+1}"
                detail_data = {
                    'Employee Info': sub.get('employee_data', {}),
                    'FnF Results': sub.get('fnf_results', {}),
                    'Deductions': sub.get('deductions_data', {})
                }

                # Flatten the data
                flat_data = {}
                for category, data in detail_data.items():
                    for key, value in data.items():
                        flat_data[f"{category}_{key}"] = value

                detail_df = pd.DataFrame([flat_data])
                detail_df.to_excel(writer, sheet_name=sheet_name, index=False)

    def export_to_csv(self) -> str:
        """Export summary to CSV"""
        try:
            summary_df = self.get_submissions_summary()
            filename = f"{self.reports_dir}/fnf_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            summary_df.to_csv(filename, index=False)
            return filename
        except Exception as e:
            logger.error(f"Failed to export to CSV: {str(e)}")
            return ""

    def get_submission_by_id(self, submission_id: str) -> Dict[str, Any]:
        """Get specific submission by ID"""
        submissions = self._load_submissions()
        return next((s for s in submissions if s.get('submission_id') == submission_id), {})



# ==================== MAIN APPLICATION FLOW ====================

def employee_information_step():
    """Step 1: Employee Information Input"""
    st.markdown('<div class="step-header">👤 Step 1: Employee Information</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        emp_id = st.text_input("Employee ID*", value="", placeholder="Enter employee ID")
        name = st.text_input("Full Name*", value="", placeholder="Enter full name")
        department = st.selectbox("Department*", 
                                 ["", "IT", "Finance", "HR", "Operations", "Sales", "Marketing", "Other"])
        email = st.text_input("Email Address*", value="", placeholder="employee@company.com")

    with col2:
        date_of_joining = st.date_input("Date of Joining*", value=None)
        last_working_day = st.date_input("Last Working Day*", value=None)

        # Calculate service period
        if date_of_joining and last_working_day:
            service_period = last_working_day - date_of_joining
            years = service_period.days // 365
            months = (service_period.days % 365) // 30
            st.info(f"Service Period: {years} years, {months} months")

            st.session_state.employee_data.update({
                'years_of_service': years,
                'months_of_service': months
            })

    # Save to session state
    employee_data = {
        'emp_id': emp_id,
        'name': name,
        'department': department,
        'email': email,
        'date_of_joining': str(date_of_joining) if date_of_joining else "",
        'last_working_day': str(last_working_day) if last_working_day else ""
    }

    st.session_state.employee_data.update(employee_data)

    # Validation
    required_fields = ['emp_id', 'name', 'department', 'email']
    missing_fields = [field for field in required_fields if not employee_data.get(field)]

    if missing_fields:
        st.error(f"Please fill required fields: {', '.join(missing_fields)}")
        return False

    if not date_of_joining or not last_working_day:
        st.error("Please select both joining and last working day")
        return False

    if last_working_day <= date_of_joining:
        st.error("Last working day should be after joining date")
        return False

    return True

def salary_details_step():
    """Step 2: Salary Details"""
    st.markdown('<div class="step-header">💰 Step 2: Salary Details</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Current Salary Components")
        basic_salary = st.number_input("Basic Salary (Monthly)*", min_value=0.0, value=0.0, step=1000.0)
        hra = st.number_input("HRA", min_value=0.0, value=0.0, step=500.0)
        special_allowance = st.number_input("Special Allowance", min_value=0.0, value=0.0, step=500.0)
        other_allowances = st.number_input("Other Allowances", min_value=0.0, value=0.0, step=200.0)

        gross_salary = basic_salary + hra + special_allowance + other_allowances
        st.metric("Gross Salary (Monthly)", f"₹{gross_salary:,.2f}")

    with col2:
        st.subheader("FnF Components")
        leave_balance = st.number_input("Leave Balance (Days)", min_value=0, max_value=365, value=0)
        notice_period_served = st.number_input("Notice Period Served (Days)", min_value=0, value=0)
        notice_period_required = st.number_input("Required Notice Period (Days)", min_value=0, value=30)

        # Additional recoveries
        other_recoveries = st.number_input("Other Recoveries (₹)", min_value=0.0, value=0.0, step=100.0,
                                         help="Laptop recovery, advance recovery, etc.")

    # Save to session state
    salary_data = {
        'basic_salary': basic_salary,
        'hra': hra,
        'special_allowance': special_allowance,
        'other_allowances': other_allowances,
        'gross_salary': gross_salary,
        'annual_salary': gross_salary * 12,
        'leave_balance': leave_balance,
        'notice_period_served': notice_period_served,
        'notice_period_required': notice_period_required,
        'other_recoveries': other_recoveries
    }

    st.session_state.employee_data.update(salary_data)

    if basic_salary <= 0:
        st.error("Basic salary is required")
        return False

    return True

def additional_fnf_details_step():
    """Step 5: Additional F&F Details (positioned next to Step 1)"""
    st.markdown('<div class="step-header">📋 Step 5: Additional F&F Details</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Additional Components")
        bonus_amount = st.number_input("Bonus/Incentive (₹)", min_value=0.0, value=0.0, step=1000.0)
        medical_reimbursement = st.number_input("Medical Reimbursement (₹)", min_value=0.0, value=0.0, step=500.0)
        travel_reimbursement = st.number_input("Travel Reimbursement (₹)", min_value=0.0, value=0.0, step=500.0)

    with col2:
        st.subheader("Deductions/Recoveries")
        advance_recovery = st.number_input("Advance Recovery (₹)", min_value=0.0, value=0.0, step=1000.0)
        loan_recovery = st.number_input("Loan Recovery (₹)", min_value=0.0, value=0.0, step=1000.0)

        # Final settlement notes
        settlement_notes = st.text_area("Settlement Notes", placeholder="Any additional notes for the settlement")

    # Save additional data
    additional_data = {
        'bonus_amount': bonus_amount,
        'medical_reimbursement': medical_reimbursement,
        'travel_reimbursement': travel_reimbursement,
        'advance_recovery': advance_recovery,
        'loan_recovery': loan_recovery,
        'settlement_notes': settlement_notes
    }

    st.session_state.employee_data.update(additional_data)

    return True

def fnf_calculation_step():
    """Step 3: FnF Calculation"""
    st.markdown('<div class="step-header">🧮 Step 3: F&F Calculation</div>', unsafe_allow_html=True)

    employee_data = st.session_state.employee_data

    if not employee_data.get('basic_salary'):
        st.error("Please complete salary details first")
        return False

    # Get deductions data
    deductions_data = investment_deductions_input()

    # Calculate components
    basic_salary = employee_data.get('basic_salary', 0)
    years_service = employee_data.get('years_of_service', 0)
    months_service = employee_data.get('months_of_service', 0)
    leave_balance = employee_data.get('leave_balance', 0)

    # Gratuity calculation
    gratuity_result = calculate_gratuity(basic_salary, years_service, months_service)

    # Leave encashment
    leave_encashment = calculate_leave_encashment(basic_salary, leave_balance)

    # Notice period recovery/payment
    notice_recovery = 0
    notice_payment = 0
    notice_required = employee_data.get('notice_period_required', 0)
    notice_served = employee_data.get('notice_period_served', 0)

    if notice_served < notice_required:
        notice_recovery = calculate_notice_pay(basic_salary, notice_required - notice_served)
    elif notice_served > notice_required:
        notice_payment = calculate_notice_pay(basic_salary, notice_served - notice_required)

    # Calculate gross settlement
    gross_components = [
        employee_data.get('basic_salary', 0),  # Pro-rata basic
        employee_data.get('hra', 0),           # Pro-rata HRA
        employee_data.get('special_allowance', 0),  # Pro-rata allowances
        employee_data.get('other_allowances', 0),
        gratuity_result.get('total_gratuity', 0),
        leave_encashment,
        notice_payment,
        employee_data.get('bonus_amount', 0),
        employee_data.get('medical_reimbursement', 0),
        employee_data.get('travel_reimbursement', 0)
    ]

    gross_settlement = sum(gross_components)

    # Calculate recoveries
    total_recoveries = (
        notice_recovery +
        employee_data.get('other_recoveries', 0) +
        employee_data.get('advance_recovery', 0) +
        employee_data.get('loan_recovery', 0)
    )

    # Net settlement before tax
    net_before_tax = gross_settlement - total_recoveries

    # Tax calculation
    annual_settlement_income = net_before_tax

    if deductions_data.get('tax_regime') == 'old':
        tax_result = calculate_tds_old_regime(annual_settlement_income, deductions_data)
    else:
        tax_result = calculate_tds_new_regime(annual_settlement_income, deductions_data)

    tds_amount = tax_result.get('total_tax', 0)

    # Final net payable
    net_payable = net_before_tax - tds_amount

    # Prepare results
    fnf_results = {
        'basic_salary_prorata': employee_data.get('basic_salary', 0),
        'allowances': (employee_data.get('hra', 0) + employee_data.get('special_allowance', 0) + 
                      employee_data.get('other_allowances', 0)),
        'gratuity': gratuity_result.get('total_gratuity', 0),
        'leave_encashment': leave_encashment,
        'bonus_incentive': employee_data.get('bonus_amount', 0),
        'other_earnings': (employee_data.get('medical_reimbursement', 0) + 
                          employee_data.get('travel_reimbursement', 0)),
        'notice_payment': notice_payment,
        'gross_settlement': gross_settlement,
        'notice_recovery': notice_recovery,
        'other_recoveries': total_recoveries - notice_recovery,
        'net_before_tax': net_before_tax,
        'taxable_income': tax_result.get('taxable_income', 0),
        'tds_amount': tds_amount,
        'net_payable': net_payable,
        'tax_regime': deductions_data.get('tax_regime', 'old'),
        'effective_rate': tax_result.get('effective_rate', 0),
        'total_tax': tds_amount,
        'gratuity_details': gratuity_result
    }

    # Display results
    st.subheader("📊 F&F Settlement Summary")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Gross Settlement", f"₹{gross_settlement:,.2f}")
        st.metric("Gratuity", f"₹{gratuity_result.get('total_gratuity', 0):,.2f}")
        st.metric("Leave Encashment", f"₹{leave_encashment:,.2f}")

    with col2:
        st.metric("Total Recoveries", f"₹{total_recoveries:,.2f}")
        st.metric("TDS Amount", f"₹{tds_amount:,.2f}")
        st.metric("Tax Rate", f"{tax_result.get('effective_rate', 0):.2f}%")

    with col3:
        st.metric("Net Payable", f"₹{net_payable:,.2f}", delta=f"After {deductions_data.get('tax_regime', 'old')} regime")

    # Detailed breakdown
    with st.expander("📋 Detailed Breakdown"):
        breakdown_df = pd.DataFrame([
            ["Basic Salary (Pro-rata)", f"₹{employee_data.get('basic_salary', 0):,.2f}"],
            ["HRA", f"₹{employee_data.get('hra', 0):,.2f}"],
            ["Special Allowance", f"₹{employee_data.get('special_allowance', 0):,.2f}"],
            ["Other Allowances", f"₹{employee_data.get('other_allowances', 0):,.2f}"],
            ["Gratuity", f"₹{gratuity_result.get('total_gratuity', 0):,.2f}"],
            ["Leave Encashment", f"₹{leave_encashment:,.2f}"],
            ["Bonus/Incentive", f"₹{employee_data.get('bonus_amount', 0):,.2f}"],
            ["Medical Reimbursement", f"₹{employee_data.get('medical_reimbursement', 0):,.2f}"],
            ["Travel Reimbursement", f"₹{employee_data.get('travel_reimbursement', 0):,.2f}"],
            ["Notice Payment", f"₹{notice_payment:,.2f}"],
            ["", ""],
            ["Gross Settlement", f"₹{gross_settlement:,.2f}"],
            ["", ""],
            ["Notice Recovery", f"-₹{notice_recovery:,.2f}"],
            ["Advance Recovery", f"-₹{employee_data.get('advance_recovery', 0):,.2f}"],
            ["Loan Recovery", f"-₹{employee_data.get('loan_recovery', 0):,.2f}"],
            ["Other Recoveries", f"-₹{employee_data.get('other_recoveries', 0):,.2f}"],
            ["", ""],
            ["Net Before Tax", f"₹{net_before_tax:,.2f}"],
            ["TDS Deducted", f"-₹{tds_amount:,.2f}"],
            ["", ""],
            ["NET PAYABLE", f"₹{net_payable:,.2f}"]
        ], columns=["Component", "Amount"])

        st.dataframe(breakdown_df, use_container_width=True, hide_index=True)

    # Save results
    st.session_state.fnf_results = fnf_results

    return True

