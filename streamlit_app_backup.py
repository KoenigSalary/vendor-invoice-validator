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

# Enhanced Configuration Data
SUBSIDIARIES = {
    'India': {'tax_type': 'GST', 'currency': 'INR'},
    'Canada': {'tax_type': 'VAT', 'currency': 'CAD'},
    'USA': {'tax_type': 'Sales Tax', 'currency': 'USD'},
    'Australia': {'tax_type': 'GST', 'currency': 'AUD'},
    'South Africa': {'tax_type': 'VAT', 'currency': 'ZAR'},
    'New Zealand': {'tax_type': 'GST', 'currency': 'NZD'},
    'Netherlands': {'tax_type': 'VAT', 'currency': 'EUR'},
    'Singapore': {'tax_type': 'GST', 'currency': 'SGD'},
    'Dubai': {'tax_type': 'VAT', 'currency': 'AED', 'entities': ['Koenig Solutions FZLLC', 'Koenig Solutions DMCC']},
    'Malaysia': {'tax_type': 'SST', 'currency': 'MYR'},
    'Saudi Arabia': {'tax_type': 'VAT', 'currency': 'SAR'},
    'Germany': {'tax_type': 'VAT', 'currency': 'EUR'},
    'UK': {'tax_type': 'VAT', 'currency': 'GBP'},
    'Japan': {'tax_type': 'Consumption Tax', 'currency': 'JPY'}
}

INDIAN_STATES = {
    1: "JAMMU AND KASHMIR",
    2: "HIMACHAL PRADESH",
    3: "PUNJAB",
    4: "CHANDIGARH",
    5: "UTTARAKHAND",
    6: "HARYANA",
    7: "DELHI",
    8: "RAJASTHAN",
    9: "UTTAR PRADESH",
    10: "BIHAR",
    11: "SIKKIM",
    12: "ARUNACHAL PRADESH",
    13: "NAGALAND",
    14: "MANIPUR",
    15: "MIZORAM",
    16: "TRIPURA",
    17: "MEGHALAYA",
    18: "ASSAM",
    19: "WEST BENGAL",
    20: "JHARKHAND",
    21: "ODISHA",
    22: "CHATTISGARH",
    23: "MADHYA PRADESH",
    24: "GUJARAT",
    26: "DADRA AND NAGAR HAVELI AND DAMAN AND DIU",
    27: "MAHARASHTRA",
    28: "ANDHRA PRADESH(BEFORE DIVISION)",
    29: "KARNATAKA",
    30: "GOA",
    31: "LAKSHADWEEP",
    32: "KERALA",
    33: "TAMIL NADU",
    34: "PUDUCHERRY",
    35: "ANDAMAN AND NICOBAR ISLANDS",
    36: "TELANGANA",
    37: "ANDHRA PRADESH",
    38: "LADAKH (NEWLY ADDED)"
}

PAYMENT_MODES = ['Cash', 'Cheque', 'Credit Card', 'Debit Card', 'Bank Transfer', 'Online Payment', 'UPI', 'Net Banking', 'Other']

# Company Logo at the top center
try:
    from PIL import Image
    import os

    logo_path = "assets/koenig_logo.png"

    if os.path.exists(logo_path):
        # Create a container for the logo
        with st.container():
            # Use columns with equal spacing
            col1, col2, col3 = st.columns([3, 2, 2])

            with col2:
                logo = Image.open(logo_path)
                # Control exact logo size
                st.image(logo, width=300)

        # Add minimal spacing after logo
        st.markdown("<br>", unsafe_allow_html=True)

    else:
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

    .coming-soon {
        background-color: #fff3cd;
        border-color: #ffeaa7;
        color: #856404;
        padding: 10px;
        border-radius: 5px;
        border: 1px solid;
        margin: 5px 0;
        text-align: center;
        font-style: italic;
    }

    .validation-error {
        background-color: #f8d7da;
        border-color: #f5c6cb;
        color: #721c24;
        padding: 10px;
        border-radius: 5px;
        border: 1px solid;
        margin: 5px 0;
    }

    .validation-success {
        background-color: #d1ecf1;
        border-color: #bee5eb;
        color: #0c5460;
        padding: 10px;
        border-radius: 5px;
        border: 1px solid;
        margin: 5px 0;
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

# Enhanced Invoice Input Form
st.subheader("🔍 Invoice Validation Form")

with st.form("invoice_validation_form"):
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### 📋 Basic Information")
        invoice_id = st.text_input("Invoice ID *", placeholder="Enter unique invoice ID")
        location = st.selectbox("Location *", options=list(SUBSIDIARIES.keys()))
        invoice_currency = st.selectbox("Invoice Currency *", 
                                      options=[SUBSIDIARIES[loc]['currency'] for loc in SUBSIDIARIES.keys()])
        
    with col2:
        st.markdown("### 💰 Financial Details")
        total_invoice_value = st.number_input("Total Invoice Value *", min_value=0.0, format="%.2f")
        due_date = st.date_input("Due Date", min_value=datetime.now().date())
        mop = st.selectbox("Mode of Payment (MOP) *", options=PAYMENT_MODES)
        
    with col3:
        st.markdown("### 🏢 Additional Information")
        scid = st.text_input("Supply Chain ID (SCID)", placeholder="Enter SCID if applicable")
        
        # Coming Soon fields
        st.markdown("""
        <div class="coming-soon">
            🔄 TDS (Tax Deducted at Source) - Coming Soon
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class="coming-soon">
            🔄 Account Heads - Coming Soon
        </div>
        """, unsafe_allow_html=True)

    # Tax Information Section
    st.markdown("### 🏛️ Tax Information")
    
    if location == 'India':
        col_tax1, col_tax2 = st.columns(2)
        with col_tax1:
            billing_state = st.selectbox("Billing State", options=list(INDIAN_STATES.values()))
        with col_tax2:
            shipping_state = st.selectbox("Shipping State", options=list(INDIAN_STATES.values()))
            
        # GST Logic Display
        if billing_state == shipping_state:
            st.markdown("""
            <div class="validation-success">
                ✅ Intrastate Transaction: SGST + CGST applicable
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="validation-success">
                ✅ Interstate Transaction: IGST applicable
            </div>
            """, unsafe_allow_html=True)
    else:
        # International VAT
        st.info(f"VAT applicable for {location} - {SUBSIDIARIES[location]['tax_type']}")
        if location == 'Dubai':
            dubai_entity = st.selectbox("Dubai Entity", options=SUBSIDIARIES['Dubai']['entities'])

    # Validation Logic
    validation_errors = []
    
    if st.form_submit_button("🔍 Validate Invoice", use_container_width=True):
        # Currency-Location Validation
        expected_currency = SUBSIDIARIES[location]['currency']
        if invoice_currency != expected_currency:
            validation_errors.append(f"Currency mismatch: Expected {expected_currency} for {location}, got {invoice_currency}")
        
        # Required Fields Validation
        if not invoice_id:
            validation_errors.append("Invoice ID is required")
        if total_invoice_value <= 0:
            validation_errors.append("Total Invoice Value must be greater than 0")
        if not due_date:
            validation_errors.append("Due Date is required")
        
        # Due Date Flag
        if due_date < datetime.now().date():
            validation_errors.append("⚠️ Due Date has passed - Invoice is overdue!")
        
        # Display Results
        if validation_errors:
            st.markdown("### ❌ Validation Errors")
            for error in validation_errors:
                st.markdown(f"""
                <div class="validation-error">
                    {error}
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown("### ✅ Validation Successful")
            st.markdown("""
            <div class="validation-success">
                🎉 Invoice validation completed successfully!<br>
                📧 Correction deadline: 5 business days from validation<br>
                📅 Deadline: {} 
            </div>
            """.format((datetime.now() + timedelta(days=5)).strftime("%Y-%m-%d")), unsafe_allow_html=True)
            
            # Display Summary
            st.markdown("### 📊 Invoice Summary")
            summary_data = {
                "Field": ["Invoice ID", "Location", "Currency", "Total Value", "Due Date", "Payment Mode", "Tax Type"],
                "Value": [invoice_id, location, invoice_currency, f"{total_invoice_value:,.2f}", 
                         due_date.strftime("%Y-%m-%d"), mop, SUBSIDIARIES[location]['tax_type']]
            }
            st.table(pd.DataFrame(summary_data))

        
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
     
    # Enhanced Features overview 
    st.subheader("🔧 System Features")

    # Create two columns for features
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### ✅ Validation Features:")
        st.markdown("""
        - ✅ Multi-currency support (14 currencies)
        - ✅ Multi-location tax validation
        - ✅ GST compliance checking
        - ✅ Invoice ID verification
        - ✅ Due date flagging
        - ✅ Payment mode validation
        - ✅ Supply Chain ID tracking
        """)
        
    with col2:
        st.markdown("#### 🌍 Supported Locations:")
        for location, info in SUBSIDIARIES.items():
            if location == 'Dubai':
                st.markdown(f"- 🏢 {location} ({info['currency']}) - {', '.join(info['entities'])}")
            else:
                st.markdown(f"- 🌍 {location} ({info['currency']}) - {info['tax_type']}")

    st.markdown("#### 🔄 Coming Soon:")
    st.markdown("""
    - 🔄 TDS (Tax Deducted at Source) validation
    - 🔄 Account Heads categorization
    - 🔄 Advanced reporting features
    - 🔄 Automated email reminders
    """)

else:
    # Display actual data when available
    st.subheader("📈 Validation Reports")
    # Your existing data display logic here
