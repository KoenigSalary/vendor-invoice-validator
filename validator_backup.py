import pandas as pd
import os
from datetime import datetime, timedelta
import re
import logging
from typing import Dict, List, Tuple, Any, Optional
from validator_utils import *
from email_notifier import EmailNotifier

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ========== ENHANCED CONFIGURATION ==========

# Koenig Solutions Global Subsidiaries Configuration
KOENIG_SUBSIDIARIES = {
    'India': {
        'currency': 'INR',
        'tax_type': 'GST',
        'tax_rates': [0, 5, 12, 18, 28],
        'has_states': True
    },
    'Canada': {
        'currency': 'CAD',
        'tax_type': 'HST/GST',
        'tax_rates': [5, 13, 15],
        'has_states': False
    },
    'USA': {
        'currency': 'USD',
        'tax_type': 'Sales Tax',
        'tax_rates': [0, 6, 8.25, 10.25],
        'has_states': False
    },
    'Australia': {
        'currency': 'AUD',
        'tax_type': 'GST',
        'tax_rates': [0, 10],
        'has_states': False
    },
    'South Africa': {
        'currency': 'ZAR',
        'tax_type': 'VAT',
        'tax_rates': [0, 15],
        'has_states': False
    },
    'New Zealand': {
        'currency': 'NZD',
        'tax_type': 'GST',
        'tax_rates': [0, 15],
        'has_states': False
    },
    'Netherlands': {
        'currency': 'EUR',
        'tax_type': 'VAT',
        'tax_rates': [0, 9, 21],
        'has_states': False
    },
    'Singapore': {
        'currency': 'SGD',
        'tax_type': 'GST',
        'tax_rates': [0, 8],
        'has_states': False
    },
    'Dubai': {
        'currency': 'AED',
        'tax_type': 'VAT',
        'tax_rates': [0, 5],
        'has_states': False,
        'entities': ['Koenig Solutions FZLLC', 'Koenig Solutions DMCC']
    },
    'Malaysia': {
        'currency': 'MYR',
        'tax_type': 'SST',
        'tax_rates': [0, 6, 10],
        'has_states': False
    },
    'Saudi Arabia': {
        'currency': 'SAR',
        'tax_type': 'VAT',
        'tax_rates': [0, 15],
        'has_states': False
    },
    'Germany': {
        'currency': 'EUR',
        'tax_type': 'VAT',
        'tax_rates': [0, 7, 19],
        'has_states': False
    },
    'UK': {
        'currency': 'GBP',
        'tax_type': 'VAT',
        'tax_rates': [0, 5, 20],
        'has_states': False
    },
    'Japan': {
        'currency': 'JPY',
        'tax_type': 'Consumption Tax',
        'tax_rates': [0, 10],
        'has_states': False
    }
}

# Indian GST State Codes (as provided by user)
INDIAN_GST_STATES = {
    1: 'JAMMU AND KASHMIR',
    2: 'HIMACHAL PRADESH',
    3: 'PUNJAB',
    4: 'CHANDIGARH',
    5: 'UTTARAKHAND',
    6: 'HARYANA',
    7: 'DELHI',
    8: 'RAJASTHAN',
    9: 'UTTAR PRADESH',
    10: 'BIHAR',
    11: 'SIKKIM',
    12: 'ARUNACHAL PRADESH',
    13: 'NAGALAND',
    14: 'MANIPUR',
    15: 'MIZORAM',
    16: 'TRIPURA',
    17: 'MEGHALAYA',
    18: 'ASSAM',
    19: 'WEST BENGAL',
    20: 'JHARKHAND',
    21: 'ODISHA',
    22: 'CHATTISGARH',
    23: 'MADHYA PRADESH',
    24: 'GUJARAT',
    26: 'DADRA AND NAGAR HAVELI AND DAMAN AND DIU',
    27: 'MAHARASHTRA',
    28: 'ANDHRA PRADESH(BEFORE DIVISION)',
    29: 'KARNATAKA',
    30: 'GOA',
    31: 'LAKSHADWEEP',
    32: 'KERALA',
    33: 'TAMIL NADU',
    34: 'PUDUCHERRY',
    35: 'ANDAMAN AND NICOBAR ISLANDS',
    36: 'TELANGANA',
    37: 'ANDHRA PRADESH',
    38: 'LADAKH'
}

# Mode of Payment Options
VALID_PAYMENT_METHODS = [
    'Bank Transfer', 'Wire Transfer', 'Credit Card', 
    'Debit Card', 'Check', 'Cash', 
    'Online Payment', 'Digital Wallet'
]

# ========== ENHANCED VALIDATION FUNCTIONS ==========

class EnhancedInvoiceValidator:
    """Enhanced Invoice Validator with Multi-Currency & Location Support"""
    
    def __init__(self):
        self.email_notifier = EmailNotifier() if 'EmailNotifier' in globals() else None
        self.validation_results = []
        
    def validate_currency_location(self, currency: str, location: str) -> Dict[str, Any]:
        """Validate currency matches location"""
        errors = []
        warnings = []
        
        if location not in KOENIG_SUBSIDIARIES:
            errors.append(f"Unknown location: {location}")
            return {'valid': False, 'errors': errors, 'warnings': warnings}
        
        expected_currency = KOENIG_SUBSIDIARIES[location]['currency']
        if currency != expected_currency:
            errors.append(f"Currency mismatch: Expected {expected_currency} for {location}, got {currency}")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'expected_currency': expected_currency,
            'tax_type': KOENIG_SUBSIDIARIES[location]['tax_type']
        }
    
    def validate_gst_logic(self, gst_number: str, client_state: str, location: str) -> Dict[str, Any]:
        """Enhanced GST validation with state-wise logic"""
        errors = []
        warnings = []
        gst_info = {}
        
        if location != 'India':
            return {'valid': True, 'errors': [], 'warnings': [], 'info': 'GST not applicable for non-Indian invoices'}
        
        if not gst_number or pd.isna(gst_number):
            errors.append("GST number is required for Indian invoices")
            return {'valid': False, 'errors': errors, 'warnings': warnings}
        
        # Validate GST format (15 characters, alphanumeric)
        if not re.match(r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}[Z]{1}[0-9A-Z]{1}$', gst_number):
            errors.append(f"Invalid GST format: {gst_number}")
            return {'valid': False, 'errors': errors, 'warnings': warnings}
        
        # Extract state code from GST
        gst_state_code = int(gst_number[:2])
        if gst_state_code not in INDIAN_GST_STATES:
            errors.append(f"Invalid GST state code: {gst_state_code}")
        else:
            gst_state_name = INDIAN_GST_STATES[gst_state_code]
            gst_info['gst_state'] = gst_state_name
            gst_info['gst_state_code'] = gst_state_code
            
            # Determine tax type based on state matching
            if client_state and not pd.isna(client_state):
                client_state_upper = client_state.upper()
                gst_state_upper = gst_state_name.upper()
                
                if client_state_upper == gst_state_upper or client_state_upper in gst_state_upper:
                    # Intrastate transaction
                    gst_info['tax_structure'] = 'SGST + CGST'
                    gst_info['transaction_type'] = 'Intrastate'
                else:
                    # Interstate transaction
                    gst_info['tax_structure'] = 'IGST'
                    gst_info['transaction_type'] = 'Interstate'
            else:
                warnings.append("Client state not provided, cannot determine SGST/CGST vs IGST")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'gst_info': gst_info
        }
    
    def validate_due_date(self, due_date: Any) -> Dict[str, Any]:
        """Validate due date and flag issues"""
        errors = []
        warnings = []
        due_date_info = {}
        
        if not due_date or pd.isna(due_date):
            warnings.append("Due date is missing - flagged for follow-up")
            due_date_info['status'] = 'MISSING'
            return {'valid': True, 'errors': errors, 'warnings': warnings, 'info': due_date_info}
        
        try:
            if isinstance(due_date, str):
                due_date = pd.to_datetime(due_date)
            
            today = datetime.now()
            
            if due_date < today:
                errors.append(f"Invoice is overdue by {(today - due_date).days} days")
                due_date_info['status'] = 'OVERDUE'
                due_date_info['overdue_days'] = (today - due_date).days
            elif due_date > today + timedelta(days=90):
                warnings.append("Due date is more than 90 days in future")
                due_date_info['status'] = 'FAR_FUTURE'
            else:
                due_date_info['status'] = 'VALID'
                due_date_info['days_until_due'] = (due_date - today).days
            
        except Exception as e:
            errors.append(f"Invalid due date format: {due_date}")
            due_date_info['status'] = 'INVALID_FORMAT'
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'info': due_date_info
        }
    
    def validate_enhanced_invoice(self, row: pd.Series) -> Dict[str, Any]:
        """Enhanced validation for a single invoice with new fields"""
        validation_result = {
            'invoice_id': row.get('InvID', row.get('Invoice_ID', 'Unknown')),
            'status': 'PASSED',
            'errors': [],
            'warnings': [],
            'enhancements': {},
            'legacy_validation': {}
        }
        
        # 1. Enhanced Currency & Location Validation
        currency = row.get('Currency', row.get('InvoiceCurrency'))
        location = row.get('Location', row.get('Country', 'India'))  # Default to India
        
        if currency and location:
            currency_validation = self.validate_currency_location(currency, location)
            if not currency_validation['valid']:
                validation_result['errors'].extend(currency_validation['errors'])
            validation_result['enhancements']['currency_location'] = currency_validation
        
        # 2. Enhanced GST Validation (preserving existing logic)
        gst_number = row.get('GSTNO', row.get('GST_Number'))
        client_state = row.get('ClientState', row.get('Client_State'))
        
        gst_validation = self.validate_gst_logic(gst_number, client_state, location)
        if not gst_validation['valid']:
            validation_result['errors'].extend(gst_validation['errors'])
        validation_result['warnings'].extend(gst_validation['warnings'])
        validation_result['enhancements']['gst'] = gst_validation
        
        # 3. Due Date Validation
        due_date = row.get('DueDate', row.get('Due_Date'))
        due_date_validation = self.validate_due_date(due_date)
        if not due_date_validation['valid']:
            validation_result['errors'].extend(due_date_validation['errors'])
        validation_result['warnings'].extend(due_date_validation['warnings'])
        validation_result['enhancements']['due_date'] = due_date_validation
        
        # 4. Invoice ID Validation
        invoice_id = validation_result['invoice_id']
        if not invoice_id or invoice_id in ['Unknown', ''] or pd.isna(invoice_id):
            validation_result['errors'].append("Invoice ID is missing or invalid")
        
        # 5. Mode of Payment Validation
        mop = row.get('ModeOfPayment', row.get('Mode_of_Payment'))
        if mop and mop not in VALID_PAYMENT_METHODS:
            validation_result['warnings'].append(f"Unusual payment method: {mop}")
        
        # 6. Total Value Validation
        total_value = row.get('Total', row.get('TotalValue', row.get('Total_Invoice_Value')))
        if total_value is not None and not pd.isna(total_value):
            try:
                total_float = float(total_value)
                if total_float <= 0:
                    validation_result['errors'].append(f"Invalid total value: {total_value}")
                elif total_float > 10000000:  # 10 million threshold
                    validation_result['warnings'].append(f"High value invoice: {total_value:,.2f}")
            except (ValueError, TypeError):
                validation_result['errors'].append(f"Invalid total value format: {total_value}")
        else:
            validation_result['errors'].append("Total value is missing")
        
        # 7. Legacy validation compatibility
        legacy_issues = []
        if not gst_number or pd.isna(gst_number):
            legacy_issues.append("Missing GSTNO (legacy check)")
        if not total_value or pd.isna(total_value):
            legacy_issues.append("Missing Total (legacy check)")
        
        validation_result['legacy_validation'] = legacy_issues
        
        # Determine final status
        if validation_result['errors']:
            validation_result['status'] = 'FAILED'
        elif validation_result['warnings']:
            validation_result['status'] = 'WARNING'
        
        return validation_result

# ========== ENHANCED MAIN FUNCTIONS (Preserving Existing Logic) ==========

def validate_invoices(data_path: str = None) -> pd.DataFrame:
    """Enhanced validation function preserving existing logic"""
    
    # Initialize enhanced validator
    enhanced_validator = EnhancedInvoiceValidator()
    
    # Load data (preserve existing logic)
    if data_path:
        if data_path.endswith('.xlsx'):
            df = pd.read_excel(data_path)
        elif data_path.endswith('.xls'):
            # Try as TSV first (preserving existing logic)
            try:
                df = pd.read_csv(data_path, sep='\t', encoding='utf-8')
                print("✅ Loaded as TSV file")
            except:
                df = pd.read_excel(data_path)
                print("✅ Loaded as Excel file")
        else:
            df = pd.read_csv(data_path)
    else:
        # Use existing data loading logic
        df = load_actual_invoice_data()
    
    # Enhanced validation with backward compatibility
    validation_results = []
    enhanced_results = []
    
    for idx, row in df.iterrows():
        # Enhanced validation
        enhanced_result = enhanced_validator.validate_enhanced_invoice(row)
        enhanced_results.append(enhanced_result)
        
        # Legacy validation result format
        legacy_result = {
            'index': idx,
            'invoice_id': enhanced_result['invoice_id'],
            'status': enhanced_result['status'],
            'issues': enhanced_result['errors'] + enhanced_result['warnings'],
            'error_count': len(enhanced_result['errors']),
            'warning_count': len(enhanced_result['warnings'])
        }
        validation_results.append(legacy_result)
    
    # Create enhanced results DataFrame
    results_df = pd.DataFrame(validation_results)
    
    # Add enhanced information to original DataFrame
    df['validation_status'] = [r['status'] for r in enhanced_results]
    df['error_count'] = [len(r['errors']) for r in enhanced_results]
    df['warning_count'] = [len(r['warnings']) for r in enhanced_results]
    df['validation_details'] = enhanced_results
    
    # Store enhanced results for email workflow
    enhanced_validator.validation_results = enhanced_results
    
    # Preserve existing validation logic
    print(f"📋 Loaded {len(df)} invoices with {len(df.columns)} columns")
    print(f"✅ Total invoices to validate: {len(df)}")
    
    # Legacy compatibility checks
    missing_gst = df['GSTNO'].isna().sum() if 'GSTNO' in df.columns else 0
    missing_total = df['Total'].isna().sum() if 'Total' in df.columns else 0
    
    if missing_gst > 0:
        print(f"⚠️ Found {missing_gst} rows with missing GSTNO")
    if missing_total > 0:
        print(f"⚠️ Found {missing_total} rows with missing Total")
    
    # Enhanced summary
    failed_count = len([r for r in enhanced_results if r['status'] == 'FAILED'])
    warning_count = len([r for r in enhanced_results if r['status'] == 'WARNING'])
    passed_count = len([r for r in enhanced_results if r['status'] == 'PASSED'])
    
    print(f"🎯 Enhanced Validation Summary:")
    print(f"  ✅ Passed: {passed_count}")
    print(f"  ⚠️ Warnings: {warning_count}")
    print(f"  ❌ Failed: {failed_count}")
    
    # Trigger enhanced email workflow for failed validations
    if enhanced_validator.email_notifier and failed_count > 0:
        failed_invoices = [r for r in enhanced_results if r['status'] == 'FAILED']
        print(f"📧 Scheduling enhanced correction workflow for {len(failed_invoices)} failed invoices")
        
        # Schedule 5-day correction workflow
        for failed_invoice in failed_invoices:
            try:
                enhanced_validator.email_notifier.schedule_correction_workflow(
                    invoice_id=failed_invoice['invoice_id'],
                    validation_errors=failed_invoice['errors'],
                    vendor_email="vendor@example.com"  # Get from invoice data
                )
            except Exception as e:
                logger.warning(f"Failed to schedule workflow for {failed_invoice['invoice_id']}: {e}")
    
    return df

# ========== PRESERVE EXISTING FUNCTIONS ==========

# Keep all existing functions for backward compatibility
def get_invoice_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """Enhanced invoice summary with multi-currency support"""
    if df.empty:
        return {'error': 'No data available'}
    
    summary = {
        'total_invoices': len(df),
        'validation_summary': {},
        'currency_breakdown': {},
        'location_breakdown': {},
        'legacy_stats': {}
    }
    
    # Enhanced validation summary
    if 'validation_status' in df.columns:
        summary['validation_summary'] = df['validation_status'].value_counts().to_dict()
    
    # Currency breakdown
    if 'Currency' in df.columns:
        summary['currency_breakdown'] = df['Currency'].value_counts().to_dict()
    
    # Location breakdown
    if 'Location' in df.columns:
        summary['location_breakdown'] = df['Location'].value_counts().to_dict()
    
    # Legacy statistics (preserve existing logic)
    total_col = 'Total' if 'Total' in df.columns else None
    if total_col:
        total_values = pd.to_numeric(df[total_col], errors='coerce').dropna()
        if not total_values.empty:
            summary['legacy_stats'] = {
                'total_amount': total_values.sum(),
                'average_amount': total_values.mean(),
                'currency_symbol': '₹'  # Default to INR
            }
    
    return summary

# Keep existing functions with enhanced capabilities
def run_validation_workflow() -> bool:
    """Enhanced validation workflow preserving existing logic"""
    try:
        # Use existing workflow logic
        today = datetime.now().strftime('%Y-%m-%d')
        data_folder = f"data/{today}"
        
        print(f"🔍 Looking for data in: {data_folder}")
        
        if not os.path.exists(data_folder):
            print(f"❌ Folder not found for today ({today}), trying fallback.")
            latest_folder = get_latest_data_folder()
            if latest_folder:
                data_folder = latest_folder
                print(f"🔁 Using fallback folder: {data_folder}")
            else:
                print("❌ No data folders found")
                return False
        
        print(f"📁 Working directory: {data_folder}")
        
        result_path = os.path.join(data_folder, 'validation_result.xlsx')
        zip_path = os.path.join(data_folder, 'invoices.zip')
        invoice_file = os.path.join(data_folder, 'invoice_download.xls')
        
        print(f"📄 Result path: {result_path}")
        print(f"📦 ZIP path: {zip_path}")
        print(f"📊 Loading invoice data from: {invoice_file}")
        
        # Enhanced validation
        df = validate_invoices(invoice_file)
        
        # Save enhanced results
        df.to_excel(result_path, index=False)
        print(f"✅ Enhanced validation results saved: {result_path}")
        
        # Continue with existing workflow (snapshot, email, etc.)
        print("📸 Comparing with previous snapshot...")
        snapshot_result = compare_with_snapshot(df, data_folder)
        
        print("💾 Saving current snapshot...")
        save_snapshot(df)
        
        print("📧 Sending email report...")
        email_result = send_email_report(result_path, zip_path)
        
        # Enhanced summary
        summary = get_invoice_summary(df)
        print(f"📋 Enhanced Validation Summary:")
        print(f"  - Total invoices processed: {summary.get('total_invoices', 0)}")
        
        if 'validation_summary' in summary:
            for status, count in summary['validation_summary'].items():
                print(f"  - {status}: {count}")
        
        if 'legacy_stats' in summary and summary['legacy_stats']:
            stats = summary['legacy_stats']
            print(f"  - Total amount: {stats.get('currency_symbol', '₹')}{stats.get('total_amount', 0):,.2f}")
            print(f"  - Average amount: {stats.get('currency_symbol', '₹')}{stats.get('average_amount', 0):,.2f}")
        
        print("✅ Enhanced validation workflow completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Enhanced workflow failed: {e}")
        logger.error(f"Enhanced workflow error: {e}")
        return False

# ========== FUTURE ENHANCEMENTS PLACEHOLDERS ==========

def validate_tds(invoice_data: Dict[str, Any]) -> Dict[str, Any]:
    """TDS Validation - Coming Soon"""
    return {
        'status': 'COMING_SOON',
        'message': 'TDS validation will be available in the next release'
    }

def validate_account_heads(account_head: str) -> Dict[str, Any]:
    """Account Heads Validation - Coming Soon"""
    return {
        'status': 'COMING_SOON',
        'message': 'Account Heads validation will be available in the next release'
    }

# ========== MAIN EXECUTION ==========

if __name__ == "__main__":
    print("🏢 Koenig Solutions Enhanced Invoice Validator")
    print("🌍 Multi-Currency & Multi-Location Support")
    print("=" * 60)
    
    # Show enhanced capabilities
    print("✅ Enhanced Features:")
    print(f"   - {len(KOENIG_SUBSIDIARIES)} international subsidiaries")
    print(f"   - {len(INDIAN_GST_STATES)} Indian GST states")
    print(f"   - {len(VALID_PAYMENT_METHODS)} payment methods")
    print("   - 5-day correction workflow")
    print("   - Multi-currency validation")
    print("   - Location-based tax logic")
    print()
    
    # Run enhanced workflow
    success = run_validation_workflow()
    if success:
        print("🎉 Enhanced validation completed successfully!")
    else:
        print("⚠️ Enhanced validation completed with issues")
            