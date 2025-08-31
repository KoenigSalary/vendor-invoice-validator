from rms_scraper import rms_download
from validator_utils import validate_invoices
from updater import update_invoice_status
from reporter import save_snapshot_report
from dotenv import load_dotenv
from datetime import datetime, timedelta
from invoice_tracker import (
    create_tables,
    save_invoice_snapshot,
    record_run_window,
    get_all_run_windows,
    get_last_run_date,
    get_first_validation_date,
    get_validation_date_ranges
)
import pandas as pd
import os
import shutil
import re
from pathlib import Path

# Conditional import for enhanced processor
try:
    from enhanced_processor import enhance_validation_results
    ENHANCED_PROCESSOR_AVAILABLE = True
except ImportError:
    print("⚠️ Enhanced processor not available, using standard validation")
    ENHANCED_PROCESSOR_AVAILABLE = False
    def enhance_validation_results(df, email_summary):
        return {'success': False, 'error': 'Enhanced processor not available'}

# Load environment variables
load_dotenv()

# === Initialize DB tables if not exists ===
create_tables()

# === Configuration ===
VALIDATION_INTERVAL_DAYS = 4  # Run validation every 4 days
VALIDATION_BATCH_DAYS = 4     # Each batch covers 4 days
ACTIVE_VALIDATION_MONTHS = 3  # Keep 3 months of active validation data
ARCHIVE_FOLDER = "archived_data"  # Folder for data older than 3 months

# === ENHANCED RMS FIELD MAPPING SYSTEM ===
def enhance_rms_field_mapping(df):
    """Enhanced RMS field mapping with comprehensive field detection"""
    print("🔍 Enhanced RMS field mapping - detecting all available fields...")
    
    # Comprehensive field mapping patterns
    field_patterns = {
        'invoice_created_by': [
            'CreatedBy', 'Created_By', 'InvoiceCreatedBy', 'Invoice_Created_By',
            'UserName', 'User_Name', 'CreatorName', 'Creator_Name',
            'EntryBy', 'Entry_By', 'InputBy', 'Input_By',
            'PreparedBy', 'Prepared_By', 'MadeBy', 'Made_By',
            'ProcessedBy', 'Processed_By', 'AddedBy', 'Added_By'
        ],
        'method_of_payment': [
            'MOP', 'Method_of_Payment', 'MethodOfPayment', 'Payment_Method',
            'PaymentMethod', 'PaymentMode', 'Payment_Mode', 'PayMode',
            'TransactionMethod', 'Transaction_Method', 'PayType', 'Pay_Type'
        ],
        'account_head': [
            'Account_Head', 'AccountHead', 'Account_Code', 'AccountCode',
            'GL_Account', 'GLAccount', 'Account_Name', 'AccountName',
            'Ledger_Code', 'LedgerCode', 'Cost_Center', 'CostCenter'
        ],
        'invoice_entry_date': [
            'EntryDate', 'Entry_Date', 'Upload_Date', 'UploadDate',
            'Creation_Date', 'CreationDate', 'Input_Date', 'InputDate',
            'Process_Date', 'ProcessDate', 'Added_Date', 'AddedDate'
        ]
    }
    
    detected_fields = {}
    
    for field_type, patterns in field_patterns.items():
        detected_field = None
        
        # Exact matches first
        for pattern in patterns:
            if pattern in df.columns:
                detected_field = pattern
                break
        
        # Case-insensitive matches
        if not detected_field:
            df_columns_lower = {col.lower(): col for col in df.columns}
            for pattern in patterns:
                if pattern.lower() in df_columns_lower:
                    detected_field = df_columns_lower[pattern.lower()]
                    break
        
        # Partial matches (contains keywords)
        if not detected_field:
            keywords = {
                'invoice_created_by': ['create', 'by', 'user', 'entry', 'made', 'prepared'],
                'method_of_payment': ['payment', 'method', 'mode', 'mop'],
                'account_head': ['account', 'head', 'code', 'ledger'],
                'invoice_entry_date': ['entry', 'upload', 'creation', 'input', 'added']
            }
            
            if field_type in keywords:
                for df_col in df.columns:
                    if any(keyword in df_col.lower() for keyword in keywords[field_type]):
                        detected_field = df_col
                        break
        
        detected_fields[field_type] = detected_field
        
        if detected_field:
            print(f"✅ {field_type}: {detected_field}")
        else:
            print(f"⚠️ {field_type}: Not found")
    
    return detected_fields

def extract_rms_enhanced_fields(row, field_mapping):
    """Extract enhanced fields from RMS data with proper formatting"""
    enhanced_data = {}
    
    # Invoice Created By
    creator_field = field_mapping.get('invoice_created_by')
    if creator_field and creator_field in row:
        creator_name = str(row[creator_field]).strip()
        if creator_name and creator_name.lower() not in ['nan', 'none', 'null', '']:
            enhanced_data['Invoice_Created_By'] = creator_name
        else:
            enhanced_data['Invoice_Created_By'] = 'Unknown'
    else:
        enhanced_data['Invoice_Created_By'] = 'Unknown'
    
    # Method of Payment
    mop_field = field_mapping.get('method_of_payment')
    if mop_field and mop_field in row:
        mop_value = str(row[mop_field]).strip()
        if mop_value and mop_value.lower() not in ['nan', 'none', 'null', '']:
            enhanced_data['Method_of_Payment'] = mop_value
        else:
            enhanced_data['Method_of_Payment'] = 'Not Specified'
    else:
        enhanced_data['Method_of_Payment'] = 'Not Available'
    
    # Account Head
    account_field = field_mapping.get('account_head')
    if account_field and account_field in row:
        account_value = str(row[account_field]).strip()
        if account_value and account_value.lower() not in ['nan', 'none', 'null', '']:
            enhanced_data['Account_Head'] = account_value
        else:
            enhanced_data['Account_Head'] = 'Not Specified'
    else:
        enhanced_data['Account_Head'] = 'Not Available'
    
    # Invoice Entry Date
    entry_date_field = field_mapping.get('invoice_entry_date')
    if entry_date_field and entry_date_field in row:
        entry_date = row[entry_date_field]
        if pd.notna(entry_date):
            enhanced_data['Invoice_Entry_Date'] = entry_date
        else:
            enhanced_data['Invoice_Entry_Date'] = 'Not Available'
    else:
        enhanced_data['Invoice_Entry_Date'] = 'Not Available'
    
    return enhanced_data

# === ORIGINAL FUNCTIONS WITH ENHANCEMENTS ===

def should_run_today():
    """Check if validation should run today based on 4-day interval"""
    try:
        last_run = get_last_run_date()
        if not last_run:
            print("🆕 No previous runs found - running first validation")
            return True
        
        last_run_date = datetime.strptime(last_run, "%Y-%m-%d")
        today = datetime.today()
        days_since_last_run = (today - last_run_date).days
        
        print(f"📅 Last run: {last_run}, Days since: {days_since_last_run}")
        
        if days_since_last_run >= VALIDATION_INTERVAL_DAYS:
            print(f"✅ Time to run validation (>= {VALIDATION_INTERVAL_DAYS} days)")
            return True
        else:
            print(f"⏳ Too early to run validation (need {VALIDATION_INTERVAL_DAYS - days_since_last_run} more days)")
            return False
            
    except Exception as e:
        print(f"⚠️ Error checking run schedule: {str(e)}, defaulting to run")
        return True

def get_current_batch_dates():
    """Get the date range for current 4-day batch"""
    today = datetime.today()
    end_date = today - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=VALIDATION_BATCH_DAYS - 1)  # 4 days back
    
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

def get_cumulative_validation_range():
    """Get the cumulative date range from first validation to current batch"""
    try:
        first_validation_date = get_first_validation_date()
        
        if not first_validation_date:
            return get_current_batch_dates()
        
        first_date = datetime.strptime(first_validation_date, "%Y-%m-%d")
        today = datetime.today()
        three_months_ago = today - timedelta(days=30 * ACTIVE_VALIDATION_MONTHS)
        
        if first_date < three_months_ago:
            archive_date = three_months_ago.strftime("%Y-%m-%d")
            print(f"🗂️ First validation ({first_validation_date}) is older than 3 months, starting from {archive_date}")
            start_str = archive_date
        else:
            start_str = first_validation_date
        
        _, end_str = get_current_batch_dates()
        
        print(f"📅 Cumulative validation range: {start_str} to {end_str}")
        return start_str, end_str
        
    except Exception as e:
        print(f"⚠️ Error calculating cumulative range: {str(e)}, using current batch")
        return get_current_batch_dates()

def archive_data_older_than_three_months():
    """Archive validation data older than 3 months"""
    print(f"🗂️ Archiving validation data older than {ACTIVE_VALIDATION_MONTHS} months...")
    
    try:
        data_dir = "data"
        archive_base = os.path.join(data_dir, ARCHIVE_FOLDER)
        validation_archive = os.path.join(archive_base, "validation_reports")
        snapshot_archive = os.path.join(archive_base, "snapshots")
        daily_data_archive = os.path.join(archive_base, "daily_data")
        
        for archive_dir in [archive_base, validation_archive, snapshot_archive, daily_data_archive]:
            if not os.path.exists(archive_dir):
                os.makedirs(archive_dir)
        
        cutoff_date = datetime.today() - timedelta(days=30 * ACTIVE_VALIDATION_MONTHS)
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        
        print(f"📅 Archiving data older than: {cutoff_str}")
        archived_count = 0
        
        if not os.path.exists(data_dir):
            return archived_count
        
        # Archive validation reports
        for filename in os.listdir(data_dir):
            try:
                file_path = os.path.join(data_dir, filename)
                if not os.path.isfile(file_path):
                    continue
                    
                date_extracted = None
                
                if filename.startswith("invoice_validation_detailed_") and filename.endswith(".xlsx"):
                    date_str = filename.replace("invoice_validation_detailed_", "").replace(".xlsx", "")
                    date_extracted = datetime.strptime(date_str, "%Y-%m-%d")
                
                if date_extracted and date_extracted < cutoff_date:
                    src = os.path.join(data_dir, filename)
                    dst = os.path.join(validation_archive, filename)
                    shutil.move(src, dst)
                    print(f"📦 Archived report: {filename}")
                    archived_count += 1
                        
            except ValueError:
                continue
            except Exception as e:
                print(f"⚠️ Error archiving file {filename}: {str(e)}")
                continue
        
        try:
            from invoice_tracker import archive_validation_records_before_date
            archive_validation_records_before_date(cutoff_str)
            print(f"✅ Database records archived before {cutoff_str}")
        except Exception as e:
            print(f"⚠️ Database archiving failed: {str(e)}")
        
        print(f"✅ Archiving completed. {archived_count} items archived to {archive_base}")
        return archived_count
        
    except Exception as e:
        print(f"❌ Archiving failed: {str(e)}")
        return 0

def download_cumulative_data(start_str, end_str):
    """Download invoice data for the cumulative validation range"""
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")
    
    print(f"📥 Downloading cumulative validation data from {start_str} to {end_str}...")
    print(f"📊 Range covers: {(end_date - start_date).days + 1} days")
    
    try:
        invoice_path = rms_download(start_date, end_date)
        print(f"✅ Cumulative data download completed. Path: {invoice_path}")
        return invoice_path
    except Exception as e:
        print(f"❌ Cumulative data download failed: {str(e)}")
        raise

# === ENHANCED FIELD DETECTION FUNCTION ===
def find_creator_column(df):
    """Enhanced RMS field detection with comprehensive pattern matching"""
    print("🔍 Enhanced creator column detection with RMS patterns...")
    
    # Extended patterns for RMS system
    possible_creator_columns = [
        # Primary RMS patterns
        'CreatedBy', 'Created_By', 'InvoiceCreatedBy', 'Invoice_Created_By',
        'UserName', 'User_Name', 'CreatorName', 'Creator_Name',
        'EntryBy', 'Entry_By', 'InputBy', 'Input_By',
        'PreparedBy', 'Prepared_By', 'MadeBy', 'Made_By',
        # Extended RMS patterns
        'ProcessedBy', 'Processed_By', 'AddedBy', 'Added_By',
        'ModifiedBy', 'Modified_By', 'UpdatedBy', 'Updated_By',
        'OperatorName', 'Operator_Name', 'HandlerName', 'Handler_Name'
    ]
    
    # Check exact matches first
    for col in possible_creator_columns:
        if col in df.columns:
            print(f"✅ Found exact creator column: {col}")
            return col
    
    # Check case-insensitive matches
    df_columns_lower = {col.lower(): col for col in df.columns}
    for col in possible_creator_columns:
        if col.lower() in df_columns_lower:
            found_col = df_columns_lower[col.lower()]
            print(f"✅ Found creator column (case-insensitive): {found_col}")
            return found_col
    
    # Check partial matches with RMS keywords
    creator_keywords = ['create', 'by', 'user', 'entry', 'made', 'prepared', 
                       'process', 'add', 'input', 'operator', 'handler']
    
    for df_col in df.columns:
        col_lower = df_col.lower()
        if any(keyword in col_lower for keyword in creator_keywords):
            # Additional validation - check if it contains actual name data
            sample_data = df[df_col].dropna().head(10)
            if not sample_data.empty:
                # Check if it looks like names (contains alphabets, not just numbers)
                sample_values = sample_data.astype(str).str.strip()
                has_names = sample_values.str.contains(r'[A-Za-z]', regex=True).any()
                if has_names:
                    print(f"✅ Found potential creator column with name data: {df_col}")
                    return df_col
    
    print("⚠️ No creator column found with RMS patterns")
    return None

# === ENHANCED VALIDATION FUNCTION ===
def validate_invoices_with_details(df):
    """Enhanced validation with comprehensive RMS field extraction"""
    print("🔍 Running enhanced invoice-level validation with RMS field mapping...")
    
    try:
        # Step 1: Enhanced RMS field mapping
        field_mapping = enhance_rms_field_mapping(df)
        
        # Step 2: Run existing validation for summary
        summary_issues, problematic_invoices_df = validate_invoices(df)
        
        # Step 3: Enhanced field detection
        creator_column = find_creator_column(df)
        
        # Step 4: Detailed validation for each invoice
        detailed_results = []
        
        print(f"📋 Analyzing {len(df)} invoices with enhanced RMS field extraction...")
        
        for index, row in df.iterrows():
            # Basic invoice information
            invoice_id = row.get('InvID', f'Row_{index}')
            invoice_number = row.get('PurchaseInvNo', row.get('InvoiceNumber', 'N/A'))
            invoice_date = row.get('PurchaseInvDate', 'N/A')
            vendor = row.get('PartyName', row.get('VendorName', 'N/A'))
            amount = row.get('Total', row.get('Amount', 0))
            
            # Enhanced RMS field extraction
            enhanced_fields = extract_rms_enhanced_fields(row, field_mapping)
            
            # Validation logic
            validation_issues = []
            severity = "✅ PASS"
            
            # Enhanced validation rules
            if pd.isna(row.get('GSTNO')) or str(row.get('GSTNO')).strip() == '':
                validation_issues.append("Missing GST Number")
                severity = "❌ FAIL"
            
            if pd.isna(row.get('Total')) or str(row.get('Total')).strip() == '':
                validation_issues.append("Missing Total Amount")
                severity = "❌ FAIL"
            elif row.get('Total', 0) == 0:
                validation_issues.append("Zero Amount")
                if severity == "✅ PASS":
                    severity = "⚠️ WARNING"
            
            try:
                amount_value = float(row.get('Total', 0))
                if amount_value < 0:
                    validation_issues.append(f"Negative Amount: {amount_value}")
                    if severity == "✅ PASS":
                        severity = "⚠️ WARNING"
            except (ValueError, TypeError):
                validation_issues.append("Invalid Amount Format")
                severity = "❌ FAIL"
            
            if pd.isna(invoice_number) or str(invoice_number).strip() == '':
                validation_issues.append("Missing Invoice Number")
                severity = "❌ FAIL"
            
            if pd.isna(invoice_date) or str(invoice_date).strip() == '':
                validation_issues.append("Missing Invoice Date")
                severity = "❌ FAIL"
            
            if pd.isna(vendor) or str(vendor).strip() == '':
                validation_issues.append("Missing Vendor Name")
                severity = "❌ FAIL"
            
            # Enhanced RMS field validations
            if enhanced_fields['Invoice_Created_By'] == 'Unknown':
                validation_issues.append("Missing Invoice Creator Name")
                if severity == "✅ PASS":
                    severity = "⚠️ WARNING"
            
            if enhanced_fields['Method_of_Payment'] == 'Not Available':
                validation_issues.append("Missing Payment Method")
                if severity == "✅ PASS":
                    severity = "⚠️ WARNING"
            
            if enhanced_fields['Account_Head'] == 'Not Available':
                validation_issues.append("Missing Account Head")
                if severity == "✅ PASS":
                    severity = "⚠️ WARNING"
            
            # Compile enhanced results
            invoice_result = {
                'Invoice_ID': invoice_id,
                'Invoice_Number': invoice_number,
                'Invoice_Date': invoice_date,
                'Vendor_Name': vendor,
                'Amount': amount,
                'GST_Number': row.get('GSTNO', ''),
                # Enhanced RMS fields
                'Invoice_Created_By': enhanced_fields['Invoice_Created_By'],
                'Method_of_Payment': enhanced_fields['Method_of_Payment'],
                'Account_Head': enhanced_fields['Account_Head'],
                'Invoice_Entry_Date': enhanced_fields['Invoice_Entry_Date'],
                # Validation results
                'Validation_Status': severity,
                'Issues_Found': len(validation_issues),
                'Issue_Details': " | ".join(validation_issues) if validation_issues else "No issues found",
                'Row_Index': index,
                'Validation_Date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            detailed_results.append(invoice_result)
        
        # Convert to DataFrame
        detailed_df = pd.DataFrame(detailed_results)
        
        # Enhanced statistics
        total_invoices = len(detailed_df)
        passed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '✅ PASS'])
        warning_invoices = len(detailed_df[detailed_df['Validation_Status'] == '⚠️ WARNING'])
        failed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '❌ FAIL'])
        
        print(f"✅ Enhanced validation completed:")
        print(f"   📊 Total invoices: {total_invoices}")
        print(f"   ✅ Passed: {passed_invoices}")
        print(f"   ⚠️ Warnings: {warning_invoices}")
        print(f"   ❌ Failed: {failed_invoices}")
        
        # Enhanced RMS field statistics
        creator_stats = detailed_df['Invoice_Created_By'].value_counts()
        mop_stats = detailed_df['Method_of_Payment'].value_counts()
        account_stats = detailed_df['Account_Head'].value_counts()
        
        print(f"   👤 Creators: {len(creator_stats)} unique ({creator_stats.get('Unknown', 0)} unknown)")
        print(f"   💳 Payment methods: {len(mop_stats)} types")
        print(f"   🏢 Account heads: {len(account_stats)} categories")
        
        return detailed_df, summary_issues, problematic_invoices_df
        
    except Exception as e:
        print(f"❌ Enhanced validation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), [], pd.DataFrame()

# === ENHANCED EMAIL SUMMARY GENERATION ===
def generate_email_summary_statistics(detailed_df, cumulative_start, cumulative_end, current_batch_start, current_batch_end, today_str):
    """Enhanced email summary with RMS field analytics"""
    print("📧 Generating enhanced email summary with RMS analytics...")
    
    try:
        if detailed_df.empty:
            return {
                'html_summary': "
No invoice data available for validation.
",
                'text_summary': "No invoice data available for validation.",
                'statistics': {}
            }
        
        # Calculate enhanced statistics
        total_invoices = len(detailed_df)
        passed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '✅ PASS'])
        warning_invoices = len(detailed_df[detailed_df['Validation_Status'] == '⚠️ WARNING'])
        failed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '❌ FAIL'])
        
        pass_rate = (passed_invoices / total_invoices * 100) if total_invoices > 0 else 0
        
        # Enhanced RMS analytics
        creator_stats = detailed_df['Invoice_Created_By'].value_counts()
        mop_stats = detailed_df['Method_of_Payment'].value_counts()
        account_stats = detailed_df['Account_Head'].value_counts()
        
        unknown_creators = creator_stats.get('Unknown', 0)
        unavailable_mop = mop_stats.get('Not Available', 0) + mop_stats.get('Not Specified', 0)
        unavailable_accounts = account_stats.get('Not Available', 0) + account_stats.get('Not Specified', 0)
        
        # Issue analysis
        all_issues = []
        for issues_text in detailed_df['Issue_Details']:
            if issues_text != "No issues found":
                issues = issues_text.split(' | ')
                all_issues.extend(issues)
        
        issue_counts = {}
        for issue in all_issues:
            issue_counts[issue] = issue_counts.get(issue, 0) + 1
        
        top_issues = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        
        # Enhanced HTML summary
        html_summary = f"""
        

            

                📊 Enhanced Invoice Validation Report - {today_str}
            

            
            

                
📅 Validation Period

                
Current Batch: {current_batch_start} to {current_batch_end}

                
Cumulative Range: {cumulative_start} to {cumulative_end}

                
Total Coverage: {(datetime.strptime(cumulative_end, '%Y-%m-%d') - datetime.strptime(cumulative_start, '%Y-%m-%d')).days + 1} days

            

            
            

                    
✅ Total Invoices


                    
{total_invoices:,}

                

                    
✅ Passed


                    
{passed_invoices:,} ({pass_rate:.1f}%)

                

                    
⚠️ Warnings


                    
{warning_invoices:,}

                

                    
❌ Failed


                    
{failed_invoices:,}

                

            
            

                
🔍 Enhanced RMS Field Analysis

                

                        
👤 Invoice Creators


                        
Total Unique: {len(creator_stats)}

                        
Unknown: {unknown_creators} ({(unknown_creators/total_invoices*100):.1f}%)

                        
Top Creator: {creator_stats.index[0] if len(creator_stats) > 0 else 'N/A'}

                    

                        
💳 Payment Methods


                        
Available Types: {len(mop_stats)}

                        
Missing: {unavailable_mop} ({(unavailable_mop/total_invoices*100):.1f}%)

                        
Top Method: {mop_stats.index[0] if len([x for x in mop_stats.index if x not in ['Not Available', 'Not Specified']]) > 0 else 'N/A'}

                    

                        
🏢 Account Heads


                        
Available Categories: {len(account_stats)}

                        
Missing: {unavailable_accounts} ({(unavailable_accounts/total_invoices*100):.1f}%)

                        
Top Category: {account_stats.index[0] if len([x for x in account_stats.index if x not in ['Not Available', 'Not Specified']]) > 0 else 'N/A'}

                    

            

            
            

                
🔍 Top Validation Issues

                
"""
        
        for issue, count in top_issues:
            percentage = (count / total_invoices * 100) if total_invoices > 0 else 0
            severity_color = "#e74c3c" if "Missing" in issue else ("#f39c12" if any(word in issue for word in ["Duplicate", "Negative", "Future", "Old"]) else "#3498db")
            html_summary += f'
{issue}: {count:,} invoices ({percentage:.1f}%)
'
        
        html_summary += f"""
                

            

        
"""
        
        # Enhanced text summary
        text_summary = f"""
📊 ENHANCED INVOICE VALIDATION REPORT - {today_str}

📅 VALIDATION PERIOD:
• Current Batch: {current_batch_start} to {current_batch_end}
• Cumulative Range: {cumulative_start} to {cumulative_end}
• Total Coverage: {(datetime.strptime(cumulative_end, '%Y-%m-%d') - datetime.strptime(cumulative_start, '%Y-%m-%d')).days + 1} days

📈 VALIDATION RESULTS:
✅ Total Invoices: {total_invoices:,}
✅ Passed: {passed_invoices:,} ({pass_rate:.1f}%)
⚠️ Warnings: {warning_invoices:,}
❌ Failed: {failed_invoices:,}

🔍 ENHANCED RMS FIELD ANALYSIS:
👤 Invoice Creators: {len(creator_stats)} unique ({unknown_creators} unknown)
💳 Payment Methods: {len(mop_stats)} types ({unavailable_mop} missing)
🏢 Account Heads: {len(account_stats)} categories ({unavailable_accounts} missing)

🔍 TOP VALIDATION ISSUES:"""
        
        for i, (issue, count) in enumerate(top_issues, 1):
            percentage = (count / total_invoices * 100) if total_invoices > 0 else 0
            text_summary += f"\n{i}. {issue}: {count:,} invoices ({percentage:.1f}%)"
        
        # Enhanced statistics object
        statistics = {
            'total_invoices': total_invoices,
            'passed_invoices': passed_invoices,
            'warning_invoices': warning_invoices,
            'failed_invoices': failed_invoices,
            'pass_rate': pass_rate,
            'top_issues': top_issues,
            # Enhanced RMS analytics
            'total_creators': len(creator_stats),
            'unknown_creators': unknown_creators,
            'total_payment_methods': len(mop_stats),
            'unavailable_payment_methods': unavailable_mop,
            'total_account_heads': len(account_stats),
            'unavailable_account_heads': unavailable_accounts,
            'validation_date': today_str,
            'current_batch_start': current_batch_start,
            'current_batch_end': current_batch_end,
            'cumulative_start': cumulative_start,
            'cumulative_end': cumulative_end
        }
        
        print(f"✅ Enhanced email summary generated with RMS analytics")
        print(f"   👤 {len(creator_stats)} creators ({unknown_creators} unknown)")
        print(f"   💳 {len(mop_stats)} payment methods ({unavailable_mop} missing)")
        print(f"   🏢 {len(account_stats)} account heads ({unavailable_accounts} missing)")
        
        return {
            'html_summary': html_summary,
            'text_summary': text_summary,
            'statistics': statistics
        }
        
    except Exception as e:
        print(f"❌ Enhanced email summary generation failed: {str(e)}")
        return {
            'html_summary': f"
Error generating enhanced summary: {str(e)}
",
            'text_summary': f"Error generating enhanced summary: {str(e)}",
            'statistics': {}
        }

# === ENHANCED EXCEL FORMATTING ===
def format_excel_report_with_styling(writer, sheet_name, df):
    """Apply professional formatting to Excel sheets"""
    try:
        from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
        from openpyxl.utils import get_column_letter
        
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        
        # Header formatting
        header_font = Font(bold=True, color='FFFFFF')
        header_fill = PatternFill(start_color='2F75B5', end_color='2F75B5', fill_type='solid')
        
        # Apply header formatting
        for col_num, column_title in enumerate(df.columns, 1):
            cell = worksheet.cell(row=1, column=col_num)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center')
        
        # Auto-adjust column widths
        for column_cells in worksheet.columns:
            length = max(len(str(cell.value)) for cell in column_cells)
            worksheet.column_dimensions[get_column_letter(column_cells[0].column)].width = min(50, max(12, length + 2))
        
        # Conditional formatting for validation status
        if 'Validation_Status' in df.columns:
            status_col = df.columns.get_loc('Validation_Status') + 1
            for row in range(2, len(df) + 2):
                cell = worksheet.cell(row=row, column=status_col)
                if '✅ PASS' in str(cell.value):
                    cell.fill = PatternFill(start_color='D5F4E6', end_color='D5F4E6', fill_type='solid')
                elif '⚠️ WARNING' in str(cell.value):
                    cell.fill = PatternFill(start_color='FEF9E7', end_color='FEF9E7', fill_type='solid')
                elif '❌ FAIL' in str(cell.value):
                    cell.fill = PatternFill(start_color='FADBD8', end_color='FADBD8', fill_type='solid')
        
    except Exception as e:
        print(f"⚠️ Excel formatting failed: {str(e)}")

# === CONTINUATION OF ORIGINAL FUNCTIONS ===

def generate_detailed_validation_report(detailed_df, today_str):
    """Generate enhanced detailed validation report for Excel export"""
    print("📋 Generating enhanced detailed validation report for Excel export...")
    
    try:
        if detailed_df.empty:
            return []
        
        summary_data = []
        
        # Overall statistics
        total_invoices = len(detailed_df)
        passed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '✅ PASS'])
        warning_invoices = len(detailed_df[detailed_df['Validation_Status'] == '⚠️ WARNING'])
        failed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '❌ FAIL'])
        
        summary_data.extend([
            {'Report_Type': 'Overall_Summary', 'Description': 'Total Invoice Count', 'Count': total_invoices, 'Percentage': '100.0%', 'Status': 'INFO'},
            {'Report_Type': 'Overall_Summary', 'Description': 'Passed Validation', 'Count': passed_invoices, 'Percentage': f'{(passed_invoices/total_invoices*100):.1f}%' if total_invoices > 0 else '0%', 'Status': 'PASS'},
            {'Report_Type': 'Overall_Summary', 'Description': 'Warnings', 'Count': warning_invoices, 'Percentage': f'{(warning_invoices/total_invoices*100):.1f}%' if total_invoices > 0 else '0%', 'Status': 'WARNING'},
            {'Report_Type': 'Overall_Summary', 'Description': 'Failed Validation', 'Count': failed_invoices, 'Percentage': f'{(failed_invoices/total_invoices*100):.1f}%' if total_invoices > 0 else '0%', 'Status': 'FAIL'}
        ])
        
        # Enhanced RMS field statistics
        creator_stats = detailed_df['Invoice_Created_By'].value_counts()
        mop_stats = detailed_df['Method_of_Payment'].value_counts()
        account_stats = detailed_df['Account_Head'].value_counts()
        
        unknown_creators = creator_stats.get('Unknown', 0)
        unavailable_mop = mop_stats.get('Not Available', 0) + mop_stats.get('Not Specified', 0)
        unavailable_accounts = account_stats.get('Not Available', 0) + account_stats.get('Not Specified', 0)
        
        summary_data.extend([
            {'Report_Type': 'RMS_Field_Analysis', 'Description': 'Total Unique Creators', 'Count': len(creator_stats), 'Percentage': '100.0%', 'Status': 'INFO'},
            {'Report_Type': 'RMS_Field_Analysis', 'Description': 'Unknown/Missing Creators', 'Count': unknown_creators, 'Percentage': f'{(unknown_creators/total_invoices*100):.1f}%' if total_invoices > 0 else '0%', 'Status': 'WARNING' if unknown_creators > 0 else 'PASS'},
            {'Report_Type': 'RMS_Field_Analysis', 'Description': 'Payment Method Types', 'Count': len(mop_stats), 'Percentage': '100.0%', 'Status': 'INFO'},
            {'Report_Type': 'RMS_Field_Analysis', 'Description': 'Missing Payment Methods', 'Count': unavailable_mop, 'Percentage': f'{(unavailable_mop/total_invoices*100):.1f}%' if total_invoices > 0 else '0%', 'Status': 'WARNING' if unavailable_mop > 0 else 'PASS'},
            {'Report_Type': 'RMS_Field_Analysis', 'Description': 'Account Head Categories', 'Count': len(account_stats), 'Percentage': '100.0%', 'Status': 'INFO'},
            {'Report_Type': 'RMS_Field_Analysis', 'Description': 'Missing Account Heads', 'Count': unavailable_accounts, 'Percentage': f'{(unavailable_accounts/total_invoices*100):.1f}%' if total_invoices > 0 else '0%', 'Status': 'WARNING' if unavailable_accounts > 0 else 'PASS'}
        ])
        
        print(f"✅ Enhanced detailed report prepared with {len(summary_data)} summary entries including RMS analytics")
        return summary_data
        
    except Exception as e:
        print(f"❌ Enhanced detailed report generation failed: {str(e)}")
        return []

# === CONTINUED ORIGINAL FUNCTIONS (read_invoice_file, validate_downloaded_files, etc.) ===

def read_invoice_file(invoice_file):
    """Robust file reading with multiple format support and proper error handling"""
    print(f"🔍 Attempting to read file: {invoice_file}")

    if not os.path.exists(invoice_file):
        raise FileNotFoundError(f"Invoice file not found: {invoice_file}")

    file_path = Path(invoice_file)
    file_ext = file_path.suffix.lower()
    file_size = os.path.getsize(invoice_file)
    print(f"📄 File: {file_path.name}, Extension: {file_ext}, Size: {file_size} bytes")
    
    if file_size < 50:
        raise ValueError(f"File appears to be too small ({file_size} bytes) - likely corrupted or empty")
            
    try:
        with open(invoice_file, 'rb') as f:
            header = f.read(50)
        print(f"🔍 File header (first 20 bytes): {header[:20]}")
    except Exception as e:
        print(f"⚠️ Could not read file header: {e}")
        header = b''
                
    df = None
    last_error = None
                    
    # Method 1: Try Excel with openpyxl engine
    try:
        print("📊 Attempting to read as Excel with openpyxl engine...")
        df = pd.read_excel(invoice_file, engine='openpyxl')
        print(f"✅ Successfully read Excel file with openpyxl. Shape: {df.shape}")
        print(f"📋 Columns: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"⚠️ openpyxl engine failed: {str(e)}")
        last_error = e
    
    # Method 2: Try Excel with xlrd engine
    if file_ext == '.xls':
        try:
            print("📊 Attempting to read as Excel with xlrd engine...")
            df = pd.read_excel(invoice_file, engine='xlrd')
            print(f"✅ Successfully read Excel file with xlrd. Shape: {df.shape}")
            print(f"📋 Columns: {list(df.columns)}")
            return df
        except Exception as e:
            print(f"⚠️ xlrd engine failed: {str(e)}")
            last_error = e
    
    # Method 3: Try CSV with different separators
    try:
        print("📄 Attempting to read as CSV...")
        separators = [',', ';', '\t', '|']
        for sep in separators:
            try:
                df_test = pd.read_csv(invoice_file, sep=sep, nrows=5)
                if df_test.shape[1] > 1:
                    df = pd.read_csv(invoice_file, sep=sep)
                    print(f"✅ Successfully read as CSV with separator '{sep}'. Shape: {df.shape}")
                    print(f"📋 Columns: {list(df.columns)}")
                    return df
            except:
                continue
    except Exception as e:
        print(f"⚠️ CSV reading failed: {str(e)}")
        last_error = e
        
    if last_error:
        raise Exception(f"Could not read invoice file in any supported format. Last error: {str(last_error)}")
    else:
        raise Exception("Could not read invoice file - unknown format or corrupted file")

def validate_downloaded_files(download_dir): 
    """Validate that downloaded files exist and are not corrupted"""
    required_files = ["invoice_download.xls", "invoices.zip"]
    validation_results = {}
        
    for fname in required_files:
        file_path = os.path.join(download_dir, fname)
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            print(f"✅ Found {fname}: {file_size} bytes")
    
            if file_size < 50:
                print(f"⚠️ Warning: {fname} seems too small ({file_size} bytes)")
                validation_results[fname] = "small"
            else:
                validation_results[fname] = "ok"
        
            try:
                with open(file_path, 'rb') as f:
                    header = f.read(20)
                print(f"🔍 {fname} header: {header}")
            except Exception as e:
                print(f"⚠️ Could not read {fname} header: {e}")
        else:
            print(f"❌ Missing file: {fname}")
            validation_results[fname] = "missing"
    
    return validation_results
        
def filter_invoices_by_date(df, start_str, end_str):
    """Filter dataframe by date range"""
    try:
        if 'PurchaseInvDate' not in df.columns:
            print("⚠️ PurchaseInvDate column not found, returning all data")
            return df
    
        start_date = datetime.strptime(start_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_str, "%Y-%m-%d")
        
        df["ParsedInvoiceDate"] = pd.to_datetime(df["PurchaseInvDate"], errors='coerce')
    
        filtered_df = df[
            (df["ParsedInvoiceDate"] >= start_date) &
            (df["ParsedInvoiceDate"] <= end_date)  
        ]
                
        print(f"📅 Filtered invoices from {start_str} to {end_str}: {len(filtered_df)} out of {len(df)}")
        return filtered_df
            
    except Exception as e:
        print(f"⚠️ Date filtering failed: {str(e)}, returning all data")
        return df

# === MAIN VALIDATION FUNCTION (PRESERVED ORIGINAL NAME) ===
def run_invoice_validation():
    """Enhanced main validation function with comprehensive RMS field mapping and improved reporting"""
    try:
        today = datetime.today()
        today_str = today.strftime("%Y-%m-%d")
        
        print(f"🚀 Starting ENHANCED cumulative validation workflow for {today_str}")
        print(f"📧 ENHANCED FEATURES: RMS field mapping, professional Excel reports, creator analytics")
        print(f"📋 NEW FIELDS: Invoice Created By, Method of Payment, Account Head, Entry Date")
        print(f"⚙️ Configuration:")
        print(f"   📅 Validation interval: {VALIDATION_INTERVAL_DAYS} days")
        print(f"   📦 Batch size: {VALIDATION_BATCH_DAYS} days")
        print(f"   🗓️ Active window: {ACTIVE_VALIDATION_MONTHS} months")
        
        # Step 1: Check if we should run today
        print("🔍 Step 1: Checking if validation should run today...")
        if not should_run_today():
            print("⏳ Skipping validation - not yet time for next 4-day interval")
            return True
        
        # Step 2: Archive old data
        print("🗂️ Step 2: Archiving data older than 3 months...")
        try:
            archived_count = archive_data_older_than_three_months()
            print(f"✅ Archived {archived_count} old items" if archived_count > 0 else "✅ No old data to archive")
        except Exception as e:
            print(f"⚠️ Archiving failed but continuing: {str(e)}")
        
        # Step 3: Calculate date ranges
        print("📊 Step 3: Calculating cumulative validation range...")
        try:
            cumulative_start, cumulative_end = get_cumulative_validation_range()
            current_batch_start, current_batch_end = get_current_batch_dates()
            
            print(f"📅 Current batch: {current_batch_start} to {current_batch_end}")
            print(f"📅 Cumulative range: {cumulative_start} to {cumulative_end}")
        except Exception as e:
            print(f"❌ Failed to calculate date ranges: {str(e)}")
            return False
        
        # Step 4: Download data
        print("📥 Step 4: Downloading cumulative validation data...")
        try:
            invoice_path = download_cumulative_data(cumulative_start, cumulative_end)
        except Exception as e:
            print(f"❌ Data download failed: {str(e)}")
            return False
        
        # Step 5: Verify files
        download_dir = os.path.join("data", today_str)
        print(f"🔍 Step 5: Verifying files in directory: {download_dir}")
         
        validation_results = validate_downloaded_files(download_dir)
        
        invoice_file = os.path.join(download_dir, "invoice_download.xls")
    
        if validation_results.get("invoice_download.xls") == "missing":
            print("❌ No invoice file downloaded. Aborting.")
            return False
    
        # Step 6: Read and parse data
        print("📊 Step 6: Reading cumulative invoice data...")
        try:
            df = read_invoice_file(invoice_file)
        
            if df is None or df.empty:
                print("❌ DataFrame is empty after reading file")
                return False
        
            print(f"✅ Successfully loaded cumulative data. Shape: {df.shape}")
            print(f"📋 Available columns: {list(df.columns)}")
        except Exception as e:
            print(f"❌ Failed to read invoice file: {str(e)}")
            return False
        
        # Step 7: Filter data by date range
        print("🔄 Step 7: Filtering to cumulative validation range...")
        try:
            filtered_df = filter_invoices_by_date(df, cumulative_start, cumulative_end)
            print(f"📅 Working with {len(filtered_df)} invoices in cumulative range")
        except Exception as e:
            print(f"⚠️ Date filtering failed: {str(e)}, using all data")
            filtered_df = df
        
        # Step 8: Enhanced validation with RMS field mapping
        print("🔄 Step 8: Running enhanced validation with RMS field mapping...")
        print("   🔄 This includes enhanced field detection for:")
        print("      👤 Invoice Created By (proper name detection)")
        print("      💳 Method of Payment (MOP field mapping)")
        print("      🏢 Account Head (account classification)")
        print("      📅 Invoice Entry Date (upload/creation date)")
        try:
            detailed_df, summary_issues, problematic_invoices_df = validate_invoices_with_details(filtered_df)
            
            if detailed_df.empty:
                print("⚠️ No detailed validation results generated")
            else:
                print(f"✅ Enhanced validation completed on {len(detailed_df)} invoices")
        except Exception as e:
            print(f"❌ Enhanced validation failed: {str(e)}")
            return False
        
        # Step 9: Generate enhanced email summary
        print("📧 Step 9: Generating enhanced email summary with RMS analytics...")
        try:
            email_summary = generate_email_summary_statistics(
                detailed_df, 
                cumulative_start, 
                cumulative_end, 
                current_batch_start, 
                current_batch_end, 
                today_str
            )
        except Exception as e:
            print(f"⚠️ Enhanced email summary generation failed: {str(e)}")
            email_summary = {
                'html_summary': f"
Error generating enhanced summary: {str(e)}
",
                'text_summary': f"Error generating enhanced summary: {str(e)}",
                'statistics': {}
            }
        
        # Step 10: Generate enhanced detailed report
        print("📋 Step 10: Generating enhanced detailed validation report...")
        try:
            detailed_report = generate_detailed_validation_report(detailed_df, today_str)
        except Exception as e:
            print(f"⚠️ Enhanced detailed report generation failed: {str(e)}")
            detailed_report = []
        
        # Step 11: Save data snapshots
        print("💾 Step 11: Preparing enhanced invoice data for saving...")
        try:
            if not detailed_df.empty:
                current_invoices_list = detailed_df.to_dict('records')
            else:
                current_invoices_list = []
            
            print(f"📋 Prepared {len(current_invoices_list)} enhanced invoice records for saving")
        except Exception as e:
            print(f"⚠️ Failed to prepare invoice list: {str(e)}")
            current_invoices_list = []
        
        # Step 12: Save validation snapshot
        try:
            save_invoice_snapshot(
                current_invoices_list, 
                run_date=today_str, 
                run_type="enhanced_cumulative_4day",
                batch_start=current_batch_start,
                batch_end=current_batch_end,
                cumulative_start=cumulative_start,
                cumulative_end=cumulative_end
            )
            print("✅ Enhanced validation snapshot saved")
        except Exception as e:
            print(f"⚠️ Failed to save snapshot: {str(e)}")
            
        # Step 13: Record run
        try:
            record_run_window(
                current_batch_start, 
                current_batch_end, 
                run_type="enhanced_cumulative_4day",
                cumulative_start=cumulative_start,
                cumulative_end=cumulative_end,
                total_days_validated=(datetime.strptime(cumulative_end, "%Y-%m-%d") - 
                                    datetime.strptime(cumulative_start, "%Y-%m-%d")).days + 1
            )
            print("✅ Enhanced cumulative run recorded")
        except Exception as e:
            print(f"⚠️ Failed to record run: {str(e)}")
        
        # Step 14: Save enhanced Excel reports
        try:
            os.makedirs("data", exist_ok=True)
            
            # Enhanced detailed validation report with professional formatting
            detailed_report_path = f"data/invoice_validation_detailed_{today_str}.xlsx"
            
            if not detailed_df.empty:
                with pd.ExcelWriter(detailed_report_path, engine='openpyxl') as writer:
                    # Sheet 1: All invoices with enhanced RMS fields
                    detailed_df.to_excel(writer, sheet_name='All_Invoices', index=False)
                    format_excel_report_with_styling(writer, 'All_Invoices', detailed_df)
                    
                    # Sheet 2: Failed invoices with RMS fields
                    failed_df = detailed_df[detailed_df['Validation_Status'] == '❌ FAIL']
                    if not failed_df.empty:
                        failed_df.to_excel(writer, sheet_name='Failed_Invoices', index=False)
                        format_excel_report_with_styling(writer, 'Failed_Invoices', failed_df)
                    
                    # Sheet 3: Warning invoices with RMS fields
                    warning_df = detailed_df[detailed_df['Validation_Status'] == '⚠️ WARNING'] 
                    if not warning_df.empty:
                        warning_df.to_excel(writer, sheet_name='Warning_Invoices', index=False)
                        format_excel_report_with_styling(writer, 'Warning_Invoices', warning_df)
                    
                    # Sheet 4: Passed invoices
                    passed_df = detailed_df[detailed_df['Validation_Status'] == '✅ PASS']
                    if not passed_df.empty:
                        passed_df.to_excel(writer, sheet_name='Passed_Invoices', index=False)
                    
                    # Sheet 5: Enhanced Creator Analysis
                    creator_analysis = detailed_df.groupby('Invoice_Created_By').agg({
                        'Invoice_ID': 'count',
                        'Amount': ['sum', 'mean'],
                        'Validation_Status': lambda x: (x == '✅ PASS').sum()
                    }).round(2)
                    creator_analysis.columns = ['Total_Invoices', 'Total_Amount', 'Average_Amount', 'Passed_Count']
                    creator_analysis = creator_analysis.reset_index().sort_values('Total_Invoices', ascending=False)
                    creator_analysis.to_excel(writer, sheet_name='Creator_Analysis', index=False)
                    
                    # Sheet 6: Method of Payment Analysis
                    mop_analysis = detailed_df.groupby('Method_of_Payment').agg({
                        'Invoice_ID': 'count',
                        'Amount': ['sum', 'mean']
                    }).round(2)
                    mop_analysis.columns = ['Invoice_Count', 'Total_Amount', 'Average_Amount']
                    mop_analysis = mop_analysis.reset_index().sort_values('Invoice_Count', ascending=False)
                    mop_analysis.to_excel(writer, sheet_name='Payment_Method_Analysis', index=False)
                    
                    # Sheet 7: Account Head Analysis
                    account_analysis = detailed_df.groupby('Account_Head').agg({
                        'Invoice_ID': 'count',
                        'Amount': ['sum', 'mean']
                    }).round(2)
                    account_analysis.columns = ['Invoice_Count', 'Total_Amount', 'Average_Amount']
                    account_analysis = account_analysis.reset_index().sort_values('Invoice_Count', ascending=False)
                    account_analysis.to_excel(writer, sheet_name='Account_Head_Analysis', index=False)
                    
                    # Sheet 8: Enhanced Summary Statistics
                    if detailed_report:
                        summary_df = pd.DataFrame(detailed_report)
                        summary_df.to_excel(writer, sheet_name='Enhanced_Summary', index=False)
                
                print(f"✅ Enhanced Excel report saved with RMS fields: {detailed_report_path}")

                # Create enhanced dashboard version
                os.makedirs(f"data/{today_str}", exist_ok=True)
                dashboard_path = f"data/{today_str}/validation_result.xlsx"
                
                # Enhanced dashboard columns with proper order
                dashboard_columns = [
                    'Invoice_ID', 'Invoice_Number', 'Invoice_Date', 'Vendor_Name', 
                    'Amount', 'GST_Number', 'Invoice_Created_By', 'Method_of_Payment',
                    'Account_Head', 'Invoice_Entry_Date', 'Validation_Status', 
                    'Issues_Found', 'Issue_Details'
                ]
                
                dashboard_df = detailed_df[dashboard_columns].copy()
                
                # Enhanced status summary
                dashboard_df['Enhanced_Status_Summary'] = dashboard_df.apply(lambda row: 
                    f"{row['Validation_Status']} - {row['Issues_Found']} issues - Creator: {row['Invoice_Created_By']}" if row['Issues_Found'] > 0 
                    else f"{row['Validation_Status']} - No issues - Creator: {row['Invoice_Created_By']}", axis=1)
                
                dashboard_df.to_excel(dashboard_path, index=False, engine='openpyxl')
                print(f"📋 Enhanced dashboard report created with RMS fields: {dashboard_path}")
                
                # Enhanced delta report
                delta_report_path = f"data/delta_report_{today_str}.xlsx"
                dashboard_df.to_excel(delta_report_path, index=False, engine='openpyxl')
                print(f"📋 Enhanced delta report created with RMS fields: {delta_report_path}")
                
                # Save enhanced email summary
                summary_path = f"data/email_summary_{today_str}.html"
                with open(summary_path, 'w', encoding='utf-8') as f:
                    f.write(email_summary['html_summary'])
                print(f"📧 Enhanced email summary saved: {summary_path}")
                
            else:
                print("⚠️ No enhanced validation results - creating empty report")
                empty_df = pd.DataFrame({
                    'Invoice_ID': [], 'Invoice_Number': [], 'Invoice_Date': [], 'Vendor_Name': [],
                    'Amount': [], 'GST_Number': [], 'Invoice_Created_By': [], 'Method_of_Payment': [],
                    'Account_Head': [], 'Invoice_Entry_Date': [], 'Validation_Status': [], 
                    'Issues_Found': [], 'Issue_Details': []
                })
                empty_df.to_excel(detailed_report_path, index=False, engine='openpyxl')
                print(f"✅ Empty enhanced report created: {detailed_report_path}")
                        
        except Exception as e:
            print(f"❌ Failed to save enhanced reports: {str(e)}")
            return False

        # Step 15: Enhanced processing (if available)
        print("🚀 Step 15: Applying enhanced features...")
        try:
            if ENHANCED_PROCESSOR_AVAILABLE:
                enhancement_result = enhance_validation_results(detailed_df, email_summary)
                
                if enhancement_result['success']:
                    print("✅ Enhancement successful!")
                    enhanced_df = enhancement_result['enhanced_df']
                    
                    # Save enhanced version
                    enhanced_report_path = f"data/enhanced_invoice_validation_detailed_{today_str}.xlsx"
                    
                    with pd.ExcelWriter(enhanced_report_path, engine='openpyxl') as writer:
                        enhanced_df.to_excel(writer, sheet_name='Enhanced_All_Invoices', index=False)
                        format_excel_report_with_styling(writer, 'Enhanced_All_Invoices', enhanced_df)
                    
                    print(f"✅ Enhanced report saved: {enhanced_report_path}")
                else:
                    print(f"⚠️ Enhancement failed: {enhancement_result['error']}")
            else:
                print("⚠️ Enhanced processor not available, using standard validation")
                
        except Exception as e:
            print(f"⚠️ Enhancement failed: {str(e)}")
                
        # Step 16: Enhanced email notifications
        try:
            from email_notifier import EmailNotifier
            
            notifier = EmailNotifier()
                
            ap_team_recipients = os.getenv('AP_TEAM_EMAIL_LIST', '').split(',')
            ap_team_recipients = [email.strip() for email in ap_team_recipients if email.strip()]
                    
            if ap_team_recipients: 
                try:
                    if hasattr(notifier, 'send_detailed_validation_report'):
                        notifier.send_detailed_validation_report(
                            today_str, 
                            ap_team_recipients, 
                            email_summary,
                            detailed_report_path if not detailed_df.empty else None,
                            current_batch_start,
                            current_batch_end,
                            cumulative_start,
                            cumulative_end
                        )
                        print(f"📧 Enhanced validation report sent to AP team: {', '.join(ap_team_recipients)}")
                    else:
                        issues_count = len(email_summary.get('statistics', {}).get('failed_invoices', []))
                        notifier.send_validation_report(today_str, ap_team_recipients, issues_count)
                        print(
    
    
    
    
