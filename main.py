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
        # Get the very first validation date
        first_validation_date = get_first_validation_date()
        
        if not first_validation_date:
            # If no previous validations, start with current batch
            return get_current_batch_dates()
        
        # Calculate if first validation is older than 3 months
        first_date = datetime.strptime(first_validation_date, "%Y-%m-%d")
        today = datetime.today()
        three_months_ago = today - timedelta(days=30 * ACTIVE_VALIDATION_MONTHS)
        
        if first_date < three_months_ago:
            # Archive old data and start from 3 months ago
            archive_date = three_months_ago.strftime("%Y-%m-%d")
            print(f"🗂️ First validation ({first_validation_date}) is older than 3 months, starting from {archive_date}")
            start_str = archive_date
        else:
            start_str = first_validation_date
        
        # End date is the current batch end
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
        # Create archive directories
        data_dir = "data"
        archive_base = os.path.join(data_dir, ARCHIVE_FOLDER)
        validation_archive = os.path.join(archive_base, "validation_reports")
        snapshot_archive = os.path.join(archive_base, "snapshots")
        daily_data_archive = os.path.join(archive_base, "daily_data")
        
        for archive_dir in [archive_base, validation_archive, snapshot_archive, daily_data_archive]:
            if not os.path.exists(archive_dir):
                os.makedirs(archive_dir)
        
        # Calculate cutoff date (3 months ago)
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
                
                # Extract date from various report types
                if filename.startswith("invoice_validation_detailed_") and filename.endswith(".xlsx"):
                    date_str = filename.replace("invoice_validation_detailed_", "").replace(".xlsx", "")
                    date_extracted = datetime.strptime(date_str, "%Y-%m-%d")
                    
                elif filename.startswith("validation_summary_") and filename.endswith(".xlsx"):
                    date_str = filename.replace("validation_summary_", "").replace(".xlsx", "")
                    date_extracted = datetime.strptime(date_str, "%Y-%m-%d")
                    
                elif filename.startswith("delta_report_") and filename.endswith(".xlsx"):
                    date_str = filename.replace("delta_report_", "").replace(".xlsx", "")
                    date_extracted = datetime.strptime(date_str, "%Y-%m-%d")
                
                # Archive if older than cutoff
                if date_extracted and date_extracted < cutoff_date:
                    src = os.path.join(data_dir, filename)
                    dst = os.path.join(validation_archive, filename)
                    shutil.move(src, dst)
                    print(f"📦 Archived report: {filename}")
                    archived_count += 1
                        
            except ValueError:
                # Skip files with invalid date formats
                continue
            except Exception as e:
                print(f"⚠️ Error archiving file {filename}: {str(e)}")
                continue
        
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

def enhance_rms_field_mapping(df):
    """Enhanced RMS field mapping with comprehensive field detection"""
    print("🔍 Analyzing RMS data structure for enhanced field mapping...")
    
    field_mapping = {
        'invoice_created_by': [],
        'method_of_payment': [],
        'account_head': [],
        'invoice_entry_date': [],
        'invoice_upload_date': []
    }
    
    # Enhanced field detection patterns
    creator_patterns = [
        'CreatedBy', 'Created_By', 'InvoiceCreatedBy', 'Invoice_Created_By',
        'UserName', 'User_Name', 'CreatorName', 'Creator_Name',
        'EntryBy', 'Entry_By', 'InputBy', 'Input_By',
        'PreparedBy', 'Prepared_By', 'MadeBy', 'Made_By',
        'OperatorName', 'Operator_Name', 'StaffName', 'Staff_Name'
    ]
    
    mop_patterns = [
        'MOP', 'Method_of_Payment', 'MethodOfPayment', 'Payment_Method',
        'PaymentMethod', 'PaymentMode', 'Payment_Mode', 'PayMode',
        'TransactionType', 'Transaction_Type', 'PayType', 'Pay_Type'
    ]
    
    account_patterns = [
        'Account_Head', 'AccountHead', 'Account_Code', 'AccountCode',
        'GL_Account', 'GLAccount', 'GeneralLedger', 'General_Ledger',
        'ChartOfAccount', 'Chart_Of_Account', 'COA', 'AccountCategory'
    ]
    
    date_patterns = [
        'EntryDate', 'Entry_Date', 'Upload_Date', 'UploadDate',
        'Creation_Date', 'CreationDate', 'Input_Date', 'InputDate',
        'Timestamp', 'Time_Stamp', 'RecordDate', 'Record_Date'
    ]
    
    # Check available columns
    available_columns = list(df.columns)
    print(f"📋 Available columns in RMS data: {len(available_columns)}")
    
    # Map creator fields
    for pattern in creator_patterns:
        matches = [col for col in available_columns if pattern.lower() in col.lower()]
        field_mapping['invoice_created_by'].extend(matches)
    
    # Map MOP fields
    for pattern in mop_patterns:
        matches = [col for col in available_columns if pattern.lower() in col.lower()]
        field_mapping['method_of_payment'].extend(matches)
    
    # Map account head fields
    for pattern in account_patterns:
        matches = [col for col in available_columns if pattern.lower() in col.lower()]
        field_mapping['account_head'].extend(matches)
    
    # Map date fields
    for pattern in date_patterns:
        matches = [col for col in available_columns if pattern.lower() in col.lower()]
        field_mapping['invoice_entry_date'].extend(matches)
        field_mapping['invoice_upload_date'].extend(matches)
    
    # Remove duplicates and get best matches
    for key in field_mapping:
        field_mapping[key] = list(set(field_mapping[key]))
        if field_mapping[key]:
            print(f"✅ Found {key} candidates: {field_mapping[key]}")
        else:
            print(f"⚠️ No candidates found for {key}")
    
    return field_mapping

def extract_rms_enhanced_fields(row, field_mapping):
    """Extract enhanced RMS fields from a data row"""
    extracted = {
        'invoice_created_by': 'Unknown',
        'method_of_payment': 'Not Specified',
        'account_head': 'Not Assigned',
        'invoice_entry_date': None,
        'invoice_upload_date': None
    }
    
    # Extract creator name
    for col in field_mapping.get('invoice_created_by', []):
        if col in row.index and pd.notna(row[col]):
            creator_val = str(row[col]).strip()
            if creator_val and creator_val.lower() not in ['', 'nan', 'none', 'null']:
                extracted['invoice_created_by'] = creator_val
                break
    
    # Extract method of payment
    for col in field_mapping.get('method_of_payment', []):
        if col in row.index and pd.notna(row[col]):
            mop_val = str(row[col]).strip()
            if mop_val and mop_val.lower() not in ['', 'nan', 'none', 'null']:
                extracted['method_of_payment'] = mop_val
                break
    
    # Extract account head
    for col in field_mapping.get('account_head', []):
        if col in row.index and pd.notna(row[col]):
            account_val = str(row[col]).strip()
            if account_val and account_val.lower() not in ['', 'nan', 'none', 'null']:
                extracted['account_head'] = account_val
                break
    
    # Extract entry date
    for col in field_mapping.get('invoice_entry_date', []):
        if col in row.index and pd.notna(row[col]):
            try:
                date_val = pd.to_datetime(row[col])
                extracted['invoice_entry_date'] = date_val.strftime('%Y-%m-%d')
                break
            except:
                continue
    
    # Extract upload date (if different from entry date)
    for col in field_mapping.get('invoice_upload_date', []):
        if col in row.index and pd.notna(row[col]):
            try:
                date_val = pd.to_datetime(row[col])
                extracted['invoice_upload_date'] = date_val.strftime('%Y-%m-%d')
                break
            except:
                continue
    
    return extracted

def find_creator_column(df):
    """Enhanced creator column detection with RMS-specific patterns"""
    print("🔍 Enhanced creator column detection...")
    
    # Enhanced patterns for RMS system
    possible_creator_columns = [
        'CreatedBy', 'Created_By', 'InvoiceCreatedBy', 'Invoice_Created_By',
        'UserName', 'User_Name', 'CreatorName', 'Creator_Name',
        'EntryBy', 'Entry_By', 'InputBy', 'Input_By',
        'PreparedBy', 'Prepared_By', 'MadeBy', 'Made_By',
        'OperatorName', 'Operator_Name', 'StaffName', 'Staff_Name',
        'RecordedBy', 'Recorded_By', 'ProcessedBy', 'Processed_By'
    ]
    
    # Check exact matches first
    for col in possible_creator_columns:
        if col in df.columns:
            # Verify the column has meaningful data
            non_null_count = df[col].notna().sum()
            if non_null_count > 0:
                print(f"✅ Found creator column: {col} ({non_null_count} non-null values)")
                return col
    
    # Check case-insensitive matches
    df_columns_lower = {col.lower(): col for col in df.columns}
    for col in possible_creator_columns:
        if col.lower() in df_columns_lower:
            found_col = df_columns_lower[col.lower()]
            non_null_count = df[found_col].notna().sum()
            if non_null_count > 0:
                print(f"✅ Found creator column (case-insensitive): {found_col} ({non_null_count} non-null values)")
                return found_col
    
    # Check partial matches with meaningful keywords
    creator_keywords = ['create', 'by', 'user', 'entry', 'made', 'prepared', 'operator', 'staff']
    for df_col in df.columns:
        col_lower = df_col.lower()
        if any(keyword in col_lower for keyword in creator_keywords):
            non_null_count = df[df_col].notna().sum()
            if non_null_count > 0:
                print(f"⚠️ Potential creator column found: {df_col} ({non_null_count} non-null values)")
                return df_col
    
    print("⚠️ No creator column found, will use 'Unknown'")
    return None

def validate_invoices_with_details(df):
    """Enhanced validation with complete RMS field mapping"""
    print("🔍 Running enhanced invoice-level validation with RMS field mapping...")
    
    try:
        # Run the existing validation to get summary issues
        summary_issues, problematic_invoices_df = validate_invoices(df)
        
        # Enhanced RMS field mapping
        rms_field_mapping = enhance_rms_field_mapping(df)
        
        # Find the creator column using enhanced detection
        creator_column = find_creator_column(df)
        
        # Now run detailed validation for each invoice
        detailed_results = []
        
        print(f"📋 Analyzing {len(df)} invoices for detailed validation with RMS fields...")
        
        for index, row in df.iterrows():
            invoice_id = row.get('InvID', f'Row_{index}')
            invoice_number = row.get('PurchaseInvNo', row.get('InvoiceNumber', 'N/A'))
            invoice_date = row.get('PurchaseInvDate', 'N/A')
            vendor = row.get('PartyName', row.get('VendorName', 'N/A'))
            amount = row.get('Total', row.get('Amount', 0))
            
            # Extract enhanced RMS fields
            rms_fields = extract_rms_enhanced_fields(row, rms_field_mapping)
            
            validation_issues = []
            severity = "✅ PASS"  # Default to pass
            
            # Enhanced validation rules
            
            # 1. Missing GSTNO
            if pd.isna(row.get('GSTNO')) or str(row.get('GSTNO')).strip() == '':
                validation_issues.append("Missing GST Number")
                severity = "❌ FAIL"
            
            # 2. Missing Total/Amount
            if pd.isna(row.get('Total')) or str(row.get('Total')).strip() == '':
                validation_issues.append("Missing Total Amount")
                severity = "❌ FAIL"
            elif row.get('Total', 0) == 0:
                validation_issues.append("Zero Amount")
                if severity == "✅ PASS":
                    severity = "⚠️ WARNING"
            
            # 3. Negative amounts
            try:
                amount_value = float(row.get('Total', 0))
                if amount_value < 0:
                    validation_issues.append(f"Negative Amount: {amount_value}")
                    if severity == "✅ PASS":
                        severity = "⚠️ WARNING"
            except (ValueError, TypeError):
                validation_issues.append("Invalid Amount Format")
                severity = "❌ FAIL"
            
            # 4. Missing Invoice Number
            if pd.isna(invoice_number) or str(invoice_number).strip() == '':
                validation_issues.append("Missing Invoice Number")
                severity = "❌ FAIL"
            
            # 5. Missing Invoice Date
            if pd.isna(invoice_date) or str(invoice_date).strip() == '':
                validation_issues.append("Missing Invoice Date")
                severity = "❌ FAIL"
            
            # 6. Missing Vendor Name
            if pd.isna(vendor) or str(vendor).strip() == '':
                validation_issues.append("Missing Vendor Name")
                severity = "❌ FAIL"
            
            # 7. Enhanced Creator Name validation
            if rms_fields['invoice_created_by'] == 'Unknown':
                validation_issues.append("Missing Invoice Creator Name")
                if severity == "✅ PASS":
                    severity = "⚠️ WARNING"
            
            # 8. Method of Payment validation
            if rms_fields['method_of_payment'] == 'Not Specified':
                validation_issues.append("Method of Payment not specified")
                if severity == "✅ PASS":
                    severity = "⚠️ WARNING"
            
            # 9. Account Head validation
            if rms_fields['account_head'] == 'Not Assigned':
                validation_issues.append("Account Head not assigned")
                if severity == "✅ PASS":
                    severity = "⚠️ WARNING"
            
            # 10. Check for duplicate invoice numbers
            if not pd.isna(invoice_number) and str(invoice_number).strip() != '':
                duplicate_count = df[df['PurchaseInvNo'] == invoice_number].shape[0]
                if duplicate_count > 1:
                    validation_issues.append(f"Duplicate Invoice Number (appears {duplicate_count} times)")
                    if severity == "✅ PASS":
                        severity = "⚠️ WARNING"
            
            # 11. Date format validation
            try:
                if not pd.isna(invoice_date):
                    pd.to_datetime(invoice_date)
            except:
                validation_issues.append("Invalid Date Format")
                severity = "❌ FAIL"
            
            # 12. Future date validation
            try:
                if not pd.isna(invoice_date):
                    inv_date = pd.to_datetime(invoice_date)
                    if inv_date > datetime.now():
                        validation_issues.append("Future Date")
                        if severity == "✅ PASS":
                            severity = "⚠️ WARNING"
            except:
                pass
            
            # Compile enhanced results for this invoice
            detailed_results.append({
                'Invoice_ID': invoice_id,
                'Invoice_Number': invoice_number,
                'Invoice_Date': invoice_date,
                'Invoice_Entry_Date': rms_fields['invoice_entry_date'],
                'Invoice_Upload_Date': rms_fields['invoice_upload_date'],
                'Vendor_Name': vendor,
                'Amount': amount,
                'Invoice_Created_By': rms_fields['invoice_created_by'],
                'Method_of_Payment': rms_fields['method_of_payment'],
                'Account_Head': rms_fields['account_head'],
                'Validation_Status': severity,
                'Issues_Found': len(validation_issues),
                'Issue_Details': " | ".join(validation_issues) if validation_issues else "No issues found",
                'GST_Number': row.get('GSTNO', ''),
                'Row_Index': index,
                'Validation_Date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        
        # Convert to DataFrame
        detailed_df = pd.DataFrame(detailed_results)
        
        # Enhanced summary statistics
        total_invoices = len(detailed_df)
        passed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '✅ PASS'])
        warning_invoices = len(detailed_df[detailed_df['Validation_Status'] == '⚠️ WARNING'])
        failed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '❌ FAIL'])
        
        print(f"✅ Enhanced detailed validation completed:")
        print(f"   📊 Total invoices: {total_invoices}")
        print(f"   ✅ Passed: {passed_invoices}")
        print(f"   ⚠️ Warnings: {warning_invoices}")
        print(f"   ❌ Failed: {failed_invoices}")
        
        # Show enhanced field statistics
        creator_stats = detailed_df['Invoice_Created_By'].value_counts()
        mop_stats = detailed_df['Method_of_Payment'].value_counts()
        account_stats = detailed_df['Account_Head'].value_counts()
        
        print(f"   👤 Creator statistics: {len(creator_stats)} unique creators")
        print(f"   💳 MOP statistics: {len(mop_stats)} unique payment methods")
        print(f"   📊 Account Head statistics: {len(account_stats)} unique account heads")
        
        if 'Unknown' in creator_stats:
            print(f"   ⚠️ Unknown creators: {creator_stats['Unknown']} invoices")
        if 'Not Specified' in mop_stats:
            print(f"   ⚠️ Unspecified MOP: {mop_stats['Not Specified']} invoices")
        if 'Not Assigned' in account_stats:
            print(f"   ⚠️ Unassigned Account Head: {account_stats['Not Assigned']} invoices")
        
        return detailed_df, summary_issues, problematic_invoices_df
        
    except Exception as e:
        print(f"❌ Enhanced detailed validation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), [], pd.DataFrame()

def generate_email_summary_statistics(detailed_df, cumulative_start, cumulative_end, current_batch_start, current_batch_end, today_str):
    """Generate enhanced email summary with RMS field analysis"""
    print("📧 Generating enhanced email summary statistics...")
    
    try:
        if detailed_df.empty:
            return {
                'html_summary': """
No invoice data available for validation.
""",
                'text_summary': "No invoice data available for validation.",
                'statistics': {}
            }
        
        # Calculate enhanced statistics
        total_invoices = len(detailed_df)
        passed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '✅ PASS'])
        warning_invoices = len(detailed_df[detailed_df['Validation_Status'] == '⚠️ WARNING'])
        failed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '❌ FAIL'])
        
        pass_rate = (passed_invoices / total_invoices * 100) if total_invoices > 0 else 0
        
        # Enhanced field analysis
        creator_stats = detailed_df['Invoice_Created_By'].value_counts()
        mop_stats = detailed_df['Method_of_Payment'].value_counts()
        account_stats = detailed_df['Account_Head'].value_counts()
        
        unknown_creators = creator_stats.get('Unknown', 0)
        unspecified_mop = mop_stats.get('Not Specified', 0)
        unassigned_accounts = account_stats.get('Not Assigned', 0)
        
        # Count issue types for detailed breakdown
        all_issues = []
        for issues_text in detailed_df['Issue_Details']:
            if issues_text != "No issues found":
                issues = issues_text.split(' | ')
                all_issues.extend(issues)
        
        issue_counts = {}
        for issue in all_issues:
            issue_counts[issue] = issue_counts.get(issue, 0) + 1
        
        # Top 5 most common issues
        top_issues = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        
        # Enhanced HTML formatted summary for email
        html_summary = f"""

            

                📊 Enhanced Invoice Validation Summary - {today_str}
            

            
            

                
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

                

                        
👤 Creators: {len(creator_stats)} unique

                        
⚠️ Unknown: {unknown_creators} ({(unknown_creators/total_invoices*100):.1f}%)

                    

                        
💳 Payment Methods: {len(mop_stats)} unique

                        
⚠️ Unspecified: {unspecified_mop} ({(unspecified_mop/total_invoices*100):.1f}%)

                    

                        
📊 Account Heads: {len(account_stats)} unique

                        
⚠️ Unassigned: {unassigned_accounts} ({(unassigned_accounts/total_invoices*100):.1f}%)

                    

            

            
            

                
🔍 Top Validation Issues

                
"""
        
        for issue, count in top_issues:
            percentage = (count / total_invoices * 100) if total_invoices > 0 else 0
            severity_color = "#e74c3c" if "Missing" in issue else ("#f39c12" if any(word in issue for word in ["Duplicate", "Negative", "Future", "Old", "not specified", "not assigned"]) else "#3498db")
            html_summary += f"""
{issue}: {count:,} invoices ({percentage:.1f}%)
"""
        
        html_summary += """

            

        
"""
        
        # Enhanced plain text summary
        text_summary = f"""📊 ENHANCED INVOICE VALIDATION SUMMARY - {today_str}

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
👤 Creators: {len(creator_stats)} unique ({unknown_creators} unknown - {(unknown_creators/total_invoices*100):.1f}%)
💳 Payment Methods: {len(mop_stats)} unique ({unspecified_mop} unspecified - {(unspecified_mop/total_invoices*100):.1f}%)
📊 Account Heads: {len(account_stats)} unique ({unassigned_accounts} unassigned - {(unassigned_accounts/total_invoices*100):.1f}%)

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
            'total_creators': len(creator_stats),
            'unknown_creators': unknown_creators,
            'total_mop': len(mop_stats),
            'unspecified_mop': unspecified_mop,
            'total_account_heads': len(account_stats),
            'unassigned_accounts': unassigned_accounts,
            'top_issues': top_issues,
            'validation_date': today_str,
            'current_batch_start': current_batch_start,
            'current_batch_end': current_batch_end,
            'cumulative_start': cumulative_start,
            'cumulative_end': cumulative_end
        }
        
        print(f"✅ Enhanced email summary statistics generated:")
        print(f"   📊 Pass Rate: {pass_rate:.1f}%")
        print(f"   👤 Creator Stats: {len(creator_stats)} total, {unknown_creators} unknown")
        print(f"   💳 MOP Stats: {len(mop_stats)} total, {unspecified_mop} unspecified")
        print(f"   📊 Account Head Stats: {len(account_stats)} total, {unassigned_accounts} unassigned")
        
        return {
            'html_summary': html_summary,
            'text_summary': text_summary,
            'statistics': statistics
        }
        
    except Exception as e:
        print(f"❌ Enhanced email summary generation failed: {str(e)}")
        return {
            'html_summary': f"""
Error generating enhanced summary: {str(e)}
""",
            'text_summary': f"Error generating enhanced summary: {str(e)}",
            'statistics': {}
        }

def generate_detailed_validation_report(detailed_df, today_str):
    """Generate enhanced detailed validation report for Excel export"""
    print("📋 Generating enhanced detailed validation report for Excel export...")
    
    try:
        if detailed_df.empty:
            return []
        
        # Enhanced summary data with RMS fields
        summary_data = []
        
        # Overall statistics
        total_invoices = len(detailed_df)
        passed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '✅ PASS'])
        warning_invoices = len(detailed_df[detailed_df['Validation_Status'] == '⚠️ WARNING'])
        failed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '❌ FAIL'])
        
        # Enhanced field statistics
        creator_stats = detailed_df['Invoice_Created_By'].value_counts()
        mop_stats = detailed_df['Method_of_Payment'].value_counts()
        account_stats = detailed_df['Account_Head'].value_counts()
        
        # Add enhanced summary entries
        enhanced_summary_entries = [
            {'Report_Type': 'Overall_Summary', 'Description': 'Total Invoice Count', 'Count': total_invoices, 'Percentage': '100.0%', 'Status': 'INFO'},
            {'Report_Type': 'Overall_Summary', 'Description': 'Passed Validation', 'Count': passed_invoices, 'Percentage': f'{(passed_invoices/total_invoices*100):.1f}%' if total_invoices > 0 else '0%', 'Status': 'PASS'},
            {'Report_Type': 'Overall_Summary', 'Description': 'Warnings', 'Count': warning_invoices, 'Percentage': f'{(warning_invoices/total_invoices*100):.1f}%' if total_invoices > 0 else '0%', 'Status': 'WARNING'},
            {'Report_Type': 'Overall_Summary', 'Description': 'Failed Validation', 'Count': failed_invoices, 'Percentage': f'{(failed_invoices/total_invoices*100):.1f}%' if total_invoices > 0 else '0%', 'Status': 'FAIL'},
            {'Report_Type': 'RMS_Field_Analysis', 'Description': 'Total Unique Creators', 'Count': len(creator_stats), 'Percentage': '100.0%', 'Status': 'INFO'},
            {'Report_Type': 'RMS_Field_Analysis', 'Description': 'Unknown/Missing Creators', 'Count': creator_stats.get('Unknown', 0), 'Percentage': f'{(creator_stats.get("Unknown", 0)/total_invoices*100):.1f}%' if total_invoices > 0 else '0%', 'Status': 'WARNING' if creator_stats.get('Unknown', 0) > 0 else 'PASS'},
            {'Report_Type': 'RMS_Field_Analysis', 'Description': 'Total Payment Methods', 'Count': len(mop_stats), 'Percentage': '100.0%', 'Status': 'INFO'},
            {'Report_Type': 'RMS_Field_Analysis', 'Description': 'Unspecified Payment Methods', 'Count': mop_stats.get('Not Specified', 0), 'Percentage': f'{(mop_stats.get("Not Specified", 0)/total_invoices*100):.1f}%' if total_invoices > 0 else '0%', 'Status': 'WARNING' if mop_stats.get('Not Specified', 0) > 0 else 'PASS'},
            {'Report_Type': 'RMS_Field_Analysis', 'Description': 'Total Account Heads', 'Count': len(account_stats), 'Percentage': '100.0%', 'Status': 'INFO'},
            {'Report_Type': 'RMS_Field_Analysis', 'Description': 'Unassigned Account Heads', 'Count': account_stats.get('Not Assigned', 0), 'Percentage': f'{(account_stats.get("Not Assigned", 0)/total_invoices*100):.1f}%' if total_invoices > 0 else '0%', 'Status': 'WARNING' if account_stats.get('Not Assigned', 0) > 0 else 'PASS'}
        ]
        
        summary_data.extend(enhanced_summary_entries)
        
        print(f"✅ Enhanced detailed validation report prepared with {len(summary_data)} summary entries")
        return summary_data
        
    except Exception as e:
        print(f"❌ Enhanced detailed report generation failed: {str(e)}")
        return []

def read_invoice_file(invoice_file):
    """
    Robust file reading with multiple format support and proper error handling
    Enhanced to handle HTML files masquerading as Excel, CSV formats, and corrupted files
    """
    print(f"🔍 Attempting to read file: {invoice_file}")

    # Check if file exists
    if not os.path.exists(invoice_file):
        raise FileNotFoundError(f"Invoice file not found: {invoice_file}")

    # Get file info
    file_path = Path(invoice_file)
    file_ext = file_path.suffix.lower()
    file_size = os.path.getsize(invoice_file)
    print(f"📄 File: {file_path.name}, Extension: {file_ext}, Size: {file_size} bytes")
    
    # Check if file is too small (likely corrupted or empty)
    if file_size < 50:
        raise ValueError(f"File appears to be too small ({file_size} bytes) - likely corrupted or empty")
            
    # Read file header to detect actual format
    try:
        with open(invoice_file, 'rb') as f:
            header = f.read(200)  # Read more bytes for better detection
        print(f"🔍 File header (first 50 bytes): {header[:50]}")
        
        # Detect file format by content
        header_str = header.decode('utf-8', errors='ignore').lower()
        
    except Exception as e:
        print(f"⚠️ Could not read file header: {e}")
        header = b''
        header_str = ''
                
    df = None
    last_error = None
    
    # Method 1: Check if it's actually HTML disguised as Excel
    if ('<html' in header_str or '<!doctype' in header_str or 
        '<table' in header_str or header_str.startswith('vouchern')):
        try:
            print("🌐 File appears to be HTML format, attempting HTML parsing...")
            
            # Try reading as HTML table
            tables = pd.read_html(invoice_file, encoding='utf-8')
            if tables and len(tables) > 0:
                # Find the largest table (likely the main data)
                largest_table = max(tables, key=lambda x: x.shape[0] * x.shape[1])
                df = largest_table
                print(f"✅ Successfully parsed HTML file. Shape: {df.shape}")
                print(f"📋 Columns: {list(df.columns)}")
                return df
            else:
                print("⚠️ No tables found in HTML")
        except Exception as e:
            print(f"⚠️ HTML parsing failed: {str(e)}")
            last_error = e

    # Method 2: Check if it's CSV format
    if (',' in header_str or ';' in header_str or '\t' in header_str):
        try:
            print("📄 File appears to be CSV format, attempting CSV parsing...")
            
            # Try common separators
            separators = [',', ';', '\t', '|']
            for sep in separators:
                try:
                    # Test with first few rows
                    df_test = pd.read_csv(invoice_file, sep=sep, nrows=5, encoding='utf-8')
                    if df_test.shape[1] > 1:  # Multiple columns detected
                        df = pd.read_csv(invoice_file, sep=sep, encoding='utf-8')
                        print(f"✅ Successfully read as CSV with separator '{sep}'. Shape: {df.shape}")
                        print(f"📋 Columns: {list(df.columns)}")
                        return df
                except:
                    continue
            print("⚠️ CSV reading failed with all separators")
        except Exception as e:
            print(f"⚠️ CSV reading failed: {str(e)}")
            last_error = e

    # Method 3: Try Excel with openpyxl engine (most reliable for .xlsx)
    try:
        print("📊 Attempting to read as Excel with openpyxl engine...")
        df = pd.read_excel(invoice_file, engine='openpyxl')
        print(f"✅ Successfully read Excel file with openpyxl. Shape: {df.shape}")
        print(f"📋 Columns: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"⚠️ openpyxl engine failed: {str(e)}")
        last_error = e
    
    # Method 4: Try Excel with xlrd engine (for older .xls files)
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
        
    # Method 5: Try reading as plain text and attempt manual parsing
    try:
        print("📝 Attempting manual text parsing...")
        with open(invoice_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read(2000)  # Read first 2000 characters
        
        print(f"📄 File content sample:\n{repr(content[:500])}")
                
        # Check if it contains tabular data patterns
        lines = content.split('\n')
        data_lines = [line.strip() for line in lines if line.strip()]
        
        if len(data_lines) > 1:
            # Try to detect delimiter
            first_line = data_lines[0]
            potential_delimiters = ['\t', ',', ';', '|']
            
            for delimiter in potential_delimiters:
                if first_line.count(delimiter) > 2:  # At least 3 columns
                    try:
                        # Create a temporary CSV-like content
                        csv_content = '\n'.join(data_lines)
                        from io import StringIO
                        df = pd.read_csv(StringIO(csv_content), sep=delimiter)
                        
                        if not df.empty and df.shape[1] > 1:
                            print(f"✅ Successfully parsed as delimited text with '{delimiter}'. Shape: {df.shape}")
                            print(f"📋 Columns: {list(df.columns)}")
                            return df
                    except:
                        continue
        
    except Exception as e:
        print(f"⚠️ Manual parsing failed: {e}")
        
    # Method 6: Last resort - try to extract any structured data
    try:
        print("🔄 Last resort: attempting to create DataFrame from any recognizable patterns...")
        
        # Read raw content and look for patterns
        with open(invoice_file, 'r', encoding='utf-8', errors='ignore') as f:
            full_content = f.read()
        
        # Look for invoice-related patterns
        if 'invoice' in full_content.lower():
            print("🔍 Found invoice-related content, creating minimal DataFrame...")
            
            # Create a minimal DataFrame with error information
            df = pd.DataFrame({
                'InvID': ['FILE_READ_ERROR'],
                'PurchaseInvNo': ['UNABLE_TO_READ'],
                'PurchaseInvDate': [pd.Timestamp.now()],
                'PartyName': ['FILE_FORMAT_ERROR'],
                'Total': [0],
                'GSTNO': ['FILE_READ_FAILED'],
                'ErrorInfo': [f'File format not supported: {file_ext}, Header: {header_str[:50]}']
            })
            
            print(f"⚠️ Created error DataFrame for debugging. Shape: {df.shape}")
            return df
            
    except Exception as e:
        print(f"⚠️ Last resort failed: {e}")
        
    # If all methods failed, raise the most relevant error
    error_details = [
        f"File extension: {file_ext}",
        f"File size: {file_size} bytes",
        f"Header content: {header_str[:100]}"
    ]
    
    if last_error:
        error_msg = f"Could not read invoice file in any supported format. Details: {'; '.join(error_details)}. Last error: {str(last_error)}"
        raise Exception(error_msg)
    else:
        error_msg = f"Could not read invoice file - unknown format or corrupted file. Details: {'; '.join(error_details)}"
        raise Exception(error_msg)
        
def handle_file_reading_emergency(download_dir, today_str):
    """Handle cases where file reading completely fails"""
    print("🚨 Emergency mode: Creating fallback report...")
    
    # Create emergency report
    emergency_data = {
        'Invoice_ID': ['EMERGENCY_ENTRY'],
        'Invoice_Number': ['FILE_READ_FAILED'],
        'Invoice_Date': [today_str],
        'Vendor_Name': ['RMS_SYSTEM_ERROR'],
        'Amount': [0],
        'Invoice_Creator_Name': ['SYSTEM_ERROR'],
        'Validation_Status': ['❌ FILE_ERROR'],
        'Issues_Found': [1],
        'Issue_Details': ['Unable to read RMS download file - format not supported'],
        'GST_Number': ['N/A']
    }
    
    emergency_df = pd.DataFrame(emergency_data)
    
    # Save emergency report
    emergency_path = f"data/emergency_report_{today_str}.xlsx"
    emergency_df.to_excel(emergency_path, index=False)
    
    print(f"🚨 Emergency report saved: {emergency_path}")
    return emergency_df

def validate_downloaded_files_enhanced(download_dir):
    """Enhanced file validation with format detection"""
    required_files = ["invoice_download.xls", "invoices.zip"]
    validation_results = {}
    
    for fname in required_files:
        file_path = os.path.join(download_dir, fname)
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            
            # Enhanced format detection
            try:
                with open(file_path, 'rb') as f:
                    header = f.read(100)
                
                # Detect actual format
                header_str = header.decode('utf-8', errors='ignore').lower()
                
                if 'html' in header_str or header_str.startswith('vouchern'):
                    validation_results[fname] = "html_format"
                    print(f"⚠️ {fname} appears to be HTML format")
                elif file_size < 50:
                    validation_results[fname] = "too_small"
                else:
                    validation_results[fname] = "ok"
                    
            except Exception as e:
                validation_results[fname] = "read_error"
                print(f"⚠️ Could not analyze {fname}: {e}")
        else:
            validation_results[fname] = "missing"
    
    return validation_results
                    
def filter_invoices_by_date(df, start_str, end_str):
    """Filter dataframe by date range"""
    try:
        if 'PurchaseInvDate' not in df.columns:
            print("⚠️ PurchaseInvDate column not found, returning all data")
            return df
    
        # Convert dates
        start_date = datetime.strptime(start_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_str, "%Y-%m-%d")
        
        # Parse invoice dates
        df["ParsedInvoiceDate"] = pd.to_datetime(df["PurchaseInvDate"], errors='coerce')
    
        # Filter by date range
        filtered_df = df[
            (df["ParsedInvoiceDate"] >= start_date) &
            (df["ParsedInvoiceDate"] <= end_date)  
        ]
                
        print(f"📅 Filtered invoices from {start_str} to {end_str}: {len(filtered_df)} out of {len(df)}")
        return filtered_df
            
    except Exception as e:
        print(f"⚠️ Date filtering failed: {str(e)}, returning all data")
        return df

def run_invoice_validation():
    """Enhanced main function with comprehensive RMS field mapping and report formatting"""
    try:
        today = datetime.today()
        today_str = today.strftime("%Y-%m-%d")
        
        print(f"🚀 Starting ENHANCED cumulative validation workflow for {today_str}")
        print(f"📧 FEATURE: Enhanced email-ready summary with RMS field analysis")
        print(f"📋 FEATURE: Complete RMS field mapping (Creator, MOP, Account Head)")
        print(f"⚙️ Configuration:")
        print(f"   📅 Validation interval: {VALIDATION_INTERVAL_DAYS} days")
        print(f"   📦 Batch size: {VALIDATION_BATCH_DAYS} days")
        print(f"   🗓️ Active window: {ACTIVE_VALIDATION_MONTHS} months")
        print(f"   📁 Archive folder: {ARCHIVE_FOLDER}")
        
        # Step 1: Check if we should run today (4-day interval)
        print("🔍 Step 1: Checking if validation should run today...")
        if not should_run_today():
            print("⏳ Skipping validation - not yet time for next 4-day interval")
            return True
        
        # Step 2: Archive data older than 3 months
        print("🗂️ Step 2: Archiving data older than 3 months...")
        try:
            archived_count = archive_data_older_than_three_months()
            if archived_count > 0:
                print(f"✅ Archived {archived_count} old items")
            else:
                print("✅ No old data to archive")
        except Exception as e:
            print(f"⚠️ Archiving failed but continuing: {str(e)}")
        
        # Step 3: Calculate cumulative validation range
        print("📊 Step 3: Calculating cumulative validation range...")
        try:
            cumulative_start, cumulative_end = get_cumulative_validation_range()
            current_batch_start, current_batch_end = get_current_batch_dates()
            
            print(f"📅 Current batch: {current_batch_start} to {current_batch_end}")
            print(f"📅 Cumulative range: {cumulative_start} to {cumulative_end}")
        except Exception as e:
            print(f"❌ Failed to calculate date ranges: {str(e)}")
            return False
        
        # Step 4: Download cumulative data
        print("📥 Step 4: Downloading cumulative validation data...")
        try:
            invoice_path = download_cumulative_data(cumulative_start, cumulative_end)
        except Exception as e:
            print(f"❌ Cumulative data download failed: {str(e)}")
            return False
        
        # Step 5: Verify downloaded files
        download_dir = os.path.join("data", today_str)
        print(f"🔍 Step 5: Verifying files in directory: {download_dir}")
         
        validation_results = validate_downloaded_files(download_dir)
        
        # Step 6: Check for required files
        invoice_file = os.path.join(download_dir, "invoice_download.xls")
    
        if validation_results.get("invoice_download.xls") == "missing":
            print("❌ No invoice file downloaded. Aborting.")
            return False
    
        print("📊 Step 7: Reading cumulative invoice data with enhanced methods...")
        try:
            df = read_invoice_file(invoice_file)
    
            if df is None or df.empty:
                print("❌ DataFrame is empty after reading file")
                # Try emergency mode
                df = handle_file_reading_emergency(download_dir, today_str)
        
            print(f"✅ Successfully loaded data. Shape: {df.shape}")
            print(f"📋 Columns: {list(df.columns)}")
    
            # Check if this is an error DataFrame
            if 'ErrorInfo' in df.columns:
                print("⚠️ File reading encountered issues, but continuing with available data")
        
        except Exception as e:
            print(f"❌ Failed to read invoice file: {str(e)}")
            # Create emergency DataFrame and continue
            df = handle_file_reading_emergency(download_dir, today_str)
            print("🚨 Continuing with emergency mode data")
                    
        # Step 8: Filter to cumulative validation range
        print("🔄 Step 8: Filtering to cumulative validation range...")
        try:
            filtered_df = filter_invoices_by_date(df, cumulative_start, cumulative_end)
            print(f"📅 Working with {len(filtered_df)} invoices in cumulative range")
        except Exception as e:
            print(f"⚠️ Date filtering failed: {str(e)}, using all data")
            filtered_df = df
        
        # Step 9: Run enhanced validation with complete RMS field mapping
        print("🔄 Step 9: Running enhanced validation with complete RMS field mapping...")
        try:
            detailed_df, summary_issues, problematic_invoices_df = validate_invoices_with_details(filtered_df)
            
            if detailed_df.empty:
                print("⚠️ No detailed validation results generated")
            else:
                print(f"✅ Enhanced validation completed on {len(detailed_df)} invoices")
        except Exception as e:
            print(f"❌ Enhanced validation failed: {str(e)}")
            return False
        
        # Step 10: Generate enhanced email summary
        print("📧 Step 10: Generating enhanced email summary with RMS field analysis...")
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
                'html_summary': f"""
Error generating enhanced summary: {str(e)}
""",
                'text_summary': f"Error generating enhanced summary: {str(e)}",
                'statistics': {}
            }
        
        # Step 11: Generate enhanced detailed validation report
        print("📋 Step 11: Generating enhanced detailed validation report...")
        try:
            detailed_report = generate_detailed_validation_report(detailed_df, today_str)
        except Exception as e:
            print(f"⚠️ Enhanced detailed report generation failed: {str(e)}")
            detailed_report = []
        
        # Step 12: Prepare enhanced invoice data for saving
        print("💾 Step 12: Preparing enhanced invoice data for saving...")
        try:
            if not detailed_df.empty:
                current_invoices_list = detailed_df.to_dict('records')
            else:
                current_invoices_list = []
            
            print(f"📋 Prepared {len(current_invoices_list)} enhanced invoice records for saving")
        except Exception as e:
            print(f"⚠️ Failed to prepare enhanced invoice list: {str(e)}")
            current_invoices_list = []
        
        # Step 13: Save enhanced validation snapshot
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
            print(f"⚠️ Failed to save enhanced snapshot: {str(e)}")
            
        # Step 14: Record this enhanced run
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
            print(f"⚠️ Failed to record enhanced run: {str(e)}")
        
        # Step 15: Save enhanced reports with complete RMS field mapping
        try:
            os.makedirs("data", exist_ok=True)
            
            # Enhanced detailed validation report (invoice-level with all RMS fields)
            detailed_report_path = f"data/invoice_validation_detailed_{today_str}.xlsx"
            
            if not detailed_df.empty:
                with pd.ExcelWriter(detailed_report_path, engine='openpyxl') as writer:
                    # Sheet 1: All invoices with complete RMS field mapping
                    detailed_df.to_excel(writer, sheet_name='All_Invoices_Enhanced', index=False)
                    
                    # Sheet 2: Failed invoices with RMS fields
                    failed_df = detailed_df[detailed_df['Validation_Status'] == '❌ FAIL']
                    if not failed_df.empty:
                        failed_df.to_excel(writer, sheet_name='Failed_Invoices', index=False)
                    
                    # Sheet 3: Warning invoices with RMS fields
                    warning_df = detailed_df[detailed_df['Validation_Status'] == '⚠️ WARNING'] 
                    if not warning_df.empty:
                        warning_df.to_excel(writer, sheet_name='Warning_Invoices', index=False)
                    
                    # Sheet 4: Passed invoices with RMS fields
                    passed_df = detailed_df[detailed_df['Validation_Status'] == '✅ PASS']
                    if not passed_df.empty:
                        passed_df.to_excel(writer, sheet_name='Passed_Invoices', index=False)
                    
                    # Sheet 5: Enhanced Creator analysis
                    creator_analysis = detailed_df['Invoice_Created_By'].value_counts().reset_index()
                    creator_analysis.columns = ['Creator_Name', 'Invoice_Count']
                    creator_analysis['Percentage'] = (creator_analysis['Invoice_Count'] / len(detailed_df) * 100).round(2)
                    creator_analysis.to_excel(writer, sheet_name='Creator_Analysis', index=False)
                    
                    # Sheet 6: Method of Payment analysis
                    mop_analysis = detailed_df['Method_of_Payment'].value_counts().reset_index()
                    mop_analysis.columns = ['Payment_Method', 'Invoice_Count']
                    mop_analysis['Percentage'] = (mop_analysis['Invoice_Count'] / len(detailed_df) * 100).round(2)
                    mop_analysis.to_excel(writer, sheet_name='MOP_Analysis', index=False)
                    
                    # Sheet 7: Account Head analysis
                    account_analysis = detailed_df['Account_Head'].value_counts().reset_index()
                    account_analysis.columns = ['Account_Head', 'Invoice_Count']
                    account_analysis['Percentage'] = (account_analysis['Invoice_Count'] / len(detailed_df) * 100).round(2)
                    account_analysis.to_excel(writer, sheet_name='Account_Head_Analysis', index=False)
                    
                    # Sheet 8: Enhanced Summary statistics
                    if detailed_report:
                        summary_df = pd.DataFrame(detailed_report)
                        summary_df.to_excel(writer, sheet_name='Enhanced_Summary', index=False)
                
                print(f"✅ Enhanced invoice-level report saved: {detailed_report_path}")

                # Create enhanced dashboard version with all RMS fields
                os.makedirs(f"data/{today_str}", exist_ok=True)
                dashboard_path = f"data/{today_str}/validation_result.xlsx"
                
                # Enhanced dashboard columns including all RMS fields
                enhanced_dashboard_columns = [
                    'Invoice_ID', 'Invoice_Number', 'Invoice_Date', 'Invoice_Entry_Date', 
                    'Invoice_Upload_Date', 'Vendor_Name', 'Amount', 'Invoice_Created_By', 
                    'Method_of_Payment', 'Account_Head', 'Validation_Status', 
                    'Issues_Found', 'Issue_Details', 'GST_Number'
                ]
                
                # Only include columns that exist in the dataframe
                available_columns = [col for col in enhanced_dashboard_columns if col in detailed_df.columns]
                dashboard_df = detailed_df[available_columns].copy()
                
                # Add enhanced status summary
                dashboard_df['Status_Summary'] = dashboard_df.apply(lambda row: 
                    f"{row['Validation_Status']} - {row['Issues_Found']} issues" if row['Issues_Found'] > 0 
                    else f"{row['Validation_Status']} - No issues", axis=1)
                
                dashboard_df.to_excel(dashboard_path, index=False, engine='openpyxl')
                print(f"📋 Enhanced dashboard report created: {dashboard_path}")
                
                # Enhanced delta report with all RMS fields
                delta_report_path = f"data/delta_report_{today_str}.xlsx"
                dashboard_df.to_excel(delta_report_path, index=False, engine='openpyxl')
                print(f"📋 Enhanced delta report created: {delta_report_path}")
                
                # Save enhanced email summary
                summary_path = f"data/email_summary_{today_str}.html"
                with open(summary_path, 'w', encoding='utf-8') as f:
                    f.write(email_summary['html_summary'])
                print(f"📧 Enhanced email summary saved: {summary_path}")
                
            else:
                print("⚠️ No enhanced validation results - creating empty report")
                # Create empty enhanced report structure
                empty_enhanced_df = pd.DataFrame({
                    'Invoice_ID': [], 'Invoice_Number': [], 'Invoice_Date': [], 'Invoice_Entry_Date': [],
                    'Invoice_Upload_Date': [], 'Vendor_Name': [], 'Amount': [], 'Invoice_Created_By': [], 
                    'Method_of_Payment': [], 'Account_Head': [], 'Validation_Status': [], 
                    'Issues_Found': [], 'Issue_Details': [], 'GST_Number': [], 'Status_Summary': []
                })
                empty_enhanced_df.to_excel(detailed_report_path, index=False, engine='openpyxl')
                print(f"✅ Empty enhanced report created: {detailed_report_path}")
                        
        except Exception as e:
            print(f"❌ Failed to save enhanced reports: {str(e)}")
            return False

        # Step 16: Enhanced processing (if available)
        print("🚀 Step 16: Applying additional enhanced features...")
        try:
            if ENHANCED_PROCESSOR_AVAILABLE:
                # Enhance the existing results
                enhancement_result = enhance_validation_results(detailed_df, email_summary)
                
                if enhancement_result['success']:
                    print("✅ Additional enhancement successful!")
                    print("📊 Enhanced processing completed with additional features")
                else:
                    print(f"⚠️ Additional enhancement failed: {enhancement_result['error']}")
                    print("📊 Continuing with enhanced RMS field validation")
            else:
                print("⚠️ Additional enhanced processor not available")
                print("📊 Using enhanced RMS field validation (already implemented)")
                
        except Exception as e:
            print(f"⚠️ Additional enhancement failed: {str(e)}")
            print("📊 Continuing with enhanced RMS field validation")
                
        # Step 17: Send enhanced email notifications
        try:
            from email_notifier import EmailNotifier
            
            notifier = EmailNotifier()
                
            # Send enhanced validation report to AP TEAM
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
                        # Fallback to basic validation report
                        statistics = email_summary.get('statistics', {})
                        issues_count = statistics.get('failed_invoices', 0) + statistics.get('warning_invoices', 0)
                        notifier.send_validation_report(today_str, ap_team_recipients, issues_count)
                        print(f"📧 Basic validation report sent to AP team: {', '.join(ap_team_recipients)}")
                        
                except Exception as email_error:
                    print(f"⚠️ Enhanced email failed: {str(email_error)}")
                    print(f"📧 Email notification workflow had issues but continuing")
                    
            else:   
                print("⚠️ No AP team email recipients configured in AP_TEAM_EMAIL_LIST")
            
            print("📧 Enhanced email notification workflow completed!")
            
        except Exception as e:
            print(f"⚠️ Email sending failed: {str(e)}")
                    
        print("✅ Enhanced cumulative validation workflow completed successfully!")
        print(f"")
        print(f"📊 ENHANCED FINAL SUMMARY:")
        print(f"   📦 Current batch: {current_batch_start} to {current_batch_end}")
        print(f"   🔄 Cumulative range: {cumulative_start} to {cumulative_end}")
        print(f"   📅 Total days validated: {(datetime.strptime(cumulative_end, '%Y-%m-%d') - datetime.strptime(cumulative_start, '%Y-%m-%d')).days + 1}")
        print(f"   📋 Total invoices processed: {len(detailed_df) if not detailed_df.empty else 0}")
        
        if not detailed_df.empty:
            stats = email_summary.get('statistics', {})
            print(f"   ✅ Passed: {stats.get('passed_invoices', 0)} ({stats.get('pass_rate', 0):.1f}%)")
            print(f"   ⚠️ Warnings: {stats.get('warning_invoices', 0)}")
            print(f"   ❌ Failed: {stats.get('failed_invoices', 0)}")
            print(f"   👤 Total Creators: {stats.get('total_creators', 0)} ({stats.get('unknown_creators', 0)} unknown)")
            print(f"   💳 Total MOP: {stats.get('total_mop', 0)} ({stats.get('unspecified_mop', 0)} unspecified)")
            print(f"   📊 Total Account Heads: {stats.get('total_account_heads', 0)} ({stats.get('unassigned_accounts', 0)} unassigned)")
        
        print(f"   ⏰ Next run in: {VALIDATION_INTERVAL_DAYS} days")
        print(f"   🗂️ Archive threshold: {ACTIVE_VALIDATION_MONTHS} months")
        print(f"   🔧 Enhanced Features: RMS field mapping, creator detection, MOP analysis, account head tracking")
        
        return True
                
    except Exception as e:
        print(f"❌ Unexpected error in enhanced validation workflow: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

# Run the enhanced validation if called directly
if __name__ == "__main__":
    success = run_invoice_validation()
    if not success:
        print("❌ Enhanced cumulative validation failed!")
        exit(1)
    else:   
        print("🎉 Enhanced cumulative validation completed successfully!")
        exit(0)
