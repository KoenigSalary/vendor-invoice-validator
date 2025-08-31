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
        
        # Archive daily data folders
        for item in os.listdir(data_dir):
            item_path = os.path.join(data_dir, item)
            if os.path.isdir(item_path) and item != ARCHIVE_FOLDER:
                try:
                    # Check if folder name is a date
                    folder_date = datetime.strptime(item, "%Y-%m-%d")
                    if folder_date < cutoff_date:
                        dst = os.path.join(daily_data_archive, item)
                        shutil.move(item_path, dst)
                        print(f"📦 Archived daily data folder: {item}")
                        archived_count += 1
                except ValueError:
                    # Skip non-date folders
                    continue
                except Exception as e:
                    print(f"⚠️ Error archiving folder {item}: {str(e)}")
                    continue
        
        # Update database to mark archived data
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

def enhance_rms_field_mapping(df):
    """Enhanced RMS field mapping with comprehensive field detection"""
    print("🔍 Enhancing RMS field mapping...")
    
    field_mapping = {
        'invoice_created_by': [],
        'method_of_payment': [],
        'account_head': [],
        'invoice_entry_date': [],
        'invoice_upload_date': []
    }
    
    # Enhanced field patterns for RMS system
    field_patterns = {
        'invoice_created_by': [
            'CreatedBy', 'Created_By', 'InvoiceCreatedBy', 'Invoice_Created_By',
            'UserName', 'User_Name', 'CreatorName', 'Creator_Name',
            'EntryBy', 'Entry_By', 'InputBy', 'Input_By',
            'PreparedBy', 'Prepared_By', 'MadeBy', 'Made_By',
            'CreatedByUser', 'Created_By_User', 'InvoiceCreator',
            'Invoice_Creator', 'ProcessedBy', 'Processed_By'
        ],
        'method_of_payment': [
            'MOP', 'Method_of_Payment', 'PaymentMethod', 'Payment_Method',
            'PaymentMode', 'Payment_Mode', 'PayType', 'Pay_Type',
            'PaymentType', 'Payment_Type', 'ModeOfPayment', 'Mode_Of_Payment'
        ],
        'account_head': [
            'AccountHead', 'Account_Head', 'AccountCode', 'Account_Code',
            'GLAccount', 'GL_Account', 'ChartOfAccount', 'Chart_Of_Account',
            'AccountName', 'Account_Name', 'LedgerAccount', 'Ledger_Account'
        ],
        'invoice_entry_date': [
            'EntryDate', 'Entry_Date', 'InvoiceEntryDate', 'Invoice_Entry_Date',
            'CreationDate', 'Creation_Date', 'InputDate', 'Input_Date',
            'ProcessedDate', 'Processed_Date', 'RecordedDate', 'Recorded_Date'
        ],
        'invoice_upload_date': [
            'UploadDate', 'Upload_Date', 'InvoiceUploadDate', 'Invoice_Upload_Date',
            'SubmittedDate', 'Submitted_Date', 'ReceivedDate', 'Received_Date'
        ]
    }
    
    # Map available columns to field categories
    for field_type, patterns in field_patterns.items():
        for pattern in patterns:
            # Exact match
            if pattern in df.columns:
                field_mapping[field_type].append(pattern)
                print(f"✅ Found exact match for {field_type}: {pattern}")
                continue
            
            # Case-insensitive match
            for col in df.columns:
                if pattern.lower() == col.lower():
                    field_mapping[field_type].append(col)
                    print(f"✅ Found case-insensitive match for {field_type}: {col}")
                    break
        
        # If no exact matches, try partial matching
        if not field_mapping[field_type]:
            for col in df.columns:
                col_lower = col.lower()
                if field_type == 'invoice_created_by':
                    if any(word in col_lower for word in ['create', 'by', 'user', 'entry', 'made', 'prepared']):
                        field_mapping[field_type].append(col)
                        print(f"⚠️ Potential match for {field_type}: {col}")
                        break
                elif field_type == 'method_of_payment':
                    if any(word in col_lower for word in ['payment', 'method', 'mode', 'type', 'mop']):
                        field_mapping[field_type].append(col)
                        print(f"⚠️ Potential match for {field_type}: {col}")
                        break
                elif field_type == 'account_head':
                    if any(word in col_lower for word in ['account', 'head', 'code', 'gl', 'ledger']):
                        field_mapping[field_type].append(col)
                        print(f"⚠️ Potential match for {field_type}: {col}")
                        break
                elif field_type in ['invoice_entry_date', 'invoice_upload_date']:
                    if any(word in col_lower for word in ['entry', 'upload', 'creation', 'input', 'processed', 'recorded']):
                        field_mapping[field_type].append(col)
                        print(f"⚠️ Potential match for {field_type}: {col}")
                        break
    
    # Remove empty mappings and duplicates
    for field_type in field_mapping:
        field_mapping[field_type] = list(set(field_mapping[field_type]))
        if not field_mapping[field_type]:
            print(f"❌ No mapping found for {field_type}")
    
    return field_mapping

def extract_rms_enhanced_fields(row, field_mapping):
    """Extract enhanced RMS fields from a row using field mapping"""
    enhanced_data = {}
    
    # Extract Invoice Created By
    if field_mapping['invoice_created_by']:
        for col in field_mapping['invoice_created_by']:
            value = str(row.get(col, '')).strip()
            if value and value.lower() not in ['', 'nan', 'none', 'null']:
                enhanced_data['Invoice_Created_By'] = value
                break
    if 'Invoice_Created_By' not in enhanced_data:
        enhanced_data['Invoice_Created_By'] = 'Unknown'
    
    # Extract Method of Payment
    if field_mapping['method_of_payment']:
        for col in field_mapping['method_of_payment']:
            value = str(row.get(col, '')).strip()
            if value and value.lower() not in ['', 'nan', 'none', 'null']:
                enhanced_data['Method_of_Payment'] = value
                break
    if 'Method_of_Payment' not in enhanced_data:
        enhanced_data['Method_of_Payment'] = 'Not Specified'
    
    # Extract Account Head
    if field_mapping['account_head']:
        for col in field_mapping['account_head']:
            value = str(row.get(col, '')).strip()
            if value and value.lower() not in ['', 'nan', 'none', 'null']:
                enhanced_data['Account_Head'] = value
                break
    if 'Account_Head' not in enhanced_data:
        enhanced_data['Account_Head'] = 'Not Specified'
    
    # Extract Invoice Entry Date
    if field_mapping['invoice_entry_date']:
        for col in field_mapping['invoice_entry_date']:
            value = row.get(col)
            if pd.notna(value) and str(value).strip() != '':
                enhanced_data['Invoice_Entry_Date'] = value
                break
    if 'Invoice_Entry_Date' not in enhanced_data:
        enhanced_data['Invoice_Entry_Date'] = 'Not Available'
    
    # Extract Invoice Upload Date
    if field_mapping['invoice_upload_date']:
        for col in field_mapping['invoice_upload_date']:
            value = row.get(col)
            if pd.notna(value) and str(value).strip() != '':
                enhanced_data['Invoice_Upload_Date'] = value
                break
    if 'Invoice_Upload_Date' not in enhanced_data:
        enhanced_data['Invoice_Upload_Date'] = 'Not Available'
    
    return enhanced_data

def find_creator_column(df):
    """Find the invoice creator column name from available columns"""
    possible_creator_columns = [
        'CreatedBy', 'Created_By', 'InvoiceCreatedBy', 'Invoice_Created_By',
        'UserName', 'User_Name', 'CreatorName', 'Creator_Name',
        'EntryBy', 'Entry_By', 'InputBy', 'Input_By',
        'PreparedBy', 'Prepared_By', 'MadeBy', 'Made_By',
        'CreatedByUser', 'Created_By_User', 'InvoiceCreator',
        'Invoice_Creator', 'ProcessedBy', 'Processed_By'
    ]
    
    # Check exact matches first
    for col in possible_creator_columns:
        if col in df.columns:
            print(f"✅ Found creator column: {col}")
            return col
    
    # Check case-insensitive matches
    df_columns_lower = {col.lower(): col for col in df.columns}
    for col in possible_creator_columns:
        if col.lower() in df_columns_lower:
            found_col = df_columns_lower[col.lower()]
            print(f"✅ Found creator column (case-insensitive): {found_col}")
            return found_col
    
    # Check partial matches
    for df_col in df.columns:
        if any(word in df_col.lower() for word in ['create', 'by', 'user', 'entry', 'made', 'prepared']):
            print(f"⚠️ Potential creator column found: {df_col}")
            return df_col
    
    print("⚠️ No creator column found, will use 'Unknown'")
    return None

def validate_invoices_with_details(df):
    """Run detailed validation that returns per-invoice validation results with enhanced RMS fields"""
    print("🔍 Running detailed invoice-level validation with enhanced RMS field mapping...")
    
    try:
        # Run the existing validation to get summary issues
        summary_issues, problematic_invoices_df = validate_invoices(df)
        
        # Enhanced RMS field mapping
        field_mapping = enhance_rms_field_mapping(df)
        
        # Now run detailed validation for each invoice
        detailed_results = []
        
        print(f"📋 Analyzing {len(df)} invoices for detailed validation...")
        
        for index, row in df.iterrows():
            invoice_id = row.get('InvID', f'Row_{index}')
            invoice_number = row.get('PurchaseInvNo', row.get('InvoiceNumber', 'N/A'))
            invoice_date = row.get('PurchaseInvDate', 'N/A')
            vendor = row.get('PartyName', row.get('VendorName', 'N/A'))
            amount = row.get('Total', row.get('Amount', 0))
            
            # Extract enhanced RMS fields
            enhanced_fields = extract_rms_enhanced_fields(row, field_mapping)
            
            validation_issues = []
            severity = "✅ PASS"  # Default to pass
            
            # Check individual validation rules
            
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
            
            # 7. Missing Creator Name validation
            if enhanced_fields['Invoice_Created_By'] == 'Unknown':
                validation_issues.append("Missing Invoice Creator Name")
                if severity == "✅ PASS":
                    severity = "⚠️ WARNING"
            
            # 8. Missing Method of Payment validation
            if enhanced_fields['Method_of_Payment'] == 'Not Specified':
                validation_issues.append("Missing Method of Payment")
                if severity == "✅ PASS":
                    severity = "⚠️ WARNING"
            
            # 9. Check for duplicate invoice numbers
            if not pd.isna(invoice_number) and str(invoice_number).strip() != '':
                duplicate_count = df[df['PurchaseInvNo'] == invoice_number].shape[0]
                if duplicate_count > 1:
                    validation_issues.append(f"Duplicate Invoice Number (appears {duplicate_count} times)")
                    if severity == "✅ PASS":
                        severity = "⚠️ WARNING"
            
            # 10. Date format validation
            try:
                if not pd.isna(invoice_date):
                    pd.to_datetime(invoice_date)
            except:
                validation_issues.append("Invalid Date Format")
                severity = "❌ FAIL"
            
            # 11. Future date validation
            try:
                if not pd.isna(invoice_date):
                    inv_date = pd.to_datetime(invoice_date)
                    if inv_date > datetime.now():
                        validation_issues.append("Future Date")
                        if severity == "✅ PASS":
                            severity = "⚠️ WARNING"
            except:
                pass
            
            # 12. Very old date validation (more than 2 years)
            try:
                if not pd.isna(invoice_date):
                    inv_date = pd.to_datetime(invoice_date)
                    two_years_ago = datetime.now() - timedelta(days=730)
                    if inv_date < two_years_ago:
                        validation_issues.append("Very Old Invoice (>2 years)")
                        if severity == "✅ PASS":
                            severity = "⚠️ WARNING"
            except:
                pass
            
            # Compile results for this invoice with enhanced fields
            detailed_results.append({
                'Invoice_ID': invoice_id,
                'Invoice_Number': invoice_number,
                'Invoice_Date': invoice_date,
                'Vendor_Name': vendor,
                'Amount': amount,
                'Invoice_Created_By': enhanced_fields['Invoice_Created_By'],  # ENHANCED FIELD
                'Method_of_Payment': enhanced_fields['Method_of_Payment'],      # NEW FIELD
                'Account_Head': enhanced_fields['Account_Head'],                # NEW FIELD
                'Invoice_Entry_Date': enhanced_fields['Invoice_Entry_Date'],    # NEW FIELD
                'Invoice_Upload_Date': enhanced_fields['Invoice_Upload_Date'],  # NEW FIELD
                'Validation_Status': severity,
                'Issues_Found': len(validation_issues),
                'Issue_Details': " | ".join(validation_issues) if validation_issues else "No issues found",
                'GST_Number': row.get('GSTNO', ''),
                'Row_Index': index,
                'Validation_Date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        
        # Convert to DataFrame
        detailed_df = pd.DataFrame(detailed_results)
        
        # Summary statistics
        total_invoices = len(detailed_df)
        passed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '✅ PASS'])
        warning_invoices = len(detailed_df[detailed_df['Validation_Status'] == '⚠️ WARNING'])
        failed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '❌ FAIL'])
        
        print(f"✅ Detailed validation completed:")
        print(f"   📊 Total invoices: {total_invoices}")
        print(f"   ✅ Passed: {passed_invoices}")
        print(f"   ⚠️ Warnings: {warning_invoices}")
        print(f"   ❌ Failed: {failed_invoices}")
        
        # Show enhanced field statistics
        creator_stats = detailed_df['Invoice_Created_By'].value_counts()
        mop_stats = detailed_df['Method_of_Payment'].value_counts()
        account_stats = detailed_df['Account_Head'].value_counts()
        
        print(f"   👤 Creator statistics: {len(creator_stats)} unique creators")
        print(f"   💳 Payment method statistics: {len(mop_stats)} unique methods")
        print(f"   📊 Account head statistics: {len(account_stats)} unique accounts")
        
        if 'Unknown' in creator_stats:
            print(f"   ⚠️ Unknown creators: {creator_stats['Unknown']} invoices")
        if 'Not Specified' in mop_stats:
            print(f"   ⚠️ Unspecified payment methods: {mop_stats['Not Specified']} invoices")
        
        return detailed_df, summary_issues, problematic_invoices_df
        
    except Exception as e:
        print(f"❌ Detailed validation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), [], pd.DataFrame()

def generate_email_summary_statistics(detailed_df, cumulative_start, cumulative_end, current_batch_start, current_batch_end, today_str):
    """Generate summary statistics specifically formatted for email body"""
    print("📧 Generating email summary statistics...")
    
    try:
        if detailed_df.empty:
            return {
                'html_summary': "
No invoice data available for validation.
",
                'text_summary': "No invoice data available for validation.",
                'statistics': {}
            }
        
        # Calculate statistics
        total_invoices = len(detailed_df)
        passed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '✅ PASS'])
        warning_invoices = len(detailed_df[detailed_df['Validation_Status'] == '⚠️ WARNING'])
        failed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '❌ FAIL'])
        
        pass_rate = (passed_invoices / total_invoices * 100) if total_invoices > 0 else 0
        issue_rate = ((warning_invoices + failed_invoices) / total_invoices * 100) if total_invoices > 0 else 0
        
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
        
        # Enhanced field statistics
        creator_stats = detailed_df['Invoice_Created_By'].value_counts()
        mop_stats = detailed_df['Method_of_Payment'].value_counts()
        account_stats = detailed_df['Account_Head'].value_counts()
        
        unknown_creators = creator_stats.get('Unknown', 0)
        unspecified_mop = mop_stats.get('Not Specified', 0)
        unspecified_accounts = account_stats.get('Not Specified', 0)
        
        total_creators = len(creator_stats)
        total_mop_types = len(mop_stats)
        total_account_heads = len(account_stats)
        
        # HTML formatted summary for email - FIXED STRING LITERAL
        html_summary = f"""
        

            

                📊 Invoice Validation Summary - {today_str}
            

            
            

                
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

                

            
            

                
🔍 Top Validation Issues

                
"""
        
        for issue, count in top_issues:
            percentage = (count / total_invoices * 100) if total_invoices > 0 else 0
            severity_color = "#e74c3c" if "Missing" in issue else ("#f39c12" if any(word in issue for word in ["Duplicate", "Negative", "Future", "Old"]) else "#3498db")
            html_summary += f'
{issue}: {count:,} invoices ({percentage:.1f}%)
'
        
        html_summary += f"""
                

            

            
            

                
👤 Enhanced RMS Field Analysis

                

                    
Total Creators: {total_creators}

                    
Unknown Creators: {unknown_creators} invoices ({(unknown_creators/total_invoices*100):.1f}%)

                    
Payment Methods: {total_mop_types}

                    
Unspecified Payment Methods: {unspecified_mop} invoices ({(unspecified_mop/total_invoices*100):.1f}%)

                    
Account Heads: {total_account_heads}

                    
Unspecified Account Heads: {unspecified_accounts} invoices ({(unspecified_accounts/total_invoices*100):.1f}%)

                

            

            
            

                
📈 Overall Health Score

                
"""
        
        if pass_rate >= 90:
            health_status = "Excellent"
            health_color = "#27ae60"
            health_icon = "🟢"
        elif pass_rate >= 75:
            health_status = "Good"
            health_color = "#f39c12"
            health_icon = "🟡"
        else:
            health_status = "Needs Attention"
            health_color = "#e74c3c"
            health_icon = "🔴"
        
        html_summary += f"""
                    

                        {health_icon} {health_status} 
                        - {pass_rate:.1f}% of invoices passed validation
                    

                

            

        
"""
        
        # Plain text summary for email clients that don't support HTML
        text_summary = f"""
📊 INVOICE VALIDATION SUMMARY - {today_str}

📅 VALIDATION PERIOD:
• Current Batch: {current_batch_start} to {current_batch_end}
• Cumulative Range: {cumulative_start} to {cumulative_end}
• Total Coverage: {(datetime.strptime(cumulative_end, '%Y-%m-%d') - datetime.strptime(cumulative_start, '%Y-%m-%d')).days + 1} days

📈 VALIDATION RESULTS:
✅ Total Invoices: {total_invoices:,}
✅ Passed: {passed_invoices:,} ({pass_rate:.1f}%)
⚠️ Warnings: {warning_invoices:,}
❌ Failed: {failed_invoices:,}

👤 ENHANCED RMS FIELD ANALYSIS:
• Total Creators: {total_creators}
• Unknown Creators: {unknown_creators} invoices ({(unknown_creators/total_invoices*100):.1f}%)
• Payment Methods: {total_mop_types}
• Unspecified Payment Methods: {unspecified_mop} invoices ({(unspecified_mop/total_invoices*100):.1f}%)
• Account Heads: {total_account_heads}
• Unspecified Account Heads: {unspecified_accounts} invoices ({(unspecified_accounts/total_invoices*100):.1f}%)

🔍 TOP VALIDATION ISSUES:"""
        
        for i, (issue, count) in enumerate(top_issues, 1):
            percentage = (count / total_invoices * 100) if total_invoices > 0 else 0
            text_summary += f"\n{i}. {issue}: {count:,} invoices ({percentage:.1f}%)"
        
        text_summary += f"""

📈 OVERALL HEALTH: {health_icon} {health_status} - {pass_rate:.1f}% pass rate

Note: Detailed invoice-level validation report is attached with Enhanced RMS Fields (Creator Names, Method of Payment, Account Head).
"""
        
        # Statistics object for programmatic use
        statistics = {
            'total_invoices': total_invoices,
            'passed_invoices': passed_invoices,
            'warning_invoices': warning_invoices,
            'failed_invoices': failed_invoices,
            'pass_rate': pass_rate,
            'issue_rate': issue_rate,
            'health_status': health_status,
            'health_score': pass_rate,
            'top_issues': top_issues,
            'total_creators': total_creators,
            'unknown_creators': unknown_creators,
            'total_mop_types': total_mop_types,
            'unspecified_mop': unspecified_mop,
            'total_account_heads': total_account_heads,
            'unspecified_accounts': unspecified_accounts,
            'validation_date': today_str,
            'current_batch_start': current_batch_start,
            'current_batch_end': current_batch_end,
            'cumulative_start': cumulative_start,
            'cumulative_end': cumulative_end,
            'total_coverage_days': (datetime.strptime(cumulative_end, '%Y-%m-%d') - datetime.strptime(cumulative_start, '%Y-%m-%d')).days + 1
        }
        
        print(f"✅ Email summary statistics generated:")
        print(f"   📊 Health Status: {health_status} ({pass_rate:.1f}%)")
        print(f"   📈 Total Issues: {len(top_issues)} types identified")
        print(f"   👤 Creator Stats: {total_creators} total, {unknown_creators} unknown")
        print(f"   💳 Payment Methods: {total_mop_types} total, {unspecified_mop} unspecified")
        print(f"   📊 Account Heads: {total_account_heads} total, {unspecified_accounts} unspecified")
        
        return {
            'html_summary': html_summary,
            'text_summary': text_summary,
            'statistics': statistics
        }
        
    except Exception as e:
        print(f"❌ Email summary generation failed: {str(e)}")
        return {
            'html_summary': f"
Error generating summary: {str(e)}
",
            'text_summary': f"Error generating summary: {str(e)}",
            'statistics': {}
        }

def generate_detailed_validation_report(detailed_df, today_str):
    """Generate detailed validation report for Excel export"""
    print("📋 Generating detailed validation report for Excel export...")
    
    try:
        if detailed_df.empty:
            return []
        
        # Add summary sheet data with enhanced RMS field statistics
        summary_data = []
        
        # Overall statistics
        total_invoices = len(detailed_df)
        passed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '✅ PASS'])
        warning_invoices = len(detailed_df[detailed_df['Validation_Status'] == '⚠️ WARNING'])
        failed_invoices = len(detailed_df[detailed_df['Validation_Status'] == '❌ FAIL'])
        
        summary_data.append({
            'Report_Type': 'Overall_Summary',
            'Description': 'Total Invoice Count',
            'Count': total_invoices,
            'Percentage': '100.0%',
            'Status': 'INFO'
        })
        
        summary_data.append({
            'Report_Type': 'Overall_Summary', 
            'Description': 'Passed Validation',
            'Count': passed_invoices,
            'Percentage': f'{(passed_invoices/total_invoices*100):.1f}%' if total_invoices > 0 else '0%',
            'Status': 'PASS'
        })
        
        summary_data.append({
            'Report_Type': 'Overall_Summary',
            'Description': 'Warnings',
            'Count': warning_invoices, 
            'Percentage': f'{(warning_invoices/total_invoices*100):.1f}%' if total_invoices > 0 else '0%',
            'Status': 'WARNING'
        })
        
        summary_data.append({
            'Report_Type': 'Overall_Summary',
            'Description': 'Failed Validation',
            'Count': failed_invoices,
            'Percentage': f'{(failed_invoices/total_invoices*100):.1f}%' if total_invoices > 0 else '0%', 
            'Status': 'FAIL'
        })
        
        # Enhanced RMS field statistics
        creator_stats = detailed_df['Invoice_Created_By'].value_counts()
        mop_stats = detailed_df['Method_of_Payment'].value_counts()
        account_stats = detailed_df['Account_Head'].value_counts()
        
        unknown_creators = creator_stats.get('Unknown', 0)
        unspecified_mop = mop_stats.get('Not Specified', 0)
        unspecified_accounts = account_stats.get('Not Specified', 0)
        
        summary_data.append({
            'Report_Type': 'RMS_Field_Analysis',
            'Description': 'Total Unique Creators',
            'Count': len(creator_stats),
            'Percentage': '100.0%',
            'Status': 'INFO'
        })
        
        summary_data.append({
            'Report_Type': 'RMS_Field_Analysis',
            'Description': 'Unknown/Missing Creators',
            'Count': unknown_creators,
            'Percentage': f'{(unknown_creators/total_invoices*100):.1f}%' if total_invoices > 0 else '0%',
            'Status': 'WARNING' if unknown_creators > 0 else 'PASS'
        })
        
        summary_data.append({
            'Report_Type': 'RMS_Field_Analysis',
            'Description': 'Total Payment Methods',
            'Count': len(mop_stats),
            'Percentage': '100.0%',
            'Status': 'INFO'
        })
        
        summary_data.append({
            'Report_Type': 'RMS_Field_Analysis',
            'Description': 'Unspecified Payment Methods',
            'Count': unspecified_mop,
            'Percentage': f'{(unspecified_mop/total_invoices*100):.1f}%' if total_invoices > 0 else '0%',
            'Status': 'WARNING' if unspecified_mop > 0 else 'PASS'
        })
        
        summary_data.append({
            'Report_Type': 'RMS_Field_Analysis',
            'Description': 'Total Account Heads',
            'Count': len(account_stats),
            'Percentage': '100.0%',
            'Status': 'INFO'
        })
        
        summary_data.append({
            'Report_Type': 'RMS_Field_Analysis',
            'Description': 'Unspecified Account Heads',
            'Count': unspecified_accounts,
            'Percentage': f'{(unspecified_accounts/total_invoices*100):.1f}%' if total_invoices > 0 else '0%',
            'Status': 'WARNING' if unspecified_accounts > 0 else 'PASS'
        })
        
        print(f"✅ Detailed validation report prepared with {len(summary_data)} summary entries")
        print(f"   📊 Including enhanced RMS field analysis")
        return summary_data
        
    except Exception as e:
        print(f"❌ Detailed report generation failed: {str(e)}")
        return []

def format_excel_report_with_styling(writer, sheet_name, df):
    """Format Excel report with professional styling and proper column widths"""
    try:
        from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
        from openpyxl.utils.dataframe import dataframe_to_rows
        
        # Write DataFrame to Excel
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Get the worksheet
        worksheet = writer.sheets[sheet_name]
        
        # Define styles
        header_font = Font(name='Arial', size=11, bold=True, color='FFFFFF')
        header_fill = PatternFill(start_color='366092', end_color='366092', fill_type='solid')
        border = Border(left=Side(style='thin'), right=Side(style='thin'), 
                       top=Side(style='thin'), bottom=Side(style='thin'))
        center_alignment = Alignment(horizontal='center', vertical='center')
        
        # Apply header styling
        for col_num in range(1, len(df.columns) + 1):
            cell = worksheet.cell(row=1, column=col_num)
            cell.font = header_font
            cell.fill = header_fill
            cell.border = border
            cell.alignment = center_alignment
        
        # Auto-adjust column widths
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            
            adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
            worksheet.column_dimensions[column_letter].width = adjusted_width
        
        # Apply borders to all cells
        for row in worksheet.iter_rows(min_row=1, max_row=len(df) + 1, 
                                      min_col=1, max_col=len(df.columns)):
            for cell in row:
                cell.border = border
                
    except Exception as e:
        print(f"⚠️ Excel formatting failed: {str(e)}")
        # If formatting fails, still write the basic data
        df.to_excel(writer, sheet_name=sheet_name, index=False)

def read_invoice_file(invoice_file):
    """
    Robust file reading with multiple format support and proper error handling
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
            header = f.read(50)
        print(f"🔍 File header (first 20 bytes): {header[:20]}")
    except Exception as e:
        print(f"⚠️ Could not read file header: {e}")
        header = b''
                
    df = None
    last_error = None
                    
    # Method 1: Try Excel with openpyxl engine (most reliable for .xlsx)
    try:
        print("📊 Attempting to read as Excel with openpyxl engine...")
        df = pd.read_excel(invoice_file, engine='openpyxl')
        print(f"✅ Successfully read Excel file with openpyxl. Shape: {df.shape}")
        print(f"📋 Columns: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"⚠️ openpyxl engine failed: {str(e)}")
        last_error = e
    
    # Method 2: Try Excel with xlrd engine (for older .xls files)
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
    
    # Method 3: Try reading as CSV with different separators
    try:
        print("📄 Attempting to read as CSV...")
        # Try common separators
        separators = [',', ';', '\t', '|']
        for sep in separators:
            try:
                df_test = pd.read_csv(invoice_file, sep=sep, nrows=5)
                if df_test.shape[1] > 1:  # Multiple columns detected
                    df = pd.read_csv(invoice_file, sep=sep)
                    print(f"✅ Successfully read as CSV with separator '{sep}'. Shape: {df.shape}")
                    print(f"📋 Columns: {list(df.columns)}")
                    return df
            except:
                continue
        print("⚠️ CSV reading failed with all separators")
    except Exception as e:
        print(f"⚠️ CSV reading failed: {str(e)}")
        last_error = e
        
    # Method 4: Try HTML parsing
    try:
        print("🌐 Attempting to read as HTML...")
        tables = pd.read_html(invoice_file, flavor='lxml')
        if tables and len(tables) > 0:
            df = tables[0]  # Get first table
            print(f"✅ Successfully read HTML file. Shape: {df.shape}")
            print(f"📋 Columns: {list(df.columns)}")
            return df
        else:
            print("⚠️ No tables found in HTML")
    except Exception as e:
        print(f"⚠️ HTML parsing failed: {str(e)}")
        last_error = e
    
    # Method 5: Try reading as plain text and show sample
    try:
        print("📝 Attempting to read file content for debugging...")
        with open(invoice_file, 'r', encoding='utf-8', errors='ignore') as f:
            content_sample = f.read(500)  # Read first 500 characters
        print(f"📄 File content sample:\n{repr(content_sample)}")
                
        # Try to detect if it's actually a different format
        if content_sample.strip().startswith('= start_date) &
            (df["ParsedInvoiceDate"] <= end_date)  
        ]
                
        print(f"📅 Filtered invoices from {start_str} to {end_str}: {len(filtered_df)} out of {len(df)}")
        return filtered_df
            
    except Exception as e:
        print(f"⚠️ Date filtering failed: {str(e)}, returning all data")
        return df

def run_invoice_validation():
    """Main function to run detailed cumulative validation with invoice-level reports and enhanced RMS field mapping"""
    try:
        today = datetime.today()
        today_str = today.strftime("%Y-%m-%d")
        
        print(f"🚀 Starting ENHANCED cumulative validation workflow for {today_str}")
        print(f"📧 FEATURE: Email-ready summary statistics with RMS field analysis")
        print(f"📋 FEATURE: Individual invoice validation reports with Enhanced RMS Fields")
        print(f"🔍 NEW: Invoice Created By, Method of Payment, Account Head field extraction")
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
    
        # Step 7: Read and parse the cumulative data
        print("📊 Step 7: Reading cumulative invoice data...")
        try:
            df = read_invoice_file(invoice_file)
        
            if df is None or df.empty:
                print("❌ DataFrame is empty after reading file")
                return False
        
            print(f"✅ Successfully loaded cumulative data. Shape: {df.shape}")
            print(f"📋 Columns: {list(df.columns)}")
        except Exception as e:
            print(f"❌ Failed to read invoice file: {str(e)}")
            return False
        
        # Step 8: Filter to cumulative validation range
        print("🔄 Step 8: Filtering to cumulative validation range...")
        try:
            filtered_df = filter_invoices_by_date(df, cumulative_start, cumulative_end)
            print(f"📅 Working with {len(filtered_df)} invoices in cumulative range")
        except Exception as e:
            print(f"⚠️ Date filtering failed: {str(e)}, using all data")
            filtered_df = df
        
        # Step 9: Run ENHANCED detailed validation on ALL cumulative data
        print("🔄 Step 9: Running ENHANCED detailed validation on cumulative data...")
        print("   🔄 This includes:")
        print(f"      📦 Current batch: {current_batch_start} to {current_batch_end}")
        print(f"      🔄 ALL previously validated data from: {cumulative_start}")
        print(f"      🔍 NEW: Enhanced RMS field extraction (Creator, MOP, Account Head)")
        try:
            detailed_df, summary_issues, problematic_invoices_df = validate_invoices_with_details(filtered_df)
            
            if detailed_df.empty:
                print("⚠️ No detailed validation results generated")
            else:
                print(f"✅ Enhanced detailed validation completed on {len(detailed_df)} invoices")
                print(f"   🔍 Enhanced fields extracted: Invoice_Created_By, Method_of_Payment, Account_Head")
        except Exception as e:
            print(f"❌ Enhanced detailed validation failed: {str(e)}")
            return False
        
        # Step 10: Generate enhanced email summary statistics
        print("📧 Step 10: Generating enhanced email summary statistics...")
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
Error generating summary: {str(e)}
",
                'text_summary': f"Error generating summary: {str(e)}",
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
            print(f"⚠️ Failed to prepare invoice list: {str(e)}")
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
            print(f"⚠️ Failed to save snapshot: {str(e)}")
            
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
            print(f"⚠️ Failed to record run: {str(e)}")
        
        # Step 15: Save ENHANCED detailed reports with all RMS fields
        try:
            os.makedirs("data", exist_ok=True)
            
            # Main enhanced detailed validation report (invoice-level with ALL RMS fields)
            detailed_report_path = f"data/invoice_validation_detailed_{today_str}.xlsx"
            
            if not detailed_df.empty:
                with pd.ExcelWriter(detailed_report_path, engine='openpyxl') as writer:
                    # Sheet 1: All invoices with ENHANCED validation status INCLUDING ALL RMS FIELDS
                    format_excel_report_with_styling(writer, 'All_Invoices_Enhanced', detailed_df)
                    
                    # Sheet 2: Failed invoices only with enhanced fields
                    failed_df = detailed_df[detailed_df['Validation_Status'] == '❌ FAIL']
                    if not failed_df.empty:
                        format_excel_report_with_styling(writer, 'Failed_Invoices_Enhanced', failed_df)
                    
                    # Sheet 3: Warning invoices only with enhanced fields
                    warning_df = detailed_df[detailed_df['Validation_Status'] == '⚠️ WARNING'] 
                    if not warning_df.empty:
                        format_excel_report_with_styling(writer, 'Warning_Invoices_Enhanced', warning_df)
                    
                    # Sheet 4: Passed invoices only with enhanced fields
                    passed_df = detailed_df[detailed_df['Validation_Status'] == '✅ PASS']
                    if not passed_df.empty:
                        format_excel_report_with_styling(writer, 'Passed_Invoices_Enhanced', passed_df)
                    
                    # Sheet 5: Enhanced Creator analysis
                    creator_stats = detailed_df['Invoice_Created_By'].value_counts().reset_index()
                    creator_stats.columns = ['Creator_Name', 'Invoice_Count']
                    creator_stats['Percentage'] = (creator_stats['Invoice_Count'] / len(detailed_df) * 100).round(2)
                    format_excel_report_with_styling(writer, 'Creator_Analysis_Enhanced', creator_stats)
                    
                    # Sheet 6: NEW - Method of Payment analysis
                    mop_stats = detailed_df['Method_of_Payment'].value_counts().reset_index()
                    mop_stats.columns = ['Payment_Method', 'Invoice_Count']
                    mop_stats['Percentage'] = (mop_stats['Invoice_Count'] / len(detailed_df) * 100).round(2)
                    format_excel_report_with_styling(writer, 'Payment_Method_Analysis', mop_stats)
                    
                    # Sheet 7: NEW - Account Head analysis
                    account_stats = detailed_df['Account_Head'].value_counts().reset_index()
                    account_stats.columns = ['Account_Head', 'Invoice_Count']
                    account_stats['Percentage'] = (account_stats['Invoice_Count'] / len(detailed_df) * 100).round(2)
                    format_excel_report_with_styling(writer, 'Account_Head_Analysis', account_stats)
                    
                    # Sheet 8: Enhanced Summary statistics
                    if detailed_report:
                        summary_df = pd.DataFrame(detailed_report)
                        format_excel_report_with_styling(writer, 'Enhanced_Summary_Stats', summary_df)
                
                print(f"✅ ENHANCED invoice-level report saved: {detailed_report_path}")
                print(f"   📊 Includes: Invoice_Created_By, Method_of_Payment, Account_Head fields")
                print(f"   📋 Analysis sheets: Creator, Payment Method, Account Head breakdowns")

                # Create enhanced dashboard version with ALL essential columns
                os.makedirs(f"data/{today_str}", exist_ok=True)
                dashboard_path = f"data/{today_str}/validation_result.xlsx"
                
                # Enhanced dashboard columns with ALL RMS fields
                enhanced_dashboard_columns = [
                    'Invoice_ID', 'Invoice_Number', 'Invoice_Date', 'Vendor_Name', 'Amount',
                    'Invoice_Created_By', 'Method_of_Payment', 'Account_Head',  # RMS FIELDS
                    'Invoice_Entry_Date', 'Invoice_Upload_Date',                # ADDITIONAL RMS FIELDS
                    'Validation_Status', 'Issues_Found', 'Issue_Details', 'GST_Number'
                ]
                
                dashboard_df = detailed_df[enhanced_dashboard_columns].copy()
                
                # Add enhanced formatted status for better readability
                dashboard_df['Status_Summary'] = dashboard_df.apply(lambda row: 
                    f"{row['Validation_Status']} - {row['Issues_Found']} issues" if row['Issues_Found'] > 0 
                    else f"{row['Validation_Status']} - No issues", axis=1)
                
                # Enhanced RMS field summary
                dashboard_df['RMS_Fields_Summary'] = dashboard_df.apply(lambda row:
                    f"Creator: {row['Invoice_Created_By'][:20]}{'...' if len(str(row['Invoice_Created_By'])) > 20 else ''} | "
                    f"Payment: {row['Method_of_Payment'][:15]}{'...' if len(str(row['Method_of_Payment'])) > 15 else ''} | "
                    f"Account: {row['Account_Head'][:15]}{'...' if len(str(row['Account_Head'])) > 15 else ''}", axis=1)
                
                with pd.ExcelWriter(dashboard_path, engine='openpyxl') as writer:
                    format_excel_report_with_styling(writer, 'Enhanced_Dashboard', dashboard_df)
                
                print(f"📋 Enhanced invoice-level dashboard report created: {dashboard_path}")
                
                # Also update the enhanced delta report format
                delta_report_path = f"data/delta_report_{today_str}.xlsx"
                with pd.ExcelWriter(delta_report_path, engine='openpyxl') as writer:
                    format_excel_report_with_styling(writer, 'Enhanced_Delta_Report', dashboard_df)
                
                print(f"📋 Enhanced invoice-level delta report created: {delta_report_path}")
                
                # Save enhanced email summary
                summary_path = f"data/email_summary_{today_str}.html"
                with open(summary_path, 'w', encoding='utf-8') as f:
                    f.write(email_summary['html_summary'])
                print(f"📧 Enhanced email summary saved: {summary_path}")
                
            else:
                print("⚠️ No detailed validation results - creating empty enhanced report")
                empty_enhanced_df = pd.DataFrame({
                    'Invoice_ID': [], 'Invoice_Number': [], 'Invoice_Date': [], 'Vendor_Name': [], 'Amount': [],
                    'Invoice_Created_By': [], 'Method_of_Payment': [], 'Account_Head': [],
                    'Invoice_Entry_Date': [], 'Invoice_Upload_Date': [],
                    'Validation_Status': [], 'Issues_Found': [], 'Issue_Details': [], 'GST_Number': [], 'Status_Summary': []
                })
                
                with pd.ExcelWriter(detailed_report_path, engine='openpyxl') as writer:
                    format_excel_report_with_styling(writer, 'Empty_Enhanced_Report', empty_enhanced_df)
                
                print(f"✅ Empty enhanced invoice-level report created: {detailed_report_path}")
                        
        except Exception as e:
            print(f"❌ Failed to save enhanced detailed reports: {str(e)}")
            return False

        # Step 16: Enhanced processing (if available)
        print("🚀 Step 16: Applying enhanced features...")
        try:
            if ENHANCED_PROCESSOR_AVAILABLE:
                # Enhance the existing results
                enhancement_result = enhance_validation_results(detailed_df, email_summary)
                
                if enhancement_result['success']:
                    print("✅ Additional enhancement successful!")
                    enhanced_df = enhancement_result['enhanced_df']
                    changes_detected = enhancement_result['changes_detected']
                    enhanced_email_content = enhancement_result['enhanced_email_content']
                    summary = enhancement_result['summary']
                    
                    # Save additionally enhanced Excel report
                    enhanced_report_path = f"data/enhanced_invoice_validation_detailed_{today_str}.xlsx"
                    
                    with pd.ExcelWriter(enhanced_report_path, engine='openpyxl') as writer:
                        # Main additionally enhanced report with all new fields
                        format_excel_report_with_styling(writer, 'Super_Enhanced_All_Invoices', enhanced_df)
                        
                        # Additionally enhanced failed invoices
                        enhanced_failed_df = enhanced_df[enhanced_df['Validation_Status'] == '❌ FAIL']
                        if not enhanced_failed_df.empty:
                            format_excel_report_with_styling(writer, 'Super_Enhanced_Failed', enhanced_failed_df)
                        
                        # Additionally enhanced summary with new metrics
                        enhanced_summary = [
                            {'Metric': 'Total Invoices', 'Value': summary['total_invoices']},
                            {'Metric': 'Currencies Processed', 'Value': summary['currencies']},
                            {'Metric': 'Global Locations', 'Value': summary['locations']},
                            {'Metric': 'Urgent Due Date Alerts', 'Value': summary['urgent_dues']},
                            {'Metric': 'Tax Calculations Completed', 'Value': summary['tax_calculated']},
                            {'Metric': 'Historical Changes Detected', 'Value': summary['historical_changes']}
                        ]
                        enhanced_summary_df = pd.DataFrame(enhanced_summary)
                        format_excel_report_with_styling(writer, 'Super_Enhanced_Summary', enhanced_summary_df)
                        
                        # Currency breakdown
                        if 'Invoice_Currency' in enhanced_df.columns:
                            currency_breakdown = enhanced_df['Invoice_Currency'].value_counts().reset_index()
                            currency_breakdown.columns = ['Currency', 'Count']
                            format_excel_report_with_styling(writer, 'Currency_Breakdown', currency_breakdown)
                        
                        # Location breakdown
                        if 'Location' in enhanced_df.columns:
                            location_breakdown = enhanced_df['Location'].str.split(' -').str[0].value_counts().reset_index()
                            location_breakdown.columns = ['Location', 'Count']
                            format_excel_report_with_styling(writer, 'Location_Breakdown', location_breakdown)
                        
                        # Tax summary
                        if 'Total_Tax_Calculated' in enhanced_df.columns:
                            tax_summary = enhanced_df.groupby(['Location', 'Tax_Type'])[['Total_Tax_Calculated']].sum().reset_index()
                            format_excel_report_with_styling(writer, 'Tax_Summary', tax_summary)
                        
                        # Due date alerts
                        if 'Due_Date_Notification' in enhanced_df.columns:
                            urgent_invoices = enhanced_df[enhanced_df['Due_Date_Notification'] == 'YES']
                            if not urgent_invoices.empty:
                                urgent_cols = ['Invoice_Number', 'Vendor_Name', 'Amount', 'Due_Date', 'Location']
                                urgent_display = urgent_invoices[[col for col in urgent_cols if col in enhanced_df.columns]]
                                format_excel_report_with_styling(writer, 'Urgent_Due_Dates', urgent_display)
                        
                        # Historical changes
                        if changes_detected:
                            changes_df = pd.DataFrame(changes_detected)
                            format_excel_report_with_styling(writer, 'Historical_Changes', changes_df)
                    
                    print(f"✅ Additionally enhanced report saved: {enhanced_report_path}")
                    
                    # Update email content to enhanced version
                    if enhanced_email_content:
                        email_summary['html_summary'] = enhanced_email_content
                        email_summary['text_summary'] = enhanced_email_content
                        
                        # Save additionally enhanced email content
                        enhanced_email_path = f"data/enhanced_email_summary_{today_str}.html"
                        with open(enhanced_email_path, 'w', encoding='utf-8') as f:
                            f.write(enhanced_email_content)
                        print(f"📧 Additionally enhanced email content saved: {enhanced_email_path}")
                    
                    # Print enhancement summary
                    print(f"🔄 Additional Enhancement Summary:")
                    print(f"   💱 Currencies: {summary['currencies']}")
                    print(f"   🌍 Locations: {summary['locations']}")
                    print(f"   ⏰ Urgent dues: {summary['urgent_dues']}")
                    print(f"   💰 Tax calculated: {summary['tax_calculated']}")
                    print(f"   🔄 Historical changes: {summary['historical_changes']}")
                    
                else:
                    print(f"⚠️ Additional enhancement failed: {enhancement_result['error']}")
                    print("📊 Continuing with enhanced validation report")
            else:
                print("⚠️ Enhanced processor not available, using enhanced validation with RMS fields")
                
        except Exception as e:
            print(f"⚠️ Additional enhancement failed: {str(e)}")
            print("📊 Continuing with enhanced validation report")
                
        # Step 17: Send enhanced email notifications
        try:
            from email_notifier import EmailNotifier
            
            notifier = EmailNotifier()
                
            # Send enhanced detailed validation report to AP TEAM
            ap_team_recipients = os.getenv('AP_TEAM_EMAIL_LIST', '').split(',')
            ap_team_recipients = [email.strip() for email in ap_team_recipients if email.strip()]
                    
            if ap_team_recipients: 
                # Try to send enhanced detailed validation report
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
                        print(f"📧 Enhanced detailed validation report sent to AP team: {', '.join(ap_team_recipients)}")
                        print(f"   📊 Including: Invoice Created By, Method of Payment, Account Head analysis")
                    else:
                        # Fallback to basic validation report
                        statistics = email_summary.get('statistics', {})
                        issues_count = statistics.get('failed_invoices', 0) + statistics.get('warning_invoices', 0)
                        notifier.send_validation_report(today_str, ap_team_recipients, issues_count)
                        print(f"📧 Basic validation report sent to AP team: {', '.join(ap_team_recipients)}")
                        print(f"⚠️ Note: Enhanced email method not available, sent basic report")
                        
                except Exception as email_error:
                    print(f"⚠️ Enhanced email failed: {str(email_error)}")
                    # Try basic validation report as fallback
                    try:
                        statistics = email_summary.get('statistics', {})
                        total_issues = statistics.get('failed_invoices', 0) + statistics.get('warning_invoices', 0)
                        notifier.send_validation_report(today_str, ap_team_recipients, total_issues)
                        print(f"📧 Fallback validation report sent to AP team")
                    except Exception as fallback_error:
                        print(f"❌ All email methods failed: {str(fallback_error)}")
                    
            else:   
                print("⚠️ No AP team email recipients configured in AP_TEAM_EMAIL_LIST")
            
            print("📧 Enhanced email notification workflow completed!")
            
        except Exception as e:
            print(f"⚠️ Email sending failed: {str(e)}")
            import traceback
            traceback.print_exc()
                    
        print("✅ ENHANCED cumulative validation workflow completed successfully!")
        print(f"")
        print(f"📊 FINAL ENHANCED SUMMARY:")
        print(f"   📦 Current batch: {current_batch_start} to {current_batch_end}")
        print(f"   🔄 Cumulative range: {cumulative_start} to {cumulative_end}")
        print(f"   📅 Total days validated: {(datetime.strptime(cumulative_end, '%Y-%m-%d') - datetime.strptime(cumulative_start, '%Y-%m-%d')).days + 1}")
        print(f"   📋 Total invoices processed: {len(detailed_df) if not detailed_df.empty else 0}")
        
        if not detailed_df.empty:
            stats = email_summary.get('statistics', {})
            print(f"   ✅ Passed: {stats.get('passed_invoices', 0)} ({stats.get('pass_rate', 0):.1f}%)")
            print(f"   ⚠️ Warnings: {stats.get('warning_invoices', 0)}")
            print(f"   ❌ Failed: {stats.get('failed_invoices', 0)}")
            print(f"   👤 Total Creators: {stats.get('total_creators', 0)}")
            print(f"   ❓ Unknown Creators: {stats.get('unknown_creators', 0)}")
            print(f"   💳 Payment Methods: {stats.get('total_mop_types', 0)}")
            print(f"   📊 Account Heads: {stats.get('total_account_heads', 0)}")
            print(f"   🏥 Health Status: {stats.get('health_status', 'Unknown')}")
        
        print(f"   🔍 ENHANCED FEATURES: Invoice Created By, Method of Payment, Account Head extraction")
        print(f"   ⏰ Next run in: {VALIDATION_INTERVAL_DAYS} days")
        print(f"   🗂️ Archive threshold: {ACTIVE_VALIDATION_MONTHS} months")
        
        return True
                
    except Exception as e:
        print(f"❌ Unexpected error in enhanced cumulative validation workflow: {str(e)}")
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
            
