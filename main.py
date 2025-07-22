# main.py

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

def find_creator_column(df):
    """Find the invoice creator column name from available columns"""
    possible_creator_columns = [
        'CreatedBy', 'Created_By', 'InvoiceCreatedBy', 'Invoice_Created_By',
        'UserName', 'User_Name', 'CreatorName', 'Creator_Name',
        'EntryBy', 'Entry_By', 'InputBy', 'Input_By',
        'PreparedBy', 'Prepared_By', 'MadeBy', 'Made_By'
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
    """Run detailed validation that returns per-invoice validation results"""
    print("🔍 Running detailed invoice-level validation...")
    
    try:
        # Run the existing validation to get summary issues
        summary_issues, problematic_invoices_df = validate_invoices(df)
        
        # Find the creator column
        creator_column = find_creator_column(df)
        
        # Now run detailed validation for each invoice
        detailed_results = []
        
        print(f"📋 Analyzing {len(df)} invoices for detailed validation...")
        
        for index, row in df.iterrows():
            invoice_id = row.get('InvID', f'Row_{index}')
            invoice_number = row.get('PurchaseInvNo', row.get('InvoiceNumber', 'N/A'))
            invoice_date = row.get('PurchaseInvDate', 'N/A')
            vendor = row.get('PartyName', row.get('VendorName', 'N/A'))
            amount = row.get('Total', row.get('Amount', 0))
            
            # Get Invoice Creator Name
            if creator_column:
                creator_name = str(row.get(creator_column, 'Unknown')).strip()
                if not creator_name or creator_name.lower() in ['', 'nan', 'none', 'null']:
                    creator_name = 'Unknown'
            else:
                creator_name = 'Unknown'
            
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
            
            # 7. Missing Creator Name (NEW VALIDATION)
            if creator_name == 'Unknown' or not creator_name:
                validation_issues.append("Missing Invoice Creator Name")
                if severity == "✅ PASS":
                    severity = "⚠️ WARNING"
            
            # 8. Check for duplicate invoice numbers
            if not pd.isna(invoice_number) and str(invoice_number).strip() != '':
                duplicate_count = df[df['PurchaseInvNo'] == invoice_number].shape[0]
                if duplicate_count > 1:
                    validation_issues.append(f"Duplicate Invoice Number (appears {duplicate_count} times)")
                    if severity == "✅ PASS":
                        severity = "⚠️ WARNING"
            
            # 9. Date format validation
            try:
                if not pd.isna(invoice_date):
                    pd.to_datetime(invoice_date)
            except:
                validation_issues.append("Invalid Date Format")
                severity = "❌ FAIL"
            
            # 10. Future date validation
            try:
                if not pd.isna(invoice_date):
                    inv_date = pd.to_datetime(invoice_date)
                    if inv_date > datetime.now():
                        validation_issues.append("Future Date")
                        if severity == "✅ PASS":
                            severity = "⚠️ WARNING"
            except:
                pass
            
            # 11. Very old date validation (more than 2 years)
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
            
            # Compile results for this invoice
            detailed_results.append({
                'Invoice_ID': invoice_id,
                'Invoice_Number': invoice_number,
                'Invoice_Date': invoice_date,
                'Vendor_Name': vendor,
                'Amount': amount,
                'Invoice_Creator_Name': creator_name,  # NEW FIELD
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
        
        # Show creator name statistics
        creator_stats = detailed_df['Invoice_Creator_Name'].value_counts()
        print(f"   👤 Creator statistics: {len(creator_stats)} unique creators")
        if 'Unknown' in creator_stats:
            print(f"   ⚠️ Unknown creators: {creator_stats['Unknown']} invoices")
        
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
                'html_summary': "<p>No invoice data available for validation.</p>",
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
        
        # Creator statistics
        creator_stats = detailed_df['Invoice_Creator_Name'].value_counts()
        unknown_creators = creator_stats.get('Unknown', 0)
        total_creators = len(creator_stats)
        
        # HTML formatted summary for email
        html_summary = f"""
        <div style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <h2 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px;">
                📊 Invoice Validation Summary - {today_str}
            </h2>
            
            <div style="background-color: #ecf0f1; padding: 15px; border-radius: 5px; margin: 15px 0;">
                <h3 style="color: #34495e; margin-top: 0;">📅 Validation Period</h3>
                <p><strong>Current Batch:</strong> {current_batch_start} to {current_batch_end}</p>
                <p><strong>Cumulative Range:</strong> {cumulative_start} to {cumulative_end}</p>
                <p><strong>Total Coverage:</strong> {(datetime.strptime(cumulative_end, '%Y-%m-%d') - datetime.strptime(cumulative_start, '%Y-%m-%d')).days + 1} days</p>
            </div>
            
            <div style="display: flex; flex-wrap: wrap; gap: 15px; margin: 20px 0;">
                <div style="background-color: #d5f4e6; padding: 15px; border-radius: 5px; flex: 1; min-width: 200px; border-left: 4px solid #27ae60;">
                    <h4 style="color: #27ae60; margin: 0 0 10px 0;">✅ Total Invoices</h4>
                    <p style="font-size: 24px; font-weight: bold; margin: 0; color: #27ae60;">{total_invoices:,}</p>
                </div>
                
                <div style="background-color: #d5f4e6; padding: 15px; border-radius: 5px; flex: 1; min-width: 200px; border-left: 4px solid #27ae60;">
                    <h4 style="color: #27ae60; margin: 0 0 10px 0;">✅ Passed</h4>
                    <p style="font-size: 24px; font-weight: bold; margin: 0; color: #27ae60;">{passed_invoices:,} ({pass_rate:.1f}%)</p>
                </div>
                
                <div style="background-color: #fef9e7; padding: 15px; border-radius: 5px; flex: 1; min-width: 200px; border-left: 4px solid #f39c12;">
                    <h4 style="color: #f39c12; margin: 0 0 10px 0;">⚠️ Warnings</h4>
                    <p style="font-size: 24px; font-weight: bold; margin: 0; color: #f39c12;">{warning_invoices:,}</p>
                </div>
                
                <div style="background-color: #fadbd8; padding: 15px; border-radius: 5px; flex: 1; min-width: 200px; border-left: 4px solid #e74c3c;">
                    <h4 style="color: #e74c3c; margin: 0 0 10px 0;">❌ Failed</h4>
                    <p style="font-size: 24px; font-weight: bold; margin: 0; color: #e74c3c;">{failed_invoices:,}</p>
                </div>
            </div>
            
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 15px 0;">
                <h3 style="color: #34495e; margin-top: 0;">🔍 Top Validation Issues</h3>
                <ol style="margin: 0; padding-left: 20px;">
        """
        
        for issue, count in top_issues:
            percentage = (count / total_invoices * 100) if total_invoices > 0 else 0
            severity_color = "#e74c3c" if "Missing" in issue else ("#f39c12" if any(word in issue for word in ["Duplicate", "Negative", "Future", "Old"]) else "#3498db")
            html_summary += f'<li style="color: {severity_color}; margin: 5px 0;"><strong>{issue}:</strong> {count:,} invoices ({percentage:.1f}%)</li>'
        
        html_summary += f"""
                </ol>
            </div>
            
            <div style="background-color: #fff3cd; padding: 15px; border-radius: 5px; margin: 15px 0; border-left: 4px solid #ffc107;">
                <h3 style="color: #856404; margin-top: 0;">👤 Invoice Creator Analysis</h3>
                <div style="background-color: #fff; padding: 10px; border-radius: 3px;">
                    <p style="margin: 5px 0;"><strong>Total Creators:</strong> {total_creators}</p>
                    <p style="margin: 5px 0;"><strong>Unknown Creators:</strong> {unknown_creators} invoices ({(unknown_creators/total_invoices*100):.1f}%)</p>
                </div>
            </div>
            
            <div style="background-color: #e8f4fd; padding: 15px; border-radius: 5px; margin: 15px 0; border-left: 4px solid #3498db;">
                <h3 style="color: #2980b9; margin-top: 0;">📈 Overall Health Score</h3>
                <div style="background-color: #fff; padding: 10px; border-radius: 3px;">
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
                    <p style="margin: 0; font-size: 18px;">
                        <span style="color: {health_color}; font-weight: bold;">{health_icon} {health_status}</span> 
                        - {pass_rate:.1f}% of invoices passed validation
                    </p>
                </div>
            </div>
        </div>
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

👤 CREATOR ANALYSIS:
• Total Creators: {total_creators}
• Unknown Creators: {unknown_creators} invoices ({(unknown_creators/total_invoices*100):.1f}%)

🔍 TOP VALIDATION ISSUES:
"""
        
        for i, (issue, count) in enumerate(top_issues, 1):
            percentage = (count / total_invoices * 100) if total_invoices > 0 else 0
            text_summary += f"{i}. {issue}: {count:,} invoices ({percentage:.1f}%)\n"
        
        text_summary += f"""
📈 OVERALL HEALTH: {health_icon} {health_status} - {pass_rate:.1f}% pass rate

Note: Detailed invoice-level validation report is attached with Creator Names.
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
        
        return {
            'html_summary': html_summary,
            'text_summary': text_summary,
            'statistics': statistics
        }
        
    except Exception as e:
        print(f"❌ Email summary generation failed: {str(e)}")
        return {
            'html_summary': f"<p>Error generating summary: {str(e)}</p>",
            'text_summary': f"Error generating summary: {str(e)}",
            'statistics': {}
        }

def generate_detailed_validation_report(detailed_df, today_str):
    """Generate detailed validation report for Excel export"""
    print("📋 Generating detailed validation report for Excel export...")
    
    try:
        if detailed_df.empty:
            return []
        
        # Add summary sheet data
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
        
        # Creator statistics
        creator_stats = detailed_df['Invoice_Creator_Name'].value_counts()
        unknown_creators = creator_stats.get('Unknown', 0)
        
        summary_data.append({
            'Report_Type': 'Creator_Analysis',
            'Description': 'Total Unique Creators',
            'Count': len(creator_stats),
            'Percentage': '100.0%',
            'Status': 'INFO'
        })
        
        summary_data.append({
            'Report_Type': 'Creator_Analysis',
            'Description': 'Unknown/Missing Creators',
            'Count': unknown_creators,
            'Percentage': f'{(unknown_creators/total_invoices*100):.1f}%' if total_invoices > 0 else '0%',
            'Status': 'WARNING' if unknown_creators > 0 else 'PASS'
        })
        
        print(f"✅ Detailed validation report prepared with {len(summary_data)} summary entries")
        return summary_data
        
    except Exception as e:
        print(f"❌ Detailed report generation failed: {str(e)}")
        return []

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
        if content_sample.strip().startswith('<!DOCTYPE') or content_sample.strip().startswith('<html'):
            print("🔍 File appears to be HTML format")
        elif content_sample.strip().startswith('{') or content_sample.strip().startswith('['):
            print("🔍 File appears to be JSON format")
        elif ',' in content_sample and '\n' in content_sample:
            print("🔍 File appears to be CSV-like format")
        
    except Exception as e:
        print(f"⚠️ Could not read file content: {e}")
        
    # If all methods failed, raise the most relevant error
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
    
            # Basic validation
            if file_size < 50:
                print(f"⚠️ Warning: {fname} seems too small ({file_size} bytes)")
                validation_results[fname] = "small"
            else:
                validation_results[fname] = "ok"
        
            # Check file header for format detection
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
    """Main function to run detailed cumulative validation with invoice-level reports and email summaries"""
    try:
        today = datetime.today()
        today_str = today.strftime("%Y-%m-%d")
        
        print(f"🚀 Starting DETAILED cumulative validation workflow for {today_str}")
        print(f"📧 NEW FEATURE: Email-ready summary statistics")
        print(f"📋 FEATURE: Individual invoice validation reports with Creator Names")
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
        
        # Step 9: Run detailed validation on ALL cumulative data
        print("🔄 Step 9: Running detailed validation on cumulative data...")
        print("   🔄 This includes:")
        print(f"      📦 Current batch: {current_batch_start} to {current_batch_end}")
        print(f"      🔄 ALL previously validated data from: {cumulative_start}")
        try:
            detailed_df, summary_issues, problematic_invoices_df = validate_invoices_with_details(filtered_df)
            
            if detailed_df.empty:
                print("⚠️ No detailed validation results generated")
            else:
                print(f"✅ Detailed validation completed on {len(detailed_df)} invoices")
        except Exception as e:
            print(f"❌ Detailed validation failed: {str(e)}")
            return False
        
        # Step 10: Generate email summary statistics
        print("📧 Step 10: Generating email summary statistics...")
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
            print(f"⚠️ Email summary generation failed: {str(e)}")
            email_summary = {
                'html_summary': f"<p>Error generating summary: {str(e)}</p>",
                'text_summary': f"Error generating summary: {str(e)}",
                'statistics': {}
            }
        
        # Step 11: Generate detailed validation report
        print("📋 Step 11: Generating detailed validation report...")
        try:
            detailed_report = generate_detailed_validation_report(detailed_df, today_str)
        except Exception as e:
            print(f"⚠️ Detailed report generation failed: {str(e)}")
            detailed_report = []
        
        # Step 12: Prepare invoice data for saving
        print("💾 Step 12: Preparing invoice data for saving...")
        try:
            if not detailed_df.empty:
                current_invoices_list = detailed_df.to_dict('records')
            else:
                current_invoices_list = []
            
            print(f"📋 Prepared {len(current_invoices_list)} detailed invoice records for saving")
        except Exception as e:
            print(f"⚠️ Failed to prepare invoice list: {str(e)}")
            current_invoices_list = []
        
        # Step 13: Save validation snapshot
        try:
            save_invoice_snapshot(
                current_invoices_list, 
                run_date=today_str, 
                run_type="detailed_cumulative_4day",
                batch_start=current_batch_start,
                batch_end=current_batch_end,
                cumulative_start=cumulative_start,
                cumulative_end=cumulative_end
            )
            print("✅ Detailed validation snapshot saved")
        except Exception as e:
            print(f"⚠️ Failed to save snapshot: {str(e)}")
            
        # Step 14: Record this run
        try:
            record_run_window(
                current_batch_start, 
                current_batch_end, 
                run_type="detailed_cumulative_4day",
                cumulative_start=cumulative_start,
                cumulative_end=cumulative_end,
                total_days_validated=(datetime.strptime(cumulative_end, "%Y-%m-%d") - 
                                    datetime.strptime(cumulative_start, "%Y-%m-%d")).days + 1
            )
            print("✅ Detailed cumulative run recorded")
        except Exception as e:
            print(f"⚠️ Failed to record run: {str(e)}")
        
        # Step 15: Save detailed reports (invoice-level with creator names)
        try:
            os.makedirs("data", exist_ok=True)
            
            # Main detailed validation report (invoice-level)
            detailed_report_path = f"data/invoice_validation_detailed_{today_str}.xlsx"
            
            if not detailed_df.empty:
                with pd.ExcelWriter(detailed_report_path, engine='openpyxl') as writer:
                    # Sheet 1: All invoices with detailed validation status INCLUDING CREATOR NAMES
                    detailed_df.to_excel(writer, sheet_name='All_Invoices', index=False)
                    
                    # Sheet 2: Failed invoices only  
                    failed_df = detailed_df[detailed_df['Validation_Status'] == '❌ FAIL']
                    if not failed_df.empty:
                        failed_df.to_excel(writer, sheet_name='Failed_Invoices', index=False)
                    
                    # Sheet 3: Warning invoices only
                    warning_df = detailed_df[detailed_df['Validation_Status'] == '⚠️ WARNING'] 
                    if not warning_df.empty:
                        warning_df.to_excel(writer, sheet_name='Warning_Invoices', index=False)
                    
                    # Sheet 4: Passed invoices only
                    passed_df = detailed_df[detailed_df['Validation_Status'] == '✅ PASS']
                    if not passed_df.empty:
                        passed_df.to_excel(writer, sheet_name='Passed_Invoices', index=False)
                    
                    # Sheet 5: Creator analysis
                    creator_stats = detailed_df['Invoice_Creator_Name'].value_counts().reset_index()
                    creator_stats.columns = ['Creator_Name', 'Invoice_Count']
                    creator_stats.to_excel(writer, sheet_name='Creator_Analysis', index=False)
                    
                    # Sheet 6: Summary statistics
                    if detailed_report:
                        summary_df = pd.DataFrame(detailed_report)
                        summary_df.to_excel(writer, sheet_name='Summary_Stats', index=False)
                
                print(f"✅ Detailed invoice-level report saved: {detailed_report_path}")

                # Create dashboard version with essential columns INCLUDING CREATOR NAME
                os.makedirs(f"data/{today_str}", exist_ok=True)
                dashboard_path = f"data/{today_str}/validation_result.xlsx"
                
                dashboard_columns = ['Invoice_ID', 'Invoice_Number', 'Invoice_Date', 'Vendor_Name', 
                                   'Amount', 'Invoice_Creator_Name', 'Validation_Status', 
                                   'Issues_Found', 'Issue_Details', 'GST_Number']
                dashboard_df = detailed_df[dashboard_columns].copy()
                
                # Add formatted status for better readability
                dashboard_df['Status_Summary'] = dashboard_df.apply(lambda row: 
                    f"{row['Validation_Status']} - {row['Issues_Found']} issues" if row['Issues_Found'] > 0 
                    else f"{row['Validation_Status']} - No issues", axis=1)
                
                dashboard_df.to_excel(dashboard_path, index=False, engine='openpyxl')
                print(f"📋 Invoice-level dashboard report created: {dashboard_path}")
                
                # Also update the delta report format with creator names
                delta_report_path = f"data/delta_report_{today_str}.xlsx"
                dashboard_df.to_excel(delta_report_path, index=False, engine='openpyxl')
                print(f"📋 Invoice-level delta report created: {delta_report_path}")
                
                # Save email summary
                summary_path = f"data/email_summary_{today_str}.html"
                with open(summary_path, 'w', encoding='utf-8') as f:
                    f.write(email_summary['html_summary'])
                print(f"📧 Email summary saved: {summary_path}")
                
            else:
                print("⚠️ No detailed validation results - creating empty report")
                empty_df = pd.DataFrame({
                    'Invoice_ID': [], 'Invoice_Number': [], 'Invoice_Date': [], 'Vendor_Name': [],
                    'Amount': [], 'Invoice_Creator_Name': [], 'Validation_Status': [], 
                    'Issues_Found': [], 'Issue_Details': [], 'GST_Number': [], 'Status_Summary': []
                })
                empty_df.to_excel(detailed_report_path, index=False, engine='openpyxl')
                print(f"✅ Empty invoice-level report created: {detailed_report_path}")
                        
        except Exception as e:
            print(f"❌ Failed to save detailed reports: {str(e)}")
            return False
                
        # Step 16: Send email notifications - AP TEAM ONLY
        try:
            from email_notifier import EmailNotifier
            
            notifier = EmailNotifier()
                
            # Send detailed validation report to AP TEAM ONLY (Tax and Aditya)
            ap_team_recipients = os.getenv('AP_TEAM_EMAIL_LIST', '').split(',')
            ap_team_recipients = [email.strip() for email in ap_team_recipients if email.strip()]
                    
            if ap_team_recipients: 
                # Try to send detailed validation report
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
                        print(f"📧 Detailed validation report sent to AP team: {', '.join(ap_team_recipients)}")
                    else:
                        # Fallback to basic validation report
                        issues_count = len(email_summary.get('statistics', {}).get('failed_invoices', []))
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
            
            print("📧 Email notification workflow completed!")
            
        except Exception as e:
            print(f"⚠️ Email sending failed: {str(e)}")
            import traceback
            traceback.print_exc()
                    
        print("✅ Detailed cumulative validation workflow completed successfully!")
        print(f"")
        print(f"📊 FINAL SUMMARY:")
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
            print(f"   🏥 Health Status: {stats.get('health_status', 'Unknown')}")
        
        print(f"   ⏰ Next run in: {VALIDATION_INTERVAL_DAYS} days")
        print(f"   🗂️ Archive threshold: {ACTIVE_VALIDATION_MONTHS} months")
        
        return True
                
    except Exception as e:
        print(f"❌ Unexpected error in detailed cumulative validation workflow: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
            
# Run the validation if called directly
if __name__ == "__main__":
    success = run_invoice_validation()
    if not success:
        print("❌ Detailed cumulative validation failed!")
        exit(1)
    else:   
        print("🎉 Detailed cumulative validation completed successfully!")
        exit(0)

