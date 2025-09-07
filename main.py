from rms_scraper import rms_download
from validator_utils import validate_invoices
from updater import update_invoice_status
from reporter import save_snapshot_report
from dotenv import load_dotenv
from datetime import datetime, timedelta
from email_notifier import EnhancedEmailSystem as EmailNotifier
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
import traceback
import os
import re
import logging
import shutil
from pathlib import Path
import re, json, glob

# --- logger bootstrap: paste directly below imports ---
logger = logging.getLogger("invoice_validator")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

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
    return True  # ← ADD THIS LINE TO FORCE RUN
    
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

GST_STATE_MAP = {
    "01":"Jammu & Kashmir","02":"Himachal Pradesh","03":"Punjab","04":"Chandigarh","05":"Uttarakhand",
    "06":"Haryana","07":"Delhi","08":"Rajasthan","09":"Uttar Pradesh","10":"Bihar","11":"Sikkim",
    "12":"Arunachal Pradesh","13":"Nagaland","14":"Manipur","15":"Mizoram","16":"Tripura","17":"Meghalaya",
    "18":"Assam","19":"West Bengal","20":"Jharkhand","21":"Odisha","22":"Chhattisgarh","23":"Madhya Pradesh",
    "24":"Gujarat","25":"Daman & Diu","26":"Dadra & Nagar Haveli and Daman & Diu","27":"Maharashtra",
    "28":"Andhra Pradesh (Old)","29":"Karnataka","30":"Goa","31":"Lakshadweep","32":"Kerala","33":"Tamil Nadu",
    "34":"Puducherry","35":"Andaman & Nicobar Islands","36":"Telangana","37":"Andhra Pradesh","38":"Ladakh"
}

def _try_load_creator_map(run_dir: str) -> dict:
    """
    Looks for a creators map the scraper may have written (json/csv).
    Keys supported: PurchaseInvNo / InvID / VoucherNo → creator name.
    """
    creators = {}
    for p in glob.glob(os.path.join(run_dir, "*creator*.*")):
        try:
            if p.lower().endswith(".json"):
                with open(p, "r", encoding="utf-8") as f:
                    creators.update(json.load(f))
            else:
                import pandas as _pd
                cdf = _pd.read_csv(p)
                key_col = next((c for c in cdf.columns if c.lower() in ("purchaseinvno","invid","voucherno","invoice_number")), None)
                val_col = next((c for c in cdf.columns if "creator" in c.lower()), None)
                if key_col and val_col:
                    creators.update(dict(zip(cdf[key_col].astype(str), cdf[val_col].astype(str))))
        except Exception as e:
            logging.warning(f"Creator map load failed for {p}: {e}")
    return creators

def _derive_location(row) -> str:
    # 1) use explicit columns if present
    for c in ("Location", "Branch", "State"):
        v = row.get(c)
        if isinstance(v, str) and v.strip():
            return v.strip()
    # 2) GST state code → state name
    gst = str(row.get("GSTNO", "")).strip()
    m = re.match(r"^(\d{2})", gst)
    if m and m.group(1) in GST_STATE_MAP:
        return GST_STATE_MAP[m.group(1)]
    # 3) parse from narration
    narr = str(row.get("Narration", ""))
    m = re.search(r"(?:Location|Loc)[:\- ]+([A-Za-z .]+)", narr, flags=re.I)
    if m:
        return m.group(1).strip().title()
    return ""

def map_payment_method(payment_info):
    """
    Standardize payment method information
    """
    if not payment_info or pd.isna(payment_info):
        return "Cash"
    
    payment_str = str(payment_info).lower().strip()
    
    # Define payment method mappings
    payment_mappings = {
        'card': ['card', 'credit', 'debit', 'visa', 'mastercard'],
        'bank_transfer': ['bank', 'transfer', 'wire', 'neft', 'rtgs', 'imps'],
        'cheque': ['cheque', 'check', 'dd', 'demand draft'],
        'online': ['online', 'digital', 'upi', 'paytm', 'gpay', 'phonepe'],
        'cash': ['cash', 'hand', 'direct']
    }
    
    # Check for matches
    for method, keywords in payment_mappings.items():
        if any(keyword in payment_str for keyword in keywords):
            return method.replace('_', ' ').title()
    
    # Default return
    return "Cash"

def _derive_payment_method(row) -> str:
    blob = " ".join(str(x) for x in [
        row.get("VoucherTypeName",""),
        row.get("Narration",""),
        row.get("PurchaseLEDGER",""),
        row.get("OtherLedger1",""),
        row.get("OtherLedger2",""),
        row.get("OtherLedger3","")
    ]).lower()

    if re.search(r"\b(neft|rtgs|imps|wire|bank\s*transfer)\b", blob): return "Bank Transfer"
    if re.search(r"\bupi|gpay|phonepe|paytm|wallet|online\b", blob):   return "Digital Payment"
    if re.search(r"\b(card|visa|mastercard|amex|pos)\b", blob):        return "Card Payment"
    if re.search(r"\bcheque|check|dd|demand\s*draft\b", blob):         return "Cheque"
    if re.search(r"\bcash|petty\s*cash\b", blob):                      return "Cash"
    return ""

def _derive_creator(row, creators_map: dict) -> str:
    for k in [row.get("VoucherNo"), row.get("PurchaseInvNo"), row.get("InvID")]:
        k = str(k) if k is not None else ""
        if k and k in creators_map:
            return str(creators_map[k]).strip().title()

    # parse from narration
    narr = str(row.get("Narration", ""))
    m = re.search(r"(?:Inv(?:oice)?\s*Created\s*By|Created\s*By|Prepared\s*By|Maker|User)[:\- ]+([A-Za-z .]+)", narr, flags=re.I)
    if m:
        return m.group(1).strip().title()
    return ""

def _derive_scid(row) -> str:
    for c in ("SCID", "Scid", "scid"):
        v = row.get(c)
        if isinstance(v, str) and v.strip():
            return v.strip()
    narr = str(row.get("Narration", ""))
    m = re.search(r"\bSCID[:\- ]*([A-Za-z0-9\-_/]+)", narr, flags=re.I)
    if m:
        return m.group(1).strip()
    return ""

def build_final_validation_report(source_df: pd.DataFrame, run_dir: str, validation_date: datetime) -> pd.DataFrame:
    """
    Build the final Excel in the requested schema, using RMS column names:
      - "MOP", "A/C Head", "Inv Currency", "Inv Created By", "DueDate", etc.
      Falls back gracefully if a column doesn't exist.
    """
    df = source_df.copy()

    # quick column finder: case/space/punct insensitive
    def _find(*candidates):
        if not candidates:
            return None
        # 1) direct lower lookup
        lower_map = {c.lower(): c for c in df.columns}
        for cand in candidates:
            if cand.lower() in lower_map:
                return lower_map[cand.lower()]
        # 2) normalized (strip non-alnum)
        norm = {re.sub(r'[^a-z0-9]+', '', c.lower()): c for c in df.columns}
        for cand in candidates:
            key = re.sub(r'[^a-z0-9]+', '', cand.lower())
            if key in norm:
                return norm[key]
        return None

    # RMS column resolutions
    col_inv_id       = _find("Invoice_ID", "InvID", "RMS_Invoice_ID")
    col_inv_no       = _find("Invoice_Number", "PurchaseInvNo", "Inv No", "Invoice No", "VoucherNo")
    col_inv_date     = _find("Invoice_Date", "PurchaseInvDate", "Inv Date", "Voucherdate")
    col_entry_date   = _find("Invoice_Entry_Date", "Inv Entry Date")
    col_mod_date     = _find("Invoice_Mod_Date", "Inv Mod Date")
    col_vendor       = _find("Vendor_Name", "PartyName")
    col_amount       = _find("Amount", "Total")
    col_location     = _find("Location", "State")
    col_mop          = _find("Method_of_Payment", "MOP")
    col_ac_head      = _find("Account_Head", "A/C Head")
    col_currency     = _find("Invoice_Currency", "Inv Currency", "Currency")
    col_creator      = _find("Invoice_Creator_Name", "Inv Created By", "Created By")
    col_gst          = _find("GST_Number", "GSTNO")
    col_duedate      = _find("Due_Date", "DueDate")
    col_tax_type     = _find("Tax_Type", "VAT")
    col_remarks      = _find("Remarks", "Narration")
    col_scid         = _find("SCID")

    # Build output
    out = pd.DataFrame({
        "Invoice_ID":              df[col_inv_id] if col_inv_id else df.index.astype(str),
        "Invoice_Number":          df[col_inv_no] if col_inv_no else "",
        "Invoice_Date":            df[col_inv_date] if col_inv_date else "",
        "Invoice_Entry_Date":      df[col_entry_date] if col_entry_date else "",
        "Invoice_Mod_Date":        df[col_mod_date] if col_mod_date else "",
        "Vendor_Name":             df[col_vendor] if col_vendor else "",
        "Amount":                  pd.to_numeric(df[col_amount], errors="coerce").fillna(0) if col_amount else 0,
        "Invoice_Creator_Name":    df[col_creator].fillna("System Generated") if col_creator else "System Generated",
        "Location":                df[col_location].fillna("") if col_location else "",
        "Invoice_Currency":        df[col_currency].fillna("INR") if col_currency else "INR",
        "Method_of_Payment":       df[col_mop].apply(map_payment_method) if col_mop else "Unknown",
        "Account_Head":            df[col_ac_head].fillna("") if col_ac_head else "",
        "Validation_Status":       "",      # your validator can fill later
        "Issues_Found":            "",      # your validator can fill later
        "Issue_Details":           "",      # your validator can fill later
        "GST_Number":              df[col_gst].fillna("") if col_gst else "",
        "Row_Index":               df.index,
        "Validation_Date":         validation_date.strftime("%Y-%m-%d"),
        "Tax_Type":                df[col_tax_type].fillna("") if col_tax_type else "",
        "Due_Date":                df[col_duedate].fillna("") if col_duedate else "",
        "Due_Date_Notification":   "",      # populate if you have SLA logic
        "Total_Tax_Calculated":    0,       # compute if needed
        "CGST_Amount":             pd.to_numeric(df.get("CGSTInputAmt", 0), errors="coerce").fillna(0),
        "SGST_Amount":             pd.to_numeric(df.get("SGSTInputAmt", 0), errors="coerce").fillna(0),
        "IGST_Amount":             pd.to_numeric(df.get("IGST/VATInputAmt", 0), errors="coerce").fillna(0),
        "VAT_Amount":              pd.to_numeric(df.get("VAT", 0), errors="coerce").fillna(0),
        "TDS_Status":              "Coming soon",   # per your requirement
        "RMS_Invoice_ID":          df[col_inv_id] if col_inv_id else "",
        "SCID":                    df[col_scid] if col_scid else "",
        "Remarks":                 df[col_remarks] if col_remarks else "",
    })

    # Save the final file to the run directory so the email can attach it
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    out_path = run_dir / "validation_result.xlsx"
    out.to_excel(out_path, index=False)
    logger.info(f"Final validation report saved: {out_path}")
    return out

    # apply safe renames when columns exist
    rename_map = {k:v for k,v in rename_map.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # creators map (optional)
    creators_map = _try_load_creator_map(run_dir)

    # Derived fields
    df["Invoice_Creator_Name"] = df.apply(lambda r: _derive_creator(r, creators_map), axis=1)
    df["Location"]             = df.apply(_derive_location, axis=1)
    df["Method_of_Payment"]    = df.apply(_derive_payment_method, axis=1)
    df["SCID"]                 = df.apply(_derive_scid, axis=1)

    # Dates / entry date / due date
    # Prefer explicit Invoice_Date_Raw if present, else Invoice_Date
    def _pick_date(row, *candidates):
        for c in candidates:
            v = row.get(c)
            if pd.notna(v) and str(v).strip():
                return str(v)
        return ""
    df["Invoice_Date"]       = df.apply(lambda r: _pick_date(r, "Invoice_Date_Raw","Invoice_Date"), axis=1)
    df["Invoice_Entry_Date"] = df.apply(lambda r: _pick_date(r, "OrderDate","VoucherDate","Invoice_Date"), axis=1)
    df["Due_Date"]           = ""  # not in export; leave blank unless you add extraction
    df["Due_Date_Notification"] = ""

    # Account Head from narration / ledger (reusing your helper if present)
    def _account_head(row):
        base = str(row.get("Narration","") or row.get("PurchaseLEDGER",""))
        try:
            return map_account_head(base)
        except Exception:
            return "Miscellaneous"
    df["Account_Head"] = df.apply(_account_head, axis=1)

    # Validation related columns – map from your computed results if you have them, else defaults
    if "Validation_Status" not in df.columns:
        df["Validation_Status"] = ""   # your pipeline later can merge the actual status
    if "Issues_Found" not in df.columns:
        df["Issues_Found"] = ""
    if "Issue_Details" not in df.columns:
        df["Issue_Details"] = ""

    # Monetary/tax columns – ensure they exist
    for col in ["Amount","CGST_Amount","SGST_Amount","IGST_Amount","VAT_Amount"]:
        if col not in df.columns:
            df[col] = 0

    # Total tax calculated
    df["Total_Tax_Calculated"] = (
        df.get("CGST_Amount", 0).fillna(0).astype(float) +
        df.get("SGST_Amount", 0).fillna(0).astype(float) +
        df.get("IGST_Amount", 0).fillna(0).astype(float) +
        df.get("VAT_Amount", 0).fillna(0).astype(float)
    )

    # Tax type heuristic
    def _tax_type(row):
        if (row.get("IGST_Amount",0) or 0) > 0 or (row.get("CGST_Amount",0) or 0) > 0 or (row.get("SGST_Amount",0) or 0) > 0:
            return "GST"
        if (row.get("VAT_Amount",0) or 0) > 0:
            return "VAT"
        return ""
    df["Tax_Type"] = df.apply(_tax_type, axis=1)

    # Row_Index & Validation_Date
    df["Row_Index"]      = df.index
    df["Validation_Date"] = validation_date.strftime("%Y-%m-%d %H:%M:%S")

    # Duplicate keys (your list has both “Invoice currency” and “Invoice_Currency” and two “Location”s)
    # We will provide them once each; “Invoice currency” (space) will mirror Invoice_Currency.
    df["Invoice currency"] = df.get("Invoice_Currency", "")

    # Columns required (in exact order; any missing are created empty)
    required_cols = [
        "Invoice_ID",
        "Invoice_Number",
        "Invoice_Date",
        "Invoice_Entry_Date",
        "Vendor_Name",
        "Amount",
        "Invoice_Creator_Name",
        "Location",
        "Invoice currency",
        "Method_of_Payment",
        "Account_Head",
        "Validation_Status",
        "Issues_Found",
        "Issue_Details",
        "GST_Number",
        "Row_Index",
        "Validation_Date",
        "Invoice_Currency",
        "Tax_Type",
        "Due_Date",
        "Due_Date_Notification",
        "Total_Tax_Calculated",
        "CGST_Amount",
        "SGST_Amount",
        "IGST_Amount",
        "VAT_Amount",
        "TDS_Status",
        "RMS_Invoice_ID",
        "SCID",
    ]
    # Source → target mapping
    defaults_map = {
        "Invoice_ID": df.get("RMS_Invoice_ID", df.get("InvID", "")),
        "Invoice_Number": df.get("Invoice_Number", df.get("VoucherNo","")),
        "GST_Number": df.get("GST_Number",""),
        "TDS_Status": df.get("TDS_Status",""),
        "RMS_Invoice_ID": df.get("RMS_Invoice_ID", df.get("InvID","")),
    }

    for k, series in defaults_map.items():
        if k not in df.columns:
            df[k] = series

    for col in required_cols:
        if col not in df.columns:
            df[col] = ""

    # Final ordered frame
    final_df = df[required_cols].copy()
    return final_df

def enhance_validation_results(detailed_df, email_summary):
    """
    Enhance validation results and return a consistent response dict.
    Ensures keys: success, enhanced_df, email_summary, message.
    """

    try:
        logging.info("🔧 Enhancing validation results...")

        # Work on a copy; ensure a DataFrame exists
        enhanced_df = detailed_df.copy() if detailed_df is not None else pd.DataFrame()
        total_invoices = len(enhanced_df)

        # Defaults
        pass_count = 0
        fail_count = 0
        warning_count = 0
        pass_rate = 0.0
        total_amount = 0.0
        validation_col = None
        amount_col = None

        if total_invoices > 0:
            # Find a validation/result/status column
            for col in enhanced_df.columns:
                low = str(col).lower()
                if ('validation' in low) or ('result' in low) or ('status' in low):
                    validation_col = col
                    break

            if validation_col:
                series = enhanced_df[validation_col].astype(str)
                pass_count = series.str.contains(r'\bpass\b', case=False, na=False).sum()
                fail_count = series.str.contains(r'\bfail\b', case=False, na=False).sum()
                warning_count = series.str.contains(r'warn', case=False, na=False).sum()
            else:
                # If no validation column, conservatively mark all as failed
                fail_count = total_invoices

            # Find an amount/total column
            for col in enhanced_df.columns:
                low = str(col).lower()
                if ('total' in low) or ('amount' in low):
                    amount_col = col
                    break

            if amount_col:
                enhanced_df[amount_col] = pd.to_numeric(enhanced_df[amount_col], errors='coerce')
                total_amount = float(enhanced_df[amount_col].fillna(0).sum())

            pass_rate = (pass_count / total_invoices * 100.0) if total_invoices else 0.0

            # Tag the frame once enhanced
            if 'enhancement_status' not in enhanced_df.columns:
                enhanced_df['enhancement_status'] = 'enhanced'
                enhanced_df['enhancement_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        result = {
            'success': True,
            'enhanced_df': enhanced_df,
            'email_summary': email_summary,
            'message': 'Enhancement completed',
            'total_invoices': total_invoices,
            'validation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'enhancement_applied': True,
            'system_status': 'operational',
            'pass_count': pass_count,
            'fail_count': fail_count,
            'warning_count': warning_count,
            'pass_rate': pass_rate,
            'total_amount': total_amount,
            'validation_column': validation_col,
            'amount_column': amount_col
        }

        logging.info(f"✅ Enhancement completed: {total_invoices} invoices processed")
        logging.info(f"📊 Pass rate: {pass_rate:.1f}%")
        return result

    except Exception as e:
        logging.error(f"⚠️ Enhancement step error: {e}")
        logging.error(traceback.format_exc())
        try:
            fallback_df = detailed_df.copy() if detailed_df is not None else pd.DataFrame()
        except Exception:
            fallback_df = pd.DataFrame()
        return {
            'success': False,
            'enhanced_df': fallback_df,
            'email_summary': email_summary,
            'message': f'Enhancement error: {e}',
            'total_invoices': len(fallback_df),
            'validation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'enhancement_applied': False,
            'system_status': 'degraded'
        }

def build_final_validation_report(df, run_dir: str, validation_dt):
    """
    Build the exact-column, full-length report for email attachment.
    Now prefers RMS field names:
      - Inv Created By  → Invoice_Creator_Name
      - MOP             → Method_of_Payment
      - A/C Head        → Account_Head
      - Inv Currency    → Invoice currency / Invoice_Currency
      - Inv Entry Date, Inv Mod Date, DueDate, Remarks → added to output
    Falls back to earlier sources when not present.
    """
    import pandas as pd
    src = df.copy()

    # map helper
    cols = {c.lower(): c for c in src.columns}
    col = lambda name: cols.get(name.lower())

    # Common base columns already seen in RMS export
    c_voucher_no       = col("VoucherNo")
    c_purchase_inv_no  = col("PurchaseInvNo")
    c_purchase_inv_dt  = col("PurchaseInvDate")
    c_voucher_dt       = col("Voucherdate")
    c_party            = col("PartyName")
    c_total            = col("Total")
    c_state            = col("State")
    c_currency_legacy  = col("Currency")
    c_narration        = col("Narration")
    c_ledger           = col("PurchaseLEDGER")
    c_gst              = col("GSTNO")
    c_invid            = col("InvID")
    c_vat              = col("VAT")
    c_igst_amt         = col("IGST/VATInputAmt")
    c_cgst_amt         = col("CGSTInputAmt")
    c_sgst_amt         = col("SGSTInputAmt")

    # New preferred RMS names you gave
    c_inv_created_by   = col("Inv Created By")
    c_mop              = col("MOP")
    c_ac_head          = col("A/C Head")
    c_inv_currency     = col("Inv Currency")
    c_inv_entry_date   = col("Inv Entry Date")
    c_inv_mod_date     = col("Inv Mod Date")
    c_due_date         = col("DueDate") or col("Due Date")
    c_remarks          = col("Remarks")

    # Build keys
    invoice_number = src[c_purchase_inv_no] if c_purchase_inv_no else (src[c_voucher_no] if c_voucher_no else "")
    invoice_date   = src[c_purchase_inv_dt] if c_purchase_inv_dt else (src[c_voucher_dt] if c_voucher_dt else "")
    entry_date     = src[c_inv_entry_date] if c_inv_entry_date else (src[c_voucher_dt] if c_voucher_dt else invoice_date)
    mod_date       = src[c_inv_mod_date] if c_inv_mod_date else ""
    due_date       = src[c_due_date] if c_due_date else ""
    remarks        = src[c_remarks] if c_remarks else (src[c_narration] if c_narration else "")

    # Creator: prefer "Inv Created By"; fallback to previous heuristics
    if c_inv_created_by:
        creator_series = src[c_inv_created_by].astype(str).replace({"": "System Generated", "N/A": "System Generated"})
    else:
        # fallback chain: try any existing creator-like column → narration regex → default
        creator_col_candidates = [col("CreatedBy"), col("Creator"), col("Invoice_Creator_Name")]
        creator_series = None
        for cand in creator_col_candidates:
            if cand:
                creator_series = src[cand].astype(str)
                break
        if creator_series is None:
            import re
            patt = re.compile(r"(created\s*by|creator|inv\s*created\s*by)\s*[:\-]\s*([A-Za-z][\w .'-]+)", re.I)
            def from_narr(x):
                m = patt.search(str(x) if x is not None else "")
                return m.group(2).strip() if m else ""
            creator_series = src[c_narration].map(from_narr) if c_narration else pd.Series([""]*len(src))
        creator_series = creator_series.fillna("")
        creator_series = creator_series.apply(lambda s: "System Generated" if str(s).strip().lower() in ("", "unknown", "system", "n/a") else s)

    # Location
    location_series = src[c_state] if c_state else pd.Series([""]*len(src))

    # Method of Payment: prefer MOP
    if c_mop:
        method_series = src[c_mop].astype(str).fillna("")
    else:
        def _payment_method(row):
            raw = " ".join([
                str(row.get(c_narration)) if c_narration else "",
                str(row.get(c_ledger)) if c_ledger else ""
            ])
            return map_payment_method(raw)
        method_series = src.apply(_payment_method, axis=1)

    # Account head: prefer A/C Head
    if c_ac_head:
        account_series = src[c_ac_head].astype(str).fillna("")
    else:
        def _account_head(row):
            raw = str(row.get(c_ledger)) if c_ledger else str(row.get(c_narration)) if c_narration else ""
            return raw if raw else map_account_head(raw)
        account_series = src.apply(_account_head, axis=1)

    # Currency: prefer Inv Currency, fallback to legacy Currency
    currency_series = src[c_inv_currency] if c_inv_currency else (src[c_currency_legacy] if c_currency_legacy else "")

    # Validation status column if present
    validation_col = None
    for c in src.columns:
        cl = c.lower()
        if "validation" in cl or "status" in cl or "result" in cl:
            validation_col = c
            break
    validation_status = src[validation_col] if validation_col else pd.Series([""]*len(src))

    # Issues (best-effort)
    issues_candidates = [col("Issue_Details"), col("Error Details"), col("Issues")]
    issue_details = src[issues_candidates[0]].fillna("") if issues_candidates[0] else pd.Series([""]*len(src))
    issues_found = issue_details.apply(lambda s: 1 if str(s).strip() else 0)

    # Taxes
    cgst = pd.to_numeric(src[c_cgst_amt], errors="coerce").fillna(0) if c_cgst_amt else 0
    sgst = pd.to_numeric(src[c_sgst_amt], errors="coerce").fillna(0) if c_sgst_amt else 0
    igst = pd.to_numeric(src[c_igst_amt], errors="coerce").fillna(0) if c_igst_amt else 0
    vat  = pd.to_numeric(src[c_vat],  errors="coerce").fillna(0) if c_vat else 0
    total_tax = (cgst if isinstance(cgst, pd.Series) else 0) + \
                (sgst if isinstance(sgst, pd.Series) else 0) + \
                (igst if isinstance(igst, pd.Series) else 0) + \
                (vat  if isinstance(vat,  pd.Series) else 0)

    # Tax type inference
    def _infer_tax_type_local(row):
        try: igt = float(row.get(c_igst_amt, 0) or 0)
        except: igt = 0
        try: cgt = float(row.get(c_cgst_amt, 0) or 0)
        except: cgt = 0
        try: sgt = float(row.get(c_sgst_amt, 0) or 0)
        except: sgt = 0
        try: v = float(row.get(c_vat, 0) or 0)
        except: v = 0
        if igt > 0: return "IGST"
        if (cgt > 0) or (sgt > 0): return "GST"
        if v > 0: return "VAT"
        return ""
    tax_type = src.apply(_infer_tax_type_local, axis=1)

    # SCID
    scid_col = col("SCID")
    if scid_col:
        scid_series = src[scid_col].astype(str)
    elif c_invid:
        scid_series = src[c_invid].astype(str)
    else:
        import re
        patt = re.compile(r"\bSCID[:\s\-]*([A-Za-z0-9\-]+)", re.I)
        def _scid(row):
            text = str(row.get(c_narration)) if c_narration else ""
            m = patt.search(text)
            return m.group(1) if m else ""
        scid_series = src.apply(_scid, axis=1)

    amount_series = src[c_total] if c_total else pd.Series([0]*len(src))

    # Compose final dataframe (keeps your original columns + the 4 new ones)
    final = pd.DataFrame({
        "Invoice_ID":           src[c_invid] if c_invid else (src[c_voucher_no] if c_voucher_no else ""),
        "Invoice_Number":       invoice_number,
        "Invoice_Date":         invoice_date,
        "Invoice_Entry_Date":   entry_date,                 # original field in your spec
        "Inv Entry Date":       entry_date,                 # explicit RMS field you asked to add
        "Inv Mod Date":         mod_date,                   # NEW
        "Vendor_Name":          src[c_party] if c_party else "",
        "Amount":               amount_series,
        "Invoice_Creator_Name": creator_series,
        "Location":             location_series,
        "Invoice currency":     currency_series,            # original label
        "Method_of_Payment":    method_series,
        "Account_Head":         account_series,
        "Validation_Status":    validation_status,
        "Issues_Found":         issues_found,
        "Issue_Details":        issue_details,
        "GST_Number":           src[c_gst] if c_gst else "",
        "Row_Index":            (src.index + 1),
        "Validation_Date":      validation_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "Invoice_Currency":     currency_series,            # duplicate output (as per your earlier format)
        "Tax_Type":             tax_type,
        "Due_Date":             due_date,                   # original label
        "DueDate":              due_date,                   # explicit RMS field you asked to add
        "Remarks":              remarks,                    # NEW (prefers Remarks, falls back to Narration)
        "Due_Date_Notification": "",
        "Total_Tax_Calculated": total_tax,
        "CGST_Amount":          cgst if isinstance(cgst, pd.Series) else 0,
        "SGST_Amount":          sgst if isinstance(sgst, pd.Series) else 0,
        "IGST_Amount":          igst if isinstance(igst, pd.Series) else 0,
        "VAT_Amount":           vat  if isinstance(vat,  pd.Series) else 0,
        "TDS_Status":           "Coming Soon",
        "RMS_Invoice_ID":       src[c_invid] if c_invid else "",
        "SCID":                 scid_series,
    })

    return final
      
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

        print("🚀 Step 16: Applying enhanced features...")
        try:
            # Enhance the existing results (single call)
            enhancement_result = enhance_validation_results(detailed_df, email_summary)

            # Normalize result & fallbacks
            if isinstance(enhancement_result, dict) and enhancement_result.get('success', False):
                print("✅ Enhancement successful!")
                msg = enhancement_result.get('message', '')
            else:
                msg = (enhancement_result.get('message', 'unknown error')
                       if isinstance(enhancement_result, dict) else str(enhancement_result))
                print(f"⚠️ Enhancement failed: {msg}")

            # Safe assignments (no KeyErrors)
            if isinstance(enhancement_result, dict):
                enhanced_df = enhancement_result.get('enhanced_df', detailed_df)
                changes_detected = enhancement_result.get('changes_detected', False)
                enhanced_email_content = enhancement_result.get('enhanced_email_content', email_summary)
            else:
                enhanced_df = detailed_df
                changes_detected = False
                enhanced_email_content = email_summary

            # raw_detailed_df is the parsed/validated RMS export you already have at this point
            from email_notifier import EmailNotifier, EnhancedEmailSystem  # make sure this import is present

            run_dir = os.path.join("data", datetime.now().strftime("%Y-%m-%d"))
            validation_date = datetime.now()

            # Use the dataframe you actually produced earlier in the workflow:
            source_df = detailed_df   # <- this exists in your run; change only if your var name is different
            final_report_df = build_final_validation_report(source_df, run_dir, validation_date)

            # Save the exact-format report
            final_report_path = os.path.join("data", f"invoice_validation_detailed_{validation_date.strftime('%Y-%m-%d')}.xlsx")
            with pd.ExcelWriter(final_report_path, engine="xlsxwriter") as _writer:
                final_report_df.to_excel(_writer, sheet_name="Validation Report", index=False)

            # Attach the real invoices.zip from the run directory + the report
            invoices_zip_path = os.path.join(run_dir, "invoices.zip")
            attachments = [p for p in (final_report_path, invoices_zip_path) if os.path.isfile(p)]

            # Send the email
            notifier = EmailNotifier()
            dsubject = f"Invoice Validation Report — {validation_date.strftime('%Y-%m-%d')}"
            html_body = open(Path(run_dir) / f"email_summary_{validation_date.strftime('%Y-%m-%d')}.html", "r", encoding="utf-8").read() \
                if (Path(run_dir) / f"email_summary_{validation_date.strftime('%Y-%m-%d')}.html").exists() else "<p>See attachments.</p>"

            # ONE call only
            ok = notifier.send_validation_report(subject, html_body, attachments=attachments)
            logger.info(f"Email sent: {ok}")

            notifier.send_validation_report(
                f"Invoice Validation Report - {validation_date:%Y-%m-%d}",
                html_body,
                attachments=attachments
            )

            # Save with the exact schema requested
            final_report_path = os.path.join("data", f"invoice_validation_detailed_{datetime.now().strftime('%Y-%m-%d')}.xlsx")
            with pd.ExcelWriter(final_report_path, engine="xlsxwriter") as _writer:
                final_report_df.to_excel(_writer, sheet_name="Validation Report", index=False)

            # Attach **real** invoices ZIP + the report
            invoices_zip_path = os.path.join(run_dir, "invoices.zip")  # your scraper is already saving this
            attachments = []
            if os.path.isfile(final_report_path):
                attachments.append(final_report_path)
            if os.path.isfile(invoices_zip_path):
                attachments.append(invoices_zip_path)

            # Email
            notifier = EmailNotifier()
            subject = f"Invoice Validation Report - {datetime.now().strftime('%Y-%m-%d')}"
            deadline = datetime.now() + timedelta(days=3)
            html_body = EnhancedEmailSystem().create_professional_html_template(
                {"failed": 0, "warnings": 0, "passed": 0},  # you can plug your stats here
                deadline
            )
            notifier.send_validation_report(subject, html_body, attachments=attachments)

            # ---------- Save enhanced Excel report ----------
            enhanced_report_path = f"data/enhanced_invoice_validation_detailed_{today_str}.xlsx"
            with pd.ExcelWriter(enhanced_report_path, engine='openpyxl') as writer:
                # All invoices (enhanced)
                enhanced_df.to_excel(writer, sheet_name='Enhanced_All_Invoices', index=False)

                # Enhanced failed invoices (robust filter)
                if 'Validation_Status' in enhanced_df.columns:
                    _vs = enhanced_df['Validation_Status'].astype(str)
                    enhanced_failed_df = enhanced_df[_vs.str.contains('FAIL', case=False, na=False)]
                    if not enhanced_failed_df.empty:
                        enhanced_failed_df.to_excel(writer, sheet_name='Enhanced_Failed', index=False)

                # Build a safe summary sheet (no hard deps on missing keys)
                total_invoices = len(enhanced_df)
                pass_count = (enhancement_result.get('pass_count')
                              if isinstance(enhancement_result, dict) else None)
                fail_count = (enhancement_result.get('fail_count')
                              if isinstance(enhancement_result, dict) else None)
                warning_count = (enhancement_result.get('warning_count')
                                 if isinstance(enhancement_result, dict) else None)
                pass_rate = (enhancement_result.get('pass_rate')
                             if isinstance(enhancement_result, dict) else None)
                total_amount = (enhancement_result.get('total_amount')
                                if isinstance(enhancement_result, dict) else None)

                # Optional extra fields your old code referenced; default them
                currencies = (enhancement_result.get('currencies')
                              if isinstance(enhancement_result, dict) else 'N/A')
                locations = (enhancement_result.get('locations')
                             if isinstance(enhancement_result, dict) else 'N/A')
                urgent_dues = (enhancement_result.get('urgent_dues')
                               if isinstance(enhancement_result, dict) else 0)
                tax_calculated = (enhancement_result.get('tax_calculated')
                                  if isinstance(enhancement_result, dict) else 0)
                historical_changes = (enhancement_result.get('historical_changes')
                                      if isinstance(enhancement_result, dict) else 0)
                changes_count = (enhancement_result.get('changes_count', 0)
                                 if isinstance(enhancement_result, dict) else 0)

                rows = [
                    {'Metric': 'Total Invoices', 'Value': total_invoices},
                    {'Metric': 'Pass Count', 'Value': pass_count if pass_count is not None else 'N/A'},
                    {'Metric': 'Fail Count', 'Value': fail_count if fail_count is not None else 'N/A'},
                    {'Metric': 'Warning Count', 'Value': warning_count if warning_count is not None else 'N/A'},
                    {'Metric': 'Pass Rate (%)', 'Value': (f"{pass_rate:.1f}" if isinstance(pass_rate, (int, float)) else 'N/A')},
                    {'Metric': 'Total Amount', 'Value': total_amount if total_amount is not None else 'N/A'},
                    {'Metric': 'Currencies Processed', 'Value': currencies},
                    {'Metric': 'Global Locations', 'Value': locations},
                    {'Metric': 'Urgent Due Date Alerts', 'Value': urgent_dues},
                    {'Metric': 'Tax Calculations Completed', 'Value': tax_calculated},
                    {'Metric': 'Historical Changes Detected', 'Value': historical_changes},
                    {'Metric': 'Changes Detected', 'Value': str(changes_detected)},
                    {'Metric': 'Changes Count', 'Value': changes_count},
                ]
                pd.DataFrame(rows).to_excel(writer, sheet_name='Enhanced_Summary', index=False)

        except Exception as e:
            logging.error(f"⚠️ Enhancement step error: {e}")
            logging.error(traceback.format_exc())
            print(f"⚠️ Enhancement step error: {e}")
            enhanced_df = detailed_df
            changes_detected = False
            enhanced_email_content = email_summary
                
        # Step 17: Send email notifications - AP TEAM ONLY
        try:
            
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
