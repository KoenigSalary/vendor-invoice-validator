import os
import pandas as pd
from datetime import datetime
from snapshot_handler import compare_with_snapshot, save_snapshot
from email_sender import send_email_report
from validator_utils import validate_invoices, get_invoice_summary

def get_latest_data_folder(base="data"):
    """Get the most recent data folder"""
    try:
        if not os.path.exists(base):
            print(f"❌ Base directory '{base}' does not exist")
            return None
            
        folders = [f for f in os.listdir(base) if f.startswith("2025-")]
        if not folders:
            print(f"⚠️ No date folders found in '{base}'")
            return None
            
        folders.sort(reverse=True)
        latest_folder = os.path.join(base, folders[0])
        print(f"📁 Latest data folder found: {latest_folder}")
        return latest_folder
    except Exception as e:
        print(f"❌ Error finding latest folder: {str(e)}")
        return None

def load_actual_invoice_data(data_dir):
    """Load actual invoice data from the data directory"""
    try:
        # Try to load the invoice_download.xls file first
        invoice_file = os.path.join(data_dir, "invoice_download.xls")
        
        if os.path.exists(invoice_file):
            print(f"📊 Loading invoice data from: {invoice_file}")
            
            # Try different methods to read the file
            try:
                # Try reading as Excel first
                df = pd.read_excel(invoice_file, engine='openpyxl')
                print("✅ Loaded as Excel file")
            except:
                try:
                    # Try reading as CSV with tab separator
                    df = pd.read_csv(invoice_file, sep='\t')
                    print("✅ Loaded as TSV file")
                except:
                    try:
                        # Try reading as regular CSV
                        df = pd.read_csv(invoice_file)
                        print("✅ Loaded as CSV file")
                    except Exception as e:
                        print(f"❌ Failed to read invoice file: {str(e)}")
                        return None
            
            print(f"📋 Loaded {len(df)} invoices with {len(df.columns)} columns")
            return df
            
        else:
            print(f"⚠️ Invoice file not found: {invoice_file}")
            return None
            
    except Exception as e:
        print(f"❌ Error loading invoice data: {str(e)}")
        return None

def create_sample_data():
    """Create sample invoice data for testing"""
    print("🧪 Creating sample invoice data for testing...")
    
    sample_data = []
    
    # Create multiple sample invoices
    base_date = datetime.today()
    
    for i in range(1, 6):  # Create 5 sample invoices
        invoice = {
            "VoucherNo": f"V{i:03d}",
            "Voucherdate": (base_date - pd.Timedelta(days=i)).strftime("%Y-%m-%d"),
            "PurchaseInvNo": f"INV{i:03d}",
            "PurchaseInvDate": (base_date - pd.Timedelta(days=i)).strftime("%Y-%m-%d"),
            "OrderNo": f"ORD{i:03d}",
            "OrderDate": (base_date - pd.Timedelta(days=i+1)).strftime("%Y-%m-%d"),
            "VoucherTypeName": "Purchase Invoice",
            "PartyName": f"Vendor {chr(64+i)} Pvt Ltd",  # Vendor A, B, C, etc.
            "GSTNO": f"07ABCDE{i:04d}F1Z5",
            "State": "Delhi",
            "PurchaseLEDGER": "Purchase Account",
            "VAT": "18%",
            "TaxableValue": 10000 * i,
            "NonTax": 0,
            "Total": int(10000 * i * 1.18),  # 18% tax
            "VATNumber": f"VAT{i:03d}",
            "Currency": "INR",
            "IGST/VATInputLedger": "IGST Input",
            "IGST/VATInputAmt": int(10000 * i * 0.18),
            "CGSTInputLedger": "CGST Input",
            "CGSTInputAmt": int(10000 * i * 0.09),
            "SGSTInputLedger": "SGST Input", 
            "SGSTInputAmt": int(10000 * i * 0.09),
            "OtherLedger1": "",
            "OtherLedgerAmt1": 0,
            "Dr/Cr1": "",
            "OtherLedger2": "",
            "OtherLedgerAnt2": 0,
            "Dr/Cr2": "",
            "OtherLedger3": "",
            "OtherLedgerAmt3": 0,
            "Dr/Cr3": "",
            "PaytyAmt": int(10000 * i * 1.18),
            "Narration": f"Purchase of goods from Vendor {chr(64+i)}",
            "TDS": 0,
            "InvID": f"INV{i:03d}"
        }
        sample_data.append(invoice)
    
    df = pd.DataFrame(sample_data)
    print(f"✅ Created {len(df)} sample invoices")
    return df

def validate_invoice_data(db_record, attachment_data):
    """
    Validate invoice by comparing database record with data extracted from attachment
    
    Args:
        db_record: Record from the invoice list panel/database
        attachment_data: Data extracted from the actual invoice document
    
    Returns:
        dict: Validation results with issues found
    """
    validation_results = {
        "status": "Valid",
        "issues": []
    }
    
    # Fields to validate from invoice list panel in RMS
    rms_fields = ["invoice_id", "scid", "upload_date", "Invoice_creator_name", "due_date"]
    for field in rms_fields:
        if field in db_record and field in attachment_data:
            if str(db_record[field]).strip() != str(attachment_data[field]).strip():
                validation_results["issues"].append({
                    "field": field,
                    "db_value": db_record[field],
                    "invoice_value": attachment_data[field],
                    "message": f"Mismatch in {field}"
                })
    
    # Fields to validate from the invoice document
    doc_fields = ["Invoice_Number", "Invoice_Date", "Vendor_Name", "Amount", 
                 "GST_Number", "Invoice_Currency", "TDS", "VAT", "Total_Invoice_Value"]
    
    for field in doc_fields:
        if field in attachment_data:
            # Various validation rules based on field type
            if field == "GST_Number" and not is_valid_gstin(attachment_data[field]):
                validation_results["issues"].append({
                    "field": field,
                    "value": attachment_data[field],
                    "message": f"Invalid {field} format"
                })
            elif field in ["Amount", "TDS", "VAT", "Total_Invoice_Value"]:
                try:
                    value = float(attachment_data[field])
                    if value < 0:
                        validation_results["issues"].append({
                            "field": field,
                            "value": attachment_data[field],
                            "message": f"Negative value for {field}"
                        })
                except ValueError:
                    validation_results["issues"].append({
                        "field": field,
                        "value": attachment_data[field],
                        "message": f"Non-numeric value for {field}"
                    })
        else:
            validation_results["issues"].append({
                "field": field,
                "message": f"Missing {field} in invoice document"
            })
    
    # Set overall status
    if validation_results["issues"]:
        validation_results["status"] = "Issues Found"
    
    return validation_results

def run_validation_workflow():
    """Main validation workflow"""
    try:
        # Your existing validation workflow code...
        
        # Add code to integrate the new validation function
        # For example, after loading invoice data:
        if df is not None and not df.empty:
            # Process attachments for each invoice
            for idx, row in df.iterrows():
                invoice_id = row.get('Invoice_ID', row.get('InvID', row.get('invoice_no', '')))
                if invoice_id:
                    # Extract data from attachment
                    attachment_data = process_invoice_attachments(
                        invoice_id=invoice_id,
                        zip_path=zip_path,
                        extract_dir=os.path.join(base_dir, "unzipped")
                    )
                    
                    if attachment_data.get('status') == 'success':
                        # Validate extracted data against database record
                        validation_result = validate_invoice_data(
                            db_record=row.to_dict(),
                            attachment_data=attachment_data.get('data', {})
                        )
                        
                        # Update validation status in the dataframe
                        df.at[idx, 'Validation_Status'] = validation_result['status']
                        if validation_result['issues']:
                            issues_text = '; '.join([issue['message'] for issue in validation_result['issues']])
                            df.at[idx, 'Issues_Found'] = len(validation_result['issues'])
                            df.at[idx, 'Issue_Details'] = issues_text
                        
                        # Add extracted data to dataframe for new fields
                        for field, value in attachment_data.get('data', {}).items():
                            if field not in df.columns:
                                df[field] = ''
                            df.at[idx, field] = value
        
        # 5. Run validation using validator_utils
        print("\n🔄 Running invoice validation...")
        try:
            issues, problematic_invoices = validate_invoices(df)
            
            # Add validation results to dataframe
            df['Validation_Status'] = '✅ Valid'
            df['Issues_Found'] = ''
            
            # Mark problematic invoices
            if not problematic_invoices.empty:
                # Find indices of problematic invoices in main dataframe
                for idx, row in problematic_invoices.iterrows():
                    if idx < len(df):
                        df.loc[idx, 'Validation_Status'] = '⚠️ Issues Found'
                        df.loc[idx, 'Issues_Found'] = 'See validation report'
            
            print(f"✅ Validation completed: {len(issues)} issues found")
            
        except Exception as e:
            print(f"❌ Validation failed: {str(e)}")
            # Add default validation columns
            df['Validation_Status'] = '❌ Validation Failed'
            df['Issues_Found'] = str(e)
            issues = [f"Validation process failed: {str(e)}"]
            problematic_invoices = pd.DataFrame()
        
        # 6. Add additional validation columns for compatibility
        if 'Correct' not in df.columns:
            df['Correct'] = df['Validation_Status'].apply(lambda x: '✅' if x == '✅ Valid' else '❌')
        if 'Flagged' not in df.columns:
            df['Flagged'] = df['Validation_Status'].apply(lambda x: '🚩' if 'Issues' in x else '')
        if 'Modified Since Last Check' not in df.columns:
            df['Modified Since Last Check'] = ''
        if 'Late Upload' not in df.columns:
            df['Late Upload'] = ''
        
        # 7. Save result
        try:
            df.to_excel(result_path, index=False, engine='openpyxl')
            print(f"✅ Validation results saved: {result_path}")
        except Exception as e:
            print(f"❌ Failed to save results: {str(e)}")
            return False
        
        # 8. Handle snapshot comparison
        try:
            snapshot_dir = os.path.join("snapshots")
            os.makedirs(snapshot_dir, exist_ok=True)
            
            print("📸 Comparing with previous snapshot...")
            delta_report = compare_with_snapshot(df, snapshot_dir, today)
            
            print("💾 Saving current snapshot...")
            save_snapshot(df, snapshot_dir, today)
            
            print(f"📊 Delta report: {len(delta_report)} changes found")
            
        except Exception as e:
            print(f"⚠️ Snapshot handling failed: {str(e)}")
            delta_report = []
        
        # 9. Email the report
        try:
            print("📧 Sending email report...")
            send_email_report(result_path, zip_path, delta_report=delta_report)
            print("✅ Email report sent successfully")
        except Exception as e:
            print(f"⚠️ Email sending failed: {str(e)}")
        
        # 10. Print summary
        print("\n📋 Validation Summary:")
        print(f"  - Total invoices processed: {len(df)}")
        print(f"  - Issues found: {len(issues) if issues else 0}")
        print(f"  - Problematic invoices: {len(problematic_invoices)}")
        print(f"  - Results saved to: {result_path}")
        
        # Display summary statistics
        try:
            summary = get_invoice_summary(df)
            if 'amount_summary' in summary:
                print(f"  - Total amount: ₹{summary['amount_summary']['total_amount']:,.2f}")
                print(f"  - Average amount: ₹{summary['amount_summary']['average_amount']:,.2f}")
        except:
            pass
        
        print("✅ Validation workflow completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Validation workflow failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_validation_workflow()
    if success:
        print("🎉 All done!")
        exit(0)
    else:
        print("❌ Validation workflow failed")
        exit(1)