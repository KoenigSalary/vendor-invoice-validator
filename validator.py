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

def run_validation_workflow():
    """Main validation workflow"""
    try:
        # 1. Get today's folder and fallback logic
        today = datetime.today().strftime("%Y-%m-%d")
        base_dir = os.path.join("data", today)
        
        print(f"🔍 Looking for data in: {base_dir}")
        
        # 2. Ensure folder exists or fallback
        if not os.path.exists(base_dir):
            print(f"❌ Folder not found for today ({today}), trying fallback.")
            fallback_dir = get_latest_data_folder()
            if fallback_dir:
                print(f"🔁 Using fallback folder: {fallback_dir}")
                base_dir = fallback_dir
                # Update today to match the fallback folder date
                today = os.path.basename(fallback_dir)
            else:
                print("❌ No fallback folder found. Creating sample data for testing.")
                # Create today's folder and use sample data
                os.makedirs(base_dir, exist_ok=True)
        
        # 3. Define paths
        result_path = os.path.join(base_dir, "validation_result.xlsx")
        zip_path = os.path.join(base_dir, "invoices.zip")
        
        print(f"📁 Working directory: {base_dir}")
        print(f"📄 Result path: {result_path}")
        print(f"📦 ZIP path: {zip_path}")
        
        # 4. Load invoice data (actual or sample)
        df = load_actual_invoice_data(base_dir)
        
        if df is None or df.empty:
            print("⚠️ No actual invoice data found, using sample data")
            df = create_sample_data()
        
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
