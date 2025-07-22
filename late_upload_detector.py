# late_upload_detector.py

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import os

class LateUploadDetector:
    def __init__(self):
        self.deadline_days = 5  # 5-day deadline
        
    def check_late_uploads(self):
        """Check for invoices uploaded after the 5-day deadline"""
        try:
            late_invoices = []
            current_date = datetime.now()
            
            # Check recent data folders
            data_dir = Path("data")
            if not data_dir.exists():
                return late_invoices
            
            # Look at folders from the last week
            for i in range(7):
                check_date = current_date - timedelta(days=i)
                folder_name = check_date.strftime("%Y-%m-%d")
                folder_path = data_dir / folder_name
                
                if folder_path.exists():
                    invoice_file = folder_path / "invoice_download.xls"
                    if invoice_file.exists():
                        try:
                            df = pd.read_excel(invoice_file)
                            late_invoices.extend(self._check_dataframe_for_late_uploads(df))
                        except Exception as e:
                            print(f"⚠️ Could not process {invoice_file}: {e}")
            
            return late_invoices
            
        except Exception as e:
            print(f"❌ Error checking late uploads: {str(e)}")
            return []
    
    def _check_dataframe_for_late_uploads(self, df):
        """Check a dataframe for late uploads"""
        late_invoices = []
        
        try:
            # Ensure required columns exist
            required_columns = ['PurchaseInvDate', 'UploadDate']  # Adjust column names as needed
            
            for _, row in df.iterrows():
                try:
                    # Parse dates
                    invoice_date = pd.to_datetime(row.get('PurchaseInvDate'), errors='coerce')
                    upload_date = pd.to_datetime(row.get('UploadDate', datetime.now()), errors='coerce')
                    
                    if pd.isna(invoice_date) or pd.isna(upload_date):
                        continue
                    
                    # Calculate days between invoice date and upload date
                    days_diff = (upload_date - invoice_date).days
                    
                    if days_diff > self.deadline_days:
                        late_invoices.append({
                            'Invoice_Number': row.get('InvoiceNumber', 'N/A'),
                            'Vendor_Name': row.get('VendorName', 'N/A'),
                            'Invoice_Date': invoice_date.strftime('%Y-%m-%d'),
                            'Upload_Date': upload_date.strftime('%Y-%m-%d'),
                            'Days_Late': days_diff - self.deadline_days,
                            'Uploaded_By': row.get('UploadedBy', 'N/A')
                        })
                        
                except Exception as e:
                    continue
                    
        except Exception as e:
            print(f"⚠️ Error processing dataframe: {str(e)}")
            
        return late_invoices