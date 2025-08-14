#!/usr/bin/env python3
"""
Enhanced Invoice Processor - Debug Version
Comprehensive CSV diagnostics and error handling
"""

import pandas as pd
import numpy as np
import logging
import os
import sys
from datetime import datetime, timedelta
import zipfile
from pathlib import Path

# Configure enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('debug_processor.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class CSVDiagnostic:
    """Comprehensive CSV file diagnostics"""
    
    def __init__(self, file_path):
        self.file_path = file_path
        self.diagnosis = {}
    
    def diagnose_file(self):
        """Run complete diagnostic on CSV file"""
        logger.info(f"🔍 Starting CSV diagnostic for: {self.file_path}")
        
        # Check 1: File existence
        if not os.path.exists(self.file_path):
            self.diagnosis['exists'] = False
            logger.error(f"❌ File does not exist: {self.file_path}")
            self.suggest_fixes()
            return False
        
        self.diagnosis['exists'] = True
        logger.info(f"✅ File exists: {self.file_path}")
        
        # Check 2: File size
        file_size = os.path.getsize(self.file_path)
        self.diagnosis['size'] = file_size
        logger.info(f"📊 File size: {file_size} bytes")
        
        if file_size == 0:
            logger.error("❌ File is empty!")
            self.create_sample_csv()
            return False
        
        # Check 3: File content preview
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                first_lines = [f.readline().strip() for _ in range(5)]
            
            logger.info("📝 First 5 lines of file:")
            for i, line in enumerate(first_lines, 1):
                if line:
                    logger.info(f"   Line {i}: {line[:100]}{'...' if len(line) > 100 else ''}")
                else:
                    logger.warning(f"   Line {i}: (empty)")
                    
        except UnicodeDecodeError:
            logger.error("❌ File encoding issue - trying different encodings")
            return self.try_different_encodings()
        
        # Check 4: CSV structure
        return self.analyze_csv_structure()
    
    def try_different_encodings(self):
        """Try reading file with different encodings"""
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                logger.info(f"🔄 Trying encoding: {encoding}")
                df = pd.read_csv(self.file_path, encoding=encoding, nrows=5)
                logger.info(f"✅ Successfully read with {encoding} encoding")
                self.diagnosis['encoding'] = encoding
                return True
            except Exception as e:
                logger.warning(f"   Failed with {encoding}: {str(e)}")
                continue
        
        logger.error("❌ Could not read file with any encoding")
        return False
    
    def analyze_csv_structure(self):
        """Analyze CSV structure and headers"""
        try:
            # Try reading just the header
            df_header = pd.read_csv(self.file_path, nrows=0)
            columns = df_header.columns.tolist()
            
            logger.info(f"📋 Found {len(columns)} columns:")
            for i, col in enumerate(columns, 1):
                logger.info(f"   Column {i}: '{col}'")
            
            self.diagnosis['columns'] = columns
            self.diagnosis['column_count'] = len(columns)
            
            if len(columns) == 0:
                logger.error("❌ No columns found in CSV!")
                return False
            
            # Try reading a few rows
            df_sample = pd.read_csv(self.file_path, nrows=3)
            logger.info(f"📊 Sample data shape: {df_sample.shape}")
            
            return True
            
        except pd.errors.EmptyDataError:
            logger.error("❌ CSV file is empty or has no data")
            return False
        except pd.errors.ParserError as e:
            logger.error(f"❌ CSV parsing error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error analyzing CSV: {str(e)}")
            return False
    
    def create_sample_csv(self):
        """Create a sample CSV file for testing"""
        logger.info("🔧 Creating sample CSV file...")
        
        sample_data = {
            'Invoice_ID': ['INV001', 'INV002', 'INV003'],
            'Invoice_Date': ['2024-08-01', '2024-08-02', '2024-08-03'],
            'Vendor_Name': ['Test Vendor A', 'Test Vendor B', 'Test Vendor C'],
            'Amount': [25000, 15000, 35000],
            'Location': ['Delhi', 'Mumbai', 'Bangalore'],
            'Status': ['Active', 'Active', 'Active']
        }
        
        df = pd.DataFrame(sample_data)
        sample_file = 'sample_invoices.csv'
        df.to_csv(sample_file, index=False)
        
        logger.info(f"✅ Sample CSV created: {sample_file}")
        logger.info("🔧 Try running the processor with: python3 enhanced_processor_debug.py sample_invoices.csv")
    
    def suggest_fixes(self):
        """Suggest fixes based on diagnosis"""
        logger.info("\n🔧 SUGGESTED FIXES:")
        
        if not self.diagnosis.get('exists', True):
            logger.info("1. Create the CSV file or check the file path")
            logger.info("2. Use the sample CSV generator in this script")
        
        if self.diagnosis.get('size', 1) == 0:
            logger.info("1. Add data to your CSV file")
            logger.info("2. Ensure the file has headers and at least one data row")

class EnhancedInvoiceProcessor:
    """Enhanced Invoice Processor with Excel output and diagnostics"""
    
    def __init__(self):
        self.setup_logging()
        self.required_columns = [
            'Invoice_ID', 'Invoice_Date', 'Vendor_Name', 'Amount'
        ]
        
    def setup_logging(self):
        """Setup comprehensive logging"""
        self.logger = logging.getLogger(__name__)
    
    def process_invoices(self, csv_file='invoices.csv'):
        """Main processing function with comprehensive error handling"""
        try:
            self.logger.info(f"Processing file: {csv_file}")
            
            # Step 1: Diagnose CSV file
            diagnostic = CSVDiagnostic(csv_file)
            if not diagnostic.diagnose_file():
                return False
            
            # Step 2: Load and validate data
            df = self.load_csv_safely(csv_file)
            if df is None:
                return False
            
            # Step 3: Add required fields
            df = self.add_enhanced_fields(df)
            
            # Step 4: Generate Excel report
            report_file = self.generate_excel_report(df)
            
            self.logger.info(f"✅ Processing completed successfully!")
            self.logger.info(f"📊 Excel report generated: {report_file}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Critical error in invoice processing: {str(e)}")
            return False
    
    def load_csv_safely(self, csv_file):
        """Safely load CSV with multiple fallback strategies"""
        strategies = [
            # Strategy 1: Standard loading
            {'encoding': 'utf-8', 'sep': ','},
            # Strategy 2: Different separator
            {'encoding': 'utf-8', 'sep': ';'},
            # Strategy 3: Different encoding
            {'encoding': 'latin-1', 'sep': ','},
            # Strategy 4: Tab separated
            {'encoding': 'utf-8', 'sep': '\t'},
        ]
        
        for i, strategy in enumerate(strategies, 1):
            try:
                self.logger.info(f"🔄 Loading strategy {i}: {strategy}")
                df = pd.read_csv(csv_file, **strategy)
                
                if df.empty:
                    self.logger.warning(f"   Strategy {i}: File loaded but is empty")
                    continue
                
                self.logger.info(f"✅ Strategy {i} successful! Shape: {df.shape}")
                self.logger.info(f"   Columns: {list(df.columns)}")
                return df
                
            except Exception as e:
                self.logger.warning(f"   Strategy {i} failed: {str(e)}")
                continue
        
        self.logger.error("❌ All loading strategies failed!")
        return None
    
    def add_enhanced_fields(self, df):
        """Add all 21 enhanced fields including S.No and Invoice_Creator_Name"""
        self.logger.info("📝 Adding enhanced fields...")
        
        # Add S.No column at the beginning
        df.insert(0, 'S.No', range(1, len(df) + 1))
        
        # Enhanced fields with default values
        enhanced_fields = {
            'Invoice_Creator_Name': 'System Generated',  # Required field
            'Invoice_Currency': 'INR',
            'Invoice_Location': 'Delhi',
            'TDS_Status': 'Not Applied',
            'RMS_Invoice_ID': lambda x: f"RMS_{x.get('Invoice_ID', 'UNK')}",
            'SCID': 'SC001',
            'MOP': 'Bank Transfer',
            'Account_Head': 'General',
            'Due_Date': lambda x: (pd.to_datetime(x.get('Invoice_Date', datetime.now())) + timedelta(days=30)).strftime('%Y-%m-%d'),
            'Days_Until_Due': 30,
            'Alert_Status': 'Normal',
            'GST_Number': '',
            'CGST_Rate': 9.0,
            'SGST_Rate': 9.0,
            'IGST_Rate': 18.0,
            'CGST_Amount': 0.0,
            'SGST_Amount': 0.0,
            'IGST_Amount': 0.0,
            'Total_Tax_Amount': 0.0,
            'Validation_Status': 'Pending',
            'Processing_Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        for field, default_value in enhanced_fields.items():
            if field not in df.columns:
                if callable(default_value):
                    df[field] = df.apply(default_value, axis=1)
                else:
                    df[field] = default_value
        
        self.logger.info(f"✅ Enhanced fields added. New shape: {df.shape}")
        return df
    
    def generate_excel_report(self, df):
        """Generate professional Excel report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"invoice_validation_report_{timestamp}.xlsx"
        
        try:
            with pd.ExcelWriter(report_file, engine='openpyxl') as writer:
                # Write main data
                df.to_excel(writer, sheet_name='Invoice_Report', index=False)
                
                # Get the workbook and worksheet
                workbook = writer.book
                worksheet = writer.sheets['Invoice_Report']
                
                # Apply formatting
                from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
                
                # Header formatting
                header_font = Font(bold=True, color="FFFFFF")
                header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                
                for col in range(1, len(df.columns) + 1):
                    cell = worksheet.cell(row=1, column=col)
                    cell.font = header_font
                    cell.fill = header_fill
                    cell.alignment = Alignment(horizontal="center")
                
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
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            self.logger.info(f"📊 Excel report generated: {report_file}")
            return report_file
            
        except Exception as e:
            self.logger.error(f"❌ Failed to generate Excel report: {str(e)}")
            # Fallback to CSV
            csv_file = f"invoice_validation_report_{timestamp}.csv"
            df.to_csv(csv_file, index=False)
            self.logger.info(f"📄 Fallback CSV report generated: {csv_file}")
            return csv_file

def main():
    """Main execution function with argument handling"""
    logger.info("🚀 Starting Enhanced Invoice Processor (Debug Version)")
    
    # Check for command line arguments
    csv_file = 'invoices.csv'
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
        logger.info(f"📁 Using CSV file from argument: {csv_file}")
    
    try:
        processor = EnhancedInvoiceProcessor()
        success = processor.process_invoices(csv_file)
        
        if success:
            logger.info("🎉 Processing completed successfully!")
        else:
            logger.error("❌ Processing failed - check the logs above")
            
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    main()
