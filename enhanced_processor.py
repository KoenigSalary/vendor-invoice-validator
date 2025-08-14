#!/usr/bin/env python3
"""
Enhanced Invoice Processor with Excel Output - Fixed Version
Generates professional Excel reports without merged cells
Includes all 28 enhanced fields with proper formatting
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import os
import sys
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EnhancedInvoiceProcessor:
    def __init__(self):
        self.current_date = datetime.now()
        
        # GST State Codes
        self.gst_state_codes = {
            'Andhra Pradesh': '37', 'Arunachal Pradesh': '12',
            'Assam': '18', 'Bihar': '10',
            'Chhattisgarh': '22', 'Goa': '30',
            'Gujarat': '24', 'Haryana': '06',
            'Himachal Pradesh': '02', 'Jharkhand': '20',
            'Karnataka': '29', 'Kerala': '32',
            'Madhya Pradesh': '23', 'Maharashtra': '27',
            'Manipur': '14', 'Meghalaya': '17',
            'Mizoram': '15', 'Nagaland': '13',
            'Odisha': '21', 'Punjab': '03',
            'Rajasthan': '08', 'Sikkim': '11',
            'Tamil Nadu': '33', 'Telangana': '36',
            'Tripura': '16', 'Uttar Pradesh': '09',
            'Uttarakhand': '05', 'West Bengal': '19',
            'Delhi': '07', 'Chandigarh': '04'
        }
        
        # International VAT rates
        self.international_vat = {
            'Dubai': 5.0, 'UK': 20.0, 'Australia': 10.0,
            'Canada': 13.0, 'Germany': 19.0, 'Singapore': 8.0,
            'Netherlands': 21.0, 'New Zealand': 15.0,
            'Malaysia': 6.0, 'South Africa': 15.0,
            'Saudi Arabia': 15.0, 'Japan': 10.0
        }
        
        # Koenig locations
        self.koenig_locations = [
            'Delhi', 'Goa', 'Bangalore', 'Dehradun', 
            'Chennai', 'Gurgaon', 'Mumbai'
        ]

    def load_invoices(self, file_path):
        """Load invoices from CSV file with enhanced error handling"""
        try:
            logger.info(f"Loading invoices from: {file_path}")
            
            # Try different encodings
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    logger.info(f"Successfully loaded with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise Exception("Could not decode file with any encoding")
            
            logger.info(f"Loaded {len(df)} invoices for processing")
            return df
            
        except Exception as e:
            logger.error(f"Error loading invoices: {e}")
            raise

    def calculate_taxes(self, row):
        """Calculate GST/VAT based on location"""
        amount = float(row.get('Amount', 0))
        location = str(row.get('Location', '')).strip()
        
        # Default values
        cgst = sgst = igst = vat = 0.0
        
        if location in self.koenig_locations:
            # Indian GST calculation
            if location == 'Delhi':  # Same state
                cgst = amount * 0.09  # 9% CGST
                sgst = amount * 0.09  # 9% SGST
            else:  # Inter-state
                igst = amount * 0.18  # 18% IGST
        
        elif location in self.international_vat:
            # International VAT
            vat_rate = self.international_vat[location] / 100
            vat = amount * vat_rate
        
        return {
            'CGST_Amount': cgst,
            'SGST_Amount': sgst,
            'IGST_Amount': igst,
            'VAT_Amount': vat,
            'Total_Tax': cgst + sgst + igst + vat,
            'Total_Amount': amount + cgst + sgst + igst + vat
        }

    def validate_invoice(self, row):
        """Validate invoice data and return status"""
        issues = []
        
        # Required field validation
        if not row.get('Invoice_ID'):
            issues.append('Missing Invoice ID')
        
        if not row.get('Vendor_Name'):
            issues.append('Missing Vendor Name')
        
        try:
            amount = float(row.get('Amount', 0))
            if amount <= 0:
                issues.append('Invalid Amount')
        except:
            issues.append('Invalid Amount Format')
        
        # Due date validation
        try:
            if row.get('Due_Date'):
                due_date = pd.to_datetime(row['Due_Date'])
                days_until_due = (due_date - self.current_date).days
                if days_until_due <= 5:
                    issues.append('Due Date Alert: <= 5 days')
        except:
            pass
        
        # Determine status
        if not issues:
            return 'Passed', ''
        elif len(issues) == 1 and 'Due Date Alert' in issues[0]:
            return 'Warning', issues[0]
        else:
            return 'Failed', '; '.join(issues)

    def enhance_data(self, df):
        """Add enhanced fields to the dataframe"""
        try:
            logger.info("Adding enhanced fields...")
            
            # Add S.No column at the beginning
            df.insert(0, 'S_No', range(1, len(df) + 1))
            
            # Add all enhanced fields
            enhanced_fields = {
                'Invoice_Creator_Name': 'System Generated',
                'Invoice_Currency': df.get('Currency', 'INR'),
                'TDS_Status': 'Pending',
                'RMS_Invoice_ID': df.get('Invoice_ID', '') + '_RMS',
                'SCID': 'SC' + df.index.astype(str).str.zfill(4),
                'MOP': 'Bank Transfer',
                'Account_Head': 'Operations',
                'PO_Number': 'PO' + df.index.astype(str).str.zfill(6),
                'Invoice_Category': 'Services',
                'Payment_Terms': 'Net 30',
                'Approval_Status': 'Pending Approval',
                'Description': 'Professional Services',
                'Vendor_Address': 'Address on File',
                'Invoice_Type': 'Standard'
            }
            
            # Add Due_Date if not present
            if 'Due_Date' not in df.columns:
                df['Due_Date'] = pd.to_datetime(df.get('Invoice_Date', self.current_date)) + timedelta(days=30)
            
            # Add enhanced fields to dataframe
            for field, default_value in enhanced_fields.items():
                if field not in df.columns:
                    df[field] = default_value
            
            # Calculate taxes and validation for each row
            tax_data = []
            validation_data = []
            
            for idx, row in df.iterrows():
                # Calculate taxes
                tax_info = self.calculate_taxes(row)
                tax_data.append(tax_info)
                
                # Validate invoice
                status, issues = self.validate_invoice(row)
                validation_data.append({'Validation_Status': status, 'Validation_Issues': issues})
            
            # Add tax and validation data
            for key in tax_data[0].keys():
                df[key] = [item[key] for item in tax_data]
            
            for key in validation_data[0].keys():
                df[key] = [item[key] for item in validation_data]
            
            logger.info(f"Enhanced fields added. New shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Error enhancing data: {e}")
            raise

    def generate_excel_report(self, df):
        """Generate Excel report with professional formatting"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"invoice_validation_report_{timestamp}.xlsx"
            
            # Create Excel writer
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Invoice Report', index=False)
            
            # Load workbook for formatting
            wb = load_workbook(filename)
            ws = wb['Invoice Report']
            
            # Define styles
            header_font = Font(bold=True, color="FFFFFF")
            header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
            center_alignment = Alignment(horizontal="center", vertical="center")
            border = Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )
            
            # Format headers
            for cell in ws[1]:
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = center_alignment
                cell.border = border
            
            # Format data cells
            for row in ws.iter_rows(min_row=2):
                for cell in row:
                    cell.border = border
                    cell.alignment = Alignment(horizontal="left", vertical="center")
            
            # Auto-adjust column widths
            for column in ws.columns:
                max_length = 0
                column_letter = column[0].column_letter
                
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                
                adjusted_width = min(max_length + 2, 50)
                ws.column_dimensions[column_letter].width = adjusted_width
            
            # Freeze first row
            ws.freeze_panes = "A2"
            
            # Save formatted workbook
            wb.save(filename)
            
            logger.info(f"Excel report generated: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error generating Excel report: {e}")
            raise

    def process_invoices(self, input_file):
        """Main processing function"""
        try:
            logger.info(f"Starting invoice processing: {input_file}")
            
            # Load and enhance data
            df = self.load_invoices(input_file)
            df_enhanced = self.enhance_data(df)
            
            # Generate Excel report
            excel_file = self.generate_excel_report(df_enhanced)
            
            # Generate summary
            total_invoices = len(df_enhanced)
            passed = len(df_enhanced[df_enhanced['Validation_Status'] == 'Passed'])
            warnings = len(df_enhanced[df_enhanced['Validation_Status'] == 'Warning'])
            failed = len(df_enhanced[df_enhanced['Validation_Status'] == 'Failed'])
            
            summary = {
                'total_invoices': total_invoices,
                'passed': passed,
                'warnings': warnings,
                'failed': failed,
                'excel_file': excel_file,
                'processing_date': self.current_date.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            logger.info(f"Processing completed successfully!")
            logger.info(f"Total: {total_invoices}, Passed: {passed}, Warnings: {warnings}, Failed: {failed}")
            
            return summary
            
        except Exception as e:
            logger.error(f"Critical error in invoice processing: {e}")
            raise

def main():
    """Main execution function"""
    try:
        input_file = sys.argv[1] if len(sys.argv) > 1 else 'invoices.csv'
        
        # Check if input file exists
        if not os.path.exists(input_file):
            logger.error(f"Input file not found: {input_file}")
            return
        
        # Process invoices
        processor = EnhancedInvoiceProcessor()
        summary = processor.process_invoices(input_file)
        
        # Print summary
        print(f"✅ Processing completed successfully!")
        print(f"📊 Excel report generated: {summary['excel_file']}")
        print(f"📋 Summary: {summary['total_invoices']} total, "
              f"{summary['passed']} passed, "
              f"{summary['warnings']} warnings, "
              f"{summary['failed']} failed")
        
    except Exception as e:
        logger.error(f"Main execution error: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
