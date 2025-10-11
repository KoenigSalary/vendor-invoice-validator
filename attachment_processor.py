# attachment_processor.py

import os
import pandas as pd
import re
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Try to import PDF processing libraries
try:
    import PyPDF2
except ImportError:
    logger.warning("PyPDF2 not installed. PDF processing will be limited.")

# Try to import OCR libraries
try:
    from PIL import Image
    import pytesseract
except ImportError:
    logger.warning("PIL or pytesseract not installed. Image OCR will be limited.")

# Try to import document processing libraries
try:
    import docx2txt
except ImportError:
    logger.warning("docx2txt not installed. Word document processing will be limited.")

def extract_data_from_pdf(file_path):
    """Extract structured data from PDF invoices"""
    try:
        text = ""
        if 'PyPDF2' not in sys.modules:
            logger.warning("PyPDF2 not available. Using fallback method for PDF.")
            # Fallback method
            with open(file_path, 'rb') as file:
                # Just read the first 1000 bytes to check if it's a PDF
                header = file.read(1000)
                if b'%PDF' in header:
                    logger.info(f"File {file_path} appears to be a PDF but PyPDF2 is not available.")
            return {"error": "PDF processing library not available"}
        
        with open(file_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            for page_num in range(len(pdf_reader.pages)):
                text += pdf_reader.pages[page_num].extract_text()
        
        # Extract relevant fields using regex patterns
        data = {}
        
        # Invoice Number pattern
        inv_num_match = re.search(r'Invoice\s*No\.?:?\s*([A-Za-z0-9\-/]+)', text)
        if inv_num_match:
            data['Invoice_Number'] = inv_num_match.group(1).strip()
        
        # Invoice Date pattern
        date_match = re.search(r'Date\s*:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{1,2}\s+[A-Za-z]{3,}\s+\d{2,4})', text)
        if date_match:
            data['Invoice_Date'] = date_match.group(1).strip()
        
        # Amount pattern
        amount_match = re.search(r'Total\s*:?\s*[₹$€£]?\s*([\d,]+\.\d{2}|\d+)', text)
        if amount_match:
            data['Amount'] = amount_match.group(1).strip().replace(',', '')
        
        # GST Number pattern
        gst_match = re.search(r'GSTIN\s*:?\s*([0-9A-Z]{15})', text)
        if gst_match:
            data['GST_Number'] = gst_match.group(1).strip()
        
        # Currency pattern
        currency_match = re.search(r'Currency\s*:?\s*([A-Z]{3})', text)
        if currency_match:
            data['Invoice_Currency'] = currency_match.group(1).strip()
        
        # TDS pattern
        tds_match = re.search(r'TDS\s*:?\s*[₹$€£]?\s*([\d,]+\.\d{2}|\d+)', text)
        if tds_match:
            data['TDS'] = tds_match.group(1).strip().replace(',', '')
        
        # VAT pattern
        vat_match = re.search(r'VAT\s*:?\s*[₹$€£]?\s*([\d,]+\.\d{2}|\d+)', text)
        if vat_match:
            data['VAT'] = vat_match.group(1).strip().replace(',', '')
        
        # Total Invoice Value pattern
        total_match = re.search(r'Grand Total\s*:?\s*[₹$€£]?\s*([\d,]+\.\d{2}|\d+)', text)
        if total_match:
            data['Total_Invoice_Value'] = total_match.group(1).strip().replace(',', '')
        
        # Location pattern
        location_match = re.search(r'Location\s*:?\s*([A-Za-z, ]+)', text)
        if location_match:
            data['Location'] = location_match.group(1).strip()
        
        # Due Date pattern
        due_date_match = re.search(r'Due Date\s*:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{1,2}\s+[A-Za-z]{3,}\s+\d{2,4})', text)
        if due_date_match:
            data['Due_Date'] = due_date_match.group(1).strip()
        
        # Invoice ID pattern
        inv_id_match = re.search(r'Invoice ID\s*:?\s*([A-Za-z0-9\-/]+)', text)
        if inv_id_match:
            data['Invoice_ID'] = inv_id_match.group(1).strip()
        
        # Payment Method pattern
        mop_match = re.search(r'Payment Method\s*:?\s*([A-Za-z ]+)', text)
        if mop_match:
            data['MOP'] = mop_match.group(1).strip()
        
        # Account Head pattern
        ah_match = re.search(r'Account Head\s*:?\s*([A-Za-z0-9 ]+)', text)
        if ah_match:
            data['AH'] = ah_match.group(1).strip()
        
        # SCID pattern
        scid_match = re.search(r'SCID\s*:?\s*([A-Za-z0-9\-]+)', text)
        if scid_match:
            data['SCID'] = scid_match.group(1).strip()
        
        # Log extraction success
        logger.info(f"Successfully extracted {len(data)} fields from PDF: {file_path}")
        
        return data
        
    except Exception as e:
        logger.error(f"Error extracting data from PDF {file_path}: {str(e)}")
        return {"error": str(e)}

def extract_data_from_image(file_path):
    """Extract structured data from image invoices using OCR"""
    try:
        if 'pytesseract' not in sys.modules or 'Image' not in sys.modules:
            logger.warning("OCR libraries not available. Cannot process image.")
            return {"error": "OCR libraries not available"}
        
        # Use Tesseract OCR to extract text from image
        text = pytesseract.image_to_string(Image.open(file_path))
        
        # Then use similar regex patterns as the PDF function
        data = {}
        
        # Invoice Number pattern
        inv_num_match = re.search(r'Invoice\s*No\.?:?\s*([A-Za-z0-9\-/]+)', text)
        if inv_num_match:
            data['Invoice_Number'] = inv_num_match.group(1).strip()
        
        # Similar patterns for other fields...
        # (Copy the same regex patterns as used in extract_data_from_pdf)
        
        logger.info(f"Successfully extracted {len(data)} fields from image: {file_path}")
        return data
        
    except Exception as e:
        logger.error(f"Error extracting data from image {file_path}: {str(e)}")
        return {"error": str(e)}

def extract_data_from_doc(file_path):
    """Extract structured data from Word documents"""
    try:
        if 'docx2txt' not in sys.modules:
            logger.warning("docx2txt not available. Cannot process Word document.")
            return {"error": "Word document processing library not available"}
        
        # Extract text from Word document
        text = docx2txt.process(file_path)
        
        # Then use similar regex patterns as the PDF function
        data = {}
        
        # Invoice Number pattern
        inv_num_match = re.search(r'Invoice\s*No\.?:?\s*([A-Za-z0-9\-/]+)', text)
        if inv_num_match:
            data['Invoice_Number'] = inv_num_match.group(1).strip()
        
        # Similar patterns for other fields...
        # (Copy the same regex patterns as used in extract_data_from_pdf)
        
        logger.info(f"Successfully extracted {len(data)} fields from document: {file_path}")
        return data
        
    except Exception as e:
        logger.error(f"Error extracting data from document {file_path}: {str(e)}")
        return {"error": str(e)}

def extract_data_from_spreadsheet(file_path):
    """Extract structured data from spreadsheets"""
    try:
        # Read the spreadsheet
        if file_path.lower().endswith('.csv'):
            df = pd.read_csv(file_path)
        else:  # Excel files
            df = pd.read_excel(file_path)
        
        # Convert the first row to a dictionary
        if not df.empty:
            data = df.iloc[0].to_dict()
            
            # Clean up data
            result = {}
            for key, value in data.items():
                if pd.notna(value) and value != "":
                    # Try to standardize field names
                    clean_key = key.strip().replace(" ", "_")
                    result[clean_key] = str(value).strip()
            
            logger.info(f"Successfully extracted {len(result)} fields from spreadsheet: {file_path}")
            return result
        
        logger.warning(f"Spreadsheet is empty: {file_path}")
        return {}
        
    except Exception as e:
        logger.error(f"Error extracting data from spreadsheet {file_path}: {str(e)}")
        return {"error": str(e)}

def process_invoice_file(file_path):
    """Process a single invoice file based on its type"""
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return {"error": "File not found"}
    
    # Determine file type and process accordingly
    file_ext = os.path.splitext(file_path)[1].lower()
    
    if file_ext in ['.pdf']:
        return extract_data_from_pdf(file_path)
    elif file_ext in ['.jpg', '.jpeg', '.png', '.tiff', '.bmp']:
        return extract_data_from_image(file_path)
    elif file_ext in ['.doc', '.docx']:
        return extract_data_from_doc(file_path)
    elif file_ext in ['.xls', '.xlsx', '.csv']:
        return extract_data_from_spreadsheet(file_path)
    else:
        logger.warning(f"Unsupported file type: {file_ext}")
        return {"error": f"Unsupported file type: {file_ext}"}

def process_invoice_attachments(invoice_id, zip_path, extract_dir):
    """Process invoice attachments from ZIP file and validate against database records"""
    import zipfile
    import sys
    
    try:
        # Extract the ZIP file if not already extracted
        if not os.path.exists(extract_dir):
            os.makedirs(extract_dir, exist_ok=True)
            if os.path.exists(zip_path) and zipfile.is_zipfile(zip_path):
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                logger.info(f"Extracted ZIP file: {zip_path}")
            else:
                logger.error(f"Invalid ZIP file: {zip_path}")
                return {"status": "error", "message": f"Invalid ZIP file: {zip_path}"}
        
        # Find relevant attachment based on invoice_id
        invoice_files = []
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                # Check if filename contains the invoice ID or similar pattern
                file_path = os.path.join(root, file)
                
                # Skip very small files (likely not invoices)
                if os.path.getsize(file_path) < 1024:  # Skip files smaller than 1KB
                    continue
                
                # Try to match invoice ID in filename
                if invoice_id and invoice_id in file:
                    invoice_files.append(file_path)
                    logger.info(f"Found matching file for invoice {invoice_id}: {file}")
                # Also check common invoice file patterns
                elif re.search(r'inv(oice)?[-_]?\d+', file.lower()):
                    invoice_files.append(file_path)
                    logger.info(f"Found potential invoice file: {file}")
        
        if not invoice_files:
            logger.warning(f"No attachment found for invoice {invoice_id}")
            return {"status": "error", "message": f"No attachment found for invoice {invoice_id}"}
        
        # Process each file based on its type
        extracted_data = {}
        files_processed = []
        
        for file_path in invoice_files:
            file_data = process_invoice_file(file_path)
            if "error" not in file_data:
                extracted_data.update(file_data)
                files_processed.append(os.path.basename(file_path))
                
                # If we found a good match, we can stop processing additional files
                if len(extracted_data) >= 5:  # If we found at least 5 fields
                    break
        
        if not extracted_data:
            logger.warning(f"No data extracted from attachments for invoice {invoice_id}")
            return {
                "status": "error",
                "message": "No data could be extracted from attachments",
                "files_processed": files_processed
            }
        
        return {
            "status": "success",
            "data": extracted_data,
            "files_processed": files_processed
        }
        
    except Exception as e:
        logger.error(f"Error processing attachments for invoice {invoice_id}: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

# For testing
if __name__ == "__main__":
    # Test with a sample file if available
    import sys
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"Testing with file: {file_path}")
        result = process_invoice_file(file_path)
        print("Extracted data:")
        for key, value in result.items():
            print(f"  {key}: {value}")
    else:
        print("Usage: python attachment_processor.py <file_path>")