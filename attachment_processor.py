# attachment_processor.py

import os
import pandas as pd
import PyPDF2
from PIL import Image
import pytesseract
import docx2txt
import re

def extract_data_from_pdf(file_path):
    """Extract structured data from PDF invoices"""
    try:
        text = ""
        with open(file_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfFileReader(file)
            for page_num in range(pdf_reader.numPages):
                text += pdf_reader.getPage(page_num).extractText()
        
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
        
        # More field extraction patterns...
        
        return data
        
    except Exception as e:
        print(f"Error extracting data from PDF {file_path}: {str(e)}")
        return {}

def extract_data_from_image(file_path):
    """Extract structured data from image invoices using OCR"""
    try:
        # Use Tesseract OCR to extract text from image
        text = pytesseract.image_to_string(Image.open(file_path))
        
        # Then use similar regex patterns as the PDF function
        data = {}
        
        # Invoice Number pattern
        inv_num_match = re.search(r'Invoice\s*No\.?:?\s*([A-Za-z0-9\-/]+)', text)
        if inv_num_match:
            data['Invoice_Number'] = inv_num_match.group(1).strip()
        
        # Similar patterns for other fields...
        
        return data
        
    except Exception as e:
        print(f"Error extracting data from image {file_path}: {str(e)}")
        return {}

def extract_data_from_doc(file_path):
    """Extract structured data from Word documents"""
    try:
        # Extract text from Word document
        text = docx2txt.process(file_path)
        
        # Then use similar regex patterns as the PDF function
        # ...
        
        return {}
        
    except Exception as e:
        print(f"Error extracting data from document {file_path}: {str(e)}")
        return {}

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
            return df.iloc[0].to_dict()
        
        return {}
        
    except Exception as e:
        print(f"Error extracting data from spreadsheet {file_path}: {str(e)}")
        return {}
