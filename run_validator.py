# run_validator.py

import os
import zipfile
import pandas as pd
from datetime import datetime
import fitz  # PyMuPDF for PDF extraction
from pathlib import Path
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
from snapshot_handler import compare_with_snapshot, save_snapshot
from email_sender import send_email_report

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_latest_data_folder(base="data"):
    """Find the most recent data folder"""
    try:
        if not os.path.exists(base):
            logger.error(f"Base directory '{base}' does not exist")
            return None
            
        folders = [f for f in os.listdir(base) if f.startswith("2025-")]
        if not folders:
            logger.warning(f"No date folders found in '{base}'")
            return None
            
        folders.sort(reverse=True)
        latest_folder = os.path.join(base, folders[0])
        logger.info(f"Latest data folder found: {latest_folder}")
        return latest_folder
        
    except Exception as e:
        logger.error(f"Error finding latest data folder: {str(e)}")
        return None

def setup_directories(base_dir):
    """Create necessary directories"""
    directories = {
        'unzip': os.path.join(base_dir, "unzipped"),
        'validated': os.path.join(base_dir, "validated_invoices"),
        'snapshots': "snapshots"
    }
    
    for name, path in directories.items():
        try:
            os.makedirs(path, exist_ok=True)
            logger.debug(f"Directory ensured: {path}")
        except Exception as e:
            logger.error(f"Failed to create directory {path}: {str(e)}")
            return None
    
    return directories

def read_invoice_excel(path):
    """Enhanced Excel reading with multiple engine fallback"""
    try:
        # Specify the engine explicitly (use openpyxl for .xlsx files)
        return pd.read_excel(path, engine="openpyxl")
    except Exception as e:
        print(f"[ERROR] Failed to read invoice file: {e}")
        logger.info(f"Reading invoice file: {path}")
        
        if not os.path.exists(path):
            logger.error(f"Invoice file not found: {path}")
            return None
        
        # Check file size
        file_size = os.path.getsize(path)
        logger.info(f"File size: {file_size} bytes")
        
        if file_size < 50:
            logger.error(f"File appears to be too small ({file_size} bytes)")
            return None
        
        # Try different engines
        engines = ['openpyxl', 'xlrd']
        
        for engine in engines:
            try:
                logger.debug(f"Attempting to read with engine: {engine}")
                df = pd.read_excel(path, engine=engine)
                logger.info(f"Successfully read with {engine}: {len(df)} rows, {len(df.columns)} columns")
                return df
            except Exception as e:
                logger.warning(f"Engine {engine} failed: {str(e)}")
                continue
        
        # Fallback to CSV reading (for TSV files with .xls extension)
        try:
            logger.debug("Attempting to read as CSV/TSV")
            # Try tab-separated first
            df = pd.read_csv(path, sep='\t')
            logger.info(f"Successfully read as TSV: {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.warning(f"TSV reading failed: {str(e)}")
            
            # Try comma-separated
            try:
                df = pd.read_csv(path, sep=',')
                logger.info(f"Successfully read as CSV: {len(df)} rows, {len(df.columns)} columns")
                return df
            except Exception as e:
                logger.error(f"All reading methods failed. Last error: {str(e)}")
                return None
                
    except Exception as e:
        logger.error(f"Unexpected error reading invoice file: {str(e)}")
>>>>>>> Stashed changes
        return None

def extract_text_from_file(file_path):
    """Enhanced text extraction with better error handling"""
    try:
        logger.debug(f"Extracting text from: {os.path.basename(file_path)}")
        
        # Check file size to avoid processing very large files
        file_size = os.path.getsize(file_path)
        if file_size > 50 * 1024 * 1024:  # 50MB limit
            logger.warning(f"File {file_path} is very large ({file_size} bytes), skipping")
            return ""
        
        text = ""
        with fitz.open(file_path) as doc:
            if len(doc) > 50:  # Limit pages to avoid huge documents
                logger.warning(f"PDF has {len(doc)} pages, processing only first 50")
                pages_to_process = 50
            else:
                pages_to_process = len(doc)
            
            for page_num in range(pages_to_process):
                try:
                    page = doc[page_num]
                    page_text = page.get_text()
                    text += page_text + "\n"
                except Exception as e:
                    logger.warning(f"Error extracting text from page {page_num}: {str(e)}")
                    continue
        
        # Clean and normalize text
        text = text.strip()
        if len(text) > 100000:  # Limit text size
            text = text[:100000]
            logger.debug("Text truncated to 100KB")
        
        logger.debug(f"Extracted {len(text)} characters from {os.path.basename(file_path)}")
        return text
        
    except Exception as e:
        logger.error(f"Failed to extract text from {file_path}: {str(e)}")
        return ""

def match_fields(text, df, return_row=False):
    """Enhanced field matching with multiple criteria"""
    try:
        if not text or df.empty:
            return ("❌ No Data", None) if return_row else "❌ No Data"
        
        # Normalize text for better matching
        text_normalized = text.upper().replace(" ", "").replace("-", "").replace("_", "")
        
        # Try different matching strategies
        matching_strategies = [
            'PurchaseInvNo',
            'VoucherNo', 
            'InvID'
        ]
        
        for column in matching_strategies:
            if column not in df.columns:
                continue
                
            logger.debug(f"Trying to match using column: {column}")
            
            for idx, row in df.iterrows():
                try:
                    # Get the value and normalize it
                    field_value = str(row.get(column, "")).strip()
                    if not field_value or field_value.lower() in ['nan', 'none', '']:
                        continue
                    
                    field_normalized = field_value.upper().replace(" ", "").replace("-", "").replace("_", "")
                    
                    # Check for exact match
                    if field_normalized in text_normalized:
                        logger.debug(f"Match found: {field_value} using {column}")
                        return ("✅ VALID", row) if return_row else "✅ VALID"
                    
                    # Check for partial match (at least 80% of the field value)
                    if len(field_normalized) >= 4 and field_normalized[:int(len(field_normalized)*0.8)] in text_normalized:
                        logger.debug(f"Partial match found: {field_value} using {column}")
                        return ("⚠️ PARTIAL", row) if return_row else "⚠️ PARTIAL"
                        
                except Exception as e:
                    logger.debug(f"Error matching row {idx}: {str(e)}")
                    continue
        
        # If no matches found with any strategy
        logger.debug("No matches found in PDF text")
        return ("❌ Not Matched", None) if return_row else "❌ Not Matched"
        
    except Exception as e:
        logger.error(f"Error in field matching: {str(e)}")
        return ("❌ Error", None) if return_row else "❌ Error"

def is_valid_zip(zip_path):
    """Enhanced ZIP file validation"""
    try:
        if not os.path.exists(zip_path):
            logger.error(f"ZIP file does not exist: {zip_path}")
            return False
        
        file_size = os.path.getsize(zip_path)
        if file_size < 100:
            logger.error(f"ZIP file is too small ({file_size} bytes): {zip_path}")
            return False
        
        logger.info(f"Validating ZIP file: {zip_path} ({file_size} bytes)")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Test the zip file integrity
            bad_file = zip_ref.testzip()
            if bad_file is None:
                # Count files in zip
                file_count = len(zip_ref.filelist)
                logger.info(f"ZIP file is valid with {file_count} files")
                return True
            else:
                logger.error(f"ZIP file is corrupt, bad file: {bad_file}")
                return False
                
    except zipfile.BadZipFile:
        logger.error(f"{zip_path} is not a valid ZIP file")
        return False
    except Exception as e:
        logger.error(f"Unexpected error validating ZIP: {str(e)}")
        return False

def extract_zip_file(zip_path, extract_dir):
    """Extract ZIP file with progress tracking"""
    try:
        logger.info(f"Extracting ZIP file to: {extract_dir}")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_list = zip_ref.filelist
            total_files = len(file_list)
            
            logger.info(f"Extracting {total_files} files...")
            
            extracted_files = []
            for i, file_info in enumerate(file_list, 1):
                try:
                    zip_ref.extract(file_info, extract_dir)
                    extracted_files.append(os.path.join(extract_dir, file_info.filename))
                    
                    if i % 100 == 0:  # Log progress every 100 files
                        logger.info(f"Extracted {i}/{total_files} files...")
                        
                except Exception as e:
                    logger.warning(f"Failed to extract {file_info.filename}: {str(e)}")
                    continue
            
            logger.info(f"Extraction complete: {len(extracted_files)}/{total_files} files extracted")
            return extracted_files
            
    except Exception as e:
        logger.error(f"Error extracting ZIP file: {str(e)}")
        return []

def process_pdf_file(args):
    """Process a single PDF file - designed for parallel processing"""
    file_path, df = args
    
    try:
        logger.debug(f"Processing: {os.path.basename(file_path)}")
        
        text = extract_text_from_file(file_path)
        if not text:
            return None
        
        result, matched_row = match_fields(text, df, return_row=True)
        
        if matched_row is not None:
            # Build result record
            record = {
                "File_Name": os.path.basename(file_path),
                "VoucherNo": matched_row.get("VoucherNo", ""),
                "Voucherdate": matched_row.get("Voucherdate", ""),
                "PurchaseInvNo": matched_row.get("PurchaseInvNo", ""),
                "PurchaseInvDate": matched_row.get("PurchaseInvDate", ""),
                "PartyName": matched_row.get("PartyName", ""),
                "GSTNO": matched_row.get("GSTNO", ""),
                "VATNumber": matched_row.get("VATNumber", ""),
                "TaxableValue": matched_row.get("TaxableValue", ""),
                "Currency": matched_row.get("Currency", ""),
                "IGST/VATInputLedger": matched_row.get("IGST/VATInputLedger", ""),
                "IGST/VATInputAmt": matched_row.get("IGST/VATInputAmt", ""),
                "CGSTInputLedger": matched_row.get("CGSTInputLedger", ""),
                "CGSTInputAmt": matched_row.get("CGSTInputAmt", ""),
                "SGSTInputLedger": matched_row.get("SGSTInputLedger", ""),
                "SGSTInputAmt": matched_row.get("SGSTInputAmt", ""),
                "Total": matched_row.get("Total", ""),
                "Inv Created By": matched_row.get("Inv Created By", "Unknown"),
                "InvID": matched_row.get("InvID", ""),
                "Narration": matched_row.get("Narration", ""),
                "Validation_Result": result,
                "Correct": "✅" if "VALID" in result else "⚠️" if "PARTIAL" in result else "",
                "Flagged": "🚩" if "Not Matched" in result or "Error" in result else "",
                "Modified Since Last Check": "",
                "Late Upload": "",
                "Processing_Time": datetime.now().isoformat()
            }
            
            return record
        else:
            logger.debug(f"No match found for: {os.path.basename(file_path)}")
            return None
            
    except Exception as e:
        logger.error(f"Error processing {file_path}: {str(e)}")
        return None

def validate_invoices():
<<<<<<< Updated upstream
    if not os.path.exists(XLS_PATH):
        print(f"[ERROR] Invoice sheet not found at {XLS_PATH}")
        return None

    df = read_invoice_excel(XLS_PATH)
    if df is None:
        return None

    print(f"[INFO] Invoice sheet loaded: {len(df)} rows.")

    # === Load mapping file (if exists) ===
    if os.path.exists(INV_CREATOR_MAP_PATH):
        df_map = pd.read_csv(INV_CREATOR_MAP_PATH)
        if "InvID" not in df_map.columns:
            possible_col = [col for col in df_map.columns if "id" in col.lower()]
            if possible_col:
                df_map = df_map.rename(columns={possible_col[0]: "InvID"})
            else:
                print("[WARN] 'InvID' column not found in map. Assigning Unknown.")
                df["Inv Created By"] = "Unknown"
                df_map = pd.DataFrame(columns=["InvID", "Inv Created By"])
        df = df.merge(df_map, on="InvID", how="left")
        print(f"[INFO] Uploader mapping loaded from: {INV_CREATOR_MAP_PATH}")
    else:
        print("[WARN] inv_created_by_map.csv not found. Assigning all as Unknown.")
        df["Inv Created By"] = "Unknown"

def is_valid_zip(zip_path):
    """Checks if the file is a valid zip file."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Attempt to list the files in the ZIP to check if it's a valid zip file
            zip_ref.testzip()
        return True
    except zipfile.BadZipFile:
        return False

    # === Step 1: Define paths ===
    ZIP_PATH = os.path.join(base_dir, "invoices.zip")

    # === Unzip invoices ===
    if os.path.exists(ZIP_PATH):
        if is_valid_zip(ZIP_PATH):  # Check if the ZIP file is valid
            with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
                zip_ref.extractall(UNZIP_DIR)
            print(f"[INFO] Invoices unzipped to: {UNZIP_DIR}")
        else:
            print(f"[ERROR] The file at {ZIP_PATH} is not a valid ZIP file.")
            return None  # Exit function if the ZIP file is invalid
    else:
        print(f"[ERROR] Invoices ZIP file not found: {ZIP_PATH}")
=======
    """Main validation function with enhanced error handling and performance"""
    try:
        start_time = time.time()
        logger.info("🚀 Starting invoice validation process...")
        
        # === Step 1: Setup directories and paths ===
        DOWNLOAD_FOLDER = "data"
        TODAY_FOLDER = datetime.today().strftime("%Y-%m-%d")
        base_dir = os.path.join(DOWNLOAD_FOLDER, TODAY_FOLDER)
        
        if not os.path.exists(base_dir):
            logger.info(f"Today's folder '{TODAY_FOLDER}' not found. Looking for latest available data...")
            base_dir = get_latest_data_folder(DOWNLOAD_FOLDER)
            if not base_dir:
                logger.error("No previous data folder found. Aborting validation.")
                return None
            logger.info(f"Using fallback folder: {base_dir}")
        
        # Setup directory structure
        directories = setup_directories(base_dir)
        if not directories:
            logger.error("Failed to setup directories")
            return None
        
        # Define file paths
        XLS_PATH = os.path.join(base_dir, "invoice_download.xls")
        ZIP_PATH = os.path.join(base_dir, "invoices.zip")
        RESULT_PATH = os.path.join(base_dir, "validation_result.xlsx")
        
        logger.info(f"Working directory: {base_dir}")
        logger.info(f"Excel file: {XLS_PATH}")
        logger.info(f"ZIP file: {ZIP_PATH}")
        
        # === Step 2: Load invoice data ===
        if not os.path.exists(XLS_PATH):
            logger.error(f"Invoice sheet not found at {XLS_PATH}")
            return None
        
        df = read_invoice_excel(XLS_PATH)
        if df is None:
            logger.error("Failed to load invoice data")
            return None
        
        logger.info(f"Invoice sheet loaded: {len(df)} rows, {len(df.columns)} columns")
        
        # === Step 3: Validate and extract ZIP file ===
        if not os.path.exists(ZIP_PATH):
            logger.error(f"ZIP file not found at {ZIP_PATH}")
            return None
        
        if not is_valid_zip(ZIP_PATH):
            logger.error("Invalid ZIP file")
            return None
        
        extracted_files = extract_zip_file(ZIP_PATH, directories['unzip'])
        if not extracted_files:
            logger.error("No files extracted from ZIP")
            return None
        
        # Filter for PDF files only
        pdf_files = [f for f in extracted_files if f.lower().endswith('.pdf')]
        logger.info(f"Found {len(pdf_files)} PDF files to process")
        
        if not pdf_files:
            logger.warning("No PDF files found in extracted content")
            return None
        
        # === Step 4: Process PDF files (with parallel processing) ===
        logger.info("🔄 Starting PDF validation...")
        results = []
        
        # Prepare arguments for parallel processing
        process_args = [(pdf_file, df) for pdf_file in pdf_files]
        
        # Use ThreadPoolExecutor for parallel processing
        max_workers = min(4, len(pdf_files))  # Limit to 4 threads or number of files
        logger.info(f"Processing with {max_workers} parallel workers")
        
        processed_count = 0
        matched_count = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_file = {executor.submit(process_pdf_file, args): args[0] for args in process_args}
            
            # Process completed tasks
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                processed_count += 1
                
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                        matched_count += 1
                    
                    # Log progress every 50 files
                    if processed_count % 50 == 0:
                        logger.info(f"Progress: {processed_count}/{len(pdf_files)} files processed, {matched_count} matched")
                        
                except Exception as e:
                    logger.error(f"Error processing {os.path.basename(file_path)}: {str(e)}")
        
        logger.info(f"📊 Processing complete: {matched_count}/{len(pdf_files)} PDFs matched")
        
        # === Step 5: Save results ===
        if results:
            result_df = pd.DataFrame(results)
            try:
                result_df.to_excel(RESULT_PATH, index=False, engine='openpyxl')
                logger.info(f"✅ Validation results saved: {RESULT_PATH}")
            except Exception as e:
                logger.error(f"Failed to save Excel results: {str(e)}")
                # Fallback to CSV
                csv_path = RESULT_PATH.replace('.xlsx', '.csv')
                result_df.to_csv(csv_path, index=False)
                logger.info(f"✅ Results saved as CSV: {csv_path}")
                RESULT_PATH = csv_path
        else:
            logger.warning("No validation results to save")
            # Create empty result file
            pd.DataFrame().to_excel(RESULT_PATH, index=False, engine='openpyxl')
            result_df = pd.DataFrame()
        
        # === Step 6: Snapshot comparison and delta reporting ===
        try:
            logger.info("📸 Performing snapshot comparison...")
            snapshot_dir = directories['snapshots']
            
            delta_report = compare_with_snapshot(result_df, snapshot_dir, TODAY_FOLDER)
            save_snapshot(result_df, snapshot_dir, TODAY_FOLDER)
            
            # Log delta statistics
            if 'stats' in delta_report:
                stats = delta_report['stats']
                logger.info(f"Delta summary - Added: {stats.get('added', 0)}, Modified: {stats.get('modified', 0)}, Deleted: {stats.get('deleted', 0)}")
                
        except Exception as e:
            logger.error(f"Snapshot comparison failed: {str(e)}")
            delta_report = {"added": pd.DataFrame(), "modified": pd.DataFrame(), "deleted": pd.DataFrame()}
        
        # === Step 7: Send email report ===
        try:
            logger.info("📧 Sending email report...")
            send_email_report(RESULT_PATH, ZIP_PATH, delta_report=delta_report)
            logger.info("✅ Email report sent successfully")
        except Exception as e:
            logger.error(f"Failed to send email report: {str(e)}")
        
        # === Final summary ===
        end_time = time.time()
        processing_time = end_time - start_time
        
        logger.info("🎉 Validation process completed!")
        logger.info(f"📊 Summary:")
        logger.info(f"  - Total PDFs processed: {len(pdf_files)}")
        logger.info(f"  - Successfully matched: {matched_count}")
        logger.info(f"  - Match rate: {(matched_count/len(pdf_files)*100):.1f}%")
        logger.info(f"  - Processing time: {processing_time:.1f} seconds")
        logger.info(f"  - Results saved to: {RESULT_PATH}")
        
        return RESULT_PATH
        
    except Exception as e:
        logger.error(f"❌ Validation process failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
>>>>>>> Stashed changes
        return None

def validate_invoices_simple():
    """Simple wrapper for backward compatibility"""
    return validate_invoices()

# Test function
def test_validator():
    """Test the validator with available data"""
    try:
        logger.info("🧪 Testing validator...")
        result = validate_invoices()
        
        if result:
            logger.info(f"✅ Test successful! Results saved to: {result}")
            return True
        else:
            logger.error("❌ Test failed!")
            return False
            
    except Exception as e:
        logger.error(f"❌ Test error: {str(e)}")
        return False

#Run Automatically if script executed directly
if __name__ == "__main__":
    # Set up argument parsing for different modes
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            test_validator()
        else:
            print("Usage:")
            print("  python run_validator.py        - Run validation")
            print("  python run_validator.py test   - Run test mode")
    else:
        validate_invoices()
