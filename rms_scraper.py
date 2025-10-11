# rms_scraper.py

import os
import time
import glob
import csv
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, 
    NoSuchElementException, 
    WebDriverException,
    ElementNotInteractableException
)

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

def setup_chrome_driver(download_dir):
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    
    # Set download directory
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Use webdriver-manager - it handles ChromeDriver automatically
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    return driver

load_dotenv()

RMS_URL = os.getenv('RMS_URL')
RMS_USERNAME = os.getenv('RMS_USERNAME') 
RMS_PASSWORD = os.getenv('RMS_PASSWORD')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load .env credentials
load_dotenv()
USERNAME = os.getenv("RMS_USER")
PASSWORD = os.getenv("RMS_PASS")

def validate_credentials():
    """Validate that credentials are loaded"""
    if not USERNAME or not PASSWORD:
        raise ValueError("❌ RMS credentials not found. Check your .env file for RMS_USER and RMS_PASS")
    logger.info("✅ RMS credentials loaded")

def setup_chrome_driver(download_dir_abs, headless=True):
    """Set up Chrome driver with download preferences"""
    try:
        chrome_options = Options()
        
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Download preferences
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": download_dir_abs,
            "download.prompt_for_download": False,
            "directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_setting_values.automatic_downloads": 1,
            "profile.default_content_settings.popups": 0
        })
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(120)
        
        logger.info(f"✅ Chrome driver setup complete. Download directory: {download_dir_abs}")
        return driver
        
    except Exception as e:
        logger.error(f"❌ Failed to setup Chrome driver: {str(e)}")
        raise

def safe_login(driver, wait):
    """Perform login with enhanced error handling"""
    try:
        logger.info("🔐 Starting login process...")
        driver.get("https://rms.koenig-solutions.com/")
        
        # Wait for and fill username
        username_field = wait.until(EC.presence_of_element_located((By.ID, "txtUser")))
        username_field.clear()
        username_field.send_keys(USERNAME)
        
        # Fill password
        password_field = driver.find_element(By.ID, "txtPwd")
        password_field.clear()
        password_field.send_keys(PASSWORD)
        
        # Click login button
        login_btn = driver.find_element(By.ID, "btnSubmit")
        login_btn.click()
        
        # Wait for login to complete
        time.sleep(3)
        
        # Check if login was successful (look for logout link or dashboard elements)
        try:
            # Try to find elements that appear after successful login
            wait.until(EC.any_of(
                EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, "Logout")),
                EC.presence_of_element_located((By.ID, "ctl00_lnkLogOut")),
                EC.presence_of_element_located((By.CLASS_NAME, "dashboard"))
            ))
            logger.info("✅ Login successful")
            return True
            
        except TimeoutException:
            # Check if we're still on login page (indicates failed login)
            if "login" in driver.current_url.lower() or driver.find_elements(By.ID, "txtUser"):
                raise Exception("Login failed - check credentials")
            else:
                logger.info("✅ Login appears successful")
                return True
                
    except Exception as e:
        logger.error(f"❌ Login failed: {str(e)}")
        raise

def navigate_to_invoice_list(driver, wait):
    """Navigate to invoice list page"""
    try:
        logger.info("📄 Navigating to Invoice List page...")
        driver.get("https://rms.koenig-solutions.com/Accounts/InvoiceList.aspx")
        
        # Wait for date from field to appear
        wait.until(EC.presence_of_element_located((By.ID, "cphMainContent_mainContent_txtDateFrom")))
        logger.info("✅ Invoice List page loaded")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to load Invoice List page: {str(e)}")
        raise

def set_date_range(driver, start_date, end_date):
    """Set the date range for invoice search"""
    try:
        logger.info(f"📅 Setting date range: {start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}")
        
        # Clear and set start date
        date_from_field = driver.find_element(By.ID, "cphMainContent_mainContent_txtDateFrom")
        date_from_field.clear()
        time.sleep(0.5)
        date_from_field.send_keys(start_date.strftime("%d-%b-%Y"))
        
        # Clear and set end date
        date_to_field = driver.find_element(By.ID, "cphMainContent_mainContent_txtDateTo")
        date_to_field.clear()
        time.sleep(0.5)
        date_to_field.send_keys(end_date.strftime("%d-%b-%Y"))
        
        logger.info("✅ Date range set successfully")
        
    except Exception as e:
        logger.error(f"❌ Failed to set date range: {str(e)}")
        raise

def set_filters_and_search(driver):
    """Set search filters and perform search"""
    try:
        logger.info("🔍 Setting filters and searching...")
        
        # Set date change filter (Invoice Date)
        try:
            date_change_radio = driver.find_element(By.ID, "cphMainContent_mainContent_rbDateChange_0")
            if not date_change_radio.is_selected():
                date_change_radio.click()
        except Exception as e:
            logger.warning(f"Date change filter warning: {e}")
        
        # Set paid/unpaid filter (All)
        try:
            paid_unpaid_radio = driver.find_element(By.ID, "cphMainContent_mainContent_rbPaidUnPaid_2")
            if not paid_unpaid_radio.is_selected():
                paid_unpaid_radio.click()
        except Exception as e:
            logger.warning(f"Paid/unpaid filter warning: {e}")
        
        # Click search button
        search_btn = driver.find_element(By.ID, "cphMainContent_mainContent_btnSearch")
        search_btn.click()
        
        # Wait for results to load
        time.sleep(15)  # Extended wait for GitHub Actions
        
        logger.info("✅ Search completed")
        
    except Exception as e:
        logger.error(f"❌ Search failed: {str(e)}")
        raise

def extract_invoice_created_by(driver, download_dir_abs):
    """Extract Invoice Created By information"""
    try:
        logger.info("🔍 Extracting Inv Created By for each invoice...")
        inv_data = []
        
        # Wait for results table
        time.sleep(2)
        
        # Find all rows in the results table
        rows = driver.find_elements(By.XPATH, "//table[@id='cphMainContent_mainContent_rptShowAss']/tbody/tr")
        
        if not rows:
            # Try alternative selectors
            rows = driver.find_elements(By.XPATH, "//table[contains(@id, 'rptShow')]/tbody/tr")
        
        if not rows:
            logger.warning("⚠️ No invoice rows found in table")
            return []
        
        logger.info(f"📊 Found {len(rows)} rows to process")
        
        for i, row in enumerate(rows, 1):
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                
                if len(cells) < 8:
                    logger.debug(f"Row {i}: Insufficient columns ({len(cells)}), skipping")
                    continue
                
                # Extract invoice number and created by
                inv_no = cells[2].text.strip() if len(cells) > 2 else ""
                inv_created_by = cells[7].text.strip() if len(cells) > 7 else ""
                
                if inv_no:  # Only add rows with invoice numbers
                    inv_data.append((inv_no, inv_created_by))
                    logger.debug(f"Row {i}: {inv_no} -> {inv_created_by}")
                else:
                    logger.debug(f"Row {i}: No invoice number, skipping")
                    
            except Exception as e:
                logger.warning(f"⚠️ Row {i} parse error: {e}")
                continue
        
        # Save mapping to CSV
        if inv_data:
            map_file = os.path.join(download_dir_abs, "inv_created_by_map.csv")
            with open(map_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Invoice No", "Inv Created By"])
                writer.writerows(inv_data)
            
            logger.info(f"📁 Saved Inv Created By map: {map_file} ({len(inv_data)} records)")
        else:
            logger.warning("⚠️ No invoice data extracted")
        
        return inv_data
        
    except Exception as e:
        logger.error(f"❌ Failed to extract invoice data: {str(e)}")
        return []

def select_all_invoices(driver):
    """Select all invoices for download"""
    try:
        logger.info("☑️ Selecting all invoices...")
        
        # Try to find and click the header checkbox
        try:
            header_checkbox = driver.find_element(By.ID, "cphMainContent_mainContent_rptShowAss_chkHeader")
            if not header_checkbox.is_selected():
                driver.execute_script("arguments[0].click();", header_checkbox)
                time.sleep(1)
                logger.info("✅ Header checkbox selected")
            else:
                logger.info("✅ Header checkbox already selected")
        except Exception as e:
            logger.warning(f"⚠️ Header checkbox issue: {e}")
            
            # Try alternative method - select individual checkboxes
            checkboxes = driver.find_elements(By.XPATH, "//input[@type='checkbox' and contains(@id, 'chk')]")
            for checkbox in checkboxes:
                try:
                    if not checkbox.is_selected():
                        checkbox.click()
                except:
                    pass
            logger.info(f"✅ Selected {len(checkboxes)} individual checkboxes")
        
    except Exception as e:
        logger.error(f"❌ Failed to select invoices: {str(e)}")
        raise

def download_files(driver, download_dir_abs):
    """Download ZIP and Excel files"""
    try:
        logger.info("📥 Starting file downloads...")
        
        # Download ZIP file
        try:
            zip_btn = driver.find_element(By.ID, "cphMainContent_mainContent_btnDownload")
            zip_btn.click()
            logger.info("📥 ZIP download triggered")
        except Exception as e:
            logger.error(f"❌ ZIP download error: {e}")
        
        # Small delay between downloads
        time.sleep(2)
        
        # Download Excel file
        try:
            excel_btn = driver.find_element(By.ID, "cphMainContent_mainContent_ExportToExcel")
            driver.execute_script("arguments[0].click();", excel_btn)
            logger.info("📄 Excel export triggered")
        except Exception as e:
            logger.warning(f"⚠️ Excel export failed: {e}")
        
        # Wait for downloads to complete
        return wait_for_downloads(download_dir_abs)
        
    except Exception as e:
        logger.error(f"❌ Download process failed: {str(e)}")
        return None, None

def wait_for_downloads(download_dir_abs, max_wait_time=300):
    """Wait for downloads to complete and rename files"""
    logger.info("⏳ Waiting for downloads to complete...")
    
    xls_file = None
    zip_file = None
    
    for second in range(max_wait_time):
        try:
            files = os.listdir(download_dir_abs)
            
            # Look for ZIP file
            if not zip_file:
                zip_matches = [f for f in files if f.endswith(".zip") and not f.endswith(".crdownload")]
                if zip_matches:
                    zip_file = zip_matches[0]
                    logger.info(f"✅ ZIP file found: {zip_file}")
            
            # Look for XLS file
            if not xls_file:
                xls_matches = [f for f in files if f.endswith(".xls") and not f.endswith(".crdownload")]
                if xls_matches:
                    xls_file = xls_matches[0]
                    logger.info(f"✅ XLS file found: {xls_file}")
            
            # Check if both files are downloaded
            if zip_file and xls_file:
                break
                
            # Show progress every 10 seconds
            if second > 0 and second % 10 == 0:
                logger.info(f"⏳ Still waiting... ({second}s elapsed)")
            
            time.sleep(1)
            
        except Exception as e:
            logger.warning(f"⚠️ Error checking downloads: {e}")
            time.sleep(1)
            continue
    
    # Rename files to standard names
    final_zip = None
    final_xls = None
    
    try:
        if zip_file:
            src_zip = os.path.join(download_dir_abs, zip_file)
            dst_zip = os.path.join(download_dir_abs, "invoices.zip")
            
            if src_zip != dst_zip:  # Only rename if different
                if os.path.exists(dst_zip):
                    os.remove(dst_zip)
                os.rename(src_zip, dst_zip)
            
            final_zip = dst_zip
            logger.info("✅ Saved ZIP as invoices.zip")
        else:
            logger.warning("❌ ZIP file not downloaded")
        
        if xls_file:
            src_xls = os.path.join(download_dir_abs, xls_file)
            dst_xls = os.path.join(download_dir_abs, "invoice_download.xls")
            
            if src_xls != dst_xls:  # Only rename if different
                if os.path.exists(dst_xls):
                    os.remove(dst_xls)
                os.rename(src_xls, dst_xls)
            
            final_xls = dst_xls
            logger.info("✅ Saved XLS as invoice_download.xls")
        else:
            logger.warning("❌ XLS file not downloaded")
            
    except Exception as e:
        logger.error(f"❌ File renaming error: {e}")
    
    return final_zip, final_xls

def rms_download(start_date, end_date, headless=True):
    """
    Main function to download invoice data from RMS
    
    Args:
        start_date: Start date for invoice search
        end_date: End date for invoice search  
        headless: Run browser in headless mode
    
    Returns:
        str: Path to downloaded invoice file, or None if failed
    """
    
    # Validate inputs
    validate_credentials()
    
    if not isinstance(start_date, datetime):
        raise ValueError("start_date must be a datetime object")
    if not isinstance(end_date, datetime):
        raise ValueError("end_date must be a datetime object")
    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date")
    
    # Setup directories
    today_str = datetime.today().strftime("%Y-%m-%d")
    download_dir = os.path.join("data", today_str)
    os.makedirs(download_dir, exist_ok=True)
    download_dir_abs = os.path.abspath(download_dir)
    
    logger.info(f"🚀 Starting RMS download for {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"📁 Download directory: {download_dir_abs}")
    
    driver = None
    try:
        # Setup Chrome driver
        driver = setup_chrome_driver(download_dir_abs, headless)
        wait = WebDriverWait(driver, 15)
        
        # Perform login
        safe_login(driver, wait)
        
        # Navigate to invoice list
        navigate_to_invoice_list(driver, wait)
        
        # Set date range
        set_date_range(driver, start_date, end_date)
        
        # Set filters and search
        set_filters_and_search(driver)
        
        # Extract invoice created by data
        inv_data = extract_invoice_created_by(driver, download_dir_abs)
        
        # Select all invoices
        select_all_invoices(driver)
        
        # Download files
        zip_file, xls_file = download_files(driver, download_dir_abs)
        
        # Verify downloads
        final_invoice_path = os.path.join(download_dir_abs, "invoice_download.xls")
        
        if os.path.exists(final_invoice_path):
            file_size = os.path.getsize(final_invoice_path)
            logger.info(f"✅ RMS download completed successfully!")
            logger.info(f"📊 Invoice data: {len(inv_data)} records")
            logger.info(f"📄 Invoice file: {final_invoice_path} ({file_size} bytes)")
            return final_invoice_path
        else:
            logger.error("❌ Invoice file not found after download")
            return None
            
    except Exception as e:
        logger.error(f"❌ RMS download failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None
        
    finally:
        if driver:
            try:
                driver.quit()
                logger.info("🔚 Browser closed")
            except:
                pass

# For testing and manual execution
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Download invoice data from RMS')
    parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD)', 
                       default=(datetime.today() - timedelta(days=4)).strftime('%Y-%m-%d'))
    parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD)', 
                       default=datetime.today().strftime('%Y-%m-%d'))
    parser.add_argument('--show-browser', action='store_true', help='Show browser (not headless)')
    
    args = parser.parse_args()
    
    try:
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
        end_date = datetime.strptime(args.end, '%Y-%m-%d')
        
        result = rms_download(start_date, end_date, headless=not args.show_browser)
        
        if result:
            print(f"✅ Success! Downloaded to: {result}")
            exit(0)
        else:
            print("❌ Download failed!")
            exit(1)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        exit(1)
def get_github_actions_chrome_options():
    """Get Chrome options optimized for GitHub Actions environment"""
    options = webdriver.ChromeOptions()
    
    # Essential for GitHub Actions
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--remote-debugging-port=9222')
    
    # Memory and performance optimization
    options.add_argument('--memory-pressure-off')
    options.add_argument('--disable-background-timer-throttling')
    options.add_argument('--disable-backgrounding-occluded-windows')
    options.add_argument('--disable-renderer-backgrounding')
    
    # Network and loading optimization
    options.add_argument('--aggressive-cache-discard')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-plugins')
    options.add_argument('--disable-default-apps')
    
    # Set user agent to avoid detection
    options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    # Window size
    options.add_argument('--window-size=1920,1080')
    
    # Download preferences
    prefs = {
        "profile.default_content_settings.popups": 0,
        "profile.default_content_setting_values.notifications": 2,
        "profile.managed_default_content_settings.images": 2  # Block images for speed
    }
    options.add_experimental_option("prefs", prefs)
    
    return options

def safe_click_with_retry(driver, element, max_retries=5):
    """Enhanced click with retry logic for GitHub Actions"""
    for attempt in range(max_retries):
        try:
            # Scroll into view
            driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
            time.sleep(2)
            
            # Check if element is clickable
            if element.is_enabled() and element.is_displayed():
                element.click()
                return True
            else:
                # Try JavaScript click
                driver.execute_script("arguments[0].click();", element)
                return True
                
        except Exception as e:
            logger.warning(f"Click attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(3 * (attempt + 1))  # Exponential backoff
                continue
    
    return False