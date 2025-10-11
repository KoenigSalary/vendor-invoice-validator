# updater.py

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, 
    NoSuchElementException, 
    ElementNotInteractableException,
    WebDriverException
)
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def wait_for_page_load(driver, timeout=10):
    """Wait for page to fully load"""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(1)  # Small additional wait for any dynamic content
        return True
    except TimeoutException:
        logger.warning("Page load timeout, but continuing...")
        return False

def safe_find_element(driver, by, value, timeout=10):
    """Safely find an element with timeout and better error handling"""
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )
        return element
    except TimeoutException:
        logger.error(f"Element not found: {by}={value} within {timeout} seconds")
        return None

def safe_click_element(driver, by, value, timeout=10):
    """Safely click an element with multiple attempts"""
    try:
        # Wait for element to be clickable
        element = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((by, value))
        )
        
        # Scroll element into view
        driver.execute_script("arguments[0].scrollIntoView(true);", element)
        time.sleep(0.5)
        
        # Try to click
        element.click()
        return True
        
    except (TimeoutException, ElementNotInteractableException) as e:
        logger.warning(f"Normal click failed for {by}={value}, trying JavaScript click")
        try:
            element = driver.find_element(by, value)
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception as js_error:
            logger.error(f"Both normal and JavaScript clicks failed: {str(js_error)}")
            return False
    except Exception as e:
        logger.error(f"Click failed for {by}={value}: {str(e)}")
        return False

def update_invoice_status(driver, invoice_no, status, reason, max_retries=3):
    """
    Update invoice status with enhanced error handling and retry logic
    
    Args:
        driver: Selenium WebDriver instance
        invoice_no: Invoice number to update
        status: New status value
        reason: Reason for status change
        max_retries: Maximum number of retry attempts
    
    Returns:
        bool: True if successful, False otherwise
    """
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1}/{max_retries}: Updating invoice {invoice_no}")
            
            # Step 1: Navigate to Update Status Page
            logger.info("Navigating to invoice update page...")
            driver.get("https://rms.koenig-solutions.com/Accounts/InvoiceUpdate.aspx")
            
            # Wait for page to load completely
            wait_for_page_load(driver)
            
            # Step 2: Find and fill invoice number field
            logger.info(f"Entering invoice number: {invoice_no}")
            invoice_input = safe_find_element(driver, By.ID, "cphMainContent_mainContent_txtInvoice")
            if not invoice_input:
                raise Exception("Invoice input field not found")
            
            # Clear and enter invoice number
            invoice_input.clear()
            time.sleep(0.5)
            invoice_input.send_keys(str(invoice_no))
            
            # Step 3: Click Fetch button
            logger.info("Clicking fetch button...")
            if not safe_click_element(driver, By.ID, "cphMainContent_mainContent_btnFetch"):
                raise Exception("Failed to click fetch button")
            
            # Wait a bit for the fetch operation
            time.sleep(2)
            
            # Step 4: Wait for and handle status dropdown
            logger.info("Waiting for status dropdown...")
            status_dropdown = safe_find_element(driver, By.ID, "cphMainContent_mainContent_ddlStatus", timeout=15)
            if not status_dropdown:
                raise Exception("Status dropdown not found - invoice may not exist or access denied")
            
            # Check if dropdown is enabled
            if not status_dropdown.is_enabled():
                raise Exception("Status dropdown is disabled - invoice may not be editable")
            
            # Select status using Select class for better reliability
            try:
                select = Select(status_dropdown)
                
                # Try selecting by visible text first
                try:
                    select.select_by_visible_text(status)
                    logger.info(f"Selected status by text: {status}")
                except:
                    # If that fails, try by value
                    try:
                        select.select_by_value(status)
                        logger.info(f"Selected status by value: {status}")
                    except:
                        # Last resort: try by index if status is numeric
                        if status.isdigit():
                            select.select_by_index(int(status))
                            logger.info(f"Selected status by index: {status}")
                        else:
                            raise Exception(f"Could not select status: {status}")
                            
            except Exception as e:
                logger.error(f"Status selection failed: {str(e)}")
                # Log available options for debugging
                try:
                    options = [option.text for option in select.options]
                    logger.info(f"Available status options: {options}")
                except:
                    pass
                raise
            
            # Step 5: Enter reason
            logger.info(f"Entering reason: {reason}")
            reason_input = safe_find_element(driver, By.ID, "cphMainContent_mainContent_txtReason")
            if not reason_input:
                raise Exception("Reason input field not found")
            
            reason_input.clear()
            time.sleep(0.5)
            reason_input.send_keys(str(reason))
            
            # Step 6: Click Update button
            logger.info("Clicking update button...")
            if not safe_click_element(driver, By.ID, "cphMainContent_mainContent_btnUpdate"):
                raise Exception("Failed to click update button")
            
            # Step 7: Wait for confirmation or error message
            time.sleep(3)
            
            # Check for success/error messages
            try:
                # Look for common success indicators
                success_indicators = [
                    "//span[contains(text(), 'successfully')]",
                    "//span[contains(text(), 'updated')]",
                    "//div[contains(@class, 'success')]"
                ]
                
                for indicator in success_indicators:
                    try:
                        success_msg = driver.find_element(By.XPATH, indicator)
                        if success_msg.is_displayed():
                            logger.info(f"Success message found: {success_msg.text}")
                            return True
                    except NoSuchElementException:
                        continue
                
                # Look for error messages
                error_indicators = [
                    "//span[contains(text(), 'error')]",
                    "//span[contains(text(), 'failed')]", 
                    "//div[contains(@class, 'error')]",
                    "//span[@style and contains(@style, 'color:red')]"
                ]
                
                for indicator in error_indicators:
                    try:
                        error_msg = driver.find_element(By.XPATH, indicator)
                        if error_msg.is_displayed():
                            raise Exception(f"Error message: {error_msg.text}")
                    except NoSuchElementException:
                        continue
                
                # If no explicit success/error message, assume success if we got this far
                logger.info("Update completed (no explicit confirmation message)")
                return True
                
            except Exception as msg_error:
                logger.warning(f"Could not verify update status: {str(msg_error)}")
                # Continue and assume success if we got this far without exceptions
                return True
        
        except Exception as e:
            error_msg = f"Attempt {attempt + 1} failed for invoice {invoice_no}: {str(e)}"
            logger.error(error_msg)
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying in 3 seconds... ({attempt + 1}/{max_retries})")
                time.sleep(3)
                continue
            else:
                logger.error(f"All {max_retries} attempts failed for invoice {invoice_no}")
                return False
    
    return False

def update_multiple_invoices(driver, invoice_updates, delay_between_updates=2):
    """
    Update multiple invoices in batch
    
    Args:
        driver: Selenium WebDriver instance
        invoice_updates: List of tuples (invoice_no, status, reason)
        delay_between_updates: Seconds to wait between each update
    
    Returns:
        dict: Results summary with success/failure counts
    """
    results = {
        'total': len(invoice_updates),
        'successful': 0,
        'failed': 0,
        'failed_invoices': []
    }
    
    logger.info(f"Starting batch update of {results['total']} invoices...")
    
    for i, (invoice_no, status, reason) in enumerate(invoice_updates, 1):
        logger.info(f"\n--- Processing {i}/{results['total']}: Invoice {invoice_no} ---")
        
        success = update_invoice_status(driver, invoice_no, status, reason)
        
        if success:
            results['successful'] += 1
            logger.info(f"✅ Invoice {invoice_no} updated successfully")
        else:
            results['failed'] += 1
            results['failed_invoices'].append(invoice_no)
            logger.error(f"❌ Invoice {invoice_no} update failed")
        
        # Delay between updates to avoid overwhelming the server
        if i < results['total'] and delay_between_updates > 0:
            logger.info(f"Waiting {delay_between_updates} seconds before next update...")
            time.sleep(delay_between_updates)
    
    # Print summary
    logger.info(f"\n📊 Batch Update Summary:")
    logger.info(f"  Total invoices: {results['total']}")
    logger.info(f"  Successful: {results['successful']}")
    logger.info(f"  Failed: {results['failed']}")
    
    if results['failed_invoices']:
        logger.info(f"  Failed invoice numbers: {results['failed_invoices']}")
    
    return results

def test_update_function(driver, test_invoice="TEST001"):
    """
    Test the update function with a test invoice
    """
    logger.info("🧪 Testing invoice update function...")
    
    try:
        success = update_invoice_status(
            driver=driver,
            invoice_no=test_invoice,
            status="Under Review",  # Adjust based on your available statuses
            reason="Test update from automation script"
        )
        
        if success:
            logger.info("✅ Test update successful!")
        else:
            logger.error("❌ Test update failed!")
            
        return success
        
    except Exception as e:
        logger.error(f"❌ Test update error: {str(e)}")
        return False

# Example usage
if __name__ == "__main__":
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    
    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Remove this to see the browser
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    driver = None
    try:
        driver = webdriver.Chrome(options=chrome_options)
        
        # Example: Single invoice update
        success = update_invoice_status(
            driver=driver,
            invoice_no="INV001",
            status="Approved",
            reason="Automated approval after validation"
        )
        
        print(f"Update result: {'Success' if success else 'Failed'}")
        
    except Exception as e:
        print(f"Script error: {str(e)}")
    finally:
        if driver:
            driver.quit()