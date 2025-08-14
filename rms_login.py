#!/usr/bin/env python3
# rms_login.py - Fixed and updated version

import os
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def make_driver(download_dir=None, driver_path=None):
    """Create and configure Chrome driver with download settings"""
    
    # Use provided paths or defaults
    download_directory = download_dir or os.getenv("DOWNLOAD_DIR") or os.path.join(os.path.expanduser("~"), "Downloads")
    chromedriver_path = driver_path or os.getenv("CHROMEDRIVER_PATH") or "/opt/homebrew/bin/chromedriver"
    
    # Ensure download directory exists
    os.makedirs(download_directory, exist_ok=True)
    
    print(f"🔧 Setting up Chrome driver...")
    print(f"   Download directory: {download_directory}")
    print(f"   ChromeDriver path: {chromedriver_path}")
    
    options = Options()
    prefs = {
        "download.default_directory": os.path.abspath(download_directory),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True
    }
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    # Create service with chromedriver path
    service = Service(chromedriver_path)
    
    return webdriver.Chrome(service=service, options=options)

def get_driver(download_dir=None, driver_path=None):
    """Alternative name for make_driver"""
    return make_driver(download_dir, driver_path)

def init_driver(download_dir=None, driver_path=None):
    """Another alternative name for make_driver"""
    return make_driver(download_dir, driver_path)

def login_rms(driver, username, password):
    """Login to RMS system"""
    try:
        print("🚀 Logging into RMS...")
        driver.get("https://rms.koenig-solutions.com/")
        
        # Wait for login form and enter credentials
        username_field = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "txtUser"))
        )
        username_field.clear()
        username_field.send_keys(username)
        
        password_field = driver.find_element(By.ID, "txtPwd")
        password_field.clear()
        password_field.send_keys(password)
        
        # Submit login form
        submit_button = driver.find_element(By.ID, "btnSubmit")
        submit_button.click()
        
        # Wait for successful login (URL change or login form disappearance)
        try:
            # Wait for either successful redirect or login form to disappear
            WebDriverWait(driver, 15).until(
                lambda d: "Login.aspx" not in d.current_url or 
                         not d.find_elements(By.ID, "txtUser")
            )
            
            # Check if we're still on login page (login failed)
            if "Login.aspx" in driver.current_url:
                print("❌ Login failed - still on login page")
                return False
            
            print("✅ Login successful")
            return True
            
        except Exception as e:
            print(f"❌ Login timeout or failed: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Error during login: {e}")
        return False

def login(driver, username, password):
    """Alternative name for login_rms"""
    return login_rms(driver, username, password)

def do_login(driver, username, password):
    """Another alternative name for login_rms"""
    return login_rms(driver, username, password)

def download_all_rms_data(salary_month, salary_year, bank_start_date, bank_end_date):
    """Download all RMS data - compatibility function for salary_reconciliation_agent.py"""
    
    username = os.getenv("RMS_USERNAME") or os.getenv("RMS_USER")
    password = os.getenv("RMS_PASSWORD") or os.getenv("RMS_PASS")
    
    if not username or not password:
        print("❌ Missing RMS credentials in .env")
        return False
    
    driver = make_driver()
    try:
        if not login_rms(driver, username, password):
            return False
        
        # Import download functions from other modules
        try:
            from rms_salary_download import download_salary
            from rms_tds_download import download_tds
            from rms_bank_soa_download import export_bank_soa_for_salary_month
            
            # Convert month number to name
            month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                          'July', 'August', 'September', 'October', 'November', 'December']
            month_name = month_names[salary_month] if 1 <= salary_month <= 12 else 'June'
            
            print(f"📥 Downloading all data for {month_name} {salary_year}...")
            
            # Download salary, TDS, and bank data
            download_salary(driver, month_name, salary_year)
            download_tds(driver, month_name, salary_year)
            export_bank_soa_for_salary_month(driver, month_name, salary_year)
            
            print("✅ All downloads completed")
            return True
            
        except ImportError as e:
            print(f"❌ Could not import download functions: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Error during download process: {e}")
        return False
    finally:
        driver.quit()

def main():
    """Main function for testing"""
    username = os.getenv("RMS_USERNAME") or os.getenv("RMS_USER")
    password = os.getenv("RMS_PASSWORD") or os.getenv("RMS_PASS")
    
    if not username or not password:
        print("❌ Missing RMS credentials in .env")
        print("   Set RMS_USERNAME and RMS_PASSWORD")
        print("   OR set RMS_USER and RMS_PASS")
        return
    
    print(f"✅ Testing RMS login with user: {username}")
    
    driver = make_driver()
    try:
        success = login_rms(driver, username, password)
        if success:
            print("✅ Login test successful!")
            # Stay logged in for 5 seconds to verify
            time.sleep(5)
        else:
            print("❌ Login test failed!")
    finally:
        driver.quit()

# Export the functions that main.py expects
__all__ = ['make_driver', 'get_driver', 'init_driver', 'login_rms', 'login', 'do_login', 'download_all_rms_data']

if __name__ == "__main__":
    main()
