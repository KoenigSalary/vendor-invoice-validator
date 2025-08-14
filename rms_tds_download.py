#!/usr/bin/env python3

import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Load environment variables
load_dotenv()

EMAIL = os.getenv("RMS_USERNAME") or os.getenv("RMS_USER")
PASSWORD = os.getenv("RMS_PASSWORD") or os.getenv("RMS_PASS")

LOGIN_URL = "https://rms.koenig-solutions.com/Login.aspx"
TDS_URL = "https://rms.koenig-solutions.com/HR/UpdateTDS.aspx"

DOWNLOAD_FOLDER = os.getenv("DOWNLOAD_DIR") or os.path.join(os.path.expanduser("~"), "Downloads")
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

def make_driver():
    """Create and configure Chrome driver"""
    options = webdriver.ChromeOptions()
    prefs = {"download.default_directory": DOWNLOAD_FOLDER}
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--start-maximized")
    return webdriver.Chrome(options=options)

def download_tds(driver, month_name, year):
    """Download TDS sheet for specified month/year"""
    try:
        print("🚀 Logging into RMS...")
        driver.get(LOGIN_URL)

        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "txtUser"))).send_keys(EMAIL)
        driver.find_element(By.ID, "txtPwd").send_keys(PASSWORD)
        driver.find_element(By.ID, "btnSubmit").click()

        WebDriverWait(driver, 15).until(EC.url_contains("rms.koenig-solutions.com"))
        print("✅ Logged in successfully.")

        print("🌐 Navigating to Update TDS panel...")
        driver.get(TDS_URL)

        # Wait for dropdown
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "ddlSearchMonth")))
        month_dropdown = Select(driver.find_element(By.ID, "ddlSearchMonth"))

        # Convert month name to number
        month_map = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
            'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        target_month = month_map.get(month_name, 6)  # Default to June if not found
        
        # Create the expected dropdown value format
        # Format appears to be: "6/1/2025 12:00:00 AM" for June 2025
        dropdown_value = f"{target_month}/1/{year} 12:00:00 AM"
        
        print(f"📅 Selecting TDS Month: {month_name} {year} (Value: {dropdown_value})")

        # Print available options for debugging
        print("📋 Available Month Dropdown Values:")
        found_match = False
        for option in month_dropdown.options:
            option_text = option.text
            option_value = option.get_attribute('value')
            print(f" - Text: {option_text} | Value: {option_value}")
            
            # Try to find a match - look for the month/year in the text or value
            if (f"{month_name}" in option_text and f"{year}" in option_text) or option_value == dropdown_value:
                print(f"✅ Found match: {option_text}")
                month_dropdown.select_by_value(option_value)
                found_match = True
                break
        
        if not found_match:
            print(f"⚠️  Exact match not found, trying to select by partial match...")
            # Try to find by partial text match
            for option in month_dropdown.options:
                if f"{month_name}" in option.text and f"{year}" in option.text:
                    print(f"✅ Using partial match: {option.text}")
                    month_dropdown.select_by_visible_text(option.text)
                    found_match = True
                    break
        
        if not found_match:
            print(f"❌ Could not find {month_name} {year} in dropdown. Using first available option.")
            month_dropdown.select_by_index(1)  # Skip the first empty option

        # Click Search/Filter
        driver.find_element(By.ID, "btnFilter").click()
        print("🔍 Search triggered... waiting for results...")

        time.sleep(5)

        # Click Export to Excel
        try:
            # Try different possible selectors for the Excel export button
            export_selectors = [
                "//span[text()='Excel']",
                "//button[text()='Excel']",
                "//span[normalize-space()='Excel']",
                "//a[contains(text(),'Excel')]"
            ]
            
            export_clicked = False
            for selector in export_selectors:
                try:
                    export_element = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                    export_element.click()
                    print(f"📥 Export to Excel clicked using selector: {selector}")
                    export_clicked = True
                    break
                except:
                    continue
            
            if not export_clicked:
                print("❌ Could not find Excel export button")
                return None
                
        except Exception as e:
            print(f"❌ Error clicking Excel export: {e}")
            return None

        # Wait for download to complete
        print("⏳ Waiting for download...")
        time.sleep(10)  # Give more time for download
        
        # Find the most recently downloaded file
        files = [f for f in os.listdir(DOWNLOAD_FOLDER) 
                if f.lower().endswith(('.xls', '.xlsx')) and 'tds' in f.lower()]
        
        if not files:
            # Look for any recently downloaded Excel files
            files = [f for f in os.listdir(DOWNLOAD_FOLDER) 
                    if f.lower().endswith(('.xls', '.xlsx'))]
        
        if files:
            # Get the most recently created file
            latest_file = max([os.path.join(DOWNLOAD_FOLDER, f) for f in files], 
                            key=os.path.getctime)
            
            # Rename the file
            new_filename = f"TDS_Deductions_{year}_{target_month:02d}_{month_name}.xls"
            new_filepath = os.path.join(DOWNLOAD_FOLDER, new_filename)
            
            if os.path.exists(new_filepath):
                os.remove(new_filepath)  # Remove if exists
            
            os.rename(latest_file, new_filepath)
            print(f"✅ TDS sheet downloaded: {new_filename}")
            return new_filepath
        else:
            print("❌ No TDS file downloaded")
            return None

    except Exception as e:
        print(f"❗ Error downloading TDS: {e}")
        return None

def export_tds(driver, month_name, year):
    """Alternative function name for compatibility"""
    return download_tds(driver, month_name, year)

def main():
    """Main function for standalone execution"""
    if not EMAIL or not PASSWORD:
        print("❌ Missing RMS credentials in .env")
        print("   Set RMS_USERNAME and RMS_PASSWORD")
        print("   OR set RMS_USER and RMS_PASS")
        return
        
    driver = make_driver()
    try:
        # Use current settings from .env or default to June 2025
        month_name = os.getenv("SALARY_MONTH", "June")
        year = int(os.getenv("SALARY_YEAR", "2025"))
        download_tds(driver, month_name, year)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()