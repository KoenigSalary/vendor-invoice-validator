#!/usr/bin/env python3

import os
import time
from datetime import datetime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Load environment variables
load_dotenv()

EMAIL = os.getenv("RMS_USERNAME")
PASSWORD = os.getenv("RMS_PASSWORD")
LOGIN_URL = "https://rms.koenig-solutions.com/Login.aspx"
AUTO_TDS_URL = "https://rms.koenig-solutions.com/Accounts/AutoTDS.aspx"

DOWNLOAD_FOLDER = os.getenv("DOWNLOAD_DIR") or os.path.join(os.path.expanduser("~"), "Downloads")
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

def make_driver():
    """Create and configure Chrome driver"""
    options = webdriver.ChromeOptions()
    prefs = {"download.default_directory": DOWNLOAD_FOLDER}
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--start-maximized")
    return webdriver.Chrome(options=options)

def download_salary(driver, month_name, year):
    """Download salary sheet for specified month/year"""
    try:
        print("🚀 Logging into RMS...")
        driver.get(LOGIN_URL)

        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "txtUser"))).send_keys(EMAIL)
        driver.find_element(By.ID, "txtPwd").send_keys(PASSWORD)
        driver.find_element(By.ID, "btnSubmit").click()

        WebDriverWait(driver, 15).until(EC.url_contains("rms.koenig-solutions.com"))
        print("✅ Logged in successfully.")

        print("🌐 Navigating to Auto TDS panel...")
        driver.get(AUTO_TDS_URL)

        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "cphMainContent_mainContent_ddlsalarymonth"))
        )

        # Convert month name to number
        month_map = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
            'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        target_month = month_map.get(month_name, 6)  # Default to June if not found

        print(f"📅 Selecting Salary Month: {month_name} {year}")

        # Select month (index is month - 1 since it's 0-indexed)
        month_dropdown = Select(driver.find_element(By.ID, "cphMainContent_mainContent_ddlsalarymonth"))
        month_dropdown.select_by_index(target_month - 1)

        # Select "Koenig" as Employee Type (value = "2")
        emp_dropdown = Select(driver.find_element(By.ID, "cphMainContent_mainContent_ddlEmpType"))
        emp_dropdown.select_by_value("2")

        # Click Export Salary Sheet
        export_button = driver.find_element(By.ID, "cphMainContent_mainContent_btndownloadSalarysheet")
        export_button.click()
        print("📥 Download initiated...")

        # Wait for download to complete and rename
        time.sleep(5)  # Give time for download to start
        
        # Find the downloaded file (it might have a different name)
        files = [f for f in os.listdir(DOWNLOAD_FOLDER) if f.endswith('.xls') or f.endswith('.xlsx')]
        if files:
            # Get the most recently downloaded file
            latest_file = max([os.path.join(DOWNLOAD_FOLDER, f) for f in files], key=os.path.getctime)
            new_filename = f"SalarySheet_{year}_{target_month:02d}_{month_name}.xls"
            new_filepath = os.path.join(DOWNLOAD_FOLDER, new_filename)
            
            if os.path.exists(new_filepath):
                os.remove(new_filepath)  # Remove if exists
            
            os.rename(latest_file, new_filepath)
            print(f"✅ Salary sheet downloaded: {new_filename}")
            return new_filepath
        else:
            print("❌ No salary file downloaded")
            return None

    except Exception as e:
        print(f"❗ Error downloading salary: {e}")
        return None

def export_salary(driver, month_name, year):
    """Alternative function name for compatibility"""
    return download_salary(driver, month_name, year)

def export_salary_sheet(driver, month_name, year):
    """Another alternative function name for compatibility"""
    return download_salary(driver, month_name, year)

def main():
    """Main function for standalone execution"""
    if not EMAIL or not PASSWORD:
        print("❌ Missing RMS_USERNAME or RMS_PASSWORD in .env")
        return
        
    driver = make_driver()
    try:
        # Use current settings from .env or default to June 2025
        month_name = os.getenv("SALARY_MONTH", "June")
        year = int(os.getenv("SALARY_YEAR", "2025"))
        download_salary(driver, month_name, year)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()