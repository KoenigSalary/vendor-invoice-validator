from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import pandas as pd

def login_to_rms(driver, username, password):
    driver.get("https://rms.koenig-solutions.com/")
    
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "txtUser")))
    driver.find_element(By.ID, "txtUser").send_keys(username)
    driver.find_element(By.ID, "txtPwd").send_keys(password)
    driver.find_element(By.ID, "btnSubmit").click()

    time.sleep(3)
    print("✅ Logged into RMS. Proceeding to Invoice List...")

def fetch_invoice_rows(driver, start_date, end_date):
    from selenium.common.exceptions import TimeoutException

    try:
        # Navigate to Invoice List page
        driver.get("https://rms.koenig-solutions.com/Accounts/InvoiceList.aspx")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "cphMainContent_mainContent_txtDateFrom"))
        )

        # Fill From and To dates
        driver.find_element(By.ID, "cphMainContent_mainContent_txtDateFrom").clear()
        driver.find_element(By.ID, "cphMainContent_mainContent_txtDateFrom").send_keys(start_date.strftime("%d-%b-%Y"))
        driver.find_element(By.ID, "cphMainContent_mainContent_txtDateTo").clear()
        driver.find_element(By.ID, "cphMainContent_mainContent_txtDateTo").send_keys(end_date.strftime("%d-%b-%Y"))

        # Select invoice date and Combine (paid+unpaid)
        driver.find_element(By.ID, "cphMainContent_mainContent_rbDateChange_0").click()
        driver.find_element(By.ID, "cphMainContent_mainContent_rbPaidUnPaid_2").click()

        # Click Search
        driver.find_element(By.ID, "cphMainContent_mainContent_btnSearch").click()
        time.sleep(3)

        # Remove any existing Excel file
        downloads_path = os.path.expanduser("~/Downloads")
        file_path = os.path.join(downloads_path, "InvoiceList.xls")
        if os.path.exists(file_path):
            os.remove(file_path)

        # Click Export to Excel
        driver.find_element(By.ID, "cphMainContent_mainContent_ExportToExcel").click()
        time.sleep(5)

        # Check if file exists now
        if not os.path.exists(file_path):
            print("❌ Excel file not found after export.")
            return []

        # Load the Excel file
        df = pd.read_excel(file_path)

        invoices = []
        for _, row in df.iterrows():
            try:
                invoices.append({
                    "invoice_no": str(row.get("Invoice No", "")).strip(),
                    "vendor": str(row.get("Vendor Name", "")).strip(),
                    "invoice_date": row.get("Invoice Date"),
                    "upload_date": row.get("Upload Date"),
                    "gstin": str(row.get("GSTIN", "")).strip(),
                    "pan": str(row.get("PAN", "")).strip(),
                    "amount": float(row.get("Amount", 0.0)),
                })
            except Exception as e:
                print(f"⚠️ Error parsing row: {e}")

        print(f"✅ Parsed {len(invoices)} invoices from Excel.")
        return invoices

    except TimeoutException:
        driver.save_screenshot("invoice_list_timeout.png")
        print("⚠️ Timeout while loading invoice list page. Screenshot saved.")
        return []
