# updater.py

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def update_invoice_status(driver, invoice_no, status, reason):
    try:
        # Navigate to Update Status Page
        driver.get("https://rms.koenig-solutions.com/Accounts/InvoiceUpdate.aspx")

        # Wait for input field to appear
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "cphMainContent_mainContent_txtInvoice"))
        )

        # Enter Invoice No
        invoice_input = driver.find_element(By.ID, "cphMainContent_mainContent_txtInvoice")
        invoice_input.clear()
        invoice_input.send_keys(invoice_no)

        # Click Fetch
        driver.find_element(By.ID, "cphMainContent_mainContent_btnFetch").click()

        # Wait for status dropdown
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "cphMainContent_mainContent_ddlStatus"))
        )

        # Select status
        status_dropdown = driver.find_element(By.ID, "cphMainContent_mainContent_ddlStatus")
        status_dropdown.send_keys(status)

        # Enter reason
        reason_input = driver.find_element(By.ID, "cphMainContent_mainContent_txtReason")
        reason_input.clear()
        reason_input.send_keys(reason)

        # Click Update
        driver.find_element(By.ID, "cphMainContent_mainContent_btnUpdate").click()

        return True

    except Exception as e:
        print(f"❌ Could not update status for Invoice {invoice_no} — {e}")
        return False
