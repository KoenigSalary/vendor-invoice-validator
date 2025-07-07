import os
import time
import glob
import csv
from datetime import datetime, timedelta
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Load .env credentials
load_dotenv()
USERNAME = os.getenv("RMS_USER")
PASSWORD = os.getenv("RMS_PASS")

def rms_download(start_date, end_date):
    today_str = datetime.today().strftime("%Y-%m-%d")
    download_dir = os.path.join("data", today_str)
    os.makedirs(download_dir, exist_ok=True)
    download_dir_abs = os.path.abspath(download_dir)

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir_abs,
        "download.prompt_for_download": False,
        "directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(driver, 15)

    try:
        # 1. Login
        driver.get("https://rms.koenig-solutions.com/")
        wait.until(EC.presence_of_element_located((By.ID, "txtUser"))).send_keys(USERNAME)
        driver.find_element(By.ID, "txtPwd").send_keys(PASSWORD)
        driver.find_element(By.ID, "btnSubmit").click()
        time.sleep(3)

        # 2. Go to Invoice List page
        driver.get("https://rms.koenig-solutions.com/Accounts/InvoiceList.aspx")
        wait.until(EC.presence_of_element_located((By.ID, "cphMainContent_mainContent_txtDateFrom")))

        # 3. Fill date range
        driver.find_element(By.ID, "cphMainContent_mainContent_txtDateFrom").clear()
        driver.find_element(By.ID, "cphMainContent_mainContent_txtDateFrom").send_keys(start_date.strftime("%d-%b-%Y"))

        driver.find_element(By.ID, "cphMainContent_mainContent_txtDateTo").clear()
        driver.find_element(By.ID, "cphMainContent_mainContent_txtDateTo").send_keys(end_date.strftime("%d-%b-%Y"))

        # 4. Set filters
        driver.find_element(By.ID, "cphMainContent_mainContent_rbDateChange_0").click()  # Invoice Date
        driver.find_element(By.ID, "cphMainContent_mainContent_rbPaidUnPaid_2").click()  # Combined
        driver.find_element(By.ID, "cphMainContent_mainContent_btnSearch").click()
        time.sleep(3)

        # 5. Extract "Inv Created By" for each invoice
        print("🔍 Extracting Inv Created By for each invoice...")
        inv_data = []
        rows = driver.find_elements(By.XPATH, "//table[@id='cphMainContent_mainContent_rptShowAss']/tbody/tr")
        for row in rows:
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 8:
                    continue
                inv_no = cells[2].text.strip()
                inv_created_by = cells[7].text.strip()
                if inv_no:
                    inv_data.append((inv_no, inv_created_by))
            except Exception as e:
                print(f"⚠️ Row parse error: {e}")

        # Save to CSV
        map_file = os.path.join(download_dir_abs, "inv_created_by_map.csv")
        with open(map_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Invoice No", "Inv Created By"])
            writer.writerows(inv_data)
        print(f"📁 Saved Inv Created By map: {map_file}")

        # 6. Select all invoices
        try:
            checkbox = driver.find_element(By.ID, "cphMainContent_mainContent_rptShowAss_chkHeader")
            if not checkbox.is_selected():
                checkbox.click()
            time.sleep(1)
        except Exception as e:
            print(f"⚠️ Header checkbox issue: {e}")

        # 7. Click ZIP Download
        try:
            driver.find_element(By.ID, "cphMainContent_mainContent_btnDownload").click()
            print("📥 ZIP download triggered.")
        except Exception as e:
            print(f"❌ ZIP download error: {e}")

        # 8. Export Excel
        try:
            export_btn = driver.find_element(By.ID, "cphMainContent_mainContent_ExportToExcel")
            driver.execute_script("arguments[0].click();", export_btn)
            print("📄 Excel export triggered.")
        except Exception as e:
            print(f"⚠️ Excel export failed: {e}")

        # 9. Wait for downloads
        print("⏳ Waiting for ZIP and XLS...")
        xls_file = None
        zip_file = None
        for _ in range(60):
            files = os.listdir(download_dir_abs)
            zip_matches = [f for f in files if f.endswith(".zip")]
            xls_matches = glob.glob(os.path.join(download_dir_abs, "*.xls"))

            if zip_matches and not zip_file:
                zip_file = zip_matches[0]
            if xls_matches and not xls_file:
                xls_file = xls_matches[0]

            if zip_file and xls_file:
                break
            time.sleep(1)

        # 10. Rename
        if zip_file:
            os.rename(os.path.join(download_dir_abs, zip_file),
                      os.path.join(download_dir_abs, "invoices.zip"))
            print("✅ Saved ZIP as invoices.zip")
        else:
            print("❌ ZIP not downloaded.")

        if xls_file:
            os.rename(xls_file, os.path.join(download_dir_abs, "invoice_download.xls"))
            print("✅ Saved XLS as invoice_download.xls")
        else:
            print("❌ XLS not downloaded.")

    except Exception as e:
        print(f"❌ Error: {e}")

    finally:
        driver.quit()

# Run manually
if __name__ == "__main__":
    end_date = datetime.today()
    start_date = end_date - timedelta(days=4)
    rms_download(start_date, end_date)
