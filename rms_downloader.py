# -*- coding: utf-8 -*-

import os, time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

# =========================
# CONFIG
# =========================
load_dotenv()

RMS_USERNAME = os.getenv("RMS_USERNAME")
RMS_PASSWORD = os.getenv("RMS_PASSWORD")

SALARY_MONTH = (os.getenv("SALARY_MONTH") or "June").strip().capitalize()
try:
    SALARY_YEAR = int(os.getenv("SALARY_YEAR") or datetime.now().year)
except Exception:
    SALARY_YEAR = datetime.now().year

# Where Chrome should download files
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR") or "/Users/praveenchaudhary/Downloads/Koenig-Management-Agent"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH") or "/opt/homebrew/bin/chromedriver"

# URLs (from your system)
AUTO_TDS_URL    = "https://rms.koenig-solutions.com/Accounts/AutoTDS.aspx"
UPDATE_TDS_URL  = "https://rms.koenig-solutions.com/HR/UpdateTDS.aspx"
BANK_BOOK_URL   = "https://rms.koenig-solutions.com/BankBook/BankBookEntry.aspx"

# Bank constants
BANK_VALUE_KOTAK_OD_0317       = "20"
BANK_VALUE_DEUTSCHE_OD_100008  = "83"

# Bank page IDs you provided
FROM_ID           = "cphMainContent_mainContent_txtDateFrom"
TO_ID             = "cphMainContent_mainContent_txtDateTo"
ACC_ID            = "cphMainContent_mainContent_ddlAccHeadFilt"
BANK_ID           = "cphMainContent_mainContent_ddlBankSearch"
SEARCH_ID         = "cphMainContent_mainContent_btnSearch"
CHKALL_ID         = "cphMainContent_mainContent_grdv_ChkAll"
EXPORT_SAL_UPL_ID = "cphMainContent_mainContent_ExportToExcelSalaryUploaded"

# Salary block IDs you provided
SAL_MONTH_ID  = "cphMainContent_mainContent_ddlsalarymonth"
EMP_TYPE_ID   = "cphMainContent_mainContent_ddlEmpType"
SAL_EXPORT_ID = "cphMainContent_mainContent_btndownloadSalarysheet"

# TDS page IDs you provided
TDS_MONTH_ID  = "ddlSearchMonth"
TDS_SEARCH_ID = "btnFilter"
TDS_EXCEL_XP  = "//span[normalize-space()='Excel'] | //button[normalize-space()='Excel']"

# =========================
# UTILITIES
# =========================
def make_driver():
    opts = Options()
    
    # Ensure the download directory exists and is absolute
    download_path = os.path.abspath(DOWNLOAD_DIR)
    os.makedirs(download_path, exist_ok=True)
    
    prefs = {
        "download.default_directory": download_path,  # Use absolute path
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    opts.add_experimental_option("prefs", prefs)
    # opts.add_argument("--headless=new")  # uncomment if you want headless
    service = Service(CHROMEDRIVER_PATH)
    return webdriver.Chrome(service=service, options=opts)

def login_rms(driver, username, password):
    driver.get("https://rms.koenig-solutions.com/")
    print("[login] opened login page")

    try:
        u = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "txtUser")))
        p = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "txtPwd")))
        u.clear(); u.send_keys(username)
        p.clear(); p.send_keys(password)
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "btnSubmit"))).click()
        print("[login] clicked submit via ('id','btnSubmit')")
    except Exception as e:
        print(f"[login] failed to submit login: {e}")
        return False

    # Basic success heuristic: txtUser field gone
    time.sleep(2)
    try:
        driver.find_element(By.ID, "txtUser")
        print("❌ still on login form")
        return False
    except Exception:
        pass
    print("[login] success")
    return True

def select_option_contains(select_el, text_contains):
    sel = Select(select_el)
    for o in sel.options:
        if text_contains.lower() in (o.text or "").lower():
            sel.select_by_visible_text(o.text)
            return o.text
    return None

def wait_for_download_and_rename(folder, prefix, timeout=120):
    """Wait for a new .xls/.xlsx to appear, then rename to 'prefix.ext' (add timestamp if exists)."""
    start = time.time()
    seen = set(os.listdir(folder))
    while time.time() - start < timeout:
        for f in os.listdir(folder):
            if f in seen:
                continue
            if f.endswith(".crdownload"):
                continue
            if f.lower().endswith((".xls", ".xlsx")):
                src = os.path.join(folder, f)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                ext = os.path.splitext(f)[1]
                dst = os.path.join(folder, f"{prefix}{ext}")
                if os.path.exists(dst):
                    dst = os.path.join(folder, f"{prefix}_{ts}{ext}")
                try:
                    os.rename(src, dst)
                    print(f"[save] {os.path.basename(dst)}")
                    return dst
                except Exception:
                    # file might be locked for a moment
                    time.sleep(1)
        time.sleep(1)
    print("⚠️  No new file detected.")
    return None

# =========================
# EXPORTS
# =========================
def export_salary_sheet(driver, month_name, year, employee_type_text="--ALL--"):
    """
    Auto TDS → Export Salary Sheet block:
      - Select Month (e.g., 'June - 2025' or 'June 2025')
      - Select Employee Type (--KOENIG--)
      - Click 'EXPORT SALARY SHEET'
      - Wait & rename
    Location is intentionally ignored as requested.
    """
    print("[salary] opening Auto TDS…")
    driver.get(AUTO_TDS_URL)

    # Month
    month_sel = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, SAL_MONTH_ID)))
    picked = select_option_contains(month_sel, f"{month_name} {year}") or select_option_contains(month_sel, f"{month_name} - {year}")
    print(f"[salary] month -> {picked or 'NOT SET'}")

    # Employee Type (--ALL--)
    emp_sel = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, EMP_TYPE_ID)))
    picked_emp = select_option_contains(emp_sel, employee_type_text) or select_option_contains(emp_sel, "--ALL--")
    print(f"[salary] employee type -> {picked_emp or 'NOT SET'}")

    # Export
    export_btn = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.ID, SAL_EXPORT_ID)))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", export_btn)
    driver.execute_script("arguments[0].click();", export_btn)
    print("✅ Salary export clicked")

    # Wait & rename
    prefix = f"Salary_Sheet_{month_name}_{year}"
    return wait_for_download_and_rename(DOWNLOAD_DIR, prefix, timeout=120)

def export_tds(driver, month_name, year):
    """
    Update TDS with enhanced overlay handling
    """
    print("[tds] opening Update TDS…")
    driver.get(UPDATE_TDS_URL)

    # Wait and remove overlays
    time.sleep(5)
    driver.execute_script("""
        var overlays = document.querySelectorAll('.hides, .bg-overlay, .preloader-container');
        overlays.forEach(el => el.remove());
    """)

    # Month selection (this part was working)
    month_sel = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, TDS_MONTH_ID)))
    picked = select_option_contains(month_sel, f"{month_name} - {year}")
    print(f"[tds] month -> {picked or 'NOT SET'}")

    # Search with overlay handling
    search_btn = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, TDS_SEARCH_ID)))
    
    # Remove overlays again
    driver.execute_script("""
        var overlays = document.querySelectorAll('.hides, .bg-overlay');
        overlays.forEach(el => el.style.display = 'none');
    """)
    
    # Try JavaScript click for search
    try:
        driver.execute_script("arguments[0].click();", search_btn)
        print("[tds] Search clicked via JavaScript")
    except:
        try:
            search_btn.click()
            print("[tds] Search clicked normally")
        except:
            driver.execute_script("document.getElementById('{}').click();".format(TDS_SEARCH_ID))
            print("[tds] Search clicked via ID")
    
    time.sleep(8)  # Wait for results

    # Excel export with multiple strategies
    excel_clicked = False
    excel_strategies = [
        "//span[normalize-space()='Excel']",
        "//button[normalize-space()='Excel']", 
        "//button[contains(@class, 'buttons-excel')]"
    ]
    
    for selector in excel_strategies:
        try:
            # Remove overlays before each attempt
            driver.execute_script("""
                var overlays = document.querySelectorAll('.preloader-container, .bg-overlay, .hides');
                overlays.forEach(el => el.style.display = 'none');
            """)
            
            element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, selector)))
            driver.execute_script("arguments[0].scrollIntoView(true);", element)
            time.sleep(1)
            driver.execute_script("arguments[0].click();", element)
            
            print(f"✅ TDS Excel export clicked using: {selector}")
            excel_clicked = True
            break
            
        except Exception as e:
            print(f"[tds] Strategy '{selector}' failed: {e}")
            continue
    
    if not excel_clicked:
        print("❌ TDS Excel export failed")
        return None

    prefix = f"TDS_{month_name}_{year}"
    return wait_for_download_and_rename(DOWNLOAD_DIR, prefix, timeout=120)

def export_bank_soa_for_bank(driver, from_str, to_str, bank_value, label_for_file):
    """
    Bank Book Entry with enhanced overlay handling
    """
    print(f"[bank] range {from_str} → {to_str} ; bank value={bank_value}")
    driver.get(BANK_BOOK_URL)

    # Wait for page to fully load
    time.sleep(5)
    
    # Remove any overlays first
    try:
        driver.execute_script("""
            var overlays = document.querySelectorAll('.bg-overlay, .hides, .preloader-container, .loading-overlay');
            for (var i = 0; i < overlays.length; i++) {
                overlays[i].style.display = 'none';
                overlays[i].remove();
            }
        """)
    except:
        pass

    # Dates
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, FROM_ID)))
    f = driver.find_element(By.ID, FROM_ID); f.clear(); f.send_keys(from_str)
    t = driver.find_element(By.ID, TO_ID);   t.clear(); t.send_keys(to_str)

    # AccHead = Salary Exp-Payable
    acc_el = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, ACC_ID)))
    _picked_acc = select_option_contains(acc_el, "Salary Exp-Payable")

    # Bank
    bank_el = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, BANK_ID)))
    Select(bank_el).select_by_value(bank_value)

    # Search with overlay handling
    search_btn = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, SEARCH_ID)))
    
    # Remove overlays again before clicking
    driver.execute_script("""
        var overlays = document.querySelectorAll('.bg-overlay, .hides');
        overlays.forEach(el => el.remove());
    """)
    
    # Try multiple click strategies for search
    try:
        search_btn.click()
    except:
        try:
            driver.execute_script("arguments[0].click();", search_btn)
        except:
            driver.execute_script("document.getElementById('{}').click();".format(SEARCH_ID))
    
    print("[bank] Search clicked, waiting for results...")
    time.sleep(8)  # Longer wait for bank results

    # Header checkbox with better handling
    try:
        chk = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, CHKALL_ID)))
        driver.execute_script("arguments[0].scrollIntoView(true);", chk)
        time.sleep(1)
        
        if not chk.is_selected():
            driver.execute_script("arguments[0].click();", chk)
            print("[bank] Header checkbox selected")
    except Exception as e:
        print(f"[bank] Checkbox failed: {e}")

    # Export with multiple strategies
    time.sleep(2)
    
    # Remove overlays one more time before export
    driver.execute_script("""
        var overlays = document.querySelectorAll('.bg-overlay, .hides, .preloader-container');
        overlays.forEach(el => el.style.display = 'none');
    """)
    
    export_strategies = [
        # Strategy 1: Direct JavaScript click by ID
        lambda: driver.execute_script("document.getElementById('{}').click();".format(EXPORT_SAL_UPL_ID)),
        
        # Strategy 2: Find element and JavaScript click
        lambda: driver.execute_script("arguments[0].click();", 
                                    driver.find_element(By.ID, EXPORT_SAL_UPL_ID)),
        
        # Strategy 3: Scroll and JavaScript click
        lambda: (
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", 
                                driver.find_element(By.ID, EXPORT_SAL_UPL_ID)),
            time.sleep(2),
            driver.execute_script("arguments[0].click();", 
                                driver.find_element(By.ID, EXPORT_SAL_UPL_ID))
        ),
        
        # Strategy 4: Regular Selenium click
        lambda: WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, EXPORT_SAL_UPL_ID))).click()
    ]
    
    export_clicked = False
    for i, strategy in enumerate(export_strategies, 1):
        try:
            print(f"[bank] Trying export strategy {i}...")
            strategy()
            print("✅ Bank SOA export clicked")
            export_clicked = True
            break
        except Exception as e:
            print(f"[bank] Export strategy {i} failed: {e}")
            continue
    
    if not export_clicked:
        print("❌ All export strategies failed")
        return None

    prefix = f"SOA_{label_for_file}_{from_str}_to_{to_str}".replace(" ", "_").replace("__", "_")
    return wait_for_download_and_rename(DOWNLOAD_DIR, prefix, timeout=120)

def export_bank_soa_for_salary_month(driver, salary_month_name, salary_year):
    """
    Salary for <month/year> → payments happen next month (1st to 26th).
    Export for Kotak OD 0317 and Deutsche OD 100008.
    """
    base = datetime.strptime(f"01 {salary_month_name} {salary_year}", "%d %B %Y")
    pay_month = base + relativedelta(months=1)
    from_str = pay_month.replace(day=1).strftime("%d-%b-%Y")
    to_str   = pay_month.replace(day=26).strftime("%d-%b-%Y")
    print(f"[bank] salary {salary_month_name} {salary_year} → payment window {from_str} to {to_str}")

    try:
        export_bank_soa_for_bank(driver, from_str, to_str, BANK_VALUE_KOTAK_OD_0317, "KotakOD0317")
    except Exception as e:
        print(f"⚠️ Kotak bank SOA failed: {e}")
    
    try:
        export_bank_soa_for_bank(driver, from_str, to_str, BANK_VALUE_DEUTSCHE_OD_100008, "DeutscheOD100008")
    except Exception as e:
        print(f"⚠️ Deutsche bank SOA failed: {e}")

# =========================
# MAIN
# =========================
def main():
    if not RMS_USERNAME or not RMS_PASSWORD:
        print("❌ Missing RMS_USERNAME or RMS_PASSWORD in .env")
        return

    print(f"➡️  Download folder: {DOWNLOAD_DIR}")
    driver = make_driver()
    try:
        if not login_rms(driver, RMS_USERNAME, RMS_PASSWORD):
            print("❌ Login failed")
            return

        print("1️⃣ Downloading Salary Sheet...")
        export_salary_sheet(driver, SALARY_MONTH, SALARY_YEAR)

        print("2️⃣ Downloading TDS...")
        try:
            export_tds(driver, SALARY_MONTH, SALARY_YEAR)
        except Exception as e:
            print(f"⚠️ TDS download failed: {e}")
            print("Continuing with Bank SOA download...")

        print("3️⃣ Downloading Bank SOA...")
        try:
            export_bank_soa_for_salary_month(driver, SALARY_MONTH, SALARY_YEAR)
        except Exception as e:
            print(f"⚠️ Bank SOA download failed: {e}")

        print("\n✅ Download process completed. Check your download folder.")
        
    except Exception as e:
        print(f"❌ Main process error: {e}")
    finally:
        time.sleep(2)
        driver.quit()

# =========================
# COMPATIBILITY FUNCTIONS FOR MAIN.PY
# =========================

def download_salary(driver, month_name, year):
    """Compatibility wrapper for main.py"""
    return export_salary_sheet(driver, month_name, year)

def download_tds(driver, month_name, year):
    """Compatibility wrapper for main.py"""
    return export_tds(driver, month_name, year)

def download_bank_soa(driver, month_name, year):
    """Compatibility wrapper for main.py"""
    return export_bank_soa_for_salary_month(driver, month_name, year)

def download_salary_bank_soa(driver, month_name, year):
    """Another compatibility name for main.py"""
    return export_bank_soa_for_salary_month(driver, month_name, year)

# Export functions that main.py looks for
__all__ = [
    'make_driver', 'login_rms', 
    'export_salary_sheet', 'export_tds', 'export_bank_soa_for_salary_month',
    'download_salary', 'download_tds', 'download_bank_soa', 'download_salary_bank_soa'
]

if __name__ == "__main__":
    main()