# Enhanced Chrome setup for GitHub Actions - ADD THIS TO YOUR rms_scraper.py

import os
import time
import random
from selenium.webdriver.chrome.options import Options

def setup_chrome_for_github_actions():
    """Setup Chrome driver optimized for GitHub Actions"""
    options = Options()
    
    # Detect if running in GitHub Actions
    is_github_actions = os.getenv('GITHUB_ACTIONS') == 'true'
    
    if is_github_actions:
        print("🔧 Configuring Chrome for GitHub Actions environment...")
        
        # Essential GitHub Actions options
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--remote-debugging-port=9222')
        
        # Performance optimization for CI
        options.add_argument('--memory-pressure-off')
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-backgrounding-occluded-windows')
        options.add_argument('--disable-renderer-backgrounding')
        options.add_argument('--disable-features=VizDisplayCompositor')
        
        # Network optimization
        options.add_argument('--aggressive-cache-discard')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-default-apps')
        options.add_argument('--disable-sync')
        
        # Stability options
        options.add_argument('--disable-software-rasterizer')
        options.add_argument('--disable-features=TranslateUI')
        options.add_argument('--disable-ipc-flooding-protection')
        
        # User agent
        options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
    else:
        print("🔧 Configuring Chrome for local environment...")
        options.add_argument('--headless')
    
    # Common options
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-web-security')
    options.add_argument('--allow-running-insecure-content')
    
    # Download preferences
    prefs = {
        "profile.default_content_settings.popups": 0,
        "profile.default_content_setting_values.notifications": 2,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    
    if is_github_actions:
        # Disable images and CSS for faster loading in CI
        prefs["profile.managed_default_content_settings.images"] = 2
        prefs["profile.managed_default_content_settings.stylesheets"] = 2
    
    options.add_experimental_option("prefs", prefs)
    
    return options

def enhanced_wait_and_click(driver, by, value, timeout=60, max_retries=3):
    """Enhanced click function with better error handling for GitHub Actions"""
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
    
    for attempt in range(max_retries):
        try:
            print(f"🔍 Attempt {attempt + 1}: Looking for element {by}={value}")
            
            # Wait for element to be present
            wait = WebDriverWait(driver, timeout)
            element = wait.until(EC.presence_of_element_located((by, value)))
            
            # Wait for element to be clickable
            element = wait.until(EC.element_to_be_clickable((by, value)))
            
            # Scroll into view
            driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
            time.sleep(2)
            
            # Add random delay to seem more human-like
            time.sleep(random.uniform(1, 3))
            
            # Try clicking
            element.click()
            print(f"✅ Successfully clicked element {by}={value}")
            return True
            
        except (TimeoutException, ElementClickInterceptedException) as e:
            print(f"⚠️ Attempt {attempt + 1} failed: {str(e)}")
            
            if attempt < max_retries - 1:
                # Try JavaScript click as fallback
                try:
                    element = driver.find_element(by, value)
                    driver.execute_script("arguments[0].click();", element)
                    print(f"✅ JavaScript click successful for {by}={value}")
                    return True
                except Exception as js_e:
                    print(f"⚠️ JavaScript click also failed: {str(js_e)}")
                    
                # Wait before retry
                wait_time = 5 * (attempt + 1)
                print(f"⏳ Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                print(f"❌ All {max_retries} attempts failed for {by}={value}")

    def enhanced_rms_download_with_retry(start_date, end_date, max_retries=3):
    """Enhanced RMS download with retry logic for large datasets"""
    
    for attempt in range(max_retries):
        print(f"📥 Download attempt {attempt + 1}/{max_retries}")
        
        try:
            # Call your existing RMS download
            result = rms_download(start_date, end_date)
            
            # Verify download completeness
            run_dir = os.path.dirname(result) if os.path.isfile(result) else result
            zip_path = os.path.join(run_dir, "invoices.zip")
            excel_path = os.path.join(run_dir, "invoice_download.xls")
            
            if os.path.exists(zip_path) and os.path.exists(excel_path):
                # Read Excel to get expected count
                df = pd.read_excel(excel_path)
                expected_count = len(df)
                
                # Check ZIP contents
                if os.path.exists(zip_path):
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        pdf_files = [f for f in zip_ref.namelist() if f.lower().endswith('.pdf')]
                        actual_count = len(pdf_files)
                    
                    print(f"📊 Expected PDFs: {expected_count}, Found: {actual_count}")
                    
                    # Check if counts match (allow 5% variance for processing delays)
                    if actual_count >= expected_count * 0.95:
                        print("✅ PDF download appears complete")
                        return result
                    else:
                        print(f"⚠️ Incomplete PDF download: {actual_count}/{expected_count}")
                        if attempt < max_retries - 1:
                            print("🔄 Waiting 30 seconds before retry...")
                            time.sleep(30)
                            continue
            
            return result
            
        except Exception as e:
            print(f"❌ Download attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                print("🔄 Retrying in 30 seconds...")
                time.sleep(30)
            else:
                raise e
    
    return result

            
