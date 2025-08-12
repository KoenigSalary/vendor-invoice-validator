# Add this to your rms_scraper.py imports section
import time
import random
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

def get_enhanced_chrome_options():
    """Get Chrome options optimized for GitHub Actions"""
    options = Options()
    
    # Essential options for GitHub Actions
    options.add_argument('--headless=new')  # Use new headless mode
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-plugins')
    options.add_argument('--disable-images')  # Speed up loading
    options.add_argument('--disable-javascript')  # Only if your site works without JS
    
    # Memory optimization
    options.add_argument('--memory-pressure-off')
    options.add_argument('--max_old_space_size=4096')
    
    # Network optimization
    options.add_argument('--aggressive-cache-discard')
    options.add_argument('--disable-background-timer-throttling')
    
    # Stability options
    options.add_argument('--disable-backgrounding-occluded-windows')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--disable-features=TranslateUI')
    
    # Set window size
    options.add_argument('--window-size=1920,1080')
    
    return options

def safe_click_with_retry(driver, element, max_retries=3, delay=2):
    """Safely click element with retry logic"""
    for attempt in range(max_retries):
        try:
            # Scroll element into view
            driver.execute_script("arguments[0].scrollIntoView(true);", element)
            time.sleep(1)
            
            # Try regular click first
            element.click()
            return True
            
        except Exception as e:
            print(f"⚠️ Click attempt {attempt + 1} failed: {str(e)}")
            
            if attempt < max_retries - 1:
                # Try JavaScript click as fallback
                try:
                    driver.execute_script("arguments[0].click();", element)
                    return True
                except:
                    time.sleep(delay * (attempt + 1))  # Exponential backoff
                    continue
            
    return False

def wait_with_retry(driver, condition, timeout=30, max_retries=3):
    """Wait for condition with retry logic"""
    for attempt in range(max_retries):
        try:
            wait = WebDriverWait(driver, timeout)
            return wait.until(condition)
        except TimeoutException:
            if attempt < max_retries - 1:
                print(f"⚠️ Wait timeout, attempt {attempt + 1}, retrying...")
                time.sleep(random.uniform(2, 5))  # Random delay
                continue
            raise
    
    return None
