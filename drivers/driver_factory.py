# drivers/driver_factory.py
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def make_driver(download_dir: str = "downloads", page_load_strategy: str = "normal") -> webdriver.Chrome:
    os.makedirs(download_dir, exist_ok=True)

    opts = Options()
    # Headless + stability flags
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-features=site-per-process,TranslateUI")
    opts.add_argument("--disable-features=IsolateOrigins,BlockInsecurePrivateNetworkRequests")
    opts.add_argument("--disable-blink-features=AutomationControlled")

    # Faster startup: don’t wait for every subresource
    opts.page_load_strategy = page_load_strategy  # "normal" | "eager" | "none"

    # Reliable downloads in headless Chrome
    prefs = {
        "download.default_directory": os.path.abspath(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "savefile.default_directory": os.path.abspath(download_dir),
    }
    opts.add_experimental_option("prefs", prefs)

    # Let Selenium Manager fetch the correct chromedriver for installed Chrome
    driver = webdriver.Chrome(options=opts)

    # Generous timeouts + script timeout (renderer stalls)
    driver.set_page_load_timeout(120)
    driver.set_script_timeout(120)
    driver.implicitly_wait(2)  # keep small; use explicit waits below
    return driver
