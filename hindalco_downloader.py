import time
import os
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from datetime import datetime
import json

# Hindalco Aluminium Price PDF Downloader for GitHub Actions

# ✅ Set up download directory and log file
DOWNLOAD_DIR = "hindalco_pdfs"
PDF_LOG_FILE = "latest_hindalco_pdf.json"

# Create download directory if it doesn't exist
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ✅ Configure Chrome WebDriver for GitHub Actions
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920x1080")
options.add_argument("--disable-extensions")
options.add_argument("--disable-plugins")
options.add_argument("--disable-images")

# ✅ Launch Chrome WebDriver
driver = webdriver.Chrome(options=options)
driver.get("https://www.hindalco.com/businesses/aluminium/primary-aluminium/primary-metal-price")
time.sleep(5)  # Human-like delay

# ✅ Find the latest PDF link
def get_latest_pdf_link():
    """Finds the latest PDF link on the page."""
    try:
        # Look for an <a> tag with "pdf" in the href and a reasonable date-like text
        pdf_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '.pdf') and contains(., '202')]"))
        )
        pdf_link = pdf_element.get_attribute("href")
        print(f"✅ Found latest PDF: {pdf_link}")
        return pdf_link
    except Exception as e:
        print(f"❌ No suitable PDF found on the page: {e}")
        return None

# ✅ Get the latest PDF URL
pdf_url = get_latest_pdf_link()
if not pdf_url:
    driver.quit()
    exit()

# ✅ Check if this PDF was already downloaded
def is_new_pdf(pdf_url):
    """Checks if the PDF is new by comparing with the last downloaded URL."""
    if os.path.exists(PDF_LOG_FILE):
        try:
            with open(PDF_LOG_FILE, "r") as f:
                log_data = json.load(f)
            if pdf_url == log_data.get("last_pdf_url"):
                print("✅ PDF already downloaded. Skipping...")
                return False
        except (json.JSONDecodeError, KeyError):
            print("✅ Log file corrupted or missing data. Will download PDF.")
    return True

# ✅ Download the PDF if it's new
def download_pdf(pdf_url, download_path):
    """Downloads the PDF file with browser-like headers."""
    try:
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        original_filename = pdf_url.split("/")[-1]
        pdf_filename = os.path.join(download_path, f"{timestamp}_{original_filename}")
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/pdf",
            "Referer": "https://www.hindalco.com/"
        }

        response = requests.get(pdf_url, headers=headers, stream=True, timeout=30)
        if response.status_code == 200:
            with open(pdf_filename, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print(f"✅ PDF successfully downloaded: {pdf_filename}")
            
            # Update the log file with the new PDF URL and timestamp
            log_data = {
                "last_pdf_url": pdf_url,
                "download_timestamp": datetime.now().isoformat(),
                "filename": pdf_filename
            }
            with open(PDF_LOG_FILE, "w") as f:
                json.dump(log_data, f, indent=2)
            
            return True
        else:
            print(f"❌ Failed to download PDF. HTTP Status Code: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error downloading PDF: {e}")
        return False

# ✅ Main logic
try:
    if is_new_pdf(pdf_url):
        success = download_pdf(pdf_url, DOWNLOAD_DIR)
        if success:
            print("✅ New PDF downloaded successfully!")
        else:
            print("❌ Failed to download new PDF.")
    else:
        print("✅ No new PDF to download today.")
except Exception as e:
    print(f"❌ Script error: {e}")
finally:
    # ✅ Close browser
    driver.quit()
