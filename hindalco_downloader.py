import os
import time
import requests
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ✅ Setup
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
PDF_LOG_FILE = os.path.join(DOWNLOAD_DIR, "latest_hindalco_pdf.txt")

# ✅ Setup Edge WebDriver in headless mode (for GitHub)
options = Options()
options.use_chromium = True
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")

driver = webdriver.Edge(options=options)
driver.get("https://www.hindalco.com/businesses/aluminium/primary-aluminium/primary-metal-price")
time.sleep(5)

def get_latest_pdf_link():
    today = datetime.today()
    for day in range(15, 0, -20):
        date_str = f"{day:02d}.{today.month:02d}.{today.year}"
        try:
            pdf_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, f"//a[contains(text(), '{date_str}')]"))
            )
            pdf_link = pdf_element.get_attribute("href")
            print(f"✅ Found latest PDF: {pdf_link}")
            return pdf_link
        except:
            print(f"❌ No PDF found for {date_str}, checking previous day...")
    return None

def is_new_pdf(pdf_url):
    if os.path.exists(PDF_LOG_FILE):
        with open(PDF_LOG_FILE, "r") as f:
            last_pdf_url = f.read().strip()
        if pdf_url == last_pdf_url:
            print("✅ PDF already downloaded. Skipping...")
            return False
    return True

def download_pdf(pdf_url, download_path):
    filename = os.path.join(download_path, pdf_url.split("/")[-1])
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/pdf",
        "Referer": "https://www.hindalco.com/"
    }
    response = requests.get(pdf_url, headers=headers, stream=True)
    if response.status_code == 200:
        with open(filename, "wb") as f:
            f.write(response.content)
        print(f"✅ PDF downloaded: {filename}")
        with open(PDF_LOG_FILE, "w") as f:
            f.write(pdf_url)
    else:
        print(f"❌ Failed to download PDF. Status Code: {response.status_code}")

# ✅ Main execution
pdf_url = get_latest_pdf_link()
if pdf_url and is_new_pdf(pdf_url):
    download_pdf(pdf_url, DOWNLOAD_DIR)

driver.quit()
