import os
import time
import datetime
import requests
from bs4 import BeautifulSoup

# Constants
BASE_URL = "https://www.hindalco.com/"
TARGET_URL = "https://www.hindalco.com/businesses/aluminium/aluminium-prices"
DOWNLOAD_DIR = "hindalco_downloads"

# Ensure folder exists
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Start from 15th of the month and go backward
today = datetime.date.today()
year = today.year
month = today.month

start_day = 15

def fetch_pdf_url():
    print(f"🔍 Checking for Hindalco PDF from {start_day:02d} {month:02d} {year}")
    for day in range(start_day, 0, -1):
        date_str = f"{day:02d}-{month:02d}-{year}"
        print(f"❌ No PDF found for {date_str}, checking previous day...")

        try:
            response = requests.get(TARGET_URL, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            links = soup.find_all("a", href=True)
            for link in links:
                href = link["href"]
                if href.lower().endswith(".pdf") and f"{day:02d}-{month:02d}-{year}" in href:
                    return BASE_URL + href.lstrip("/"), date_str
        except Exception as e:
            print(f"Error accessing site: {e}")
            time.sleep(2)
    return None, None

def download_pdf(pdf_url, date_str):
    file_name = f"Hindalco_Aluminium_Price_{date_str}.pdf"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    print(f"✅ PDF found! Downloading to {file_path}")
    
    try:
        with requests.get(pdf_url, stream=True, timeout=20) as r:
            r.raise_for_status()
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"✅ Download complete: {file_path}")
    except Exception as e:
        print(f"❌ Download failed: {e}")

# Main Execution
pdf_url, date_found = fetch_pdf_url()
if pdf_url:
    download_pdf(pdf_url, date_found)
else:
    print("❌ No Hindalco Aluminium Price PDF found in the date range.")
