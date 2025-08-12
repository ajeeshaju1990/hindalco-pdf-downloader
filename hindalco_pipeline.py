import os, re, json, pathlib, datetime, time
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

import pdfplumber
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter

# ---------------- CONFIG ----------------
START_URL = "https://www.hindalco.com/businesses/aluminium/primary-aluminium/primary-metal-price"

PDF_DIR  = pathlib.Path("hindalco_pdfs")
DATA_DIR = pathlib.Path("data")
PDF_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

LATEST_JSON          = pathlib.Path("latest_hindalco_pdf.json")      # single source of truth
LAST_PROCESSED_FILE  = DATA_DIR / "last_hindalco_processed.txt"      # guard against duplicates
EXCEL_FILE           = DATA_DIR / "hindalco_prices.xlsx"

COLUMNS = ["Sl.no.", "Description", "Grade", "Basic Price", "Circular Date", "Circular Link"]

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# match pieces like ...-08-august-2025.pdf  OR ...-8-august-2025.pdf
FILENAME_DATE_RE = re.compile(r"(\d{1,2})[-_ ]([A-Za-z]+)[-_ ](\d{4})", re.IGNORECASE)


# ---------------- UTILITIES ----------------
def get_html(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
    r.raise_for_status()
    return r.text

def find_latest_pdf_url(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.find_all("a", href=True)
    candidates = []
    for a in anchors:
        href = a["href"].strip()
        if href.lower().endswith(".pdf"):
            abs_url = urljoin(START_URL, href)
            text = (a.get_text(" ", strip=True) or "").lower()
            # Prefer primary price PDFs
            score = 0
            if "ready" in text or "reckoner" in text or "price" in text:
                score += 2
            if any(m in href.lower() for m in ["ready", "reckoner", "price"]):
                score += 2
            # use position as tie-breaker
            candidates.append((score, abs_url))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]

def read_latest_json() -> dict:
    if LATEST_JSON.exists():
        try:
            return json.loads(LATEST_JSON.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def write_latest_json(pdf_url: str, filename: str):
    obj = {
        "last_pdf_url": pdf_url,
        "download_timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "filename": filename
    }
    LATEST_JSON.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def download_pdf(pdf_url: str) -> pathlib.Path:
    headers = {
        "User-Agent": UA,
        "Accept": "application/pdf,*/*;q=0.9",
        "Referer": START_URL
    }
    with requests.get(pdf_url, headers=headers, timeout=60, stream=True, allow_redirects=True) as r:
        r.raise_for_status()
        ctype = r.headers.get("Content-Type", "").lower()
        if "application/pdf" not in ctype:
            raise RuntimeError(f"Expected PDF but got Content-Type={ctype!r}")
        name = os.path.basename(urlparse(r.url).path)
        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        fname = f"{timestamp}_{name}" if name else f"hindalco_{timestamp}.pdf"
        dest = PDF_DIR / fname
        with open(dest, "wb") as f:
            for chunk in r.iter_content(65536):
                if chunk:
                    f.write(chunk)
    return dest

def parse_date_from_filename(filename: str) -> str:
    m = FILENAME_DATE_RE.search(filename)
    if not m:
        return datetime.date.today().strftime("%d.%m.%Y")
    day, mon_text, year = m.groups()
    months = {m: i for i, m in enumerate(
        ["january","february","march","april","may","june","july","august","september","october","november","december"], start=1)}
    dd = int(day); yyyy = int(year)
    mm = months.get(mon_text.lower())
    if not mm:
        return datetime.date.today().strftime("%d.%m.%Y")
    try:
        d = datetime.date(yyyy, mm, dd)
        return d.strftime("%d.%m.%Y")
    except ValueError:
        return datetime.date.today().strftime("%d.%m.%Y")

def divide_thousands(x: str | float | int) -> float | None:
    s = str(x).replace(",", "").strip()
    if not s:
        return None
    try:
        return round(float(s)/1000.0, 3)
    except ValueError:
        return None

def extract_target_row(pdf_path: pathlib.Path) -> tuple[str, str]:
    """
    Return (description, raw_price) for the row containing P0610 + P1020 + 'EC Grade'.
    """
    must_have = ["P0610", "P1020", "EC GRADE"]
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # Tables first
            try:
                tables = page.extract_tables()
            except Exception:
                tables = []
            for tbl in tables or []:
                for row in tbl:
                    cells = [(c or "").strip() for c in row]
                    line = " ".join(cells).upper()
                    if all(x in line for x in must_have):
                        price = ""
                        for c in reversed(cells):
                            if re.search(r"\d", c):
                                price = c.replace(",", "").strip()
                                break
                        desc = " ".join(cells[:-1]).strip()
                        return desc, price
            # Fallback: line scan
            words = page.extract_words(use_text_flow=True, keep_blank_chars=False)
            lines = {}
            for w in words:
                y = round(w["top"], 1)
                lines.setdefault(y, []).append(w)
            for _, wlist in lines.items():
                text_line = " ".join([w["text"] for w in sorted(wlist, key=lambda x: x["x0"])])
                u = text_line.upper()
                if all(x in u for x in must_have):
                    m = re.search(r"(\d{5,7}(?:\.\d+)?)\s*$", text_line.replace(",", ""))
                    price = m.group(1) if m else ""
                    desc = text_line.strip()
                    return desc, price
    raise RuntimeError("Could not find target row (needs P0610 / P1020 / EC Grade).")

def sort_df(df: pd.DataFrame) -> pd.DataFrame:
    dtd = pd.to_datetime(df["Circular Date"], dayfirst=True, errors="coerce")
    df = df.assign(_d=dtd).sort_values(by=["_d", "Sl.no."], ascending=[False, True], kind="stable").drop(columns=["_d"])
    df["Basic Price"] = pd.to_numeric(df["Basic Price"], errors="coerce").round(3)
    df["Circular Date"] = pd.to_datetime(df["Circular Date"], dayfirst=True, errors="coerce").dt.strftime("%d.%m.%Y")
    return df

def save_excel_formatted(df: pd.DataFrame, path: pathlib.Path):
    df.to_excel(path, index=False)
    wb = load_workbook(path)
    ws = wb.active
    center = Alignment(horizontal="center", vertical="center")

    # Autofit widths
    for cidx, cname in enumerate(df.columns, start=1):
        max_len = len(str(cname))
        for v in df[cname].astype(str).values:
            max_len = max(max_len, len(v))
        ws.column_dimensions[get_column_letter(cidx)].width = max(10, min(max_len + 2, 80))

    header_row = 1
    link_col_idx  = COLUMNS.index("Circular Link") + 1
    price_col_idx = COLUMNS.index("Basic Price") + 1

    for r in range(1, ws.max_row + 1):
        for c in range(1, ws.max_column + 1):
            cell = ws.cell(row=r, column=c)
            cell.alignment = center
            if r > header_row and c == price_col_idx:
                cell.number_format = "0.000"
            if r > header_row and c == link_col_idx and isinstance(cell.value, str) and cell.value.startswith("http"):
                cell.hyperlink = cell.value

    ws.freeze_panes = "A2"
    wb.save(path)


# ---------------- MAIN ----------------
def main():
    # 1) Find latest PDF URL from site
    html = get_html(START_URL)
    pdf_url = find_latest_pdf_url(html)
    if not pdf_url:
        print("No PDF link found on Hindalco page. Exiting.")
        return

    # 2) Dedup by last_pdf_url
    latest = read_latest_json()
    last_url = latest.get("last_pdf_url")
    if last_url == pdf_url:
        print("No new PDF (same URL). Skipping download & Excel update.")
        return

    # 3) Download
    pdf_path = download_pdf(pdf_url)
    write_latest_json(pdf_url, str(pdf_path))
    print(f"Downloaded: {pdf_path.name}")

    # 4) Avoid double-processing by filename guard
    last_name = LAST_PROCESSED_FILE.read_text(encoding="utf-8").strip() if LAST_PROCESSED_FILE.exists() else ""
    if pdf_path.name == last_name:
        print("Latest PDF already processed. Skipping Excel update.")
        return

    # 5) Extract row
    desc, raw_price = extract_target_row(pdf_path)
    price = divide_thousands(raw_price)
    if price is None:
        raise RuntimeError(f"Could not parse numeric price: {raw_price!r}")

    # 6) Build record
    circ_date = parse_date_from_filename(pdf_path.name)
    grade = "P1020"                         # per your instruction
    link = pdf_url

    # 7) Append to Excel
    if EXCEL_FILE.exists():
        df = pd.read_excel(EXCEL_FILE, dtype={"Sl.no.": "Int64"})
        for c in COLUMNS:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[COLUMNS]
        next_slno = int(df["Sl.no."].max()) + 1 if df["Sl.no."].notna().any() else 1
    else:
        df = pd.DataFrame(columns=COLUMNS)
        next_slno = 1

    new_row = {
        "Sl.no.": next_slno,
        "Description": desc,
        "Grade": grade,
        "Basic Price": price,
        "Circular Date": circ_date,
        "Circular Link": link,
    }

    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df = sort_df(df)
    save_excel_formatted(df, EXCEL_FILE)

    LAST_PROCESSED_FILE.write_text(pdf_path.name, encoding="utf-8")
    print(f"Excel updated: {EXCEL_FILE} (added Sl.no. {next_slno})")

if __name__ == "__main__":
    main()
