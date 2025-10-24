import os, re, json, pathlib, datetime, argparse
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

LATEST_JSON           = pathlib.Path("latest_hindalco_pdf.json")      # stores last_pdf_url, filename, timestamp
LAST_PROCESSED_FILE   = DATA_DIR / "last_hindalco_processed.txt"      # guard for latest mode
PROCESSED_SET_FILE    = DATA_DIR / "processed_files.txt"              # set of filenames processed (backfill + normal)
EXCEL_FILE            = DATA_DIR / "hindalco_prices.xlsx"

COLUMNS = ["Sl.no.", "Description", "Grade", "Basic Price", "Circular Date", "Circular Link"]

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# e.g. ...-08-august-2025.pdf  OR ...-8-august-2025.pdf
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
            score = 0
            if "ready" in text or "reckoner" in text or "price" in text:
                score += 2
            if any(m in href.lower() for m in ["ready", "reckoner", "price"]):
                score += 2
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
    if not s: return None
    try: return round(float(s)/1000.0, 3)
    except ValueError: return None

def extract_target_row(pdf_path: pathlib.Path) -> tuple[str, str]:
    """Return (description, raw_price) for the row containing P0610 + P1020 + 'EC Grade'."""
    must_have = ["P0610", "P1020", "EC GRADE"]
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # tables
            try: tables = page.extract_tables()
            except Exception: tables = []
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
            # text lines
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
    raise RuntimeError(f"Could not find target row in: {pdf_path.name}")

def load_processed_set() -> set[str]:
    if PROCESSED_SET_FILE.exists():
        return set(x.strip() for x in PROCESSED_SET_FILE.read_text(encoding="utf-8").splitlines() if x.strip())
    return set()

def save_processed_set(s: set[str]):
    PROCESSED_SET_FILE.write_text("\n".join(sorted(s)), encoding="utf-8")

# ---------- CLEANUP / ORDERING ----------
def clean_and_renumber(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicates and renumber Sl.no. based on Circular Date ascending (oldest=1)."""
    dtd = pd.to_datetime(df["Circular Date"], dayfirst=True, errors="coerce")
    df = df.assign(_date=dtd)

    # dedupe by (date, grade) — grade is always P1020
    df = df.drop_duplicates(subset=["Circular Date", "Grade"], keep="last")

    # renumber by ascending date
    df = df.sort_values(by=["_date", "Sl.no."], ascending=[True, True], kind="stable").reset_index(drop=True)
    df["Sl.no."] = range(1, len(df) + 1)

    # display newest first
    df = df.sort_values(by=["_date", "Sl.no."], ascending=[False, True], kind="stable")

    # finalize types/format
    df["Basic Price"] = pd.to_numeric(df["Basic Price"], errors="coerce").round(3)
    df["Circular Date"] = pd.to_datetime(df["Circular Date"], dayfirst=True, errors="coerce").dt.strftime("%d.%m.%Y")

    return df.drop(columns=["_date"])

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

# ==============================
# CHANGES: Daily expansion helpers (forward-fill gap days → 'Daily' sheet)
# ==============================
from datetime import datetime as _dt, timedelta as _td

EXCEL_PATH = str(EXCEL_FILE)  # reuse the same file path

def _parse_circ_date(s: str) -> pd.Timestamp:
    """Parse 'Circular Date' like 18.10.2025 or 18-10-2025 (day-first) to Timestamp."""
    if pd.isna(s):
        return pd.NaT
    s = str(s).strip().replace(".", "-")
    return pd.to_datetime(s, dayfirst=True, errors="coerce")

def _ensure_min_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure required columns exist in the circulars table (do NOT rename 'Sl.no.').
    """
    required = ["Description", "Grade", "Basic Price", "Circular Date", "Circular Link"]
    out = df.copy()
    for col in required:
        if col not in out.columns:
            out[col] = pd.NA
    return out

def _build_daily_from_circulars(df_circ: pd.DataFrame) -> pd.DataFrame:
    """
    Build a daily table by filling every calendar day between circulars with the last
    known circular's price/date/link.

    Boundary rule:
    - Only forward-fill up to **yesterday (IST)**.
    - Only fill beyond the latest circular date if **latest_circ_date < yesterday**.
    """
    circ = df_circ.copy()
    circ["Circular Date"] = circ["Circular Date"].apply(_parse_circ_date)
    circ = circ.dropna(subset=["Circular Date"]).sort_values("Circular Date").reset_index(drop=True)

    if circ.empty:
        return pd.DataFrame(columns=["Date","Description","Grade","Basic Price","Circular Date","Circular Link"])

    # Compute "yesterday" in IST without extra deps (UTC+5:30)
    utc_now = _dt.utcnow()
    now_ist = utc_now + _td(hours=5, minutes=30)
    yesterday = (now_ist.date() - _td(days=1))

    latest_circ_date = circ["Circular Date"].max().date()
    expand_until = yesterday if latest_circ_date < yesterday else latest_circ_date

    records = []
    for i in range(len(circ)):
        row = circ.iloc[i]
        start = row["Circular Date"].date()
        if i < len(circ) - 1:
            next_start = circ.iloc[i + 1]["Circular Date"].date()
            end = min(expand_until, next_start - _td(days=1))
        else:
            end = expand_until

        if end < start:
            continue

        day = start
        while day <= end:
            records.append({
                "Date": _dt.strptime(day.strftime("%d-%m-%Y"), "%d-%m-%Y").strftime("%d-%m-%Y"),  # dd-mm-YYYY
                "Description": row.get("Description", pd.NA),
                "Grade": row.get("Grade", pd.NA),
                "Basic Price": row.get("Basic Price", pd.NA),
                "Circular Date": row["Circular Date"].strftime("%d.%m.%Y"),  # keep dot format
                "Circular Link": row.get("Circular Link", pd.NA),
            })
            day += _td(days=1)

    daily = pd.DataFrame.from_records(
        records,
        columns=["Date","Description","Grade","Basic Price","Circular Date","Circular Link"]
    )

    if not daily.empty:
        daily = daily.sort_values(
            by=pd.to_datetime(daily["Date"], dayfirst=True),
            ascending=False
        ).reset_index(drop=True)
        daily["Basic Price"] = pd.to_numeric(daily["Basic Price"], errors="coerce")

    return daily

def write_two_sheets_circulars_and_daily(excel_path: str = EXCEL_PATH) -> None:
    """
    Append/replace ONLY the 'Daily' sheet so we don't lose formatting on your original sheet.
    We read circular rows from the FIRST sheet (whatever its name is).
    """
    try:
        df_in = pd.read_excel(excel_path, engine="openpyxl", sheet_name=0)
    except FileNotFoundError:
        print(f"⚠️ {excel_path} not found; skipping Daily build.")
        return

    df_circ = _ensure_min_columns(df_in)
    df_daily = _build_daily_from_circulars(df_circ)

    # Append/replace ONLY the 'Daily' sheet, keep other sheets as-is (and formatted)
    try:
        with pd.ExcelWriter(
            excel_path, engine="openpyxl", mode="a", if_sheet_exists="replace"
        ) as writer:
            df_daily.to_excel(writer, index=False, sheet_name="Daily")
        print(f"✅ Updated 'Daily' sheet in {excel_path}.")
    except ValueError:
        # When file has no workbook yet, fallback to full write (very first run)
        with pd.ExcelWriter(excel_path, engine="openpyxl", mode="w") as writer:
            df_in.to_excel(writer, index=False, sheet_name="Circulars")
            df_daily.to_excel(writer, index=False, sheet_name="Daily")
        print(f"✅ Created 'Circulars' + 'Daily' in {excel_path}.")

# ---------- WRITE/APPEND PIPELINE ----------
def write_df(df: pd.DataFrame):
    df = clean_and_renumber(df)
    save_excel_formatted(df, EXCEL_FILE)
    # ==============================
    # CHANGES: Build/refresh the 'Daily' sheet every time we update the Excel
    # ==============================
    try:
        write_two_sheets_circulars_and_daily(EXCEL_PATH)
    except Exception as e:
        print(f"⚠️ Failed to update 'Daily' sheet: {e}")

def append_row_to_excel(desc: str, price_thousands: float, date_str: str, link: str):
    grade = "P1020"  # fixed rule
    if EXCEL_FILE.exists():
        df = pd.read_excel(EXCEL_FILE, dtype={"Sl.no.": "Int64"})
    else:
        df = pd.DataFrame(columns=COLUMNS)

    for c in COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[COLUMNS]

    df = pd.concat([df, pd.DataFrame([{
        "Sl.no.": pd.NA,
        "Description": desc,
        "Grade": grade,
        "Basic Price": price_thousands,
        "Circular Date": date_str,
        "Circular Link": link or "",
    }])], ignore_index=True)

    write_df(df)

# ---------------- MODES ----------------
def run_normal():
    html = get_html(START_URL)
    pdf_url = find_latest_pdf_url(html)
    if not pdf_url:
        print("No PDF link found on Hindalco page. Exiting."); return

    latest = read_latest_json()
    if latest.get("last_pdf_url") == pdf_url:
        print("No new PDF (same URL). Skipping download & Excel update."); return

    pdf_path = download_pdf(pdf_url)
    write_latest_json(pdf_url, str(pdf_path))
    print(f"Downloaded: {pdf_path.name}")

    last_name = LAST_PROCESSED_FILE.read_text(encoding="utf-8").strip() if LAST_PROCESSED_FILE.exists() else ""
    if pdf_path.name == last_name:
        print("Latest PDF already processed. Skipping Excel update."); return

    desc, raw_price = extract_target_row(pdf_path)
    price = divide_thousands(raw_price)
    if price is None:
        raise RuntimeError(f"Could not parse numeric price: {raw_price!r}")

    date_str = parse_date_from_filename(pdf_path.name)
    append_row_to_excel(desc, price, date_str, pdf_url)

    LAST_PROCESSED_FILE.write_text(pdf_path.name, encoding="utf-8")
    processed = load_processed_set(); processed.add(pdf_path.name); save_processed_set(processed)
    print(f"Excel updated: {EXCEL_FILE}")

def run_backfill():
    pdfs = sorted(PDF_DIR.glob("*.pdf"), key=lambda p: p.stat().st_mtime)  # oldest→newest
    if not pdfs:
        print("No PDFs to backfill in hindalco_pdfs/."); return

    processed = load_processed_set()
    if EXCEL_FILE.exists():
        df = pd.read_excel(EXCEL_FILE, dtype={"Sl.no.": "Int64"})
    else:
        df = pd.DataFrame(columns=COLUMNS)
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[COLUMNS]

    added = 0
    for pdf_path in pdfs:
        if pdf_path.name in processed:  # already imported
            continue
        try:
            desc, raw_price = extract_target_row(pdf_path)
            price = divide_thousands(raw_price)
            if price is None:
                print(f"Could not parse price in {pdf_path.name}; skipping."); continue
            date_str = parse_date_from_filename(pdf_path.name)
            df = pd.concat([df, pd.DataFrame([{
                "Sl.no.": pd.NA,
                "Description": desc,
                "Grade": "P1020",
                "Basic Price": price,
                "Circular Date": date_str,
                "Circular Link": "",   # unknown for historical
            }])], ignore_index=True)
            processed.add(pdf_path.name)
            added += 1
        except Exception as e:
            print(f"Error processing {pdf_path.name}: {e}")

    save_processed_set(processed)
    if added == 0:
        print("Backfill complete. No new rows to add.")
    else:
        write_df(df)
        print(f"Backfill complete. Added {added} row(s). Cleaned duplicates and renumbered.")

def run_repair():
    if not EXCEL_FILE.exists():
        print("No Excel file to repair yet."); return
    df = pd.read_excel(EXCEL_FILE, dtype={"Sl.no.": "Int64"})
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[COLUMNS]
    write_df(df)
    print("Repair complete: duplicates removed and Sl.no. renumbered by date asc.")

# ---------------- ENTRYPOINT ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill", default="false", help="true/false (process all existing PDFs once)")
    ap.add_argument("--repair",   default="false", help="true/false (cleanup Excel: dedupe + renumber)")
    args = ap.parse_args()

    if str(args.repair).strip().lower() in ("true", "1", "yes", "y"):
        run_repair()
    elif str(args.backfill).strip().lower() in ("true", "1", "yes", "y"):
        run_backfill()
    else:
        run_normal()

if __name__ == "__main__":
    main()
