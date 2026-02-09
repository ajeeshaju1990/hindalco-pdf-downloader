import os, re, json, pathlib, datetime, argparse
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import pdfplumber
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter

# -------------------- CONFIG --------------------
START_URL = "https://www.hindalco.com/businesses/aluminium/primary-aluminium/primary-metal-price"
PDF_DIR = pathlib.Path("hindalco_pdfs")
DATA_DIR = pathlib.Path("data")
PDF_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

# stores last_pdf_url, filename, timestamp
LATEST_JSON = pathlib.Path("latest_hindalco_pdf.json")
# guard for latest mode (filename)
LAST_PROCESSED_FILE = DATA_DIR / "last_hindalco_processed.txt"
# set of filenames processed (backfill + normal)
PROCESSED_SET_FILE = DATA_DIR / "processed_files.txt"
EXCEL_FILE = DATA_DIR / "hindalco_prices.xlsx"

# final DAILY columns
DAILY_COLUMNS = ["Date", "Description", "Grade", "Basic Price", "Circular Date", "Circular Link"]

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# e.g. ...-08-august-2025.pdf OR ...-8-august-2025.pdf
FILENAME_DATE_RE = re.compile(r"(\d{1,2})\d{4}", re.IGNORECASE)


# -------------------- HTML & DOWNLOAD --------------------
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
        # allow 'application/pdf' even with parameters, just check substring
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


# -------------------- PDF PARSE & HELPERS --------------------
def parse_date_from_filename(filename: str) -> str:
    """
    Extract dd.mm.YYYY from '...-07-february-2026.pdf' style names.
    Fallback to today's date if parse fails.
    """
    m = FILENAME_DATE_RE.search(filename)
    if not m:
        return datetime.date.today().strftime("%d.%m.%Y")
    day, mon_text, year = m.groups()
    months = {m: i for i, m in enumerate(
        ["january","february","march","april","may","june","july",
         "august","september","october","november","december"], start=1)}
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
        return round(float(s) / 1000.0, 3)
    except ValueError:
        return None


def extract_target_row(pdf_path: pathlib.Path) -> tuple[str, str]:
    """Return (description, raw_price) for row containing P0610 + P1020 + 'EC Grade'."""
    must_have = ["P0610", "P1020", "EC GRADE"]
    with pdfplumber.open(pdf_path) as pdf:
        # 1) try tables
        for page in pdf.pages:
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

        # 2) fall back to text lines on first page
        words = pdf.pages[0].extract_words(use_text_flow=True, keep_blank_chars=False) if pdf.pages else []
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


# -------------------- EVENTS ↔ DAILY --------------------
def _to_dt(s: str) -> datetime.date | None:
    try:
        return datetime.datetime.strptime(s, "%d.%m.%Y").date()
    except Exception:
        return None


def _fmt_date_dash(d: datetime.date) -> str:
    # "Date" column format: dd-mm-YYYY
    return d.strftime("%d-%m-%Y")


def _fmt_date_dot(d: datetime.date) -> str:
    # "Circular Date" format: dd.mm.YYYY
    return d.strftime("%d.%m.%Y")


def load_events_from_excel_if_any() -> list[dict]:
    """
    Read existing Excel and extract unique circular-events.
    Works with old 'Sl.no.' format or new 'Date' format.
    """
    if not EXCEL_FILE.exists():
        return []

    df = pd.read_excel(EXCEL_FILE)
    cols = [c.strip() for c in df.columns]

    # Normalize to the same set of columns to derive events
    keep = ["Description", "Grade", "Basic Price", "Circular Date", "Circular Link"]
    for k in keep:
        if k not in df.columns:
            df[k] = pd.NA

    if "Sl.no." in cols:
        # Old event sheet; each row already a circular-entry
        ev = df[keep].copy()
    else:
        # Already daily; collapse to unique events per Circular Date (keep last link)
        ev = df[keep].copy()
        ev = ev.sort_values(by="Circular Date").drop_duplicates(subset=["Circular Date"], keep="last")

    # Coerce types
    ev["Basic Price"] = pd.to_numeric(ev["Basic Price"], errors="coerce")
    ev["Circular Date DT"] = ev["Circular Date"].apply(_to_dt)
    ev = ev.dropna(subset=["Circular Date DT"]).sort_values("Circular Date DT")

    events = []
    for _, r in ev.iterrows():
        events.append({
            "desc": r.get("Description", "") or "",
            "grade": r.get("Grade", "P1020") or "P1020",
            "price": float(r.get("Basic Price")) if pd.notna(r.get("Basic Price")) else None,
            "cdate": r["Circular Date DT"],
            "clink": r.get("Circular Link", "") or "",
        })
    return events


def add_event(events: list[dict], desc: str, grade: str, price: float, circular_date_str: str, link: str):
    """Merge a new event; keep the newest info for a given Circular Date."""
    cdt = _to_dt(circular_date_str)
    if not cdt:
        return events
    # remove any existing same cdate, then append
    events = [e for e in events if e["cdate"] != cdt]
    events.append({"desc": desc, "grade": grade or "P1020", "price": price, "cdate": cdt, "clink": link or ""})
    events.sort(key=lambda e: e["cdate"])
    return events


def build_daily_from_events(events: list[dict], end_date: datetime.date | None = None) -> pd.DataFrame:
    """
    Create daily rows from first event date through end_date (default today),
    forward-filling Description/Grade/Price/Circular Date/Link from the latest circular <= that day.
    """
    if not events:
        return pd.DataFrame(columns=DAILY_COLUMNS)

    events = sorted(events, key=lambda e: e["cdate"])
    start = events[0]["cdate"]
    today = end_date or datetime.date.today()
    if start > today:
        start = today

    rows = []
    idx = 0
    current = events[0]
    for d in (start + datetime.timedelta(n) for n in range((today - start).days + 1)):
        # advance current event if we crossed to a newer one
        while idx + 1 < len(events) and events[idx + 1]["cdate"] <= d:
            idx += 1
            current = events[idx]
        rows.append({
            "Date": _fmt_date_dash(d),  # dd-mm-YYYY
            "Description": current["desc"],
            "Grade": current["grade"] or "P1020",
            "Basic Price": round(float(current["price"]), 3) if current["price"] is not None else None,
            "Circular Date": _fmt_date_dot(current["cdate"]),
            "Circular Link": current["clink"],
        })

    # newest first for display
    df = pd.DataFrame(rows)
    df["DateDT"] = pd.to_datetime(df["Date"], format="%d-%m-%Y", errors="coerce")
    df = df.sort_values(by="DateDT", ascending=False).drop(columns=["DateDT"])
    return df[DAILY_COLUMNS]


# -------------------- WRITE EXCEL (formatting) --------------------
def save_excel_formatted(df: pd.DataFrame, path: pathlib.Path):
    """
    Robust writer that:
      - guards empty DataFrames,
      - measures column widths on string representations (no len(float) calls),
      - applies number format to Basic Price,
      - sets hyperlinks on valid 'Circular Link' values.
    """
    # FIX: guard empty df to avoid malformed workbook and width computation on nothing
    if df is None or df.empty:
        pd.DataFrame(columns=DAILY_COLUMNS).to_excel(path, index=False)
        return

    # Write initial sheet
    df.to_excel(path, index=False)

    # Load workbook for styling
    wb = load_workbook(path)
    ws = wb.active
    center = Alignment(horizontal="center", vertical="center")

    # FIX: Autofit widths robustly (convert every value to safe string before measuring)
    for cidx, cname in enumerate(df.columns, start=1):
        max_len = len(str(cname))
        # Extract raw values (may include float/None/NaN)
        for v in df[cname].tolist():
            if v is None:
                sv = ""
            else:
                try:
                    # Use pandas isna if available (covers NaN/NaT)
                    if pd.isna(v):
                        sv = ""
                    else:
                        sv = str(v)
                except Exception:
                    sv = str(v)
            if len(sv) > max_len:
                max_len = len(sv)
        ws.column_dimensions[get_column_letter(cidx)].width = max(12, min(max_len + 2, 80))

    # numeric/date formats + center align + hyperlinks
    header_row = 1
    price_col_idx = DAILY_COLUMNS.index("Basic Price") + 1
    link_col_idx = DAILY_COLUMNS.index("Circular Link") + 1

    for r in range(1, ws.max_row + 1):
        for c in range(1, ws.max_column + 1):
            cell = ws.cell(row=r, column=c)
            cell.alignment = center
            if r > header_row and c == price_col_idx:
                cell.number_format = "0.000"

        if r > header_row:
            val = ws.cell(row=r, column=link_col_idx).value
            # FIX: only set hyperlink for proper http(s) strings
            if isinstance(val, str) and val.startswith("http"):
                ws.cell(row=r, column=link_col_idx).hyperlink = val

    ws.freeze_panes = "A2"
    wb.save(path)


# -------------------- MODES --------------------
def load_processed_set() -> set[str]:
    if PROCESSED_SET_FILE.exists():
        return set(x.strip() for x in PROCESSED_SET_FILE.read_text(encoding="utf-8").splitlines() if x.strip())
    return set()


def save_processed_set(s: set[str]):
    PROCESSED_SET_FILE.write_text("\n".join(sorted(s)), encoding="utf-8")


def run_normal():
    """
    Daily mode:
      - If new circular → download & add as an event.
      - Always rebuild DAILY sheet up to today (filling missing dates from last circular).
    """
    events = load_events_from_excel_if_any()

    # fetch latest url
    html = get_html(START_URL)
    pdf_url = find_latest_pdf_url(html)
    latest = read_latest_json()
    last_url = latest.get("last_pdf_url")

    if pdf_url and pdf_url != last_url:
        # new circular detected → download and add one event
        pdf_path = download_pdf(pdf_url)
        write_latest_json(pdf_url, str(pdf_path))
        print(f"Downloaded: {pdf_path.name}")

        # avoid duplicate process by filename
        last_name = LAST_PROCESSED_FILE.read_text(encoding="utf-8").strip() if LAST_PROCESSED_FILE.exists() else ""
        if pdf_path.name != last_name:
            desc, raw_price = extract_target_row(pdf_path)
            price = divide_thousands(raw_price)
            if price is None:
                raise RuntimeError(f"Could not parse numeric price: {raw_price!r}")
            circular_date_str = parse_date_from_filename(pdf_path.name)
            events = add_event(events, desc, "P1020", price, circular_date_str, pdf_url)
            LAST_PROCESSED_FILE.write_text(pdf_path.name, encoding="utf-8")

            processed = load_processed_set()
            processed.add(pdf_path.name)
            save_processed_set(processed)
            print(f"Added event for {circular_date_str}")
        else:
            print("Latest PDF already processed; skipping parse.")
    else:
        print("No new circular; will forward-fill daily series.")

    # rebuild daily to today (fills gaps)
    daily_df = build_daily_from_events(events, end_date=datetime.date.today())
    save_excel_formatted(daily_df, EXCEL_FILE)
    print(f"Wrote daily sheet with {len(daily_df)} rows → {EXCEL_FILE}")


def run_backfill():
    """
    Backfill:
      - Parse ALL PDFs in hindalco_pdfs/ into events (skip already processed files).
      - Rebuild DAILY sheet to today.
    """
    pdfs = sorted(PDF_DIR.glob("*.pdf"), key=lambda p: p.stat().st_mtime)  # oldest→newest
    events = load_events_from_excel_if_any()
    processed = load_processed_set()
    added = 0

    for pdf_path in pdfs:
        if pdf_path.name in processed:
            continue
        try:
            desc, raw_price = extract_target_row(pdf_path)
            price = divide_thousands(raw_price)
            if price is None:
                print(f"Could not parse price in {pdf_path.name}; skipping.")
                continue
            circular_date_str = parse_date_from_filename(pdf_path.name)
            events = add_event(events, desc, "P1020", price, circular_date_str, link="")
            processed.add(pdf_path.name)
            added += 1
        except Exception as e:
            print(f"Error processing {pdf_path.name}: {e}")

    save_processed_set(processed)
    daily_df = build_daily_from_events(events, end_date=datetime.date.today())
    save_excel_formatted(daily_df, EXCEL_FILE)

    if added == 0:
        print("Backfill complete. No new events added.")
    else:
        print(f"Backfill complete. Added {added} event(s). Rebuilt daily sheet with {len(daily_df)} rows.")


def run_repair():
    """Repair only: rebuild daily sheet from whatever is in Excel (events or existing daily)."""
    events = load_events_from_excel_if_any()
    if not events:
        print("No events present to rebuild from.")
        return
    daily_df = build_daily_from_events(events, end_date=datetime.date.today())
    save_excel_formatted(daily_df, EXCEL_FILE)
    print(f"Repair complete. Rebuilt daily sheet with {len(daily_df)} rows.")


# -------------------- ENTRYPOINT --------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill", default="false", help="true/false (process all existing PDFs once)")
    ap.add_argument("--repair", default="false", help="true/false (rebuild daily sheet from existing data)")
    args = ap.parse_args()

    if str(args.repair).strip().lower() in ("true", "1", "yes", "y"):
        run_repair()
    elif str(args.backfill).strip().lower() in ("true", "1", "yes", "y"):
        run_backfill()
    else:
        run_normal()


if __name__ == "__main__":
    main()
