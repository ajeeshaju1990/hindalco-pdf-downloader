import os
import re
import json
import pathlib
import datetime
import argparse
from urllib.parse import urljoin, urlparse

import requests
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

LATEST_JSON = pathlib.Path("latest_hindalco_pdf.json")  # stores last_pdf_url, filename, timestamp
LAST_PROCESSED_FILE = DATA_DIR / "last_hindalco_processed.txt"  # guard for latest mode (filename)
PROCESSED_SET_FILE = DATA_DIR / "processed_files.txt"  # set of filenames processed (backfill + normal)
EXCEL_FILE = DATA_DIR / "hindalco_prices.xlsx"

# final DAILY columns
DAILY_COLUMNS = ["Date", "Description", "Grade", "Basic Price", "Circular Date", "Circular Link"]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Robust date extractor from filename/url:
# Matches patterns like "07-february-2026" or "7 february 2026" anywhere in string
MONTHS_PATTERN = (
    "january|february|march|april|may|june|july|august|september|october|november|december"
)
FILENAME_DATE_RE = re.compile(rf"(?i)(\d{{1,2}})[\-_ HS_PATTERN}\d{{4}}")


# -------------------- HTML & DOWNLOAD --------------------
def get_html(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
    r.raise_for_status()
    return r.text


def find_latest_pdf_url(html: str) -> str | None:
    """
    Find the most likely 'ready reckoner' PDF link on the page.
    Scoring is based on anchor text + href.
    """
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.find_all("a", href=True)
    candidates: list[tuple[int, str]] = []

    for a in anchors:
        href = (a["href"] or "").strip()
        if not href.lower().endswith(".pdf"):
            continue

        abs_url = urljoin(START_URL, href)
        text = (a.get_text(" ", strip=True) or "").lower()

        score = 0
        if any(k in text for k in ("ready", "reckoner", "price")):
            score += 2
        if any(k in href.lower() for k in ("ready", "reckoner", "price")):
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
        "filename": filename,
    }
    LATEST_JSON.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def _normalize_url(u: str) -> str:
    """Normalize URL for comparison (drop query/fragment)."""
    try:
        p = urlparse(u)
        return p._replace(query="", fragment="").geturl()
    except Exception:
        return u


def download_pdf(pdf_url: str) -> pathlib.Path:
    headers = {
        "User-Agent": UA,
        "Accept": "application/pdf,*/*;q=0.9",
        "Referer": START_URL,
    }

    with requests.get(pdf_url, headers=headers, timeout=60, stream=True, allow_redirects=True) as r:
        r.raise_for_status()
        ctype = (r.headers.get("Content-Type", "") or "").lower()

        # Hindalco may serve 'application/pdf; charset=binary' etc. so substring is enough
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
def parse_date_from_filename(filename_or_url: str) -> str:
    """
    Extract circular date as dd.mm.YYYY from filenames/URLs like:
      20260210_045958_primary-ready-reckoner-07-february-2026.pdf
      primary-ready-reckoner-7-february-2026.pdf
      .../primary-ready-reckoner-07-february-2026.pdf

    If parse fails, fallback to today's date.
    """
    s = filename_or_url or ""
    m = FILENAME_DATE_RE.search(s)
    if not m:
        return datetime.date.today().strftime("%d.%m.%Y")

    day, mon_text, year = m.group(1), m.group(2), m.group(3)

    months = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }

    mm = months.get(mon_text.lower())
    if not mm:
        return datetime.date.today().strftime("%d.%m.%Y")

    try:
        d = datetime.date(int(year), mm, int(day))
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
    """Return (description, raw_price) for the row containing P0610 + P1020 + 'EC Grade'."""
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

        # 2) fallback: text lines on first page
        if pdf.pages:
            words = pdf.pages[0].extract_words(use_text_flow=True, keep_blank_chars=False)
        else:
            words = []

        lines: dict[float, list[dict]] = {}
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
        return datetime.datetime.strptime(str(s), "%d.%m.%Y").date()
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
    cols = [str(c).strip() for c in df.columns]

    keep = ["Description", "Grade", "Basic Price", "Circular Date", "Circular Link"]
    for k in keep:
        if k not in df.columns:
            df[k] = pd.NA

    if "Sl.no." in cols:
        # old format: each row already a circular entry
        ev = df[keep].copy()
    else:
        # daily format: keep last per circular date
        ev = df[keep].copy()
        ev = ev.sort_values(by="Circular Date").drop_duplicates(subset=["Circular Date"], keep="last")

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


def add_event(events: list[dict], desc: str, grade: str, price: float,
              circular_date_str: str, link: str) -> list[dict]:
    """Merge a new event; keep newest info for same Circular Date."""
    cdt = _to_dt(circular_date_str)
    if not cdt:
        return events

    events = [e for e in events if e["cdate"] != cdt]
    events.append({
        "desc": desc,
        "grade": grade or "P1020",
        "price": price,
        "cdate": cdt,
        "clink": link or "",
    })
    events.sort(key=lambda e: e["cdate"])
    return events


def build_daily_from_events(events: list[dict], end_date: datetime.date | None = None) -> pd.DataFrame:
    """
    Create daily rows from first event date through end_date (default today),
    forward-filling values from latest circular <= that day.
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
        while idx + 1 < len(events) and events[idx + 1]["cdate"] <= d:
            idx += 1
            current = events[idx]

        rows.append({
            "Date": _fmt_date_dash(d),
            "Description": current["desc"],
            "Grade": current["grade"] or "P1020",
            "Basic Price": round(float(current["price"]), 3) if current["price"] is not None else None,
            "Circular Date": _fmt_date_dot(current["cdate"]),
            "Circular Link": current["clink"],
        })

    df = pd.DataFrame(rows)
    df["DateDT"] = pd.to_datetime(df["Date"], format="%d-%m-%Y", errors="coerce")
    df = df.sort_values(by="DateDT", ascending=False).drop(columns=["DateDT"])
    return df[DAILY_COLUMNS]


# -------------------- WRITE EXCEL (formatting) --------------------
def save_excel_formatted(df: pd.DataFrame, path: pathlib.Path):
    """
    Writes df to Excel and applies formatting.
    FIXED: safe width computation (no len(float)), safe NaN/None handling.
    """
    # Defensive: ensure workbook exists even if df empty
    if df is None or df.empty:
        pd.DataFrame(columns=DAILY_COLUMNS).to_excel(path, index=False)
        return

    df.to_excel(path, index=False)

    wb = load_workbook(path)
    ws = wb.active
    center = Alignment(horizontal="center", vertical="center")

    # Autofit widths - robust to floats/NaN/None
    for cidx, cname in enumerate(df.columns, start=1):
        max_len = len(str(cname))
        for v in df[cname].tolist():
            try:
                if v is None or pd.isna(v):
                    sv = ""
                else:
                    sv = str(v)
            except Exception:
                sv = str(v)

            if len(sv) > max_len:
                max_len = len(sv)

        ws.column_dimensions[get_column_letter(cidx)].width = max(12, min(max_len + 2, 80))

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
            if isinstance(val, str) and val.startswith("http"):
                ws.cell(row=r, column=link_col_idx).hyperlink = val

    ws.freeze_panes = "A2"
    wb.save(path)


# -------------------- MODES --------------------
def load_processed_set() -> set[str]:
    if PROCESSED_SET_FILE.exists():
        return set(
            x.strip()
            for x in PROCESSED_SET_FILE.read_text(encoding="utf-8").splitlines()
            if x.strip()
        )
    return set()


def save_processed_set(s: set[str]):
    PROCESSED_SET_FILE.write_text("\n".join(sorted(s)), encoding="utf-8")


def run_normal():
    """
    Daily mode:
      - If new circular → download & add event
      - Always rebuild DAILY series up to today
    """
    events = load_events_from_excel_if_any()

    html = get_html(START_URL)
    pdf_url = find_latest_pdf_url(html)

    latest = read_latest_json()
    last_url = latest.get("last_pdf_url", "")

    # Normalize for stable comparisons (avoid ?query changes)
    pdf_url_norm = _normalize_url(pdf_url) if pdf_url else ""
    last_url_norm = _normalize_url(last_url) if last_url else ""

    if pdf_url and pdf_url_norm != last_url_norm:
        pdf_path = download_pdf(pdf_url)
        write_latest_json(pdf_url, str(pdf_path))
        print(f"Downloaded: {pdf_path.name}")

        # Avoid duplicate processing by filename
        last_name = LAST_PROCESSED_FILE.read_text(encoding="utf-8").strip() if LAST_PROCESSED_FILE.exists() else ""
        if pdf_path.name != last_name:
            desc, raw_price = extract_target_row(pdf_path)
            price = divide_thousands(raw_price)
            if price is None:
                raise RuntimeError(f"Could not parse numeric price: {raw_price!r}")

            # Date from filename (or URL if needed)
            circular_date_str = parse_date_from_filename(pdf_path.name)
            if circular_date_str == datetime.date.today().strftime("%d.%m.%Y"):
                # If filename doesn't contain a date, try URL
                circular_date_str = parse_date_from_filename(pdf_url)

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

    daily_df = build_daily_from_events(events, end_date=datetime.date.today())
    save_excel_formatted(daily_df, EXCEL_FILE)
    print(f"Wrote daily sheet with {len(daily_df)} rows → {EXCEL_FILE}")


def run_backfill():
    """
    Backfill:
      - Parse ALL PDFs in hindalco_pdfs/ into events (skip already processed files)
      - Rebuild DAILY sheet to today
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
    """Repair only: rebuild daily sheet from whatever is in Excel."""
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
