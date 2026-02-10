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

LATEST_JSON = pathlib.Path("latest_hindalco_pdf.json")
LAST_PROCESSED_FILE = DATA_DIR / "last_hindalco_processed.txt"
PROCESSED_SET_FILE = DATA_DIR / "processed_files.txt"
EXCEL_FILE = DATA_DIR / "hindalco_prices.xlsx"

DAILY_COLUMNS = ["Date", "Description", "Grade", "Basic Price", "Circular Date", "Circular Link"]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# --- filename date: "07-february-2026" etc ---
FILENAME_DATE_RE = re.compile(
    r"(?i)(\d{1,2})[\-_ ]+(january|february|march|april|may|june|july|august|"
    r"september|october|november|december)[\-_ ]+(\d{4})"
)

# --- excel date: accept dd.mm.yyyy OR dd-mm-yyyy OR dd/mm/yyyy (and even embedded in text) ---
EXCEL_DATE_RE = re.compile(r"(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})")
URL_RE = re.compile(r"(https?://\S+)")


# -------------------- HTML & DOWNLOAD --------------------
def get_html(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=60)
    r.raise_for_status()
    return r.text


def find_latest_pdf_url(html: str) -> str | None:
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


# -------------------- DATE HELPERS --------------------
def parse_date_from_filename(filename_or_url: str) -> str:
    s = filename_or_url or ""
    m = FILENAME_DATE_RE.search(s)
    if not m:
        return datetime.date.today().strftime("%d.%m.%Y")

    day, mon_text, year = m.groups()
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


def _extract_excel_date(s) -> datetime.date | None:
    """Extract a date from messy Excel cells: accepts dd.mm.yyyy / dd-mm-yyyy / dd/mm/yyyy."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    txt = str(s)
    m = EXCEL_DATE_RE.search(txt)
    if not m:
        return None
    dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
    try:
        return datetime.date(int(yyyy), int(mm), int(dd))
    except ValueError:
        return None


def _extract_first_url(*vals) -> str:
    """Return first http(s) URL found in any of the provided strings."""
    for v in vals:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        m = URL_RE.search(str(v))
        if m:
            return m.group(1)
    return ""


# -------------------- PDF PARSE --------------------
def divide_thousands(x: str | float | int) -> float | None:
    s = str(x).replace(",", "").strip()
    if not s:
        return None
    try:
        return round(float(s) / 1000.0, 3)
    except ValueError:
        return None


def extract_target_row(pdf_path: pathlib.Path) -> tuple[str, str]:
    must_have = ["P0610", "P1020", "EC GRADE"]

    with pdfplumber.open(pdf_path) as pdf:
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

        words = pdf.pages[0].extract_words(use_text_flow=True, keep_blank_chars=False) if pdf.pages else []
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
def _fmt_date_dash(d: datetime.date) -> str:
    return d.strftime("%d-%m-%Y")


def _fmt_date_dot(d: datetime.date) -> str:
    return d.strftime("%d.%m.%Y")


def load_events_from_excel_if_any() -> list[dict]:
    """
    Robustly read Excel and extract unique events.
    - Accepts Circular Date as dd.mm.yyyy OR dd-mm-yyyy OR dd/mm/yyyy
    - If date cell contains extra junk (e.g., URL), extracts the first date
    - If Circular Link missing, tries to extract URL from other cells
    """
    if not EXCEL_FILE.exists():
        return []

    df = pd.read_excel(EXCEL_FILE)
    cols = [str(c).strip() for c in df.columns]

    keep = ["Description", "Grade", "Basic Price", "Circular Date", "Circular Link"]
    for k in keep:
        if k not in df.columns:
            df[k] = pd.NA

    # If already daily: reduce to unique circular events by Circular Date (keep last)
    ev = df[keep].copy()
    # parse circular date robustly
    ev["Circular Date DT"] = ev["Circular Date"].apply(_extract_excel_date)

    # Also try to extract date from "Circular Link" cell if date missing (some edits merge cells)
    miss = ev["Circular Date DT"].isna()
    if miss.any():
        ev.loc[miss, "Circular Date DT"] = ev.loc[miss, "Circular Link"].apply(_extract_excel_date)

    ev = ev.dropna(subset=["Circular Date DT"])

    # numeric price
    ev["Basic Price"] = pd.to_numeric(ev["Basic Price"], errors="coerce")

    # fill missing URL from any cell
    ev["Circular Link"] = ev.apply(
        lambda r: r["Circular Link"] if isinstance(r["Circular Link"], str) and r["Circular Link"].startswith("http")
        else _extract_first_url(r["Circular Link"], r["Circular Date"], r["Description"]),
        axis=1
    )

    # dedupe by circular date keep last
    ev = ev.sort_values(by="Circular Date DT").drop_duplicates(subset=["Circular Date DT"], keep="last")

    events = []
    for _, r in ev.iterrows():
        events.append({
            "desc": r.get("Description", "") or "",
            "grade": r.get("Grade", "P1020") or "P1020",
            "price": float(r.get("Basic Price")) if pd.notna(r.get("Basic Price")) else None,
            "cdate": r["Circular Date DT"],
            "clink": r.get("Circular Link", "") or "",
        })
    events.sort(key=lambda e: e["cdate"])
    return events


def add_event(events: list[dict], desc: str, grade: str, price: float, circular_date: datetime.date, link: str) -> list[dict]:
    events = [e for e in events if e["cdate"] != circular_date]
    events.append({"desc": desc, "grade": grade or "P1020", "price": price, "cdate": circular_date, "clink": link or ""})
    events.sort(key=lambda e: e["cdate"])
    return events


def build_daily_from_events(events: list[dict], end_date: datetime.date | None = None) -> pd.DataFrame:
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
    if df is None or df.empty:
        pd.DataFrame(columns=DAILY_COLUMNS).to_excel(path, index=False)
        return

    df.to_excel(path, index=False)

    wb = load_workbook(path)
    ws = wb.active
    center = Alignment(horizontal="center", vertical="center")

    # safe autofit widths
    for cidx, cname in enumerate(df.columns, start=1):
        max_len = len(str(cname))
        for v in df[cname].tolist():
            try:
                sv = "" if (v is None or pd.isna(v)) else str(v)
            except Exception:
                sv = str(v)
            max_len = max(max_len, len(sv))
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


# -------------------- STATE FILES --------------------
def load_processed_set() -> set[str]:
    if PROCESSED_SET_FILE.exists():
        return set(x.strip() for x in PROCESSED_SET_FILE.read_text(encoding="utf-8").splitlines() if x.strip())
    return set()


def save_processed_set(s: set[str]):
    PROCESSED_SET_FILE.write_text("\n".join(sorted(s)), encoding="utf-8")


# -------------------- MODES --------------------
def run_normal():
    events = load_events_from_excel_if_any()

    html = get_html(START_URL)
    pdf_url = find_latest_pdf_url(html)

    latest = read_latest_json()
    last_url = latest.get("last_pdf_url", "")

    if pdf_url and _normalize_url(pdf_url) != _normalize_url(last_url):
        pdf_path = download_pdf(pdf_url)
        write_latest_json(pdf_url, str(pdf_path))
        print(f"Downloaded: {pdf_path.name}")

        last_name = LAST_PROCESSED_FILE.read_text(encoding="utf-8").strip() if LAST_PROCESSED_FILE.exists() else ""
        if pdf_path.name != last_name:
            desc, raw_price = extract_target_row(pdf_path)
            price = divide_thousands(raw_price)
            if price is None:
                raise RuntimeError(f"Could not parse numeric price: {raw_price!r}")

            cdate_str = parse_date_from_filename(pdf_path.name)
            cdate_dt = _extract_excel_date(cdate_str)  # reuse parser
            if not cdate_dt:
                cdate_dt = datetime.date.today()

            events = add_event(events, desc, "P1020", price, cdate_dt, pdf_url)

            LAST_PROCESSED_FILE.write_text(pdf_path.name, encoding="utf-8")

            processed = load_processed_set()
            processed.add(pdf_path.name)
            save_processed_set(processed)

            print(f"Added event for {cdate_dt.strftime('%d.%m.%Y')}")
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
    - Parse ALL PDFs in hindalco_pdfs/
    - BUT: do NOT skip a PDF just because it's in processed_files.txt
      if its circular date is NOT present in current events.
    """
    pdfs = sorted(PDF_DIR.glob("*.pdf"), key=lambda p: p.stat().st_mtime)
    events = load_events_from_excel_if_any()
    events_cdates = set(e["cdate"] for e in events)

    processed = load_processed_set()
    added = 0

    for pdf_path in pdfs:
        cdate_str = parse_date_from_filename(pdf_path.name)
        cdate_dt = _extract_excel_date(cdate_str)

        # If we cannot detect date, fall back to mtime date (rare)
        if not cdate_dt:
            cdate_dt = datetime.date.fromtimestamp(pdf_path.stat().st_mtime)

        # ✅ key change:
        # Skip only if BOTH:
        #  - file is in processed list AND
        #  - event for that circular date already exists
        if pdf_path.name in processed and cdate_dt in events_cdates:
            continue

        try:
            desc, raw_price = extract_target_row(pdf_path)
            price = divide_thousands(raw_price)
            if price is None:
                print(f"Could not parse price in {pdf_path.name}; skipping.")
                continue

            events = add_event(events, desc, "P1020", price, cdate_dt, link="")
            events_cdates.add(cdate_dt)

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
