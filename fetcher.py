"""
fetcher.py
----------
Fetches corporate action announcements from NSE's public API,
cleans inconsistent data formats using Pandas, and writes
structured records to the corporate_actions SQL table.

NSE publishes data for: dividends, splits, buybacks, rights, bonus issues.
The raw API response has inconsistent date formats, mixed-case action types,
and duplicate entries — all handled here before writing to SQL.

Usage:
    python fetcher.py
"""

import requests
import pandas as pd
from datetime import datetime, date
from sqlalchemy import text

from db.connection import engine

# ─────────────────────────────────────────────
# NSE API CONFIG
# ─────────────────────────────────────────────

NSE_BASE   = "https://www.nseindia.com"
NSE_API    = f"{NSE_BASE}/api/corporates-corporateActions"
HEADERS    = {
    "User-Agent"      : "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept"          : "application/json, text/plain, */*",
    "Accept-Language" : "en-US,en;q=0.9",
    "Referer"         : f"{NSE_BASE}/companies-listing/corporate-filings/corporate-actions",
}

# Canonical action type mapping — normalises NSE's inconsistent labels
ACTION_TYPE_MAP = {
    "div"      : "dividend",  "dividend"  : "dividend",
    "split"    : "split",     "face value": "split",
    "buyback"  : "buyback",   "buy back"  : "buyback",
    "rights"   : "rights",    "right"     : "rights",
    "bonus"    : "bonus",
}

HIGH_PRIORITY = {"split", "buyback", "rights", "bonus"}


# ─────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────

def fetch_nse_actions(index: str = "equities") -> list:
    """
    Open a session with NSE (required for cookie), then hit the
    corporate actions API. Returns raw list of action dicts.
    """
    session = requests.Session()
    try:
        session.get(NSE_BASE, headers=HEADERS, timeout=10)   # seed session cookie
        resp = session.get(NSE_API, headers=HEADERS, params={"index": index}, timeout=10)
        resp.raise_for_status()
        return resp.json().get("data", [])
    except Exception as e:
        print(f"  NSE fetch error: {e}")
        return []


# ─────────────────────────────────────────────
# CLEAN
# ─────────────────────────────────────────────

def clean_actions(raw: list) -> pd.DataFrame:
    """
    Parse and clean raw NSE API response into a structured Pandas DataFrame.
    Handles:
      - Inconsistent date formats (dd-MMM-yyyy, yyyy-mm-dd, dd/mm/yyyy)
      - Mixed-case and verbose action type labels
      - Missing or blank fields
      - Duplicate rows (same symbol + action + ex_date)
    """
    if not raw:
        return pd.DataFrame()

    df = pd.DataFrame(raw)

    # ── Rename columns to standard names ──
    col_map = {
        "symbol"     : "symbol",
        "comp"       : "company_name",
        "subject"    : "details",
        "exDate"     : "ex_date",
        "recDate"    : "record_date",
        "faceVal"    : "face_value",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # ── Normalise action type from 'details' field ──
    def parse_action_type(text: str) -> str:
        if pd.isna(text):
            return "other"
        text = text.lower().strip()
        for key, canonical in ACTION_TYPE_MAP.items():
            if key in text:
                return canonical
        return "other"

    df["action_type"] = df.get("details", pd.Series(dtype=str)).apply(parse_action_type)

    # ── Parse dates — handle multiple formats ──
    for col in ["ex_date", "record_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce").dt.date

    # ── Clean symbol — strip whitespace, uppercase ──
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].str.strip().str.upper()

    # ── Drop rows with no symbol or ex_date ──
    df = df.dropna(subset=["symbol", "ex_date"])

    # ── Drop duplicates ──
    df = df.drop_duplicates(subset=["symbol", "action_type", "ex_date"])

    # ── Select final columns ──
    keep = ["symbol", "company_name", "action_type", "ex_date", "record_date", "details"]
    df   = df[[c for c in keep if c in df.columns]]
    df["fetched_at"] = datetime.now()

    return df.reset_index(drop=True)


# ─────────────────────────────────────────────
# WRITE TO SQL
# ─────────────────────────────────────────────

def write_actions(df: pd.DataFrame):
    """
    Insert cleaned corporate actions into SQL.
    Uses INSERT ... ON CONFLICT DO NOTHING to skip existing records.
    """
    if df.empty:
        print("  No actions to write.")
        return

    inserted = 0
    with engine.connect() as conn:
        for _, row in df.iterrows():
            result = conn.execute(text("""
                INSERT INTO corporate_actions
                    (symbol, company_name, action_type, ex_date, record_date, details, fetched_at)
                VALUES
                    (:symbol, :company_name, :action_type, :ex_date, :record_date, :details, :fetched_at)
                ON CONFLICT (symbol, action_type, ex_date) DO NOTHING
            """), row.to_dict())
            inserted += result.rowcount
        conn.commit()

    print(f"  {inserted:,} new records written to SQL (skipped {len(df) - inserted:,} duplicates).")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run():
    print(f"\n{'='*50}")
    print(f"  NSE FETCHER  |  {date.today()}")
    print(f"{'='*50}\n")

    print("Fetching from NSE API...")
    raw = fetch_nse_actions()
    print(f"  {len(raw):,} raw records received.")

    print("Cleaning data...")
    df  = clean_actions(raw)
    print(f"  {len(df):,} clean records after deduplication.")

    print("Writing to SQL...")
    write_actions(df)
    print("\nFetch complete.\n")


if __name__ == "__main__":
    run()
