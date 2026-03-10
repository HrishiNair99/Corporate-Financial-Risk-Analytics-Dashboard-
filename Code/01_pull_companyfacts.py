# 01_pull_companyfacts.py
import json
import os
import time
from typing import Dict, List, Optional

import pandas as pd
import requests

# ---------------------------
# Config
# ---------------------------
RAW_DIR = os.path.join("data", "raw")
CLEAN_DIR = os.path.join("data", "clean")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CLEAN_DIR, exist_ok=True)

# Input list (30 tickers) and output mapping file (ticker/cik/sector/raw_path)
TICKERS_INPUT_PATH = os.path.join("data", "tickers_input.csv")
RESOLVED_TICKERS_OUT = os.path.join("data", "clean", "tickers_resolved.csv")

# Manual overrides for tickers not present in SEC company_tickers.json (delisted/older)
MANUAL_CIK = {
    "YELL": "0000716006",  # Yellow Corp (delisted ticker)
}

# SEC asks for a real User-Agent with contact info (keep your real email here)
HEADERS = {
    "User-Agent": "Hrishikesh (learning project) nairhrishi935@gmail.com",
    "Accept-Encoding": "gzip, deflate",
}

TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"


# ---------------------------
# Helpers
# ---------------------------
def load_tickers_input(path: str) -> pd.DataFrame:
    """
    Supports BOTH formats:
      A) Normal CSV with columns: ticker, sector
      B) Your current format: each row is a single quoted string like "M,Retail"
    Returns df with columns: ticker, sector
    """
    # First try "normal" parse
    try:
        df = pd.read_csv(path)
        cols = [c.strip().lower() for c in df.columns]
        if "ticker" in cols:
            # normalize column names
            df.columns = [c.strip().lower() for c in df.columns]
            if "sector" not in df.columns:
                df["sector"] = ""
            out = df[["ticker", "sector"]].copy()
            out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
            out["sector"] = out["sector"].fillna("").astype(str).str.strip()
            out = out[out["ticker"].ne("")].drop_duplicates(subset=["ticker"], keep="first")
            return out
    except Exception:
        pass

    # Fallback: single-column rows like "M,Retail"
    raw = pd.read_csv(path, header=None)
    s = raw[0].astype(str).str.replace('"', "", regex=False).str.strip()
    s = s[s.ne("")]

    # drop header if present as a literal string
    if len(s) > 0 and s.iloc[0].lower().replace(" ", "") in ("ticker,sector", "ticker,sector\r"):
        s = s.iloc[1:]

    parts = s.str.split(",", n=1, expand=True)
    parts.columns = ["ticker", "sector"]
    parts["ticker"] = parts["ticker"].astype(str).str.upper().str.strip()
    parts["sector"] = parts["sector"].fillna("").astype(str).str.strip()
    parts = parts[parts["ticker"].ne("")].drop_duplicates(subset=["ticker"], keep="first")
    return parts


def get_json_with_retries(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
    timeout: int = 30,
    max_retries: int = 6,
) -> dict:
    """
    Simple retry handler for SEC endpoints (429 / transient 5xx).
    """
    backoff = 0.5
    last_err: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, headers=headers, timeout=timeout)
            if r.status_code == 429:
                # rate limited
                time.sleep(backoff)
                backoff = min(backoff * 2, 8)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(backoff)
            backoff = min(backoff * 2, 8)

    raise RuntimeError(f"Failed after {max_retries} retries for URL: {url}. Last error: {last_err}")


def load_ticker_map(session: requests.Session) -> Dict[str, str]:
    data = get_json_with_retries(session, TICKER_MAP_URL, HEADERS)
    ticker_to_cik: Dict[str, str] = {}

    for _, row in data.items():
        ticker = str(row["ticker"]).upper()
        cik = str(row["cik_str"]).zfill(10)  # 10-digit CIK for URL usage
        ticker_to_cik[ticker] = cik

    return ticker_to_cik


def fetch_companyfacts(session: requests.Session, cik_10: str) -> dict:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_10}.json"
    return get_json_with_retries(session, url, HEADERS)


# ---------------------------
# Main
# ---------------------------
def main():
    if not os.path.exists(TICKERS_INPUT_PATH):
        raise FileNotFoundError(
            f"Missing {TICKERS_INPUT_PATH}. Put your 30-ticker file there (ticker,sector)."
        )

    ticker_df = load_tickers_input(TICKERS_INPUT_PATH)
    tickers: List[str] = ticker_df["ticker"].tolist()
    sector_map = dict(zip(ticker_df["ticker"], ticker_df["sector"]))

    with requests.Session() as session:
        ticker_to_cik = load_ticker_map(session)

        resolved_rows = []

        for t in tickers:
            # resolve cik
            if t in ticker_to_cik:
                cik = ticker_to_cik[t]
            elif t in MANUAL_CIK:
                cik = str(MANUAL_CIK[t]).zfill(10)
                print(f"[INFO] {t} not in SEC ticker map. Using manual CIK {cik}.")
            else:
                print(f"[SKIP] No CIK found for ticker: {t}")
                continue

            outpath = os.path.join(RAW_DIR, f"{t}_CIK{cik}.json")

            # resumable scaling
            if os.path.exists(outpath):
                print(f"[SKIP] Already exists: {outpath}")
                resolved_rows.append(
                    {"ticker": t, "cik": cik, "sector": sector_map.get(t, ""), "raw_path": outpath, "pulled": False}
                )
                continue

            print(f"[INFO] Pulling {t} -> CIK {cik}")

            try:
                facts = fetch_companyfacts(session, cik)
            except Exception as e:
                print(f"[ERROR] Fetch failed for {t} (CIK {cik}): {e}")
                continue

            with open(outpath, "w", encoding="utf-8") as f:
                json.dump(facts, f)

            print(f"[OK] Saved {outpath}")
            resolved_rows.append(
                {"ticker": t, "cik": cik, "sector": sector_map.get(t, ""), "raw_path": outpath, "pulled": True}
            )

            # be polite to SEC servers
            time.sleep(0.25)

    pd.DataFrame(resolved_rows).to_csv(RESOLVED_TICKERS_OUT, index=False)
    print(f"\n[OK] Saved resolved tickers: {RESOLVED_TICKERS_OUT}")
    print(f"[INFO] Raw JSON count in {RAW_DIR}: {len([f for f in os.listdir(RAW_DIR) if f.endswith('.json')])}")


if __name__ == "__main__":
    main()