# src/06_compute_ratios.py
import os
import numpy as np
import pandas as pd

# Use the extractor output that actually contains debt/interest/net income
IN_PATH = os.path.join("data", "clean", "core_metrics_all_companies.csv")
OUT_PATH = os.path.join("data", "clean", "metrics_with_ratios_all_companies.csv")


def safe_div_series(a: pd.Series, b: pd.Series) -> pd.Series:
    """
    Vectorized safe division:
    - NaN if numerator/denominator NaN
    - +inf if denominator == 0 (treat as extreme risk)
    """
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")

    out = a / b
    out = out.where(~b.eq(0), np.inf)
    out = out.where(~(a.isna() | b.isna()), np.nan)
    return out


def ensure_columns(df: pd.DataFrame, cols: list[str]) -> None:
    """If a column is missing, create it filled with NaN (prevents crashes)."""
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan


def main():
    if not os.path.exists(IN_PATH):
        raise FileNotFoundError(f"Missing input file: {IN_PATH}")

    df = pd.read_csv(IN_PATH)

    # --- required identifiers ---
    required_id = ["ticker", "fiscal_year"]
    missing_id = [c for c in required_id if c not in df.columns]
    if missing_id:
        raise ValueError(f"Input is missing required columns: {missing_id}")

    # --- normalize identifiers ---
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    if "cik" in df.columns:
        df["cik"] = df["cik"].astype(str).str.strip()

    # --- safe fiscal_year handling ---
    df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce")
    df = df[df["fiscal_year"].notna()].copy()
    df["fiscal_year"] = df["fiscal_year"].astype(int)

    # --- sort + dedup ticker-year ---
    df = df.sort_values(["ticker", "fiscal_year"]).reset_index(drop=True)
    before = len(df)
    df = df.drop_duplicates(subset=["ticker", "fiscal_year"], keep="last").copy()
    dropped = before - len(df)
    if dropped:
        print(f"⚠️ Dropped duplicate ticker-year rows: {dropped}")

    # --- ensure expected numeric columns exist ---
    expected_numeric = [
        "total_assets",
        "total_liabilities",
        "stockholders_equity",
        "revenue",
        "operating_cashflow",
        "ebit",
        "interest_expense",
        "total_debt",
        "net_income",
    ]
    ensure_columns(df, expected_numeric)

    # --- numeric coercion ---
    for c in expected_numeric:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # --- warn if any expected columns are entirely missing upstream ---
    upstream_all_nan = [c for c in expected_numeric if df[c].isna().all()]
    if upstream_all_nan:
        print("\n⚠️ Columns present but ALL NaN (likely not extracted in your input):")
        for c in upstream_all_nan:
            print(f"  - {c}")

    # ============================================================
    # Ratios (computed ONLY from extracted core metrics)
    # ============================================================

    # 1) debt_to_asset = total_debt / total_assets
    df["debt_to_asset"] = safe_div_series(df["total_debt"], df["total_assets"])

    # 2) interest_coverage = ebit / abs(interest_expense) (abs handles sign conventions)
    df["interest_coverage"] = safe_div_series(df["ebit"], df["interest_expense"].abs())

    # 3) debt_to_ebit = total_debt / ebit (EBIT proxy; not EBITDA)
    df["debt_to_ebit"] = safe_div_series(df["total_debt"], df["ebit"])

    # 4) cashflow_to_debt = operating_cashflow / total_debt
    df["cashflow_to_debt"] = safe_div_series(df["operating_cashflow"], df["total_debt"])

    # 5) revenue_trend = YoY % change in revenue
    df["revenue_trend"] = df.groupby("ticker")["revenue"].pct_change()

    # helpful flags
    df["debt_missing_flag"] = df["total_debt"].isna()
    df["interest_missing_flag"] = df["interest_expense"].isna()
    df["interest_zero_flag"] = df["interest_expense"].fillna(np.nan).eq(0)

    # --- write output ---
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df.to_csv(OUT_PATH, index=False)

    print(f"\nSaved: {OUT_PATH}")
    print(f"Tickers: {df['ticker'].nunique()} | Rows: {len(df)}")

    print("\nMissing counts (key fields):")
    for c in ["total_debt", "interest_expense", "debt_to_asset", "interest_coverage", "cashflow_to_debt", "revenue_trend"]:
        print(f"{c}: {int(df[c].isna().sum())}")

    # ============================================================
    # DISPLAY / EXPORT (ALL ROWS, THESE COLUMNS)
    # ============================================================
    show_cols = [
        "ticker",
        "fiscal_year",
        "total_debt",
        "interest_expense",
        "debt_to_asset",
        "interest_coverage",
        "cashflow_to_debt",
        "revenue_trend",
    ]
    show_cols = [c for c in show_cols if c in df.columns]

    # optional: save a "pretty" subset for Excel viewing
    PRETTY_PATH = os.path.join("data", "clean", "metrics_with_ratios_all_companies_pretty.csv")
    df[show_cols].to_csv(PRETTY_PATH, index=False)
    print(f"\nPretty view saved: {PRETTY_PATH}")

    # print EVERYTHING (all rows) for those columns
    print("\nALL rows (selected columns):")
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    print(df[show_cols].to_string(index=False))


if __name__ == "__main__":
    main()