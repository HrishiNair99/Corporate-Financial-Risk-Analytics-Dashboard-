import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

DB_USER = "openpg"
DB_PASS = "MyNewStrongPass_123!"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "bond_risk"

ENGINE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

OUT_PATH = os.path.join("data", "clean", "altman_validation.csv")

# Altman Z-Score danger threshold for distress zone
ALTMAN_DISTRESS = 1.81
ALTMAN_SAFE     = 2.99

def compute_altman(df: pd.DataFrame) -> pd.DataFrame:
    """
    Altman Z-Score (book-value variant — no market cap required).
    
    Z = 1.2(X1) + 1.4(X2) + 3.3(X3) + 0.6(X4) + 1.0(X5)
    
    X1 = (Total Assets - Total Liabilities) / Total Assets  [working capital proxy]
    X2 = Net Income / Total Assets                           [retained earnings proxy]
    X3 = EBIT / Total Assets                                 [operating efficiency]
    X4 = Stockholders Equity / Total Liabilities             [book value solvency]
    X5 = Revenue / Total Assets                              [asset turnover]
    
    Interpretation:
        Z > 2.99  → Safe
        1.81–2.99 → Grey Zone
        Z < 1.81  → Distress
    """
    df = df.copy()

    # Guard: avoid division by zero
    df["total_assets"]      = pd.to_numeric(df["total_assets"],      errors="coerce")
    df["total_liabilities"] = pd.to_numeric(df["total_liabilities"], errors="coerce")
    df["stockholders_equity"] = pd.to_numeric(df["stockholders_equity"], errors="coerce")
    df["revenue"]           = pd.to_numeric(df["revenue"],           errors="coerce")
    df["ebit"]              = pd.to_numeric(df["ebit"],              errors="coerce")
    df["net_income"]        = pd.to_numeric(df["net_income"],        errors="coerce")

    assets = df["total_assets"].replace(0, np.nan)
    liab   = df["total_liabilities"].replace(0, np.nan)

    df["X1"] = (df["total_assets"] - df["total_liabilities"]) / assets
    df["X2"] = df["net_income"] / assets                          # retained earnings proxy
    df["X3"] = df["ebit"] / assets
    df["X4"] = df["stockholders_equity"] / liab                   # book value variant
    df["X5"] = df["revenue"] / assets

    df["altman_z"] = (
        1.2 * df["X1"] +
        1.4 * df["X2"] +
        3.3 * df["X3"] +
        0.6 * df["X4"] +
        1.0 * df["X5"]
    )

    # Altman zone classification
    df["altman_zone"] = pd.cut(
        df["altman_z"],
        bins=[-np.inf, ALTMAN_DISTRESS, ALTMAN_SAFE, np.inf],
        labels=["Distress", "Grey Zone", "Safe"]
    )

    return df


def classify_agreement(row):
    """
    Compare Altman zone against your deterioration flag.

    Your model buckets:
        Risk = High Deterioration OR Moderate Deterioration  (any detected deterioration)
        Safe = Stable

    Altman buckets:
        Risk      = Distress
        Ambiguous = Grey Zone
        Safe      = Safe

    Grey Zone is treated as ambiguous — neither agreement nor disagreement.
    """
    your_risky  = row["deterioration_flag"] in ("High Deterioration", "Moderate Deterioration")
    your_safe   = row["deterioration_flag"] == "Stable"

    altman_risky = row["altman_zone"] == "Distress"
    altman_safe  = row["altman_zone"] == "Safe"
    altman_grey  = row["altman_zone"] == "Grey Zone"

    # Full agreement
    if your_risky and altman_risky:
        return "Both flag risk"
    if your_safe and altman_safe:
        return "Both flag safe"

    # Divergence — most analytically interesting
    if your_risky and altman_safe:
        return "Yours flags risk — Altman doesn't"
    if altman_risky and your_safe:
        return "Altman flags risk — yours doesn't"

    # Grey Zone cases — ambiguous, don't overstate
    if altman_grey and your_risky:
        return "Grey Zone — yours leans risk"
    if altman_grey and your_safe:
        return "Grey Zone — yours leans safe"

    return "Inconclusive"


def main():
    engine = create_engine(ENGINE_URL)

    with engine.connect() as conn:
        # Pull financials + deterioration flag + your score
        df = pd.read_sql(text("""
            SELECT
                f.ticker,
                f.fiscal_year,
                f.total_assets,
                f.total_liabilities,
                f.stockholders_equity,
                f.revenue,
                f.ebit,
                f.net_income,
                c.sector,
                c.entity,
                d.score_used,
                d.deterioration_flag,
                d.deterioration_velocity
            FROM financials f
            JOIN company_master c  ON c.ticker = f.ticker
            JOIN deterioration_results d
                ON d.ticker = f.ticker AND d.fiscal_year = f.fiscal_year
            ORDER BY f.ticker, f.fiscal_year
        """), conn)

    # Compute Altman Z-Score
    df = compute_altman(df)

    # Agreement classification
    df["model_agreement"] = df.apply(classify_agreement, axis=1)

    # ── Summary prints ────────────────────────────────────────────────────────

    print("\n=== ALTMAN Z-SCORE vs YOUR MODEL (latest year) ===")
    latest = df.sort_values("fiscal_year").groupby("ticker").last().reset_index()
    print(
        latest[[
            "ticker", "sector", "altman_z", "altman_zone",
            "score_used", "deterioration_flag", "model_agreement"
        ]]
        .sort_values("altman_z")
        .to_string(index=False)
    )

    print("\n=== AGREEMENT DISTRIBUTION (latest year) ===")
    print(latest["model_agreement"].value_counts().to_string())

    print("\n=== DIVERGENCES — most interesting for README ===")
    diverge = latest[latest["model_agreement"].isin([
        "Altman flags risk — yours doesn't",
        "Yours flags risk — Altman doesn't"
    ])]
    print(diverge[[
        "ticker", "sector", "entity", "altman_z", "altman_zone",
        "score_used", "deterioration_flag", "model_agreement"
    ]].to_string(index=False))

    print("\n=== YELL BACKTEST — Altman trajectory ===")
    yell = df[df["ticker"] == "YELL"][[
        "fiscal_year", "altman_z", "altman_zone",
        "score_used", "deterioration_flag"
    ]]
    print(yell.to_string(index=False))

    # ── Save ──────────────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    print(f"\nSaved: {OUT_PATH}")
    print(f"Rows: {len(df)} | Companies: {df['ticker'].nunique()}")


if __name__ == "__main__":
    main()