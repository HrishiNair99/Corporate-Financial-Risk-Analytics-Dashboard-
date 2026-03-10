import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ── DB Config ────────────────────────────────────────────────────────────────
DB_USER = "openpg"
DB_PASS = "MyNewStrongPass_123!"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "bond_risk"

ENGINE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ── Input CSV ────────────────────────────────────────────────────────────────
IN_PATH = os.path.join("data", "clean", "altman_validation.csv")

# ── DDL: store benchmark results cleanly in their own table ──────────────────
DDL = """
CREATE TABLE IF NOT EXISTS altman_validation (
    ticker              TEXT NOT NULL REFERENCES company_master(ticker),
    fiscal_year         INT  NOT NULL,

    -- Altman-style proxy outputs
    altman_z            NUMERIC(12,6),
    altman_zone         TEXT,

    -- Optional: components (present in your CSV)
    x1                  NUMERIC(12,6),
    x2                  NUMERIC(12,6),
    x3                  NUMERIC(12,6),
    x4                  NUMERIC(12,6),
    x5                  NUMERIC(12,6),

    -- Your model context (useful for Power BI comparisons)
    score_used          NUMERIC(12,6),
    deterioration_flag  TEXT,
    model_agreement     TEXT,

    loaded_at           TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ticker, fiscal_year)
);

CREATE INDEX IF NOT EXISTS idx_altman_validation_ticker ON altman_validation(ticker);
CREATE INDEX IF NOT EXISTS idx_altman_validation_year   ON altman_validation(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_altman_validation_zone   ON altman_validation(altman_zone);
CREATE INDEX IF NOT EXISTS idx_altman_validation_agree  ON altman_validation(model_agreement);
"""

UPSERT = """
INSERT INTO altman_validation (
    ticker, fiscal_year,
    altman_z, altman_zone,
    x1, x2, x3, x4, x5,
    score_used, deterioration_flag, model_agreement
)
VALUES (
    :ticker, :fiscal_year,
    :altman_z, :altman_zone,
    :x1, :x2, :x3, :x4, :x5,
    :score_used, :deterioration_flag, :model_agreement
)
ON CONFLICT (ticker, fiscal_year) DO UPDATE SET
    altman_z           = EXCLUDED.altman_z,
    altman_zone        = EXCLUDED.altman_zone,
    x1                 = EXCLUDED.x1,
    x2                 = EXCLUDED.x2,
    x3                 = EXCLUDED.x3,
    x4                 = EXCLUDED.x4,
    x5                 = EXCLUDED.x5,
    score_used         = EXCLUDED.score_used,
    deterioration_flag = EXCLUDED.deterioration_flag,
    model_agreement    = EXCLUDED.model_agreement,
    loaded_at          = NOW();
"""

def clean_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Replace pandas NaN/NA with None for SQL NULL binding."""
    return df.where(df.notna(), None)

def main():
    if not os.path.exists(IN_PATH):
        raise FileNotFoundError(f"CSV not found: {IN_PATH}")

    df = pd.read_csv(IN_PATH)

    # Normalize columns to expected names (your script creates X1..X5 uppercase)
    rename_map = {}
    for col in df.columns:
        if col == "X1": rename_map[col] = "x1"
        if col == "X2": rename_map[col] = "x2"
        if col == "X3": rename_map[col] = "x3"
        if col == "X4": rename_map[col] = "x4"
        if col == "X5": rename_map[col] = "x5"
    if rename_map:
        df = df.rename(columns=rename_map)

    # Required columns check (others are optional but recommended)
    required = ["ticker", "fiscal_year", "altman_z", "altman_zone"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"altman_validation.csv is missing required columns: {missing}")

    # Clean types
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce").astype("Int64")

    # Numeric columns (only convert if present)
    num_cols = ["altman_z", "x1", "x2", "x3", "x4", "x5", "score_used"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Text columns (only if present)
    text_cols = ["altman_zone", "deterioration_flag", "model_agreement"]
    for c in text_cols:
        if c in df.columns:
            df[c] = df[c].astype("string")

    # Keep only the columns we store (create missing optional cols as null)
    target_cols = [
        "ticker", "fiscal_year",
        "altman_z", "altman_zone",
        "x1", "x2", "x3", "x4", "x5",
        "score_used", "deterioration_flag", "model_agreement"
    ]
    for c in target_cols:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[target_cols].copy()

    # Drop rows with no fiscal_year
    df = df[df["fiscal_year"].notna()].copy()
    df["fiscal_year"] = df["fiscal_year"].astype(int)

    # Deduplicate to avoid batch ON CONFLICT issues
    df = df.drop_duplicates(subset=["ticker", "fiscal_year"], keep="last")

    engine = create_engine(ENGINE_URL)

    with engine.begin() as conn:
        conn.execute(text(DDL))

        # FK safety check: ensure all tickers exist in company_master
        # (If you prefer strictness, you can error out instead of filtering.)
        existing = pd.read_sql(text("SELECT ticker FROM company_master"), conn)
        existing_set = set(existing["ticker"].astype(str).str.upper().str.strip())
        before = len(df)
        df = df[df["ticker"].isin(existing_set)].copy()
        dropped = before - len(df)
        if dropped:
            print(f"⚠️ Dropped {dropped} rows because ticker not found in company_master (FK).")

        conn.execute(text(UPSERT), clean_nulls(df).to_dict(orient="records"))

        n = conn.execute(text("SELECT COUNT(*) FROM altman_validation")).scalar()
        print(f"✅ Upsert complete. altman_validation now has {n} rows.")

    print(f"Loaded from: {IN_PATH}")
    print(f"Rows inserted/updated this run: {len(df)}")
    print(f"Companies in file: {df['ticker'].nunique()}")

if __name__ == "__main__":
    main()