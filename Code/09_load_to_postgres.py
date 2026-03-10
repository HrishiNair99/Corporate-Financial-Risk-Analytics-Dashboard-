import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# ── Config ───────────────────────────────────────────────────────────────────
IN_PATH  = os.path.join("data", "clean", "scored_with_deterioration_all_companies.csv")

DB_USER  = "openpg"
DB_PASS  = "MyNewStrongPass_123!"
DB_HOST  = "localhost"
DB_PORT  = "5432"
DB_NAME  = "bond_risk"

ENGINE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# ── Column definitions ────────────────────────────────────────────────────────
# Explicit dtype maps — every numeric column listed so coercion is never skipped

FINANCIALS_NUMERIC = [
    "total_assets", "total_liabilities", "stockholders_equity",
    "revenue", "operating_cashflow", "ebit",
    "interest_expense", "net_income", "total_debt",
]

RISK_SCORES_NUMERIC = [
    "debt_to_asset", "interest_coverage", "debt_to_ebit",
    "cashflow_to_debt", "revenue_trend",
    "debt_to_asset_score", "interest_coverage_score", "debt_to_ebit_score",
    "cashflow_to_debt_score", "revenue_trend_score",
    "composite_score", "composite_score_calibrated",
]

DETERIORATION_NUMERIC = [
    "score_used", "composite_score", "composite_score_calibrated",
    "score_change_yoy", "deterioration_velocity",
]

DETERIORATION_INT = ["decline_streak_len", "low_score_streak"]


# ── DDL ───────────────────────────────────────────────────────────────────────

DDL = """
-- Dimension: companies
CREATE TABLE IF NOT EXISTS company_master (
    ticker      TEXT PRIMARY KEY,
    entity      TEXT,
    sector      TEXT,
    cik         TEXT
);

-- Dimension: years (Power BI time intelligence)
CREATE TABLE IF NOT EXISTS dim_year (
    fiscal_year         INT  PRIMARY KEY,
    year_label          TEXT,
    is_most_recent_year BOOLEAN DEFAULT FALSE
);

-- Fact: raw financials
CREATE TABLE IF NOT EXISTS financials (
    ticker                  TEXT        NOT NULL REFERENCES company_master(ticker),
    fiscal_year             INT         NOT NULL REFERENCES dim_year(fiscal_year),
    total_assets            NUMERIC(20,2),
    total_liabilities       NUMERIC(20,2),
    stockholders_equity     NUMERIC(20,2),
    revenue                 NUMERIC(20,2),
    operating_cashflow      NUMERIC(20,2),
    ebit                    NUMERIC(20,2),
    interest_expense        NUMERIC(20,2),
    net_income              NUMERIC(20,2),
    total_debt              NUMERIC(20,2),
    loaded_at               TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ticker, fiscal_year)
);

-- Fact: ratio scores
CREATE TABLE IF NOT EXISTS risk_scores (
    ticker                      TEXT        NOT NULL REFERENCES company_master(ticker),
    fiscal_year                 INT         NOT NULL REFERENCES dim_year(fiscal_year),
    debt_to_asset               NUMERIC(10,6),
    interest_coverage           NUMERIC(10,6),
    debt_to_ebit                NUMERIC(10,6),
    cashflow_to_debt            NUMERIC(10,6),
    revenue_trend               NUMERIC(10,6),
    debt_to_asset_score         NUMERIC(10,6),
    interest_coverage_score     NUMERIC(10,6),
    debt_to_ebit_score          NUMERIC(10,6),
    cashflow_to_debt_score      NUMERIC(10,6),
    revenue_trend_score         NUMERIC(10,6),
    composite_score             NUMERIC(10,6),
    composite_score_calibrated  NUMERIC(10,6),
    debt_missing_flag           BOOLEAN,
    interest_missing_flag       BOOLEAN,
    loaded_at                   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ticker, fiscal_year)
);

-- Fact: deterioration signals
CREATE TABLE IF NOT EXISTS deterioration_results (
    ticker                      TEXT        NOT NULL REFERENCES company_master(ticker),
    fiscal_year                 INT         NOT NULL REFERENCES dim_year(fiscal_year),
    score_used                  NUMERIC(10,6),
    composite_score             NUMERIC(10,6),
    composite_score_calibrated  NUMERIC(10,6),
    score_change_yoy            NUMERIC(10,6),
    decline_streak_len          INT,
    low_score_streak            INT,
    deterioration_flag          TEXT,
    deterioration_velocity      NUMERIC(10,6),
    loaded_at                   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ticker, fiscal_year)
);

-- Indexes for Power BI slicer/filter performance
CREATE INDEX IF NOT EXISTS idx_financials_ticker        ON financials(ticker);
CREATE INDEX IF NOT EXISTS idx_financials_year          ON financials(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_risk_scores_ticker       ON risk_scores(ticker);
CREATE INDEX IF NOT EXISTS idx_risk_scores_year         ON risk_scores(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_deterioration_ticker     ON deterioration_results(ticker);
CREATE INDEX IF NOT EXISTS idx_deterioration_flag       ON deterioration_results(deterioration_flag);
CREATE INDEX IF NOT EXISTS idx_company_master_sector    ON company_master(sector);
"""

# ── Upsert SQL ────────────────────────────────────────────────────────────────

UPSERT_COMPANY_MASTER = """
INSERT INTO company_master (ticker, entity, sector, cik)
VALUES (:ticker, :entity, :sector, :cik)
ON CONFLICT (ticker) DO UPDATE SET
    entity = EXCLUDED.entity,
    sector = EXCLUDED.sector,
    cik    = EXCLUDED.cik;
"""

UPSERT_DIM_YEAR = """
INSERT INTO dim_year (fiscal_year, year_label, is_most_recent_year)
VALUES (:fiscal_year, :year_label, :is_most_recent_year)
ON CONFLICT (fiscal_year) DO UPDATE SET
    year_label          = EXCLUDED.year_label,
    is_most_recent_year = EXCLUDED.is_most_recent_year;
"""

UPSERT_FINANCIALS = """
INSERT INTO financials (
    ticker, fiscal_year, total_assets, total_liabilities,
    stockholders_equity, revenue, operating_cashflow,
    ebit, interest_expense, net_income, total_debt
)
VALUES (
    :ticker, :fiscal_year, :total_assets, :total_liabilities,
    :stockholders_equity, :revenue, :operating_cashflow,
    :ebit, :interest_expense, :net_income, :total_debt
)
ON CONFLICT (ticker, fiscal_year) DO UPDATE SET
    total_assets        = EXCLUDED.total_assets,
    total_liabilities   = EXCLUDED.total_liabilities,
    stockholders_equity = EXCLUDED.stockholders_equity,
    revenue             = EXCLUDED.revenue,
    operating_cashflow  = EXCLUDED.operating_cashflow,
    ebit                = EXCLUDED.ebit,
    interest_expense    = EXCLUDED.interest_expense,
    net_income          = EXCLUDED.net_income,
    total_debt          = EXCLUDED.total_debt,
    loaded_at           = NOW();
"""

UPSERT_RISK_SCORES = """
INSERT INTO risk_scores (
    ticker, fiscal_year,
    debt_to_asset, interest_coverage, debt_to_ebit,
    cashflow_to_debt, revenue_trend,
    debt_to_asset_score, interest_coverage_score, debt_to_ebit_score,
    cashflow_to_debt_score, revenue_trend_score,
    composite_score, composite_score_calibrated,
    debt_missing_flag, interest_missing_flag
)
VALUES (
    :ticker, :fiscal_year,
    :debt_to_asset, :interest_coverage, :debt_to_ebit,
    :cashflow_to_debt, :revenue_trend,
    :debt_to_asset_score, :interest_coverage_score, :debt_to_ebit_score,
    :cashflow_to_debt_score, :revenue_trend_score,
    :composite_score, :composite_score_calibrated,
    :debt_missing_flag, :interest_missing_flag
)
ON CONFLICT (ticker, fiscal_year) DO UPDATE SET
    debt_to_asset               = EXCLUDED.debt_to_asset,
    interest_coverage           = EXCLUDED.interest_coverage,
    debt_to_ebit                = EXCLUDED.debt_to_ebit,
    cashflow_to_debt            = EXCLUDED.cashflow_to_debt,
    revenue_trend               = EXCLUDED.revenue_trend,
    debt_to_asset_score         = EXCLUDED.debt_to_asset_score,
    interest_coverage_score     = EXCLUDED.interest_coverage_score,
    debt_to_ebit_score          = EXCLUDED.debt_to_ebit_score,
    cashflow_to_debt_score      = EXCLUDED.cashflow_to_debt_score,
    revenue_trend_score         = EXCLUDED.revenue_trend_score,
    composite_score             = EXCLUDED.composite_score,
    composite_score_calibrated  = EXCLUDED.composite_score_calibrated,
    debt_missing_flag           = EXCLUDED.debt_missing_flag,
    interest_missing_flag       = EXCLUDED.interest_missing_flag,
    loaded_at                   = NOW();
"""

UPSERT_DETERIORATION = """
INSERT INTO deterioration_results (
    ticker, fiscal_year,
    score_used, composite_score, composite_score_calibrated,
    score_change_yoy, decline_streak_len, low_score_streak,
    deterioration_flag, deterioration_velocity
)
VALUES (
    :ticker, :fiscal_year,
    :score_used, :composite_score, :composite_score_calibrated,
    :score_change_yoy, :decline_streak_len, :low_score_streak,
    :deterioration_flag, :deterioration_velocity
)
ON CONFLICT (ticker, fiscal_year) DO UPDATE SET
    score_used                  = EXCLUDED.score_used,
    composite_score             = EXCLUDED.composite_score,
    composite_score_calibrated  = EXCLUDED.composite_score_calibrated,
    score_change_yoy            = EXCLUDED.score_change_yoy,
    decline_streak_len          = EXCLUDED.decline_streak_len,
    low_score_streak            = EXCLUDED.low_score_streak,
    deterioration_flag          = EXCLUDED.deterioration_flag,
    deterioration_velocity      = EXCLUDED.deterioration_velocity,
    loaded_at                   = NOW();
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def clean_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Replace all pandas NA/NaN with None for clean SQL NULL binding."""
    return df.where(df.notna(), None)


def coerce_numeric(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """
    FIX: Explicitly coerce all numeric columns.
    Handles empty strings, 'None' strings, and other non-numeric garbage
    that pd.read_csv can leave behind when dtype=str is used.
    """
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        else:
            df[c] = np.nan
    return df


def safe_bool(series: pd.Series) -> pd.Series:
    """
    FIX: astype(bool) turns NaN → True and 'False' string → True.
    Map explicitly — unmapped values (NaN etc.) become None (NULL).
    """
    mapping = {
        True: True,    False: False,
        1: True,       0: False,
        "True": True,  "False": False,
        "true": True,  "false": False,
        "1": True,     "0": False,
    }
    return series.map(mapping)


def safe_cik(series: pd.Series) -> pd.Series:
    """
    FIX: astype(str) on NaN produces the string 'nan', not NULL.
    Only convert to string when value is genuinely present.
    """
    return series.apply(lambda x: str(x).strip() if pd.notna(x) else None)


def dedup(df: pd.DataFrame, keys: list, label: str) -> pd.DataFrame:
    """
    FIX: Postgres ON CONFLICT cannot handle two rows with the same PK
    in the same executemany batch — it errors before the clause fires.
    Dedup before every upsert. Keep last (most recently processed row wins).
    """
    before = len(df)
    df = df.drop_duplicates(subset=keys, keep="last").reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        print(f"  ⚠️  [{label}] Dropped {dropped} duplicate rows on {keys}")
    return df


def build_dim_year(years: list) -> pd.DataFrame:
    """
    Build the year dimension table.
    is_most_recent_year = True for the highest year in the dataset.
    Power BI uses this for default slicer selection and 'latest year' measures.
    """
    max_year = max(years)
    rows = []
    for y in sorted(set(years)):
        rows.append({
            "fiscal_year":         int(y),
            "year_label":          str(y),
            "is_most_recent_year": (y == max_year),
        })
    return pd.DataFrame(rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not os.path.exists(IN_PATH):
        raise FileNotFoundError(f"Input file not found: {IN_PATH}")

    # Read everything as str first — safer than letting pandas guess dtypes.
    # Each column is explicitly coerced to its correct type below.
    df = pd.read_csv(IN_PATH, dtype=str)
    df["ticker"]      = df["ticker"].str.upper().str.strip()
    df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce")
    df = df[df["fiscal_year"].notna()].copy()
    df["fiscal_year"] = df["fiscal_year"].astype(int)

    print(f"Loaded {len(df)} rows for {df['ticker'].nunique()} companies\n")

    engine = create_engine(ENGINE_URL)

    with engine.begin() as conn:

        # ── Create all tables and indexes ──────────────────────────────────
        conn.execute(text(DDL))
        print("Tables and indexes created / verified")

        # ── 1. company_master (must load first — all fact tables FK to this)
        master_df = (
            df[["ticker", "entity", "sector", "cik"]]
            .drop_duplicates(subset=["ticker"], keep="last")
            .copy()
        )
        master_df["cik"] = safe_cik(master_df["cik"])   # FIX: no 'nan' strings
        conn.execute(
            text(UPSERT_COMPANY_MASTER),
            clean_nulls(master_df).to_dict(orient="records")
        )
        print(f"company_master        : {len(master_df)} rows upserted")

        # ── 2. dim_year (must load before fact tables — FK dependency) ──────
        year_df = build_dim_year(df["fiscal_year"].tolist())
        conn.execute(
            text(UPSERT_DIM_YEAR),
            year_df.to_dict(orient="records")
        )
        print(f"dim_year              : {len(year_df)} rows upserted "
              f"({year_df['fiscal_year'].min()}–{year_df['fiscal_year'].max()})")

        # ── 3. financials ──────────────────────────────────────────────────
        fin_df = df.reindex(columns=["ticker", "fiscal_year"] + FINANCIALS_NUMERIC).copy()
        fin_df = coerce_numeric(fin_df, FINANCIALS_NUMERIC)   # FIX: explicit coerce
        fin_df = dedup(fin_df, ["ticker", "fiscal_year"], "financials")
        conn.execute(
            text(UPSERT_FINANCIALS),
            clean_nulls(fin_df).to_dict(orient="records")
        )
        print(f"financials            : {len(fin_df)} rows upserted")

        # ── 4. risk_scores ─────────────────────────────────────────────────
        bool_cols = ["debt_missing_flag", "interest_missing_flag"]
        score_df  = df.reindex(columns=["ticker", "fiscal_year"] + RISK_SCORES_NUMERIC + bool_cols).copy()
        score_df  = coerce_numeric(score_df, RISK_SCORES_NUMERIC)  # FIX: explicit coerce
        for c in bool_cols:
            score_df[c] = safe_bool(score_df[c])                   # FIX: no NaN→True corruption
        score_df = dedup(score_df, ["ticker", "fiscal_year"], "risk_scores")
        conn.execute(
            text(UPSERT_RISK_SCORES),
            clean_nulls(score_df).to_dict(orient="records")
        )
        print(f"risk_scores           : {len(score_df)} rows upserted")

        # ── 5. deterioration_results ───────────────────────────────────────
        det_df = df.reindex(
            columns=["ticker", "fiscal_year"]
                    + DETERIORATION_NUMERIC
                    + DETERIORATION_INT
                    + ["deterioration_flag"]
        ).copy()
        det_df = coerce_numeric(det_df, DETERIORATION_NUMERIC)     # FIX: explicit coerce
        for c in DETERIORATION_INT:
            det_df[c] = pd.to_numeric(det_df[c], errors="coerce")
        det_df = dedup(det_df, ["ticker", "fiscal_year"], "deterioration_results")
        conn.execute(
            text(UPSERT_DETERIORATION),
            clean_nulls(det_df).to_dict(orient="records")
        )
        print(f"deterioration_results : {len(det_df)} rows upserted")

        # ── Row count sanity check ─────────────────────────────────────────
        print("\n── Final row counts ──────────────────────────────────────────")
        for table in ["company_master", "dim_year", "financials",
                      "risk_scores", "deterioration_results"]:
            n = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"  {table:<30} {n} rows")

        # ── FK orphan check (should always be 0) ──────────────────────────
        print("\n── Orphaned FK rows (should all be 0) ───────────────────────")
        for table in ["financials", "risk_scores", "deterioration_results"]:
            orphans = conn.execute(text(f"""
                SELECT COUNT(*) FROM {table} t
                WHERE NOT EXISTS (
                    SELECT 1 FROM company_master m WHERE m.ticker = t.ticker
                )
            """)).scalar()
            print(f"  {table:<30} {orphans} orphaned rows")

        # ── dim_year completeness check ────────────────────────────────────
        print("\n── dim_year coverage ─────────────────────────────────────────")
        years = conn.execute(text(
            "SELECT fiscal_year, is_most_recent_year FROM dim_year ORDER BY fiscal_year"
        )).fetchall()
        for y, is_recent in years:
            tag = " ← most recent" if is_recent else ""
            print(f"  {y}{tag}")

    print("\nAll tables loaded successfully.")


if __name__ == "__main__":
    main()