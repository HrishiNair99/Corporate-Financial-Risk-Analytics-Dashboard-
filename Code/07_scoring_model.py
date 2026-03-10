# src/07b_score_companies_reweighted_optionB.py
import os
import numpy as np
import pandas as pd

IN_PATH  = os.path.join("data", "clean", "scored_all_companies.csv")
OUT_PATH = os.path.join("data", "clean", "scored_all_companies_reweighted.csv")

# Must match your original weights exactly
WEIGHTS = {
    "debt_to_asset": 0.15,        # lower is better
    "interest_coverage": 0.30,    # higher is better
    "debt_to_ebit": 0.20,         # lower is better
    "cashflow_to_debt": 0.25,     # higher is better
    "revenue_trend": 0.10,        # higher is better
}

# Must match your original calibration gamma exactly
GAMMA = 1.6


def composite_reweighted_rowwise(df: pd.DataFrame) -> pd.Series:
    """
    Option B:
      - If a component SCORE is missing for a row, drop it from numerator
      - Renormalize remaining weights so they sum to 1 for that row
      - If all components missing => NaN
    Returns composite score on 0–100 scale.
    """
    score_cols = [
        "debt_to_asset_score",
        "interest_coverage_score",
        "debt_to_ebit_score",
        "cashflow_to_debt_score",
        "revenue_trend_score",
    ]

    # weights aligned to score cols
    w = np.array([
        WEIGHTS["debt_to_asset"],
        WEIGHTS["interest_coverage"],
        WEIGHTS["debt_to_ebit"],
        WEIGHTS["cashflow_to_debt"],
        WEIGHTS["revenue_trend"],
    ], dtype=float)

    # matrix of component scores (0–10)
    mat = np.column_stack([pd.to_numeric(df[c], errors="coerce").to_numpy(dtype=float) for c in score_cols])

    present = ~np.isnan(mat)              # component exists?
    numerator = np.nansum(mat * w, axis=1)
    denom = np.sum(present * w, axis=1)   # sum weights of present components

    comp_0_10 = np.where(denom > 0, numerator / denom, np.nan)
    comp_0_100 = comp_0_10 * 10.0
    return pd.Series(comp_0_100, index=df.index)


def calibrate(score_0_100: pd.Series) -> pd.Series:
    """
    Apply same calibration as your 07 script:
      calibrated = 100 * (raw_0_1 ** GAMMA)
    """
    raw_0_1 = (pd.to_numeric(score_0_100, errors="coerce") / 100.0).clip(0, 1)
    return 100.0 * (raw_0_1 ** GAMMA)


def main():
    if not os.path.exists(IN_PATH):
        raise FileNotFoundError(f"Input not found: {IN_PATH}. Run src/07_score_companies.py first.")

    df = pd.read_csv(IN_PATH)

    # Minimal schema checks
    required = [
        "ticker", "fiscal_year",
        "debt_to_asset_score",
        "interest_coverage_score",
        "debt_to_ebit_score",
        "cashflow_to_debt_score",
        "revenue_trend_score",
        "composite_score",
        "composite_score_calibrated",
        "interest_missing_flag",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {IN_PATH}: {missing}")

    # Compute Option B scores
    df["composite_score_reweighted"] = composite_reweighted_rowwise(df)
    df["composite_score_calibrated_reweighted"] = calibrate(df["composite_score_reweighted"])

    # Quick diagnostics: show rows where interest coverage missing (the clustering problem)
    print("\n=== Rows where interest_missing_flag = True (old vs new) ===")
    miss = df[df["interest_missing_flag"] == True].copy()
    if miss.empty:
        print("None.")
    else:
        cols = [
            "ticker", "fiscal_year",
            "interest_missing_flag",
            "composite_score", "composite_score_calibrated",
            "composite_score_reweighted", "composite_score_calibrated_reweighted",
        ]
        print(miss.sort_values(["fiscal_year", "ticker"])[cols].to_string(index=False))

    # Check whether new calibrated score still clusters
    latest = df.sort_values("fiscal_year").groupby("ticker").tail(1).copy()
    top_dupes = (
        latest.groupby("composite_score_calibrated_reweighted")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(10)
    )

    print("\n=== Top repeated NEW calibrated scores (latest year) ===")
    print(top_dupes.to_string(index=False))

    # Save new CSV (keeps original columns + adds the new ones)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    print(f"\nSaved: {OUT_PATH}")


if __name__ == "__main__":
    main()