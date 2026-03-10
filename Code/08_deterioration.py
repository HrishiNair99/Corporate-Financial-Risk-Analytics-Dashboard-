import os
import numpy as np
import pandas as pd

IN_PATH  = os.path.join("data", "clean", "scored_all_companies.csv")
OUT_PATH = os.path.join("data", "clean", "scored_with_deterioration_all_companies.csv")

# Thresholds
CRASH_THRESHOLD  = -30.0  # single-year drop this severe => High Deterioration immediately
LOW_SCORE_CUTOFF =  30.0  # score below this is danger zone
LOW_SCORE_STREAK =   3    # consecutive years below cutoff => Moderate Deterioration (catches chronic distress like YELL)
VEL_WINDOW       =   3    # rolling window for velocity smoothing


def main():
    df = pd.read_csv(IN_PATH)

    # ── Clean identifiers ───────────────────────────────────────────────────
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce")
    df = df[df["fiscal_year"].notna()].copy()
    df["fiscal_year"] = df["fiscal_year"].astype(int)

    # ── Choose score column (prefer calibrated) ─────────────────────────────
    if "composite_score_calibrated" in df.columns:
        score_col = "composite_score_calibrated"
    elif "composite_score" in df.columns:
        score_col = "composite_score"
    else:
        raise ValueError("No composite score column found in input.")

    df["score_used"] = pd.to_numeric(df[score_col], errors="coerce")
    df = df.sort_values(["ticker", "fiscal_year"]).reset_index(drop=True)

    # ── YoY score change ────────────────────────────────────────────────────
    df["score_change_yoy"] = df.groupby("ticker")["score_used"].diff()

    # ──────────────────────────────────────────────────────────────────────
    # 1) Decline streak — consecutive years of falling score
    # ──────────────────────────────────────────────────────────────────────
    df["decline_streak_len"] = 0
    for ticker, g_idx in df.groupby("ticker").groups.items():
        idxs = list(g_idx)
        streak = 0
        for i in idxs:
            ch = df.at[i, "score_change_yoy"]
            if pd.isna(ch) or ch >= 0:
                streak = 0
            else:
                streak += 1
            df.at[i, "decline_streak_len"] = streak if not pd.isna(ch) else 0

    # ──────────────────────────────────────────────────────────────────────
    # 2) Low score streak — consecutive years with score below danger cutoff
    #    Catches chronically distressed companies that bounce around the
    #    danger zone without a clean consecutive decline (e.g. YELL)
    # ──────────────────────────────────────────────────────────────────────
    df["low_score_streak"] = 0
    for ticker, g_idx in df.groupby("ticker").groups.items():
        idxs = list(g_idx)
        streak = 0
        for i in idxs:
            if df.at[i, "score_used"] < LOW_SCORE_CUTOFF:
                streak += 1
            else:
                streak = 0
            df.at[i, "low_score_streak"] = streak

    # ──────────────────────────────────────────────────────────────────────
    # 3) Deterioration flag — applied in priority order
    #    Stable  → no sustained decline
    #    Watch   → 2 consecutive declining years
    #    Critical → any of:
    #               - 3+ consecutive declining years
    #               - single crash drop >= CRASH_THRESHOLD
    #               - 3+ consecutive years below LOW_SCORE_CUTOFF
    # ──────────────────────────────────────────────────────────────────────
    df["deterioration_flag"] = "Stable"
    df.loc[df["decline_streak_len"] == 2,                        "deterioration_flag"] = "Moderate Deterioration"
    df.loc[df["decline_streak_len"] >= 3,                        "deterioration_flag"] = "High Deterioration"
    df.loc[df["score_change_yoy"] <= CRASH_THRESHOLD,            "deterioration_flag"] = "High Deterioration"
    df.loc[df["low_score_streak"] >= LOW_SCORE_STREAK,           "deterioration_flag"] = "High Deterioration"

    # ──────────────────────────────────────────────────────────────────────
    # 4) Deterioration velocity
    #    Rolling mean of drop magnitudes over last N years.
    #    Decline year contributes the positive magnitude of the drop.
    #    Stable/recovering year contributes 0.
    #    Higher velocity = faster deterioration.
    # ──────────────────────────────────────────────────────────────────────
    drop_magnitude = df["score_change_yoy"].apply(
        lambda x: float(-x) if (not pd.isna(x) and x < 0) else 0.0
    )
    df["deterioration_velocity"] = (
        df.assign(_drop=drop_magnitude)
          .groupby("ticker")["_drop"]
          .transform(lambda s: s.rolling(VEL_WINDOW, min_periods=1).mean())
          .fillna(0.0)
          .astype(float)
    )

    # ── Drop intermediate columns not needed in final output ────────────────
    df = df.drop(columns=["_drop", "drop_magnitude"], errors="ignore")

    # ── Summary print ───────────────────────────────────────────────────────
    show_cols = [
        "ticker", "fiscal_year", score_col,
        "score_change_yoy", "decline_streak_len", "low_score_streak",
        "deterioration_flag", "deterioration_velocity",
    ]
    print(f"\nUsing score column: {score_col}")
    print(f"\nDeterioration flag distribution:")
    print(df["deterioration_flag"].value_counts().to_string())

    print(f"\nLatest year per company:")
    latest = df.sort_values("fiscal_year").groupby("ticker").last().reset_index()
    print(
        latest[["ticker", "sector", score_col, "deterioration_flag", "deterioration_velocity"]]
        .sort_values(score_col)
        .to_string(index=False)
    )

    print(f"\nYELL trajectory (backtest validation):")
    print(df[df["ticker"] == "YELL"][show_cols].to_string(index=False))

    # ── Save ────────────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    print(f"\nSaved: {OUT_PATH}")
    print(f"Rows: {len(df)} | Companies: {df['ticker'].nunique()}")


if __name__ == "__main__":
    main()