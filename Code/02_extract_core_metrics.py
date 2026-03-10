# src/02_extract_core_metrics.py
import json
import os
import pandas as pd

RAW_DIR = os.path.join("data", "raw")
TICKERS_PATH = os.path.join("data", "clean", "tickers_resolved.csv")
OUT_PATH = os.path.join("data", "clean", "core_metrics_all_companies.csv")

YEARS_TO_KEEP = 5

TAG_MAP = {
    "total_assets": [
        "Assets",
        "AssetsCurrent",
        "NoncurrentAssets",
    ],
    "total_liabilities": [
        "Liabilities",
        "LiabilitiesAndStockholdersEquity",
    ],
    "stockholders_equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "PartnersCapital",
    ],
    "revenue": [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
        "SalesRevenueGoodsNet",
        "SalesRevenueServicesNet",
        "HealthCareOrganizationRevenue",
        "HealthCareOrganizationPatientServiceRevenue",
        "HealthCareOrganizationRevenueLessPatientServiceRevenue",
        "PremiumsEarnedNet",
        "PremiumsAndOtherRevenues",
        "FreightRevenue",
        "RegulatedAndUnregulatedOperatingRevenue",
        "RevenueFromContractWithCustomerMember",
    ],
    "operating_cashflow": [
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
        "NetCashProvidedByUsedInOperatingActivitiesDiscontinuedOperations",
        "CashGeneratedFromOperations",
    ],
    "ebit": [
        "OperatingIncomeLoss",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",
        "ProfitLoss",
        "GrossProfit",  # last resort
    ],
    # IMPORTANT: We keep existing tags and only ADD candidates (no removals)
    # Preferred is accrual-based InterestExpense; cash-based (InterestPaid*) is fallback.
    "interest_expense": [
        "InterestExpense",
        "InterestAndDebtExpense",
        "InterestExpenseDebt",
        "FinanceLeaseInterestExpense",
        "InterestExpenseNonoperating",
        "InterestExpenseBorrowings",
        # added candidates (from your scout output)
        "interest_paid_cf",
        "InterestExpenseNet",
        "InterestExpenseNetOfInterestIncome",
        "InterestExpenseNetOfCapitalizedInterest",
        "InterestExpenseNetOfCapitalizedInterestAndInterestIncome",
        "InterestExpenseNetOfInterestCapitalized",
    ],
    "net_income": [
        "NetIncomeLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
        "NetIncomeLossAvailableToCommonStockholdersDiluted",
        "ProfitLoss",
    ],
    "total_debt": [
        "DebtAndCapitalLeaseObligations",
        "LongTermDebtAndCapitalLeaseObligations",
        "LongTermDebt",
        "LongTermDebtNoncurrent",
        "LongTermDebtCurrent",
        "LongTermDebtAndCapitalLeaseObligationsCurrent",
        "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities",
        "DebtCurrent",
        "NotesAndLoansPayable",
    ],
    # components for derived fallback
    "debt_current": [
        "DebtCurrent",
        "ShortTermBorrowings",
        "ShortTermDebt",
        "CommercialPaper",
        "LongTermDebtCurrent",
        "LongTermDebtAndCapitalLeaseObligationsCurrent",
    ],
    "long_term_debt_noncurrent": [
        "LongTermDebtNoncurrent",
        "LongTermDebt",
        "LongTermDebtAndCapitalLeaseObligations",
    ],
}

EBIT_GROSS_PROFIT_FALLBACK = "GrossProfit"


def extract_last_n_years(companyfacts, tag_list, unit="USD", years_to_keep=5):
    """
    STRICT behavior (keep stable):
    - Only uses the exact 'unit' key (default 'USD')
    - Filters to 10-K and FY
    - Keeps last observation per fiscal_year
    """
    facts = companyfacts.get("facts", {}).get("us-gaap", {})
    rows = []
    tag_used = None

    for tag in tag_list:
        if tag not in facts:
            continue

        units = facts[tag].get("units", {})
        if unit not in units:
            continue

        tag_rows = []
        for item in units[unit]:
            form = str(item.get("form", ""))
            if not form.startswith("10-K"):
                continue

            if item.get("fp") not in (None, "FY"):
                continue

            end = item.get("end")
            val = item.get("val")
            if end is None or val is None:
                continue

            try:
                fiscal_year = int(str(end)[:4])
            except Exception:
                continue

            tag_rows.append(
                {"fiscal_year": fiscal_year, "end": end, "val": val, "tag": tag}
            )

        if tag_rows:
            rows = tag_rows
            tag_used = tag
            break

    if not rows:
        return pd.DataFrame(columns=["fiscal_year", "end", "val", "tag"]), None

    df = pd.DataFrame(rows)
    df = df.sort_values(["fiscal_year", "end"]).drop_duplicates(
        subset=["fiscal_year"], keep="last"
    )

    df = df.sort_values("fiscal_year", ascending=False).head(years_to_keep)
    return df.sort_values("fiscal_year"), tag_used


def _coerce_numeric(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")


def main():
    tickers_df = pd.read_csv(TICKERS_PATH, dtype=str).fillna("")
    tickers_df["ticker"] = tickers_df["ticker"].str.upper().str.strip()
    tickers_df["cik"] = tickers_df["cik"].str.strip().str.zfill(10)

    all_outputs = []
    failed = []

    for _, row in tickers_df.iterrows():
        ticker = row["ticker"]
        cik = row["cik"]
        sector = row.get("sector", "")

        fname = f"{ticker}_CIK{cik}.json"
        path = os.path.join(RAW_DIR, fname)

        if not os.path.exists(path):
            print(f"\nSKIP {ticker}: raw file not found -> {path}")
            failed.append(
                {"ticker": ticker, "cik": cik, "sector": sector, "reason": "raw_file_missing"}
            )
            continue

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        entity = data.get("entityName", "")
        print("\n==============================")
        print(f"{ticker} — {entity}")
        print("==============================")

        assets_df, _ = extract_last_n_years(
            data, TAG_MAP["total_assets"], years_to_keep=YEARS_TO_KEEP
        )
        years = assets_df["fiscal_year"].tolist()

        if not years:
            print("  No 10-K fiscal years found from Assets tag.")
            failed.append({"ticker": ticker, "cik": cik, "sector": sector, "reason": "no_fy_years"})
            continue

        if len(years) < YEARS_TO_KEEP:
            print(f"  SKIP {ticker}: only {len(years)} usable fiscal years (need {YEARS_TO_KEEP})")
            failed.append(
                {"ticker": ticker, "cik": cik, "sector": sector, "reason": f"only_{len(years)}_years"}
            )
            continue

        print(f"  Fiscal years found (last {YEARS_TO_KEEP}): {years}")
        result = pd.DataFrame({"fiscal_year": years})

        tag_audit = {"ticker": ticker, "entity": entity}

        metrics_to_extract = [
            "total_assets",
            "total_liabilities",
            "stockholders_equity",
            "revenue",
            "operating_cashflow",
            "ebit",
            "interest_expense",
            "net_income",
            "total_debt",
            "debt_current",
            "long_term_debt_noncurrent",
        ]

        for metric in metrics_to_extract:
            dfm, tag_used = extract_last_n_years(
                data, TAG_MAP[metric], years_to_keep=YEARS_TO_KEEP
            )
            tag_audit[f"{metric}_tag"] = tag_used if tag_used else "NOT_FOUND"

            if not dfm.empty:
                result = result.merge(
                    dfm[["fiscal_year", "val"]].rename(columns={"val": metric}),
                    on="fiscal_year",
                    how="left",
                )
            else:
                result[metric] = None

        # normalize numeric types BEFORE any math / fills
        _coerce_numeric(
            result,
            [
                "total_assets",
                "total_liabilities",
                "stockholders_equity",
                "revenue",
                "operating_cashflow",
                "ebit",
                "interest_expense",
                "net_income",
                "total_debt",
                "debt_current",
                "long_term_debt_noncurrent",
            ],
        )

        # Fix: LiabilitiesAndStockholdersEquity -> subtract equity
        if tag_audit.get("total_liabilities_tag") == "LiabilitiesAndStockholdersEquity":
            mask = result["total_liabilities"].notna() & result["stockholders_equity"].notna()
            result.loc[mask, "total_liabilities"] = (
                result.loc[mask, "total_liabilities"] - result.loc[mask, "stockholders_equity"]
            )
            if mask.any():
                print(
                    f"  ℹ️  total_liabilities corrected: (LiabilitiesAndStockholdersEquity - Equity) for {mask.sum()} rows"
                )

        # EBIT fallback flag
        result["ebit_is_gross_profit_flag"] = (
            tag_audit.get("ebit_tag") == EBIT_GROSS_PROFIT_FALLBACK
        )
        if len(result) and bool(result["ebit_is_gross_profit_flag"].iloc[0]):
            print(f"  ⚠️  EBIT fallback: using GrossProfit for {ticker} — flag set")

        # Derived total_debt fallback (only where missing)
        derived_mask = result["total_debt"].isna()
        can_derive = result["debt_current"].notna() | result["long_term_debt_noncurrent"].notna()
        fill_mask = derived_mask & can_derive

        if fill_mask.any():
            derived = (
                result.loc[fill_mask, "debt_current"].fillna(0)
                + result.loc[fill_mask, "long_term_debt_noncurrent"].fillna(0)
            )
            result.loc[fill_mask, "total_debt"] = derived.to_numpy(dtype="float64")
            print(
                f"  ℹ️  total_debt derived (DebtCurrent + LongTermDebtNoncurrent) for {fill_mask.sum()} rows"
            )

        # Still-missing liabilities fallback: Assets - Equity
        if result["total_liabilities"].isna().any():
            mask = (
                result["total_liabilities"].isna()
                & result["stockholders_equity"].notna()
                & result["total_assets"].notna()
            )
            result.loc[mask, "total_liabilities"] = (
                result.loc[mask, "total_assets"] - result.loc[mask, "stockholders_equity"]
            )
            if mask.any():
                print(f"  ℹ️  total_liabilities derived from Assets - Equity for {mask.sum()} rows")

        # Add identifiers
        result["ticker"] = ticker
        result["cik"] = cik
        result["sector"] = sector
        result["entity"] = entity

        # Drop debt component columns (only used for derivation)
        result.drop(columns=["debt_current", "long_term_debt_noncurrent"], inplace=True, errors="ignore")

        nan_cols = result.columns[result.isna().any()].tolist()
        if nan_cols:
            print(f"  ⚠️  NaN columns: {nan_cols}")
        else:
            print("  ✅  All metrics extracted cleanly")

        print(f"\n  Core metrics (last {YEARS_TO_KEEP} fiscal years):")
        display_cols = [
            "fiscal_year",
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
        print(
            result[[c for c in display_cols if c in result.columns]]
            .sort_values("fiscal_year")
            .to_string(index=False)
        )

        all_outputs.append(result)

    # --- Output folder ---
    os.makedirs(os.path.join("data", "clean"), exist_ok=True)

    # --- Always print end marker ---
    print("\n" + "=" * 50)
    print("END OF EXTRACTION RUN")
    print("=" * 50)

    # --- If nothing extracted, exit cleanly ---
    if not all_outputs:
        print("No company outputs were generated (all_outputs is empty).")
        if failed:
            failed_df = pd.DataFrame(failed)
            failed_path = os.path.join("data", "clean", "failed_tickers.csv")
            failed_df.to_csv(failed_path, index=False)
            print(f"Failed tickers saved: {failed_path}")
        return

    # --- Combine + save ---
    combined = pd.concat(all_outputs, ignore_index=True)
    combined.to_csv(OUT_PATH, index=False)
    print(f"Saved: {OUT_PATH}")
    print(f"Total companies: {combined['ticker'].nunique()} | Total rows: {len(combined)}")

    # --- NaN summary (exact format, no dtype line) ---
    print("\nNaN summary across all companies:")
    nan_summary = combined.isna().sum()
    nan_summary = nan_summary[nan_summary > 0]

    if nan_summary.empty:
        print("  ✅  No NaNs remaining!")
    else:
        for col, cnt in nan_summary.items():
            print(f"{col:<20} {int(cnt)}")

    # save summary to file too
    summary_path = os.path.join("data", "clean", "nan_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("NaN summary across all companies:\n")
        if nan_summary.empty:
            f.write("No NaNs remaining!\n")
        else:
            for col, cnt in nan_summary.items():
                f.write(f"{col:<20} {int(cnt)}\n")
    print(f"\nSaved NaN summary to: {summary_path}")

    # --- Failed tickers ---
    if failed:
        failed_df = pd.DataFrame(failed)
        failed_path = os.path.join("data", "clean", "failed_tickers.csv")
        failed_df.to_csv(failed_path, index=False)
        print(f"\nFailed tickers saved: {failed_path}")
        print(failed_df.to_string(index=False))


if __name__ == "__main__":
    main()