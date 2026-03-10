
# Corporate Bond Default Risk Profiler
### An Early Warning System Built Entirely on Public SEC Filings

---

## The Story That Started This Project

In August 2023, Yellow Corporation — one of America's largest trucking companies — filed for Chapter 11 bankruptcy, wiping out thousands of jobs and billions in shareholder value.

Credit rating agencies were late. Market analysts were late. The headlines were a surprise.

**The public filings were not.**

Yellow's SEC EDGAR 10-K filings told a different story — one of chronically collapsing risk scores, four consecutive years below the danger threshold, and a deterioration velocity that had been accelerating since 2019.

This project asks a simple question: **what if you had been watching the filings?**

Using only publicly available SEC data, this model flagged Yellow Corporation as critically distressed in 2021 — **two full years before the bankruptcy filing**. The Altman Z-Score, the most widely cited academic bankruptcy predictor, stayed in its "Grey Zone" the entire time and never once triggered a danger signal.

That gap — between what the filings showed and what the models said — is what this project is built to close.

---

## What This Project Does

This system ingests 5 years of financial data from SEC EDGAR for 30 public companies across three sectors — Retail, Healthcare, and Supply Chain — and produces a composite credit risk score for each company, each year.

It then tracks how that score changes over time, flags companies showing sustained deterioration, and validates its findings against the Altman Z-Score academic baseline.

The output is a four-page Power BI dashboard that lets an analyst or investor monitor credit deterioration across an entire portfolio in real time.

---

## Key Findings

**Healthcare is the most distressed sector.**
Average composite risk score of 60.83 — nearly 16 points below Retail (76.64) and Supply Chain (72.87). Driven by sustained debt load increases at Humana, Elevance Health, and Medtronic.

**Your model catches what Altman misses — and explains why.**
Of 30 companies, both models agree on 16. The 14 divergences are analytically meaningful:
- Kroger and McKesson are flagged High Deterioration by this model but Safe by Altman — because Altman's heavy asset turnover weighting rewards high-revenue distributors regardless of margin compression
- AbbVie is flagged Distress by Altman but Stable here — because aggressive share buybacks destroy book equity without impairing cash flow, a known Altman weakness

**The YELL backtest validates the model's predictive signal.**

| Year | Composite Score | Altman Z | Flag |
|------|----------------|----------|------|
| 2018 | 37.2 | 3.14 | Stable |
| 2019 | 8.9 | 2.20 | Stable |
| 2020 | 12.7 | 1.94 | Stable |
| 2021 | 8.2 | 1.93 | **High Deterioration** |
| 2022 | 23.5 | 2.31 | **High Deterioration** |
| 2023 | — | — | **Chapter 11 filed** |

This model flagged Critical in 2021. Altman never left Grey Zone.

---

## The Five Risk Ratios

Each company is scored annually across five financial dimensions, each normalized 0–10 and weighted into a composite score out of 100. Lower score = higher default risk.

| Ratio | Formula | What It Measures |
|-------|---------|-----------------|
| Debt to Asset | Total Debt / Total Assets | Overall leverage burden |
| Interest Coverage | EBIT / Interest Expense | Ability to service debt |
| Debt to EBIT | Total Debt / EBIT | Years of earnings needed to repay debt |
| Cash Flow to Debt | Operating Cash Flow / Total Debt | Liquidity-based repayment capacity |
| Revenue Trend | 3-year YoY revenue growth | Top-line trajectory |

**Composite score thresholds:**
- Above 65 → Stable
- 40–65 → Watch
- Below 40 → High Deterioration

---

## Deterioration Flag Logic

A score alone is a snapshot. Deterioration is a trajectory.

Three conditions trigger a **High Deterioration** flag:
1. Three or more consecutive years of declining composite score
2. A single-year score crash of 30+ points
3. Three or more consecutive years with a score below 30 (chronic distress)

A **Moderate Deterioration** flag triggers on two consecutive declining years.

This logic correctly flagged Yellow Corporation in 2021 via condition 3 — chronic low score streak — even during a year when its score partially recovered from the prior year's trough.

---

## Altman Z-Score Validation

The Altman Z-Score (book value variant) was calculated for all 30 companies using the same SEC EDGAR data:

```
Z = 1.2(X1) + 1.4(X2) + 3.3(X3) + 0.6(X4) + 1.0(X5)

X1 = (Assets - Liabilities) / Assets     Working capital proxy
X2 = Net Income / Assets                  Retained earnings proxy
X3 = EBIT / Assets                        Operating efficiency
X4 = Equity / Liabilities                 Book value solvency
X5 = Revenue / Assets                     Asset turnover
```

Thresholds: Z > 2.99 Safe | 1.81–2.99 Grey Zone | Z < 1.81 Distress

Agreement classification:
- This model Risk = High Deterioration OR Moderate Deterioration
- Altman Risk = Distress Zone
- Grey Zone treated as ambiguous — not forced into agreement or disagreement

**Results (latest year, 30 companies):**
- Both flag safe: 8
- Both flag risk: 2
- Grey Zone — leans risk: 3
- Grey Zone — leans safe: 7
- Your model flags risk, Altman doesn't: 4
- Altman flags risk, yours doesn't: 2 (ABBVIE buyback effect, NSC derailment liabilities)

---

## Company Universe

**Retail (10):** Macy's, Bath & Body Works, Best Buy, Costco, Dollar General, Kroger, O'Reilly Automotive, Ralph Lauren, Target, Walmart

**Healthcare (10):** AbbVie, Cardinal Health, Elevance Health, Humana, LabCorp, McKesson, Medtronic, Regeneron, Surgery Partners, Universal Health Services

**Supply Chain (10):** Fastenal, FedEx, Hub Group, J.B. Hunt, Matson, Norfolk Southern, Old Dominion, Saia, UPS, Yellow Corporation *(backtest)*

---

## Technical Architecture

```
SEC EDGAR API
      ↓
01_download_companyfacts.py     Raw JSON download (XBRL company facts)
      ↓
02_extract_core_metrics.py      XBRL tag extraction with fallback hierarchy
      ↓
03_scoring.py                   5 ratio calculation + composite score
      ↓
08_deterioration_flags.py       Flag logic + velocity scoring
      ↓
Altman_Z_Score_validation.py    Academic baseline comparison
      ↓
09_load_postgres.py             6-table PostgreSQL schema load
      ↓
Power BI Dashboard              4-page interactive report
```

---

## PostgreSQL Schema

```sql
company_master          -- 30 rows, dimension table
dim_year                -- 6 rows, year dimension for Power BI
financials              -- 150 rows, raw financial metrics
risk_scores             -- 150 rows, ratios + composite scores
deterioration_results   -- 150 rows, flags + velocity + streaks
altman_validation       -- 150 rows, Altman Z + agreement classification
company_latest          -- View, one row per company at latest year
```

All fact tables reference `company_master` and `dim_year` via foreign keys. Indexes on ticker, fiscal_year, deterioration_flag, sector, and altman_zone for Power BI query performance.

---

## Power BI Dashboard — 4 Pages

**Page 1 — Portfolio Overview**
RAG-coded leaderboard of all 30 companies. KPI cards for High Risk / Watch / Stable counts. Sector average comparison.

**Page 2 — Company Deep Dive**
Per-company trend analysis. Composite score trajectory with danger threshold. Altman Z trend. Raw ratio trends. Dynamic risk commentary generated from DAX. Drill-down via company name slicer.

**Page 3 — Watch List & Validation**
Filtered view of High Risk and Moderate Deterioration companies only. YELL backtest dual-axis chart with bankruptcy annotation. Model agreement breakdown. Analytical narrative explaining key divergences.

**Page 4 — Sector Comparison**
Radar chart comparing all 3 sectors across 5 ratio dimensions. Score trend over time by sector. Risk flag distribution by sector.

---

## Data Sources and Limitations

**Source:** SEC EDGAR XBRL API (`/api/xbrl/companyfacts`) — 100% free, no API key required.

**Coverage period:** Primarily FY2021–FY2025. Yellow Corporation data ends FY2022 (pre-bankruptcy).

**Known limitations:**
- Interest coverage missing for companies with zero debt — handled via weight rebalancing (missing metric dropped from denominator, not zeroed)
- Working capital approximated as Total Assets minus Total Liabilities (current assets not separately extracted)
- Retained earnings approximated via net income (retained earnings not consistently reported in XBRL)
- Altman Z uses book value variant — no market cap data incorporated
- Healthcare sector skewed by AbbVie's buyback-driven negative book equity

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data extraction | Python, SEC EDGAR REST API |
| Data processing | pandas, numpy |
| Database | PostgreSQL 15 |
| ORM / loading | SQLAlchemy, psycopg2 |
| Visualisation | Power BI Desktop |
| Version control | Git / GitHub |

---

## Repository Structure

```
Bond-defaulter-Profiler/
│
├── data/
│   ├── raw/                    EDGAR JSON files per company
│   └── clean/                  Processed CSVs
│
├── src/
│   ├── 00_build_tickers.py
│   ├── 01_download_companyfacts.py
│   ├── 02_extract_core_metrics.py
│   ├── 03_scoring.py
│   ├── 08_deterioration_flags.py
│   ├── 09_load_postgres.py
│   ├── 09_load_altman_validation.py
│   └── Altman_Z_Score_validation.py
│
├── dashboard/
│   └── BondRiskProfiler.pbix
│
└── README.md
```

---

## How to Run

```bash
# 1. Clone and install dependencies
pip install pandas numpy sqlalchemy psycopg2-binary requests

# 2. Download EDGAR data
python src/01_download_companyfacts.py

# 3. Extract and score
python src/02_extract_core_metrics.py
python src/03_scoring.py
python src/08_deterioration_flags.py

# 4. Run Altman validation
python src/Altman_Z_Score_validation.py

# 5. Load to PostgreSQL (update credentials in script)
python src/09_load_postgres.py
python src/09_load_altman_validation.py

# 6. Open Power BI and refresh data source
```

---

## The Bottom Line

Credit rating agencies are reactive by design. They rate based on what has already happened. This model is built to be proactive — tracking the trajectory of deterioration before it becomes a headline.

Built entirely on free public data. No Bloomberg terminal. No proprietary feeds. Just the filings that every public company is already required to submit.

*"The information was always there. You just had to know where to look."*

---

*Project by [Your Name] | Data Analyst Portfolio | 2025*
*Data source: SEC EDGAR (public domain) | All analysis for educational purposes only*
