# SecuritiesAnalysis

This repository contains Bloomberg API utilities for studying equity securities and their supply chains. Each script is designed so you can swap in a different list of tickers or benchmark indices to reapply the workflows to any sector covered by Bloomberg data.

## Environment

All scripts assume access to Bloomberg Desktop or Server API via `blpapi` and rely on common data-wrangling packages:

```bash
pip install blpapi pandas numpy matplotlib openpyxl
```

> **Note:** `openpyxl` is only required for the Excel export utilities.

## Project structure

### `gsox_vs_benchmarks.py`

* Pulls daily price or total-return histories for one or more assets and comparison indices via Bloomberg HistoricalDataRequest.
* Cleans and aligns the time series, calculates returns, rolling correlations, betas, and drawdowns, then exports CSV and PNG charts.
* **Reusing for other sectors:**
  * Update the `TICKERS` dictionary at the top of the file with the Bloomberg mnemonics you care about (e.g., change the semiconductor indices to energy or healthcare benchmarks).
  * Adjust `START_DATE`, `END_DATE`, and rolling window constants to match your analysis horizon.
  * Toggle `USE_TOTAL_RETURN` if total return series are available for your chosen instruments.

### `supply_chain_excel_parity_blpapi.py`

* Recreates the `SUPPLY_CHAIN_SUPPLIERS` / `SUPPLY_CHAIN_CUSTOMERS` Excel formulas through the API, including optional overrides for relationship counts, quantification flags, currency, and sorting.
* Enriches each counterparty by optionally calling the `RELATIONSHIP_AMOUNT` field for dollar exposures.
* **Reusing for other sectors:**
  * Pass a sector-specific list of issuer tickers via `--tickers` or `--tickers-file`.
  * Customize overrides such as `--sum-count`, `--quantified`, `--supplier-sort`, and `--currency` to align with your Excel setup.
  * Modify sleep pacing if your environment needs different throttling.

### `top_x_customers_by_tickers.py`

* Automates a two-step workflow for customers: bulk download the Bloomberg supply-chain table, then enrich each counterparty with BDP-style metrics (amount, as-of date, revenue/cost percentages, and account type when available).
* Writes the combined dataset to an Excel workbook for further filtering or pivoting.
* **Reusing for other sectors:**
  * Provide tickers for the companies in your target industry with `--tickers`, `--tickers-file`, or by editing `DEFAULT_TICKERS`.
  * Override parameters like `--sum-count-override`, `--quantified-override`, `--currency`, and `--sleep-ms` to match entitlement and performance considerations.
  * Rename columns where necessary if your entitlement provides differently labeled counterparty names or tickers.

### `top_x_suppliers_by_tickers.py`

* Mirrors the customer script but focuses on supplier relationships, including cost account type data.
* Produces a sector-wide supplier roll-up in Excel with the same enrichment fields.
* **Reusing for other sectors:**
  * Supply the relevant issuer universe via CLI arguments or by editing `DEFAULT_TICKERS`.
  * Tune overrides such as `--supplier-sort-override` to change sorting behavior (e.g., by cost vs. revenue share).
  * Leverage the `--currency` flag to normalize exposures when analyzing multi-region supply chains.

## Workflow tips for sector rotation

1. **Build a universe:** Start from an industry classification, ETF constituent list, or internal coverage universe. Feed those tickers directly to the supply chain scripts.
2. **Swap benchmarks:** When analyzing performance, replace the semiconductor indices in `gsox_vs_benchmarks.py` with sector-specific or macro factors (e.g., `XLE Index`, `SXDE Index`, `NDX Index`).
3. **Enrich relationships:** Use the supplier/customer Excel outputs to identify key dependencies and rerun the analytics after any corporate action by updating the ticker list.
4. **Iterate quickly:** Because all overrides are CLI flags or top-level constants, you can script multiple sector runs (e.g., with shell loops or orchestrators) without touching the core logic.

## Data outputs

* `data/combined.csv` and `charts/*.png` from the benchmark comparison script.
* `customers.csv` / `suppliers.csv` from the Excel parity script.
* `total_customers.xlsx` / `total_suppliers.xlsx` from the top-X scripts.

Use these artifacts as inputs into downstream sector dashboards, relative value studies, or supply chain risk assessments.
