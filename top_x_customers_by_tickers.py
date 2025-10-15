#!/usr/bin/env python3
# pip install blpapi pandas openpyxl

import argparse
import time
from typing import Any, Dict, Iterable, List, Optional

import blpapi
import pandas as pd

# ---- Default tickers (used if --tickers/--tickers-file not provided) ----
DEFAULT_TICKERS = [
    "NVDA UW Equity","AVGO UW Equity","INTC UW Equity","QCOM UW Equity","MU UW Equity",
    "AMD UW Equity","AVT UW Equity","TXN UW Equity","ADI UW Equity","MRVL UW Equity",
    "GFS UW Equity","ON UW Equity","MCHP UW Equity","SWKS UW Equity","QRVO UW Equity",
    "VSH UN Equity","MPWR UW Equity","CRUS UW Equity","AEIS UW Equity","DIOD UW Equity",
    "SYNA UW Equity","SMTC UW Equity","MTSI UW Equity","FORM UW Equity","ALGM UW Equity",
    "SLAB UW Equity","AOSL UW Equity","RMBS UW Equity","ALAB UW Equity","CRDO UW Equity",
    "LSCC UW Equity","POWI UW Equity","MXL UW Equity","AMBA UW Equity","SKYT UR Equity",
    "SITM UQ Equity","INDI UR Equity","LASR UW Equity","CEVA UW Equity","NVTS UQ Equity",
    "KOPN UR Equity","NVEC UR Equity","AEVA UW Equity","ATOM UR Equity",
]

# =========================
# Bloomberg session helpers
# =========================

def open_session(host: str | None = None, port: int | None = None) -> blpapi.Session:
    """Open a Bloomberg API session (Desktop API by default)."""
    if host and port:
        opts = blpapi.SessionOptions()
        opts.setServerHost(host)
        opts.setServerPort(port)
        session = blpapi.Session(opts)
    else:
        session = blpapi.Session()
    if not session.start():
        raise RuntimeError("Failed to start Bloomberg session")
    if not session.openService("//blp/refdata"):
        raise RuntimeError("Failed to open //blp/refdata")
    return session

def elem_to_py(elem: blpapi.Element):
    """Convert a blpapi Element (bulk/complex/scalar) to Python types."""
    if elem.isArray():
        return [elem_to_py(elem.getValueAsElement(i)) for i in range(elem.numValues())]
    if elem.isComplexType():
        out = {}
        for i in range(elem.numElements()):
            sub = elem.getElement(i)
            out[str(sub.name())] = elem_to_py(sub)
        return out
    try:
        return elem.getValue()
    except Exception:
        return None

# =========================
# BDS-equivalent (CUSTOMERS)
# =========================

def bds_supply_chain_customers_session(
    session: blpapi.Session,
    ticker: str,
    sum_count_override: str = "20",
    quantified_override: str = "Y",
) -> pd.DataFrame:
    """
    Python equivalent of:
      =BDS("TICKER","SUPPLY_CHAIN_CUSTOMERS",
           "SUPPLY_CHAIN_SUM_COUNT_OVERRIDE=20,QUANTIFIED_OVERRIDE=Y")
    Returns a DataFrame of bulk rows for that ticker, with 'customer_ticker' = base ticker.
    """
    svc = session.getService("//blp/refdata")
    req = svc.createRequest("ReferenceDataRequest")
    req.getElement("securities").appendValue(ticker)
    req.getElement("fields").appendValue("SUPPLY_CHAIN_CUSTOMERS")

    ovr = req.getElement("overrides")
    for k, v in [
        ("SUPPLY_CHAIN_SUM_COUNT_OVERRIDE", sum_count_override),
        ("QUANTIFIED_OVERRIDE", quantified_override),
    ]:
        e = ovr.appendElement()
        e.setElement("fieldId", k)
        e.setElement("value", v)

    session.sendRequest(req)

    rows: List[Dict[str, Any]] = []
    while True:
        ev = session.nextEvent()
        if ev.eventType() in (blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE):
            for msg in ev:
                if not msg.hasElement("securityData"):
                    continue
                sd_arr = msg.getElement("securityData")
                for i in range(sd_arr.numValues()):
                    sd = sd_arr.getValueAsElement(i)
                    if not sd.hasElement("fieldData"):
                        continue
                    fd = sd.getElement("fieldData")
                    if not fd.hasElement("SUPPLY_CHAIN_CUSTOMERS"):
                        continue
                    bulk = fd.getElement("SUPPLY_CHAIN_CUSTOMERS")
                    py = elem_to_py(bulk)
                    if isinstance(py, list):
                        rows.extend([r if isinstance(r, dict) else {"value": r} for r in py])
                    elif isinstance(py, dict):
                        rows.append(py)
        if ev.eventType() == blpapi.Event.RESPONSE:
            break

    df = pd.DataFrame(rows)
    # Add the base ticker for context (issuer you queried)
    df.insert(0, "customer_ticker", ticker)
    return df

# =========================
# BDP-equivalent enrichment (CUSTOMERS)
# =========================

def _bdp_customer_enrichment(
    session: blpapi.Session,
    base_ticker: str,
    related_customer_value: str,
    currency: str = "USD",
    quantified: str = "Y",
) -> dict:
    """
    Fetch BDP-style fields for a customer relative to a base ticker:
      RELATIONSHIP_AMOUNT (USD), RELATIONSHIP_AS_OF_DATE,
      SUPPLY_CHAIN_REVENUE_PERCENTAGE, SUPPLY_CHAIN_COST_PERCENTAGE,
      and (best-effort) SUPPLY_CHAIN_REVENUE_ACCOUNT_TYPE (if available).
    """
    svc = session.getService("//blp/refdata")
    req = svc.createRequest("ReferenceDataRequest")
    req.getElement("securities").appendValue(base_ticker)

    fields_el = req.getElement("fields")
    for f in [
        "RELATIONSHIP_AMOUNT",
        "RELATIONSHIP_AS_OF_DATE",
        "SUPPLY_CHAIN_REVENUE_PERCENTAGE",
        "SUPPLY_CHAIN_COST_PERCENTAGE",
        # Some sites expose this; we read it if present:
        "SUPPLY_CHAIN_REVENUE_ACCOUNT_TYPE",
    ]:
        fields_el.appendValue(f)

    ovr = req.getElement("overrides")
    for k, v in [
        ("RELATIONSHIP_OVERRIDE", "C"),   # Customer context
        ("QUANTIFIED_OVERRIDE",   quantified),
        ("EQY_FUND_CRNCY",        currency),
        ("RELATED_COMPANY_OVERRIDE", related_customer_value),
    ]:
        e = ovr.appendElement(); e.setElement("fieldId", k); e.setElement("value", v)

    session.sendRequest(req)

    out = {
        "relationship_amount_usd": None,
        "relationship_as_of_date": None,
        "supply_chain_revenue_percentage": None,
        "supply_chain_cost_percentage": None,
        "supply_chain_revenue_account_type": None,  # best-effort
    }

    while True:
        ev = session.nextEvent()
        if ev.eventType() in (blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE):
            for msg in ev:
                if not msg.hasElement("securityData"): continue
                sarr = msg.getElement("securityData")
                for i in range(sarr.numValues()):
                    sdata = sarr.getValueAsElement(i)
                    if not sdata.hasElement("fieldData"): continue
                    fdata = sdata.getElement("fieldData")

                    def get_float(el_name: str):
                        try:
                            return fdata.getElement(el_name).getValueAsFloat() if fdata.hasElement(el_name) else None
                        except Exception:
                            return None
                    def get_value(el_name: str):
                        try:
                            return fdata.getElement(el_name).getValue() if fdata.hasElement(el_name) else None
                        except Exception:
                            return None

                    out["relationship_amount_usd"]         = get_float("RELATIONSHIP_AMOUNT")
                    out["relationship_as_of_date"]          = get_value("RELATIONSHIP_AS_OF_DATE")
                    out["supply_chain_revenue_percentage"]  = get_float("SUPPLY_CHAIN_REVENUE_PERCENTAGE")
                    out["supply_chain_cost_percentage"]     = get_float("SUPPLY_CHAIN_COST_PERCENTAGE")
                    # Revenue account type may not exist in all entitlements; read if present.
                    out["supply_chain_revenue_account_type"]= get_value("SUPPLY_CHAIN_REVENUE_ACCOUNT_TYPE")

        if ev.eventType() == blpapi.Event.RESPONSE:
            break

    return out

def enrich_customers_with_bdp_session(
    session: blpapi.Session,
    customer_df: pd.DataFrame,
    currency: str = "USD",
    quantified: str = "Y",
    sleep_ms: int = 50,
) -> pd.DataFrame:
    """
    Enrich each customer row using:
      - base ticker:    customer_ticker (issuer you queried)
      - related key:    prefer 'Equity Ticker' (customer BBG ticker), else 'customer_name' (string)
    Adds amount/as-of/rev%/cost%/(optional)rev_account_type columns.
    """
    if customer_df.empty:
        return customer_df.assign(
            relationship_amount_usd=None,
            relationship_as_of_date=None,
            supply_chain_revenue_percentage=None,
            supply_chain_cost_percentage=None,
            supply_chain_revenue_account_type=None,
        )

    if "customer_ticker" not in customer_df.columns:
        raise ValueError("customer_df must include 'customer_ticker' (base issuer).")

    related_col = "Equity Ticker" if "Equity Ticker" in customer_df.columns else None
    if related_col is None:
        related_col = "customer_name" if "customer_name" in customer_df.columns else None
    if related_col is None:
        raise ValueError("customer_df needs 'Equity Ticker' or 'customer_name' for RELATED_COMPANY_OVERRIDE.")

    results = []
    for _, row in customer_df.iterrows():
        base = str(row["customer_ticker"]).strip() if pd.notna(row["customer_ticker"]) else ""
        related = str(row[related_col]).strip() if pd.notna(row[related_col]) else ""
        if not base or not related:
            results.append({
                "relationship_amount_usd": None,
                "relationship_as_of_date": None,
                "supply_chain_revenue_percentage": None,
                "supply_chain_cost_percentage": None,
                "supply_chain_revenue_account_type": None,
            })
            continue

        vals = _bdp_customer_enrichment(
            session,
            base_ticker=base,
            related_customer_value=related,
            currency=currency,
            quantified=quantified,
        )
        results.append(vals)

        if sleep_ms:
            time.sleep(sleep_ms / 1000.0)

    res_df = pd.DataFrame(results, index=customer_df.index)
    return pd.concat([customer_df, res_df], axis=1)

# =========================
# Orchestration (CUSTOMERS)
# =========================

def process_customers_to_excel(
    tickers: List[str],
    out_xlsx: str,
    host: str | None = None,
    port: int | None = None,
    sum_count_override: str = "20",
    quantified_override: str = "Y",
    currency: str = "USD",
    sleep_ms: int = 50,
) -> pd.DataFrame:
    """
    For each ticker:
      1) BDS-equivalent pull of CUSTOMERS
      2) BDP-equivalent enrichment (customer context)
      3) Append to total_customer_df
    Then write total_customer_df to an Excel file and return it.
    """
    session = open_session(host, port)
    try:
        frames = []
        for tkr in tickers:
            # Step 1: base customer table
            base_df = bds_supply_chain_customers_session(
                session,
                tkr,
                sum_count_override=sum_count_override,
                quantified_override=quantified_override,
            )
            if base_df.empty:
                continue

            # If your payload exposes an obvious name column, you can standardize it like this:
            # if "Counterparty Name" in base_df.columns and "customer_name" not in base_df.columns:
            #     base_df = base_df.rename(columns={"Counterparty Name": "customer_name"})

            # Step 2: enrich with BDP fields (customer context)
            enriched = enrich_customers_with_bdp_session(
                session,
                base_df,
                currency=currency,
                quantified=quantified_override,
                sleep_ms=sleep_ms,
            )
            frames.append(enriched)

        total_customer_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        # Step 3: write to Excel
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xw:
            total_customer_df.to_excel(xw, index=False, sheet_name="Customers")

        return total_customer_df
    finally:
        session.stop()

# =========================
# CLI
# =========================

def _resolve_tickers(cli: Optional[str], file: Optional[str]) -> List[str]:
    if file:
        with open(file, "r", encoding="utf-8") as f:
            return [ln.strip() for ln in f if ln.strip()]
    if cli:
        return [t.strip() for t in cli.split(",") if t.strip()]
    return DEFAULT_TICKERS[:]  # use defaults if nothing provided

def main():
    ap = argparse.ArgumentParser(description="Build an Excel of customer relationships for a list of tickers (Bloomberg API).")
    ap.add_argument(
        "--tickers",
        default=",".join(DEFAULT_TICKERS),
        help="Comma-separated tickers (default: built-in list)"
    )
    ap.add_argument("--tickers-file", help="Path to newline-delimited tickers")
    ap.add_argument("--out-xlsx", default="total_customers.xlsx", help="Output Excel path (default: total_customers.xlsx)")
    ap.add_argument("--host", help="Server API host (omit for Desktop API)")
    ap.add_argument("--port", type=int, help="Server API port (omit for Desktop API)")

    # BDS-like overrides
    ap.add_argument("--sum-count-override", default="20", help="SUPPLY_CHAIN_SUM_COUNT_OVERRIDE (default 20)")
    ap.add_argument("--quantified-override", default="Y", help="QUANTIFIED_OVERRIDE (default Y)")

    # Enrichment options
    ap.add_argument("--currency", default="USD", help="EQY_FUND_CRNCY for RELATIONSHIP_AMOUNT (default USD)")
    ap.add_argument("--sleep-ms", type=int, default=50, help="Delay between BDP calls (ms)")

    args = ap.parse_args()
    tickers = _resolve_tickers(args.tickers, args.tickers_file)

    total_df = process_customers_to_excel(
        tickers=tickers,
        out_xlsx=args.out_xlsx,
        host=args.host,
        port=args.port,
        sum_count_override=args.sum_count_override,
        quantified_override=args.quantified_override,
        currency=args.currency,
        sleep_ms=args.sleep_ms,
    )
    print(f"Done. Rows written: {len(total_df)}  ->  {args.out_xlsx}")

if __name__ == "__main__":
    main()
