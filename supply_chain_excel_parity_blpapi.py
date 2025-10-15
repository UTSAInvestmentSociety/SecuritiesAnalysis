#!/usr/bin/env python3
"""
supply_chain_excel_parity_blpapi.py

Replicates these Excel formulas via native Bloomberg API (blpapi):

Suppliers sheet
  =BDS("TICKER","SUPPLY_CHAIN_SUPPLIERS",
       "SUPPLY_CHAIN_SUM_COUNT_OVERRIDE=20,QUANTIFIED_OVERRIDE=Y,SUP_CHAIN_RELATIONSHIP_SORT_OVR=C")
  =@IF(ISBLANK(A5),"",BDP("TICKER","RELATIONSHIP_AMOUNT",
       "RELATIONSHIP_OVERRIDE=S,QUANTIFIED_OVERRIDE=Y,EQY_FUND_CRNCY=USD,RELATED_COMPANY_OVERRIDE="&A5))

Customers sheet
  =BDS("TICKER","SUPPLY_CHAIN_CUSTOMERS",
       "SUPPLY_CHAIN_SUM_COUNT_OVERRIDE=20,QUANTIFIED_OVERRIDE=Y")
  =@IF(ISBLANK(A5),"",BDP("TICKER","RELATIONSHIP_AMOUNT",
       "RELATIONSHIP_OVERRIDE=C,QUANTIFIED_OVERRIDE=Y,EQY_FUND_CRNCY=USD,RELATED_COMPANY_OVERRIDE="&A5))

Usage (Desktop API):
  python supply_chain_excel_parity_blpapi.py --tickers "NVDA US Equity,MSFT US Equity" --print

Usage (Server API):
  python supply_chain_excel_parity_blpapi.py --host your.server --port 8194 --tickers-file tickers.txt

Outputs:
  customers.csv  (ticker, customer_name, pct, amount_usd, asof)
  suppliers.csv  (ticker, supplier_name, pct, amount_usd, asof)
"""

import argparse
import math
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import blpapi

# -------- Field mnemonics --------
FIELD_SUPPLIERS_BDS = "SUPPLY_CHAIN_SUPPLIERS"
FIELD_CUSTOMERS_BDS = "SUPPLY_CHAIN_CUSTOMERS"
FIELD_REL_AMOUNT_BDP = "RELATIONSHIP_AMOUNT"

# -------- Default overrides (match your Excel) --------
OVR_SUM_COUNT = "20"     # SUPPLY_CHAIN_SUM_COUNT_OVERRIDE
OVR_QUANTIFIED = "Y"     # QUANTIFIED_OVERRIDE
OVR_SUP_SORT = "C"       # SUP_CHAIN_RELATIONSHIP_SORT_OVR (for suppliers page)
OVR_CRNCY = "USD"        # EQY_FUND_CRNCY

# -------- Heuristics to parse bulk rows --------
NAME_KEYS = [
    "counterparty_name", "name", "rel_name", "rel_name_long",
    "company_name", "supplier_name", "customer_name"
]
PCT_KEYS = [
    "rel_pct_rev", "rel_pct_cost", "pct_of_revenue", "pct_of_cost",
    "relationship_percent", "pct", "pct_rev", "pct_cost", "percent"
]
ASOF_KEYS = ["asof", "as_of_date", "effective_date", "period_end_date", "date"]

# -------- Helpers --------
def _first_str(d: Dict[str, Any], candidates: Iterable[str]) -> Optional[str]:
    for k in candidates:
        if k in d and isinstance(d[k], str) and d[k].strip():
            return d[k].strip()
    for k, v in d.items():
        if "name" in k.lower() and isinstance(v, str) and v.strip():
            return v.strip()
    return None

def _first_pct(d: Dict[str, Any], candidates: Iterable[str]) -> Optional[float]:
    def to_float(x: Any) -> Optional[float]:
        if x is None:
            return None
        if isinstance(x, (int, float)) and not (isinstance(x, float) and math.isnan(x)):
            return float(x)
        if isinstance(x, str):
            s = x.strip().replace("%", "")
            try:
                return float(s)
            except Exception:
                return None
        return None

    for k in candidates:
        if k in d:
            f = to_float(d[k])
            if f is not None:
                return f
    for k, v in d.items():
        lk = k.lower()
        if "pct" in lk or "percent" in lk:
            f = to_float(v)
            if f is not None:
                return f
    return None

def _first_asof(d: Dict[str, Any], candidates: Iterable[str]) -> Optional[str]:
    for k in candidates:
        if k in d and d[k]:
            return str(d[k])
    for k, v in d.items():
        if "date" in k.lower() and v:
            return str(v)
    return None

def _open_session(host: Optional[str], port: Optional[int]) -> blpapi.Session:
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

def _element_to_python(elem: blpapi.Element) -> Any:
    if elem.isArray():
        return [_element_to_python(elem.getValueAsElement(i)) for i in range(elem.numValues())]
    if elem.isComplexType():
        out = {}
        for i in range(elem.numElements()):
            sub = elem.getElement(i)
            out[str(sub.name())] = _element_to_python(sub)
        return out
    try:
        return elem.getValue()
    except Exception:
        return None

def _bds(session: blpapi.Session, ticker: str, field: str, overrides: Dict[str, str]) -> List[Dict[str, Any]]:
    svc = session.getService("//blp/refdata")
    req = svc.createRequest("ReferenceDataRequest")
    req.getElement("securities").appendValue(ticker)
    req.getElement("fields").appendValue(field)

    if overrides:
        ovrds = req.getElement("overrides")
        for k, v in overrides.items():
            o = ovrds.appendElement()
            o.setElement("fieldId", k)
            o.setElement("value", v)

    session.sendRequest(req)

    rows: List[Dict[str, Any]] = []
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
                    if not fdata.hasElement(field): continue
                    bulk = fdata.getElement(field)
                    py = _element_to_python(bulk)
                    if isinstance(py, list):
                        rows.extend([it if isinstance(it, dict) else {"value": it} for it in py])
                    elif isinstance(py, dict):
                        rows.append(py)
        if ev.eventType() == blpapi.Event.RESPONSE:
            break
    return rows

def _bdp_rel_amount(session: blpapi.Session,
                    ticker: str,
                    related_name: str,
                    rel_override: str,  # "S" or "C"
                    currency: str,
                    quantified: str) -> Optional[float]:
    svc = session.getService("//blp/refdata")
    req = svc.createRequest("ReferenceDataRequest")
    req.getElement("securities").appendValue(ticker)
    req.getElement("fields").appendValue(FIELD_REL_AMOUNT_BDP)

    ovr = req.getElement("overrides")
    for k, v in [
        ("RELATIONSHIP_OVERRIDE", rel_override),
        ("QUANTIFIED_OVERRIDE", quantified),
        ("EQY_FUND_CRNCY", currency),
        ("RELATED_COMPANY_OVERRIDE", related_name),
    ]:
        e = ovr.appendElement()
        e.setElement("fieldId", k)
        e.setElement("value", v)

    session.sendRequest(req)

    val: Optional[float] = None
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
                    if fdata.hasElement(FIELD_REL_AMOUNT_BDP):
                        try:
                            val = fdata.getElement(FIELD_REL_AMOUNT_BDP).getValueAsFloat()
                        except Exception:
                            val = None
        if ev.eventType() == blpapi.Event.RESPONSE:
            break
    return val

def _normalize(role: str, ticker: str, raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for row in raw_rows:
        lower = {k.lower(): v for k, v in row.items()}
        name = _first_str(lower, NAME_KEYS)
        pct  = _first_pct(lower, PCT_KEYS)
        asof = _first_asof(lower, ASOF_KEYS)
        out.append({
            "ticker": ticker,
            f"{role}_name": name,
            "relationship_size_pct": pct,
            "asof": asof,
        })
    return out

def fetch_supply_chain(
    tickers: List[str],
    host: Optional[str] = None,
    port: Optional[int] = None,
    sum_count: str = OVR_SUM_COUNT,
    quantified: str = OVR_QUANTIFIED,
    supplier_sort: str = OVR_SUP_SORT,
    currency: str = OVR_CRNCY,
    sleep_ms_between_amounts: int = 50,  # gentle pacing for RELATIONSHIP_AMOUNT calls
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    session = _open_session(host, port)
    try:
        cust_all: List[Dict[str, Any]] = []
        supp_all: List[Dict[str, Any]] = []

        for tkr in tickers:
            # BDS Customers
            cust_rows = _bds(session, tkr, FIELD_CUSTOMERS_BDS, {
                "SUPPLY_CHAIN_SUM_COUNT_OVERRIDE": sum_count,
                "QUANTIFIED_OVERRIDE": quantified,
            })
            cust_norm = _normalize("customer", tkr, cust_rows)

            # BDS Suppliers
            supp_rows = _bds(session, tkr, FIELD_SUPPLIERS_BDS, {
                "SUPPLY_CHAIN_SUM_COUNT_OVERRIDE": sum_count,
                "QUANTIFIED_OVERRIDE": quantified,
                "SUP_CHAIN_RELATIONSHIP_SORT_OVR": supplier_sort,
            })
            supp_norm = _normalize("supplier", tkr, supp_rows)

            # Per-related-company RELATIONSHIP_AMOUNT (USD) via BDP
            for r in cust_norm:
                nm = r.get("customer_name")
                amt = _bdp_rel_amount(session, tkr, nm, "C", currency, quantified) if nm else None
                r["relationship_amount_usd"] = amt
                cust_all.append(r)
                if sleep_ms_between_amounts:
                    time.sleep(sleep_ms_between_amounts / 1000.0)

            for r in supp_norm:
                nm = r.get("supplier_name")
                amt = _bdp_rel_amount(session, tkr, nm, "S", currency, quantified) if nm else None
                r["relationship_amount_usd"] = amt
                supp_all.append(r)
                if sleep_ms_between_amounts:
                    time.sleep(sleep_ms_between_amounts / 1000.0)

        customers_df = pd.DataFrame(cust_all, columns=[
            "ticker", "customer_name", "relationship_size_pct", "relationship_amount_usd", "asof"
        ]).drop_duplicates()

        suppliers_df = pd.DataFrame(supp_all, columns=[
            "ticker", "supplier_name", "relationship_size_pct", "relationship_amount_usd", "asof"
        ]).drop_duplicates()

        return customers_df, suppliers_df
    finally:
        session.stop()

# -------- CLI --------
def _resolve_tickers(cli: Optional[str], file: Optional[str]) -> List[str]:
    if file:
        with open(file, "r", encoding="utf-8") as f:
            return [ln.strip() for ln in f if ln.strip()]
    if cli:
        return [t.strip() for t in cli.split(",") if t.strip()]
    raise SystemExit("No tickers provided. Use --tickers or --tickers-file.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", help='Comma-separated tickers, e.g. "NVDA US Equity,MSFT US Equity"')
    ap.add_argument("--tickers-file", help="Path to newline-delimited tickers")
    ap.add_argument("--host", help="Server API host (omit for Desktop API)")
    ap.add_argument("--port", type=int, help="Server API port (omit for Desktop API)")
    ap.add_argument("--sum-count", default=OVR_SUM_COUNT, help="SUPPLY_CHAIN_SUM_COUNT_OVERRIDE (default 20)")
    ap.add_argument("--quantified", default=OVR_QUANTIFIED, help="QUANTIFIED_OVERRIDE (default Y)")
    ap.add_argument("--supplier-sort", default=OVR_SUP_SORT, help="SUP_CHAIN_RELATIONSHIP_SORT_OVR (default C)")
    ap.add_argument("--currency", default=OVR_CRNCY, help="EQY_FUND_CRNCY for RELATIONSHIP_AMOUNT (default USD)")
    ap.add_argument("--sleep-ms", type=int, default=50, help="Delay between BDP calls (ms)")
    ap.add_argument("--out-customers", default="customers.csv", help="CSV path for customers")
    ap.add_argument("--out-suppliers", default="suppliers.csv", help="CSV path for suppliers")
    ap.add_argument("--print", action="store_true", help="Print head(10) samples")
    args = ap.parse_args()

    tickers = _resolve_tickers(args.tickers, args.tickers_file)
    customers_df, suppliers_df = fetch_supply_chain(
        tickers,
        host=args.host,
        port=args.port,
        sum_count=args.sum_count,
        quantified=args.quantified,
        supplier_sort=args.supplier_sort,
        currency=args.currency,
        sleep_ms_between_amounts=args.sleep_ms,
    )

    customers_df.to_csv(args.out_customers, index=False)
    suppliers_df.to_csv(args.out_suppliers, index=False)

    if args.print:
        print("\n== Customers (sample) ==")
        print(customers_df.head(10).to_string(index=False))
        print("\n== Suppliers (sample) ==")
        print(suppliers_df.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
