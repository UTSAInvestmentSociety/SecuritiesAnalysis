#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compare GSOX and RGUSTSC vs S&P 500 and MSCI World using Bloomberg blpapi.

Outputs:
- charts/*.png (rebase, excess returns, correlation/beta, drawdowns)
- data/combined.csv (aligned daily panel)

Requirements:
    pip install blpapi pandas numpy matplotlib

Bloomberg:
    Ensure Desktop API or Server API connectivity is available on this machine.
"""

import os
import sys
import datetime as dt
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import blpapi

# ----------------------------- User settings -----------------------------

TICKERS = {
    "GSOX":    "GSOX Index",     # Nasdaq Global Semiconductor Index
    "RGUSTSC": "RGUSTSC Index",  # Change if your mnemonic differs
    "SPX":     "SPX Index",      # S&P 500 (Price)
    "MXWO":    "MXWO Index",     # MSCI World (Price)
}

START_DATE = "2015-01-01"   # change as needed
END_DATE   = dt.date.today().strftime("%Y-%m-%d")

# Use total return when available; falls back to PX_LAST if TR field missing.
USE_TOTAL_RETURN = False

# Rolling windows (in trading days ~ 21 per month)
ROLL_RET_WIN = 63     # ~3 months
ROLL_RISK_WIN = 126   # ~6 months

# Output folders
OUT_DIR_DATA = "data"
OUT_DIR_CHARTS = "charts"

# ---------------------------- Bloomberg utils ----------------------------

def start_session(host: str = "localhost", port: int = 8194) -> blpapi.Session:
    opts = blpapi.SessionOptions()
    opts.setServerHost(host)
    opts.setServerPort(port)
    sess = blpapi.Session(opts)
    if not sess.start():
        raise RuntimeError("Failed to start Bloomberg session.")
    if not sess.openService("//blp/refdata"):
        raise RuntimeError("Failed to open //blp/refdata.")
    return sess

def _fmt_date(date_str: str) -> str:
    return pd.to_datetime(date_str).strftime("%Y%m%d")

def historical_request(
    sess: blpapi.Session,
    tickers: List[str],
    fields: List[str],
    start_date: str,
    end_date: str,
    periodicity: str = "DAILY",
) -> Dict[str, pd.DataFrame]:
    svc = sess.getService("//blp/refdata")
    req = svc.createRequest("HistoricalDataRequest")

    el_secs = req.getElement("securities")
    for t in tickers:
        el_secs.appendValue(t)

    el_fields = req.getElement("fields")
    for f in fields:
        el_fields.appendValue(f)

    req.set("periodicityAdjustment", "CALENDAR")
    req.set("periodicitySelection", periodicity)
    req.set("startDate", _fmt_date(start_date))
    req.set("endDate", _fmt_date(end_date))
    req.set("adjustmentSplit", True)
    req.set("adjustmentAbnormal", True)
    req.set("adjustmentNormal", True)
    req.set("maxDataPoints", 1000000)

    cid = sess.sendRequest(req)
    out: Dict[str, pd.DataFrame] = {}

    while True:
        ev = sess.nextEvent()
        et = ev.eventType()

        # Process both partial and final responses
        if et in (blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE):
            for msg in ev:
                if msg.messageType() == blpapi.Name("HistoricalDataResponse"):
                    sec_data = msg.getElement("securityData")
                    sec_name = sec_data.getElementAsString("security")
                    fld_data = sec_data.getElement("fieldData")

                    rows = []
                    for i in range(fld_data.numValues()):
                        row = fld_data.getValueAsElement(i)
                        dt_val = row.getElementAsDatetime("date")
                        rec = {"date": pd.to_datetime(dt_val)}
                        for f in fields:
                            rec[f] = row.getElement(f).getValue() if row.hasElement(f) and not row.getElement(f).isNull() else np.nan
                        rows.append(rec)

                    df = pd.DataFrame(rows).set_index("date").sort_index()
                    out[sec_name] = df

            if et == blpapi.Event.RESPONSE:
                break

        # (Optional) handle other event types if desired

    return out


# ---------------------------- Analytics utils ----------------------------

def first_non_nan(series: pd.Series) -> float:
    for v in series.values:
        if pd.notna(v):
            return v
    return np.nan

def drawdown(levels: pd.Series) -> pd.Series:
    roll_max = levels.cummax()
    return (levels / roll_max) - 1.0

def rolling_beta(y_ret: pd.Series, x_ret: pd.Series, win: int) -> pd.Series:
    cov = y_ret.rolling(win).cov(x_ret)
    var = x_ret.rolling(win).var()
    return cov / var

def rolling_period_return(returns: pd.Series, win: int) -> pd.Series:
    # Product of (1+r) over window minus 1
    return (1.0 + returns).rolling(win).apply(np.prod, raw=True) - 1.0

def compute_pairwise_metrics(
    rets: pd.DataFrame, asset: str, bench_a: str, bench_b: str
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns (excess_df, corr_df, beta_df) for asset vs two benchmarks.
    - excess_df columns: [f"{asset}-vs-{bench_a}", f"{asset}-vs-{bench_b}"]
    - corr_df   columns: [f"Corr({asset},{bench_a})", f"Corr({asset},{bench_b})"]
    - beta_df   columns: [f"Beta({asset},{bench_a})", f"Beta({asset},{bench_b})"]
    """
    # Rolling period returns (e.g., 3-month)
    r_asset = rolling_period_return(rets[asset], ROLL_RET_WIN)
    r_a = rolling_period_return(rets[bench_a], ROLL_RET_WIN)
    r_b = rolling_period_return(rets[bench_b], ROLL_RET_WIN)

    excess_df = pd.DataFrame({
        f"{asset}-vs-{bench_a}": r_asset - r_a,
        f"{asset}-vs-{bench_b}": r_asset - r_b
    }).dropna(how="all")

    corr_df = pd.DataFrame({
        f"Corr({asset},{bench_a})": rets[asset].rolling(ROLL_RISK_WIN).corr(rets[bench_a]),
        f"Corr({asset},{bench_b})": rets[asset].rolling(ROLL_RISK_WIN).corr(rets[bench_b]),
    }).dropna(how="all")

    beta_df = pd.DataFrame({
        f"Beta({asset},{bench_a})": rolling_beta(rets[asset], rets[bench_a], ROLL_RISK_WIN),
        f"Beta({asset},{bench_b})": rolling_beta(rets[asset], rets[bench_b], ROLL_RISK_WIN),
    }).dropna(how="all")

    return excess_df, corr_df, beta_df


# ---------------------------- Main pipeline ------------------------------

def main():
    os.makedirs(OUT_DIR_DATA, exist_ok=True)
    os.makedirs(OUT_DIR_CHARTS, exist_ok=True)

    # Field logic
    base_field = "PX_LAST"
    tr_field = "TOT_RETURN_INDEX_NET_DVDS"  # generic total-return field
    fields = [tr_field] if USE_TOTAL_RETURN else [base_field]

    # 1) Connect & fetch
    sess = start_session()
    bb_tickers = [TICKERS["GSOX"], TICKERS["RGUSTSC"], TICKERS["SPX"], TICKERS["MXWO"]]

    # Try TR first (if selected), then gracefully fall back per-ticker if missing
    raw = historical_request(sess, bb_tickers, fields, START_DATE, END_DATE, "DAILY")

    # If TR was requested, check for empties and re-fetch PX_LAST as fallback
    if USE_TOTAL_RETURN:
        need_fallback = [t for t in bb_tickers if (t not in raw) or raw[t].dropna(how="all").empty]
        if need_fallback:
            fb = historical_request(sess, need_fallback, [base_field], START_DATE, END_DATE, "DAILY")
            for t in need_fallback:
                raw[t] = fb.get(t)

    sess.stop()

    # Build tidy frame
    panel = []
    name_map = {
        TICKERS["GSOX"]: "GSOX",
        TICKERS["RGUSTSC"]: "RGUSTSC",
        TICKERS["SPX"]: "SPX",
        TICKERS["MXWO"]: "MXWO",
    }

    for bbt, short in name_map.items():
        if bbt not in raw or raw[bbt] is None or raw[bbt].empty:
            print(f"Warning: No data returned for {bbt}. Skipping.", file=sys.stderr)
            continue
        df = raw[bbt].copy()
        # prefer TR field if present, else PX_LAST
        val_col = tr_field if (USE_TOTAL_RETURN and tr_field in df.columns) else base_field
        if val_col not in df.columns:
            # if only one field exists, use it
            val_col = df.columns[0]
        df = df[[val_col]].rename(columns={val_col: short})
        panel.append(df)

    if not panel:
        raise RuntimeError("No data available for any ticker.")

    data = pd.concat(panel, axis=1).sort_index()
    data = data[~data.index.duplicated(keep="first")]

    # Forward/backfill small gaps (holiday mismatches) then align to common dates
    data = data.ffill().bfill()
    data = data.dropna(how="any")

    # Save raw aligned panel
    data.to_csv(os.path.join(OUT_DIR_DATA, "combined.csv"), index=True)

    # 2) Build analytics
    # Rebased indices (100 at start)
    levels_rebased = data.apply(lambda s: (s / s.iloc[0]) * 100.0)

    # Daily returns
    rets = data.pct_change().dropna()

    # Pairwise metrics for both assets vs both benchmarks
    ex_gsox, corr_gsox, beta_gsox = compute_pairwise_metrics(rets, "GSOX", "SPX", "MXWO")
    ex_rgus, corr_rgus, beta_rgus = compute_pairwise_metrics(rets, "RGUSTSC", "SPX", "MXWO")

    # Drawdowns from peak (rebased levels)
    dd = pd.DataFrame({
        "GSOX":    (levels_rebased["GSOX"]).pipe(drawdown),
        "RGUSTSC": (levels_rebased["RGUSTSC"]).pipe(drawdown),
        "SPX":     (levels_rebased["SPX"]).pipe(drawdown),
        "MXWO":    (levels_rebased["MXWO"]).pipe(drawdown),
    })

    # 3) Plotting
    # 3.1 Rebased performance
    plt.figure(figsize=(11, 6))
    levels_rebased[["GSOX", "RGUSTSC", "SPX", "MXWO"]].plot(ax=plt.gca())
    plt.title(f"Rebased Performance (100 = {levels_rebased.index[0].date()})")
    plt.ylabel("Index Level (Rebased)")
    plt.xlabel("")
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR_CHARTS, "01_rebased_performance.png"), dpi=160)
    plt.close()

    # 3.2 Rolling excess returns (both assets vs both benchmarks)
    ex_all = pd.concat([ex_gsox, ex_rgus], axis=1).dropna(how="all")
    plt.figure(figsize=(11, 5))
    ex_all.rename(columns={
        f"GSOX-vs-SPX": "GSOX - SPX",
        f"GSOX-vs-MXWO": "GSOX - MXWO",
        f"RGUSTSC-vs-SPX": "RGUSTSC - SPX",
        f"RGUSTSC-vs-MXWO": "RGUSTSC - MXWO",
    }).plot(ax=plt.gca())
    plt.title(f"Rolling {ROLL_RET_WIN}-Day Excess Return (Asset minus Benchmark)")
    plt.ylabel("Excess Return (period)")
    plt.xlabel("")
    plt.axhline(0, linewidth=1)
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR_CHARTS, "02_rolling_excess_return.png"), dpi=160)
    plt.close()

    # 3.3 Rolling correlations (both assets vs both benchmarks)
    corr_all = pd.concat([corr_gsox, corr_rgus], axis=1).dropna(how="all")
    plt.figure(figsize=(11, 5))
    corr_all.plot(ax=plt.gca())
    plt.title(f"Rolling {ROLL_RISK_WIN}-Day Correlation")
    plt.ylabel("Correlation")
    plt.xlabel("")
    plt.axhline(0, linewidth=1)
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR_CHARTS, "03_rolling_correlation.png"), dpi=160)
    plt.close()

    # 3.4 Rolling betas (both assets vs both benchmarks)
    beta_all = pd.concat([beta_gsox, beta_rgus], axis=1).dropna(how="all")
    plt.figure(figsize=(11, 5))
    beta_all.plot(ax=plt.gca())
    plt.title(f"Rolling {ROLL_RISK_WIN}-Day Beta")
    plt.ylabel("Beta")
    plt.xlabel("")
    plt.axhline(1.0, linewidth=1)
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR_CHARTS, "04_rolling_beta.png"), dpi=160)
    plt.close()

    # 3.5 Drawdowns for all four
    plt.figure(figsize=(11, 6))
    dd[["GSOX", "RGUSTSC", "SPX", "MXWO"]].plot(ax=plt.gca())
    plt.title("Drawdowns from Peak")
    plt.ylabel("Drawdown")
    plt.xlabel("")
    plt.axhline(0, linewidth=1)
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR_CHARTS, "05_drawdowns.png"), dpi=160)
    plt.close()

    print("Done. Outputs:")
    print(f"- {os.path.join(OUT_DIR_DATA, 'combined.csv')}")
    print(f"- {OUT_DIR_CHARTS}/01_rebased_performance.png")
    print(f"- {OUT_DIR_CHARTS}/02_rolling_excess_return.png")
    print(f"- {OUT_DIR_CHARTS}/03_rolling_correlation.png")
    print(f"- {OUT_DIR_CHARTS}/04_rolling_beta.png")
    print(f"- {OUT_DIR_CHARTS}/05_drawdowns.png")

if __name__ == "__main__":
    main()
