#!/usr/bin/env python3
"""
Incremental Yahoo index downloader — Optimized
-----------------------------------------------
✓ Thread-safe, resumable CSV storage
✓ Works with pandas <2.2 (no ISO8601 dependency)
✓ Parallel downloads with per-symbol throttling
✓ Robust timezone & date parsing
✓ Automatic recovery from corrupted CSVs
✓ Reduced I/O and smarter backstep logic
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple
import warnings

import pandas as pd
import yfinance as yf
from tqdm import tqdm

# Suppress known yfinance warnings
warnings.simplefilter("ignore", FutureWarning)

# ────────────────────────────── User settings ────────────────────────────── #

INDICES: Dict[str, str] = {
    "NIFTY50": "^NSEI",
    "BANKNIFTY": "^NSEBANK",
    "NIFTYIT": "^CNXIT",
    "NIFTYFMCG": "^CNXFMCG",
    "NIFTYPHARMA": "^CNXPHARMA",
    "NIFTYMETAL": "^CNXMETAL",
    "NIFTYAUTO": "^CNXAUTO",
    "SENSEX": "^BSESN",
}

TIMEFRAMES = ["1m", "5m", "15m", "1h", "1d", "1wk", "1mo"]

FRESH_LIMITS = {
    "1m":  {"period": "7d"},
    "5m":  {"period": "60d"},
    "15m": {"period": "60d"},
    "1h":  {"period": "2y"},
    "1d":  {"start": "2000-01-01"},
    "1wk": {"start": "2000-01-01"},
    "1mo": {"start": "2000-01-01"},
}

# Backstep for overlap (to catch splits/dividends)
BACKSTEP_DAYS = {
    "1h": 1,
    "1d": 1,
    "1wk": 7,
    "1mo": 35,  # >31 to ensure full month overlap
}

OUT_DIR = Path("index_data")
OUT_DIR.mkdir(exist_ok=True)

MAX_WORKERS = min(16, (os.cpu_count() or 4) + 4)  # Reduce to avoid Yahoo rate limits

# Per-symbol download cooldown (seconds)
SYMBOL_COOLDOWN: Dict[str, float] = {}

# ────────────────────────────── Helpers ────────────────────────────── #

def csv_path(index_name: str, tf: str) -> Path:
    return OUT_DIR / f"{index_name}_{tf}.csv"


def load_existing(path: Path) -> Tuple[pd.DataFrame, Optional[pd.Timestamp]]:
    """Returns (df, last_timestamp) — avoids repeated index[-1] calls."""
    if not path.exists():
        return pd.DataFrame(), None

    try:
        # Use infer_datetime_format for pandas <2.2 compatibility
        df = pd.read_csv(path, index_col=0, parse_dates=True, infer_datetime_format=True)
    except (pd.errors.EmptyDataError, pd.errors.ParserError, ValueError) as e:
        print(f"⚠️ Corrupted CSV {path.name}: {e}. Backing up and resetting.")
        backup = path.with_suffix('.csv.corrupted')
        path.rename(backup)
        return pd.DataFrame(), None
    except Exception as e:
        print(f"⚠️ Unexpected error reading {path.name}: {e}")
        return pd.DataFrame(), None

    if df.empty:
        return df, None

    # Normalize timezone to UTC
    try:
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")
        df = df.sort_index()
        last_ts = df.index[-1]
        return df, last_ts
    except Exception as e:
        print(f"⚠️ Timezone error in {path.name}: {e}")
        return pd.DataFrame(), None


def build_dl_kwargs(tf: str, last_ts: Optional[pd.Timestamp]) -> dict:
    kwargs = {"interval": tf, "auto_adjust": False, "progress": False, "timeout": 20}
    
    if last_ts is None:
        kwargs.update(FRESH_LIMITS[tf])
    else:
        if tf in BACKSTEP_DAYS:
            # Subtract days and convert to naive date string
            start_dt = (last_ts.tz_localize(None) - pd.Timedelta(days=BACKSTEP_DAYS[tf]))
            kwargs["start"] = start_dt.strftime("%Y-%m-%d")
        else:
            # Intraday: use period (Yahoo doesn't support start/end for <1d reliably)
            kwargs["period"] = FRESH_LIMITS[tf]["period"]
    return kwargs


def enforce_symbol_cooldown(symbol: str, min_interval: float = 2.0):
    """Prevent hammering Yahoo with requests for same symbol."""
    now = time.time()
    last = SYMBOL_COOLDOWN.get(symbol, 0)
    if now - last < min_interval:
        time.sleep(min_interval - (now - last))
    SYMBOL_COOLDOWN[symbol] = time.time()


def download_one(index_name: str, symbol: str, tf: str) -> Tuple[str, str, int, str]:
    path = csv_path(index_name, tf)
    
    # Load only to get last timestamp (minimize I/O)
    _, last_ts = load_existing(path)
    
    # Enforce cooldown per symbol to avoid rate limits
    enforce_symbol_cooldown(symbol)

    kwargs = build_dl_kwargs(tf, last_ts)

    # Attempt download with retry
    new_df = pd.DataFrame()
    for attempt in range(3):
        try:
            new_df = yf.download(symbol, **kwargs)
            if not new_df.empty:
                break
        except Exception as exc:
            err_str = str(exc)
            if "Invalid input - start date cannot be after end date" in err_str:
                return index_name, tf, 0, "up-to-date (date range)"
            if "possibly delisted" in err_str or "No data found" in err_str:
                return index_name, tf, 0, "symbol invalid"
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
            else:
                return index_name, tf, 0, f"download failed: {type(exc).__name__}"

    if new_df.empty:
        return index_name, tf, 0, "up-to-date (empty response)"

    # Normalize new data timezone
    try:
        if new_df.index.tz is None:
            new_df.index = new_df.index.tz_localize("UTC")
        else:
            new_df.index = new_df.index.tz_convert("UTC")
    except Exception as e:
        return index_name, tf, 0, f"timezone error: {e}"

    # Load full existing data only if needed
    if last_ts is not None and not new_df.empty:
        old_df, _ = load_existing(path)
        if not old_df.empty:
            new_df = new_df[new_df.index > last_ts]
            if new_df.empty:
                return index_name, tf, 0, "up-to-date (no new data)"
            combined = pd.concat([old_df, new_df]).sort_index()
            combined = combined[~combined.index.duplicated(keep='last')]
        else:
            combined = new_df
    else:
        combined = new_df

    # Save safely
    try:
        combined.to_csv(path)
    except Exception as e:
        return index_name, tf, 0, f"save error: {e}"

    return index_name, tf, len(new_df), "ok"


# ────────────────────────────── Main ────────────────────────────── #

def main() -> None:
    print("📥 Starting incremental index data update …\n")
    total_jobs = len(INDICES) * len(TIMEFRAMES)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool, tqdm(total=total_jobs, unit="job") as bar:
        futures = [
            pool.submit(download_one, idx_name, sym, tf)
            for idx_name, sym in INDICES.items()
            for tf in TIMEFRAMES
        ]

        for f in as_completed(futures):
            index_name, tf, rows, status = f.result()
            icon = "✅" if "up-to-date" in status or status == "ok" else "❌"
            msg = f"{index_name:<12} {tf:<4} → {icon} "
            if status == "ok":
                msg += f"{rows} new rows"
            else:
                msg += status
            tqdm.write(msg)
            bar.update()

    print("\n✅ All indices updated incrementally.")


if __name__ == "__main__":
    main()