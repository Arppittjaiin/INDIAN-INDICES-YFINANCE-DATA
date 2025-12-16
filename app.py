# optimized_indices_dashboard.py
"""
Optimized single-file version of the user's data pipeline (Terminal Version).
Preserves all original behavior and outputs while improving:
 - performance (vectorized pandas ops, caching),
 - API usage (retries, backoff, batched downloads, reuse),
 - memory usage (parquet cache),
 - structure & maintainability (clear functions, docstrings),
 - User experience (clear terminal output).
"""

import os
import time
import math
from datetime import datetime, timedelta
from zipfile import ZipFile
from pathlib import Path
from typing import List, Tuple, Dict, Any
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf

# -------------------------
# Configuration & constants
# -------------------------
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
MAX_RETRIES = 3
BASE_RETRY_DELAY = 3  # seconds; exponential backoff multiplier applied
PARQUET_CACHE = True  # prefer parquet for speed and storage efficiency
ZIP_NAME = "indices_data.zip"
LOG_FORMAT = "%(asctime)s %(levelname)s: %(message)s"
# Tune thread pool size conservatively to avoid hitting yfinance/remote rate limits.
# Lower number => safer. Increase if you know API limits.
MAX_THREADS = min(8, (os.cpu_count() or 1) + 2)

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("indices_dashboard")

# Indices with primary + fallback tickers
INDICES = {
    "Nifty 50": ["^NSEI", "NIFTYBEES.NS"],
    "Sensex": ["^BSESN", "SENSEX.BO"],
    "BankNifty": ["^NSEBANK", "BANKBEES.NS"],
}

# Supported timeframes (display --> yfinance interval code)
VALID_TIMEFRAMES = {
    "1m": "1m",
    "2m": "2m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "60m",
    "1d": "1d",
    "1wk": "1wk",
    "1mo": "1mo",
}

# -------------------------
# Utility helpers
# -------------------------
def safe_path(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return path

def _exponential_backoff_delay(attempt: int) -> float:
    # attempt starts at 1
    return BASE_RETRY_DELAY * (2 ** (attempt - 1))

def _to_parquet_path(csv_path: Path) -> Path:
    return csv_path.with_suffix(".parquet")

def _read_csv_quick(path: Path, nrows: int = 5) -> pd.DataFrame:
    """
    Fast partial read to detect corruption or bad datetimes.
    """
    return pd.read_csv(path, nrows=nrows)

# -------------------------
# Startup cleanup (optimized)
# -------------------------
def purge_invalid_csvs(data_dir: Path = DATA_DIR) -> None:
    """
    Remove CSV files that are corrupted or contain obviously invalid datetimes
    (year < 1980 or Unix epoch 1970). We do a small partial read for speed.
    """
    if not data_dir.exists():
        return

    for csv_path in data_dir.rglob("*.csv"):
        try:
            df_sample = _read_csv_quick(csv_path, nrows=10)
            # fast heuristic: find any column name like datetime/date/timestamp
            dt_cols = [c for c in df_sample.columns if any(k in c.lower() for k in ("datetime", "date", "timestamp", "time"))]
            if dt_cols:
                # parse minimally, coerce errors
                parsed = pd.to_datetime(df_sample[dt_cols[0]], errors="coerce")
                # if any parsed year is invalid or NaT, remove file (original script removed corrupted files)
                if parsed.isna().any() or (parsed.dt.year < 1980).any() or (parsed.dt.year == 1970).any():
                    csv_path.unlink(missing_ok=True)
                    logger.info(f"🧹 Deleted invalid/corrupt CSV: {csv_path}")
            else:
                # If no datetime-like column found in sample, assume file is invalid
                csv_path.unlink(missing_ok=True)
                logger.info(f"🧹 Deleted CSV without datetime: {csv_path}")
        except Exception as exc:
            # remove file on any read/parsing error to match original script behavior
            csv_path.unlink(missing_ok=True)
            logger.info(f"🧹 Deleted corrupted file: {csv_path} ({exc})")


# Run purge at import/startup (keeps original behavior)
purge_invalid_csvs()

# -------------------------
# Data cleaning (vectorized & robust)
# -------------------------
def clean_ohlc_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Standardize OHLC dataframe:
    - ensures a 'datetime' column of timezone-naive IST-local timestamps
    - keeps open/high/low/close/volume if present
    - filters out rows with datetime < 1980
    Returns cleaned dataframe and info dict similar to original script.
    """
    info: Dict[str, Any] = {"rows": 0}
    if df is None:
        return pd.DataFrame(), {"rows": 0, "error": "None input"}

    if df.empty:
        return pd.DataFrame(), {"rows": 0, "error": "Empty input dataframe"}

    # Work on a copy
    df = df.copy()

    # If DatetimeIndex, convert to column (fast)
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index()

    # Drop duplicated columns quickly
    df = df.loc[:, ~df.columns.duplicated()]

    # Normalize column names
    df.columns = [str(c).lower().strip() for c in df.columns]

    # Detect datetime-like column
    possible_dt_cols = [c for c in df.columns if any(k in c for k in ("datetime", "date", "timestamp", "time"))]
    if not possible_dt_cols:
        # fallback: assume first column
        possible_dt_cols = [df.columns[0]]

    dt_col = possible_dt_cols[0]
    if dt_col != "datetime":
        df = df.rename(columns={dt_col: "datetime"})

    # Convert to datetime (vectorized)
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

    # Drop NaT rows
    df = df.dropna(subset=["datetime"])
    if df.empty:
        return pd.DataFrame(), {"rows": 0, "error": "All datetime values invalid"}

    # Filter out pre-1980 rows
    df = df[df["datetime"].dt.year >= 1980]
    if df.empty:
        return pd.DataFrame(), {"rows": 0, "error": "All rows before 1980"}

    # Handle timezone-aware datetimes: convert to IST then drop tz information
    # Vectorized tz handling: check dtype once
    try:
        # Check for timezone-aware dtype using proper pandas API to avoid deprecation warning
        if isinstance(df["datetime"].dtype, pd.DatetimeTZDtype):
            df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    except Exception as e:
        # Non-fatal; proceed assuming naive timestamps are in IST
        logger.debug(f"Timezone conversion warning: {e}")

    # Keep only relevant columns, preserve original order where possible
    keep_cols = ["datetime"]
    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            keep_cols.append(col)

    df = df[keep_cols].drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)

    info.update({
        "rows": len(df),
        "first_dt": df["datetime"].min(),
        "last_dt": df["datetime"].max(),
    })

    return df, info

# -------------------------
# Disk cache helpers
# -------------------------
def _load_existing_dataframe(file_path: Path) -> pd.DataFrame:
    """
    Load existing CSV/parquet, then attempt to clean and return DataFrame.
    Returns empty DataFrame on failure.
    """
    if not file_path.exists():
        return pd.DataFrame()
    try:
        if PARQUET_CACHE and file_path.with_suffix(".parquet").exists():
            df = pd.read_parquet(file_path.with_suffix(".parquet"))
        else:
            df = pd.read_csv(file_path, parse_dates=["datetime"], low_memory=True)
        df, _ = clean_ohlc_data(df)
        return df
    except Exception as exc:
        logger.warning(f"Failed to load existing file {file_path}: {exc}")
        # If file appears corrupt, remove it to mimic original script's cleanup behavior
        try:
            file_path.unlink(missing_ok=True)
            pq = file_path.with_suffix(".parquet")
            pq.unlink(missing_ok=True)
            logger.info(f"Removed corrupted cache files for {file_path}")
        except Exception:
            pass
        return pd.DataFrame()

def _save_dataframe(file_path: Path, df: pd.DataFrame) -> None:
    """
    Save DataFrame to CSV and parquet (if enabled). Uses atomic write to avoid partial files.
    """
    safe_path(file_path)
    tmp_csv = file_path.with_suffix(".tmp")
    try:
        df.to_csv(tmp_csv, index=False)
        tmp_csv.replace(file_path)
        if PARQUET_CACHE:
            pq_path = _to_parquet_path(file_path)
            tmp_pq = pq_path.with_suffix(".tmp")
            df.to_parquet(tmp_pq, index=False)
            tmp_pq.replace(pq_path)
    except Exception as exc:
        logger.error(f"Failed to save {file_path}: {exc}")
        # Attempt fallback: direct to_csv
        try:
            df.to_csv(file_path, index=False)
        except Exception as e2:
            logger.error(f"Fallback save also failed: {e2}")

# -------------------------
# yfinance download logic (optimized)
# -------------------------
def safe_download_yf(ticker: str, interval: str, start: str = None, period: str = None) -> pd.DataFrame:
    """
    Download with retries, backoff, and simple validity checks.
    Keeps behavior consistent with original safe_download but faster & safer.
    """
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Use yf.download with threads=False to reduce parallel HTTP usage (safer)
            df = yf.download(
                tickers=ticker,
                interval=interval,
                start=start,
                period=period,
                progress=False,
                auto_adjust=True,
                threads=False,
            )
            # yfinance returns DataFrame; empty -> return empty for caller to handle
            if isinstance(df, pd.DataFrame) and not df.empty:
                # Check index validity quickly
                idx = getattr(df, "index", None)
                if isinstance(idx, pd.DatetimeIndex) and idx.min().year >= 1980:
                    return df
                # else continue and retry if possible
                logger.debug(f"Invalid date range for {ticker} {interval}: min year {idx.min().year if idx is not None else 'N/A'}")
            else:
                logger.debug(f"Empty dataframe from yfinance for {ticker} {interval}")
        except Exception as exc:
            last_exc = exc
            logger.debug(f"yfinance attempt {attempt} for {ticker} {interval} failed: {exc}")
        # Backoff if not last attempt
        if attempt < MAX_RETRIES:
            sleep_t = _exponential_backoff_delay(attempt)
            time.sleep(sleep_t)
    if last_exc:
        logger.warning(f"yfinance final error for {ticker} {interval}: {last_exc}")
    return pd.DataFrame()

def try_tickers_download(ticker_list: List[str], interval: str, start: str = None, period: str = None) -> Tuple[str, pd.DataFrame]:
    """
    Try primary then fallback tickers. Return the ticker that succeeded plus DataFrame.
    """
    for tkr in ticker_list:
        df = safe_download_yf(tkr, interval, start=start, period=period)
        if not df.empty:
            logger.info(f"✅ Data fetched from {tkr} ({interval}) -> {len(df)} rows")
            return tkr, df
        else:
            logger.info(f"⚠️ No valid data for {tkr} ({interval}), trying fallback...")
    return "", pd.DataFrame()

# -------------------------
# High-level per-index download & save
# -------------------------
def download_index_data(index_name: str, tickers: List[str], interval: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Download, clean, merge with existing cached data, and save for given index+interval.
    Returns final dataframe and info dict (rows, first_dt, last_dt or error).
    """
    safe_index_name = index_name.lower().replace(" ", "_")
    index_dir = DATA_DIR / safe_index_name
    index_dir.mkdir(parents=True, exist_ok=True)
    file_path = index_dir / f"{safe_index_name}_{interval}.csv"

    # Load existing cleaned data (if any) and determine start_date for incremental update
    existing = _load_existing_dataframe(file_path)
    start_date = None
    if not existing.empty:
        # Continue from next day of last datetime (preserves original logic)
        start_dt = existing["datetime"].max()
        # Continue from the last available date to ensure partial days (intraday) are updated
        # and checking for any corrections in the last daily candle.
        start_date = start_dt.strftime("%Y-%m-%d")

        # Avoid requesting future data (yfinance errors out)
        if start_date > datetime.now().strftime("%Y-%m-%d"):
             logger.info(f"⏭️  Data up to date for {index_name} {interval} (next start {start_date} > today).")
             return existing, {"rows": 0, "first_dt": existing["datetime"].min(), "last_dt": existing["datetime"].max()}

        logger.info(f"📂 Existing data found for {index_name} {interval}, updating from {start_date}")

    # Choose period for initial download if no existing data
    if interval == "1m":
        default_period = "7d"
    elif interval.endswith("m") or interval.endswith("h") or interval.endswith("60m"):
        default_period = "60d"
    else:
        default_period = "max"

    # Try to fetch using tickers (primary -> fallback)
    ticker_used, df = try_tickers_download(tickers, interval, start=start_date, period=None if start_date else default_period)

    # If 1m failed with incremental attempt, retry explicitly with 7d period (original fallback)
    if df.empty and interval == "1m":
        logger.info("⚠️ Retrying 1m with 7d period for all tickers")
        ticker_used, df = try_tickers_download(tickers, interval, start=None, period="7d")

    if df.empty:
        error_msg = f"No data downloaded for {index_name} {interval}"
        logger.warning(error_msg)
        return existing, {"rows": 0, "error": error_msg}

    # yfinance sometimes returns multiindex columns when multiple tickers requested. Flatten if present.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Reset index to get 'datetime' column
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index()

    # Drop duplicated columns that may have arisen
    df = df.loc[:, ~df.columns.duplicated()]

    # Normalize column names
    df.columns = [str(c).lower().strip() for c in df.columns]

    # Rename 'date' -> 'datetime' if necessary
    if "date" in df.columns and "datetime" not in df.columns:
        df = df.rename(columns={"date": "datetime"})
    elif "datetime" not in df.columns:
        # fallback: take first column
        df = df.rename(columns={df.columns[0]: "datetime"})

    # Clean and standardize
    clean_df, info = clean_ohlc_data(df)
    if clean_df.empty:
        error_msg = info.get("error", "Unknown cleaning error")
        logger.warning(f"Cleaning failed for {index_name} {interval}: {error_msg}")
        return existing, {"rows": 0, "error": error_msg}

    # Merge with existing dataset while keeping original behavior (concatenate, drop duplicate datetimes)
    added_rows = 0
    if not existing.empty:
        final_df = pd.concat([existing, clean_df], axis=0, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
        added_rows = len(final_df) - len(existing)
    else:
        final_df = clean_df.copy()
        added_rows = len(final_df)

    # Save to CSV and parquet (atomic)
    _save_dataframe(file_path, final_df)

    logger.info(f"💾 Saved {len(final_df)} rows to {file_path} (+{added_rows} new)")
    info["added_rows"] = added_rows
    return final_df, info

# -------------------------
# Zip utility
# -------------------------
def zip_all_data(out_name: str = ZIP_NAME) -> Path:
    """Zip all CSVs under DATA_DIR into a zip file and return the path."""
    zip_path = DATA_DIR / out_name
    with ZipFile(zip_path, "w") as zipf:
        for csv_path in DATA_DIR.rglob("*.csv"):
            arcname = csv_path.relative_to(DATA_DIR)
            zipf.write(csv_path, arcname)
    return zip_path

# -------------------------
# Main Execution (Terminal)
# -------------------------
def main():
    logger.info("Starting bulk download...")
    total = len(INDICES) * len(VALID_TIMEFRAMES)
    count = 0
    summary = []
    
    with ThreadPoolExecutor(max_workers=min(MAX_THREADS, total)) as executor:
        future_to_meta = {}
        for idx_name, tickers in INDICES.items():
            for tf_display, tf_code in VALID_TIMEFRAMES.items():
                future = executor.submit(download_index_data, idx_name, tickers, tf_code)
                future_to_meta[future] = (idx_name, tf_display)
        
        for fut in as_completed(future_to_meta):
            idx_name, tf_display = future_to_meta[fut]
            try:
                df, info = fut.result()
            except Exception as e:
                df = pd.DataFrame()
                info = {"rows": 0, "error": str(e)}
            
            count += 1
            if df.empty:
                error = info.get("error", "Unavailable")
                logger.warning(f"[{count}/{total}] ⚠️  {idx_name} {tf_display} — {error}")
                summary.append([idx_name, tf_display, f"⚠️  {error}"])
            elif info.get("rows", 0) == 0:
                if info.get("error"):
                    error = info["error"]
                    logger.warning(f"[{count}/{total}] ⚠️  {idx_name} {tf_display} — {error}")
                    summary.append([idx_name, tf_display, f"⚠️  {error}"])
                else:
                    logger.info(f"[{count}/{total}] ✅ {idx_name} {tf_display} is up to date")
                    summary.append([idx_name, tf_display, "✅ Up to date"])
            else:
                added = info.get("added_rows", 0)
                if added > 0:
                    logger.info(f"[{count}/{total}] ✅ {idx_name} {tf_display} updated — {added} new rows added")
                    summary.append([idx_name, tf_display, f"✅ +{added} rows"])
                else:
                    logger.info(f"[{count}/{total}] ✅ {idx_name} {tf_display} refreshed — data verified up to date")
                    summary.append([idx_name, tf_display, "✅ Verified"])

    zip_path = zip_all_data()
    logger.info(f"💾 All data zipped to: {zip_path}")

    # Summary
    logger.info("\n📊 Summary of Downloaded Data:")
    for item in summary:
        logger.info(f"{item[0]} {item[1]}: {item[2]}")
    logger.info("🌟 Bulk download completed successfully!")

if __name__ == "__main__":
    main()
