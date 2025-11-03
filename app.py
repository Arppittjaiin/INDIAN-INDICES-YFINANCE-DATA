import os
import time
from datetime import timedelta
from zipfile import ZipFile
import pandas as pd
import streamlit as st
import yfinance as yf

# ============================================================
# 🧼 Clean startup: remove invalid or 1970 timestamp files
# ============================================================
def purge_invalid_csvs(data_dir="data"):
    if not os.path.exists(data_dir):
        return
    for root, _, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".csv"):
                path = os.path.join(root, file)
                try:
                    df = pd.read_csv(path, nrows=5)
                    if "datetime" in df.columns:
                        years = pd.to_datetime(df["datetime"], errors="coerce").dt.year
                        if years.lt(1980).any():
                            os.remove(path)
                            print(f"🧹 Deleted invalid file: {path}")
                except Exception:
                    os.remove(path)
                    print(f"🧹 Deleted corrupted file: {path}")

purge_invalid_csvs()

# ============================================================
# 📊 Index tickers with fallbacks
# ============================================================
INDICES = {
    "Nifty 50": ["^NSEI", "NIFTYBEES.NS"],
    "Sensex": ["^BSESN", "SENSEX.BO"],
    "BankNifty": ["^NSEBANK", "BANKBEES.NS"],
}

# Supported timeframes
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

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
MAX_RETRIES = 3
RETRY_DELAY = 5


# ============================================================
# 🧹 Cleaning helper - FIXED VERSION
# ============================================================
def clean_ohlc_data(df: pd.DataFrame):
    """Clean OHLC data with better error handling"""
    if df.empty:
        return pd.DataFrame(), {"rows": 0, "error": "Empty input dataframe"}
    
    df = df.copy()
    
    # First, handle the index if it contains datetime
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index()
    
    # Remove duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]
    df.columns = [str(c).lower().strip() for c in df.columns]
    
    # Find datetime column
    dt_col = next((c for c in df.columns if any(k in c for k in ["datetime", "date", "timestamp"])), None)
    if not dt_col:
        return pd.DataFrame(), {"rows": 0, "error": "No datetime column found"}
    
    # Rename to standardize
    if dt_col != "datetime":
        df.rename(columns={dt_col: "datetime"}, inplace=True)
    
    # Convert to datetime - handle if it's already datetime
    if not pd.api.types.is_datetime64_any_dtype(df["datetime"]):
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    
    # Remove NaT values
    df = df.dropna(subset=["datetime"])
    
    if df.empty:
        return pd.DataFrame(), {"rows": 0, "error": "All datetime values invalid"}
    
    # Filter out pre-1980 dates (but don't fail if none exist)
    initial_rows = len(df)
    df = df[df["datetime"].dt.year >= 1980]
    
    if df.empty:
        return pd.DataFrame(), {"rows": 0, "error": f"All {initial_rows} rows had dates before 1980"}
    
    # Handle timezone - make timezone-naive for IST
    try:
        if df["datetime"].dt.tz is not None:
            # Convert to IST then remove timezone info
            df["datetime"] = df["datetime"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
        # If already naive, assume it's in IST
    except Exception as e:
        print(f"⚠️ Timezone conversion warning: {e}")
    
    # Keep relevant columns
    keep_cols = ["datetime"] + [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[keep_cols].drop_duplicates(subset=["datetime"]).sort_values("datetime")
    
    info = {
        "rows": len(df),
        "first_dt": df["datetime"].min(),
        "last_dt": df["datetime"].max(),
    }
    
    return df, info


# ============================================================
# 📥 Download functions - IMPROVED
# ============================================================
def safe_download(ticker, interval, start=None, period=None):
    """Download with retry & filtering out empty/1970 data."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            df = yf.download(
                ticker,
                interval=interval,
                start=start,
                period=period,
                progress=False,
                auto_adjust=True,
            )
            
            if df.empty:
                print(f"⚠️ Empty dataframe for {ticker} {interval}")
                continue
            
            # Check if data is valid
            if hasattr(df.index, 'year') and df.index.min().year > 1980:
                print(f"✅ Valid data from {ticker}: {len(df)} rows, range {df.index.min()} to {df.index.max()}")
                return df
            else:
                print(f"⚠️ Invalid date range for {ticker}")
                
        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed for {ticker} {interval}: {e}")
        
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY * attempt)
    
    return pd.DataFrame()


def try_tickers_download(ticker_list, interval, start=None, period=None):
    """Try primary then fallback tickers."""
    for tkr in ticker_list:
        df = safe_download(tkr, interval, start=start, period=period)
        if not df.empty:
            print(f"✅ Data fetched from {tkr}")
            return df
        else:
            print(f"⚠️ No data for {tkr}, trying fallback...")
    return pd.DataFrame()


def download_index_data(index_name, tickers, interval):
    """Download, clean, and save one timeframe."""
    index_dir = os.path.join(DATA_DIR, index_name.lower().replace(" ", "_"))
    os.makedirs(index_dir, exist_ok=True)
    file_path = os.path.join(index_dir, f"{index_name.lower().replace(' ', '_')}_{interval}.csv")
    
    # Load existing data
    existing = pd.DataFrame()
    start_date = None
    if os.path.exists(file_path):
        try:
            existing = pd.read_csv(file_path, parse_dates=["datetime"])
            if not existing.empty:
                existing, _ = clean_ohlc_data(existing)
                if not existing.empty:
                    start_date = (existing["datetime"].max() + timedelta(days=1)).strftime("%Y-%m-%d")
                    print(f"📂 Existing data found, updating from {start_date}")
        except Exception as e:
            print(f"⚠️ Error loading existing file: {e}")
    
    # Determine period based on interval
    if interval == "1m":
        period = "7d"
    elif "m" in interval or "h" in interval:
        period = "60d"
    else:
        period = "max"
    
    # Download new data
    df = try_tickers_download(tickers, interval, start=start_date, period=None if start_date else period)
    
    # Fallback for 1m data
    if df.empty and interval == "1m":
        print("⚠️ Retrying 1m with 7d period...")
        df = try_tickers_download(tickers, interval, period="7d")
    
    if df.empty:
        error_msg = f"No data downloaded for {index_name} {interval}"
        print(f"❌ {error_msg}")
        return existing, {"rows": 0, "error": error_msg}
    
    # Handle multi-level columns from yfinance (when downloading multiple tickers)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    
    # Reset index to get datetime as column
    df = df.reset_index()
    
    # Remove duplicate columns AFTER flattening multi-index
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Standardize column names
    df.columns = [str(c).lower().strip() for c in df.columns]
    
    # Ensure datetime column exists with proper naming
    if "date" in df.columns:
        df.rename(columns={"date": "datetime"}, inplace=True)
    elif "datetime" not in df.columns:
        # If there's still no datetime column, look for variations
        possible_dt_cols = [c for c in df.columns if isinstance(c, str) and ('date' in c.lower() or c.lower() in ['timestamp', 'time'])]
        if possible_dt_cols:
            df.rename(columns={possible_dt_cols[0]: "datetime"}, inplace=True)
        else:
            # Last resort: first column is probably datetime
            df.rename(columns={df.columns[0]: "datetime"}, inplace=True)
    
    # Clean the data
    clean_df, info = clean_ohlc_data(df)
    
    if clean_df.empty:
        error_msg = info.get("error", "Unknown cleaning error")
        print(f"❌ Cleaning failed: {error_msg}")
        return existing, {"rows": 0, "error": error_msg}
    
    # Merge with existing data
    if not existing.empty:
        final_df = pd.concat([existing, clean_df]).drop_duplicates(subset=["datetime"]).sort_values("datetime")
    else:
        final_df = clean_df
    
    # Save to CSV
    final_df.to_csv(file_path, index=False)
    print(f"💾 Saved {len(final_df)} rows to {file_path}")
    
    return final_df, info


def zip_all_data():
    """Zip all valid CSVs."""
    zip_path = os.path.join(DATA_DIR, "indices_data.zip")
    with ZipFile(zip_path, "w") as zipf:
        for root, _, files in os.walk(DATA_DIR):
            for f in files:
                if f.endswith(".csv"):
                    full_path = os.path.join(root, f)
                    arcname = os.path.relpath(full_path, DATA_DIR)
                    zipf.write(full_path, arcname)
    return zip_path


# ============================================================
# 🎨 Streamlit UI
# ============================================================
st.set_page_config(page_title="Nifty & Sensex — Cleaned OHLC Dashboard", layout="wide")
st.title("📈 Nifty & Sensex — Cleaned OHLC Dashboard")

col1, col2 = st.columns(2)
index_choice = col1.selectbox("Select Index", list(INDICES.keys()), index=0)
tf_choice = col2.selectbox("Select Timeframe", list(VALID_TIMEFRAMES.keys()), index=6)

st.divider()

# --- Single Download ---
if st.button("📥 Download / Refresh Data"):
    with st.spinner(f"Fetching {index_choice} ({tf_choice}) data..."):
        df, info = download_index_data(index_choice, INDICES[index_choice], VALID_TIMEFRAMES[tf_choice])
    
    if df.empty or info.get("rows", 0) == 0:
        error = info.get("error", "Unknown error")
        st.error(f"⚠️ No valid data for {index_choice} ({tf_choice}) — {error}")
    else:
        st.success(f"✅ {index_choice} ({tf_choice}) ready — {info['rows']} rows (from {info['first_dt']} to {info['last_dt']})")
        st.dataframe(df.tail(20))
        
        if "close" in df.columns:
            st.line_chart(df.set_index("datetime")["close"])
        
        zip_path = zip_all_data()
        with open(zip_path, "rb") as f:
            st.download_button("⬇️ Download All Data (ZIP)", data=f, file_name="indices_data.zip")

# --- Bulk Download ---
elif st.button("🌐 Bulk Download / Refresh All Symbols & Timeframes"):
    progress = st.progress(0)
    total = len(INDICES) * len(VALID_TIMEFRAMES)
    count = 0
    summary = []
    
    with st.spinner("Fetching all indices and timeframes..."):
        for idx_name, tickers in INDICES.items():
            st.markdown(f"### 🏦 {idx_name}")
            for tf, tf_code in VALID_TIMEFRAMES.items():
                count += 1
                df, info = download_index_data(idx_name, tickers, tf_code)
                
                if df.empty or info.get("rows", 0) == 0:
                    error = info.get("error", "Unavailable")
                    st.warning(f"⚠️ {tf} — {error}")
                    summary.append([idx_name, tf, f"⚠️ {error}"])
                else:
                    st.success(f"✅ {tf} done — {info['rows']} rows")
                    summary.append([idx_name, tf, f"✅ {info['rows']} rows"])
                
                progress.progress(count / total)
            st.divider()
        
        zip_path = zip_all_data()
        with open(zip_path, "rb") as f:
            st.download_button("⬇️ Download All CSVs (ZIP)", data=f, file_name="all_indices_data.zip")
        
        # --- Summary Table ---
        st.subheader("📊 Summary of Downloaded Data")
        df_summary = pd.DataFrame(summary, columns=["Index", "Timeframe", "Status"])
        st.dataframe(df_summary)
    
    st.success("🌟 Bulk download completed successfully!")

else:
    st.info("Click **Download / Refresh Data** or **Bulk Download** to start.")