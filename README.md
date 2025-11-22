# 📈 Nifty & Sensex — Cleaned OHLC Dashboard

## ✅ What This README Covers

* Summarizes the project purpose and structure.
* Documents features, UI flow, and backend logic.
* Provides installation, setup, and execution steps.
* Lists configuration, dependencies, and project architecture.
* Includes badges and usage examples.

---

# 📌 Project Overview

A **Streamlit-based OHLC Dashboard** that downloads, cleans, caches, and visualizes intraday & historical data for major Indian indices — **Nifty 50**, **Sensex**, and **BankNifty**.

This optimized version enhances:

* ⚡ **Performance** (vectorized data ops, caching, threading)
* 🔁 **Data reliability** (robust cleaning, retry logic, fallback tickers)
* 🧹 **Automatic corruption handling** (invalid/corrupt CSV cleanup)
* 📦 **Local storage management** (CSV + Parquet + ZIP archive export)

Source: fileciteturn0file0

---

# 🏷️ Badges

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Streamlit](https://img.shields.io/badge/Streamlit-App-red)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow)

---

# ⚙️ Features

* 🧭 **Supports 3 major Indian indices**: Nifty 50, Sensex, BankNifty.
* ⏱️ **Multiple timeframes**: 1m → 1mo.
* 🔁 **Fallback tickers** for reliable data retrieval.
* 📥 **Incremental downloads** (updates only missing data).
* 🧽 **Corrupt CSV auto-cleaning** on startup.
* 🧹 **Data cleaning pipeline** ensuring valid datetime & OHLC format.
* 🗄️ **Local caching** via CSV + Parquet.
* 📦 **Bulk download** for *all* indices/timeframes.
* 📊 **Interactive charts & data preview**.
* 🗂️ **One-click ZIP export** of all CSVs.
* 🚀 **Threaded bulk fetching** for faster refresh.

---

# 🧠 Tech Stack

* **Python**
* **Streamlit** (UI)
* **yfinance** (data ingestion)
* **Pandas** (data processing)
* **ThreadPoolExecutor** (parallel tasks)
* **Parquet** (fast local caching)
* **ZipFile** (export utility)

---

# 🛠️ Installation & Setup

```bash
# 1. Clone the repository
git clone https://github.com/Arppittjaiin/INDIAN-INDICES-YFINANCE-DATA.git
cd INDIAN-INDICES-YFINANCE-DATA

# 2. Create a virtual environment (recommended)
python -m venv venv
source venv/bin/activate   # macOS / Linux
venv\Scripts\activate      # Windows

# 3. Install dependencies
pip install -r requirements.txt
```

If you do not have a `requirements.txt`, generate one:

```bash
pip install streamlit pandas yfinance
pip freeze > requirements.txt
```

---

# 🚀 Usage

Run the Streamlit app:

```bash
streamlit run app.py
```

Inside the UI:

* Select Index → Select Timeframe
* Click **“📥 Download / Refresh Data”**
* Or run **Bulk Download** for all symbols/timeframes.

Example output includes:

* A cleaned OHLC dataframe
* A closing price chart
* ZIP download button for all stored CSVs

---

# 🛠️ How It Works

## 🔎 API & Data Pipeline

### 1. **Download Logic**

* Tries primary ticker → falls back to alternative tickers.
* Retries on failure with exponential backoff.
* Auto-adjusted yfinance OHLC data.

### 2. **Data Cleaning**

Cleans using `clean_ohlc_data()`:

* Validates datetime columns.
* Converts timezone-aware timestamps to IST.
* Removes invalid rows (<1980 or NaT).
* Standardizes OHLC column names.
* Drops duplicates.

### 3. **Caching**

* Uses CSV + Parquet.
* On load, files are validated.
* Corrupted files are automatically deleted.

### 4. **Bulk Download Mode**

* Parallel execution with up to 8 threads.
* Dynamic progress updates.
* Summary table of all index/timeframe results.

---

# 🗂️ Folder Structure

```
project/
│
├── app.py                # Streamlit dashboard
├── data/                 # Auto-created on first run
│   ├── nifty_50/         # Cached CSV/Parquet
│   ├── Sensex/
│   ├── banknifty/
│   └── indices_data.zip  # Exported ZIP
└── requirements.txt
```

---

# 📊 Configuration

* **DATA_DIR**: directory for cached symbols
* **MAX_RETRIES**: retry attempts for yfinance
* **PARQUET_CACHE**: toggle Parquet usage
* **VALID_TIMEFRAMES**: allowed intraday/daily intervals
* **INDICES**: primary + fallback tickers
* **Threading**: controlled via `MAX_THREADS`

---

# 📦 Dependencies

Recommended `requirements.txt`:

```txt
streamlit>=1.29
yfinance>=0.2.30
pandas>=2.0
numpy
pyarrow
```

---

# 👨‍💻 Author

**Author: Arpit Jain (AJ)**

---

# 📄 License

This project is licensed under the **MIT License**.

---

