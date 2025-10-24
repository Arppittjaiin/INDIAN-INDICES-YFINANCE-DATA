# Indian Index Data Downloader

Incremental Yahoo Finance data downloader for Indian stock market indices (NSE & BSE).

## Features

- ✅ **Thread-safe & Resumable**: Safe concurrent downloads with CSV-based storage
- ✅ **Multiple Timeframes**: 1m, 5m, 15m, 1h, 1d, 1wk, 1mo
- ✅ **Incremental Updates**: Only downloads new data on subsequent runs
- ✅ **Robust Error Handling**: Handles corrupted CSVs, API errors, and timezone issues
- ✅ **Parallel Downloads**: Utilizes multiple threads for faster updates

## Supported Indices

- **NSE**: NIFTY50, BANKNIFTY, NIFTYIT, NIFTYFMCG, NIFTYPHARMA, NIFTYMETAL, NIFTYAUTO
- **BSE**: SENSEX

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/indian-index-downloader.git
cd indian-index-downloader

# Install dependencies
pip install -r requirements.txt
```

## Usage

```bash
python app.py
```

The script will:
1. Create an `index_data/` directory if it doesn't exist
2. Download historical data for all indices and timeframes
3. On subsequent runs, only fetch new data since the last update

## Data Output

CSV files are saved in `index_data/` with the format: `{INDEX_NAME}_{TIMEFRAME}.csv`

Example: `NIFTY50_1d.csv`, `BANKNIFTY_5m.csv`

Each CSV contains OHLCV data:
- Open
- High
- Low
- Close
- Volume

## Configuration

Edit `app.py` to customize:

- **INDICES**: Add or remove indices
- **TIMEFRAMES**: Modify timeframe list
- **FRESH_LIMITS**: Adjust historical data limits
- **MAX_WORKERS**: Control parallel download threads

## Requirements

- Python 3.8+
- pandas
- yfinance
- tqdm

## Notes

- Yahoo Finance has rate limits; the script includes retry logic and backoff
- Intraday data (1m, 5m, 15m) has limited historical availability
- Data is stored in the UTC timezone
- Corrupted CSV files are automatically backed up and recreated

## License

MIT License

## Contributing

Pull requests are welcome! For major changes, please open an issue first.

## Disclaimer

This tool is for educational and research purposes. Ensure you comply with Yahoo Finance's terms of service.
