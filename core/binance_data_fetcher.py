import pandas as pd
import requests
import time

from datetime import datetime


class BinanceDataFetcher:
    def __init__(self, base_urls=None, timeout=10):
        # Daftar endpoint alternatif resmi Binance
        self.base_urls = base_urls or [
            "https://data-api.binance.vision",
            # "https://api1.binance.com",
            # "https://api2.binance.com",
            # "https://api3.binance.com",
            # "https://api4.binance.com",
            # "https://api-gcp.binance.com",
        ]
        self.timeout = timeout

    def _to_timestamp(self, date_str):
        """Konversi YYYY-MM-DD menjadi timestamp (ms)."""
        return int(datetime.strptime(date_str, "%Y-%m-%d").timestamp() * 1000)

    def get_historical_klines(
        self, symbol, interval="1h", start_date=None, end_date=None, limit=1000
    ):
        """Fetch full historical candlestick data (auto-paginated)."""

        params = {"symbol": symbol, "interval": interval, "limit": limit}
        all_data = []

        start_ts = self._to_timestamp(start_date) if start_date else None
        end_ts = self._to_timestamp(end_date) if end_date else None

        while True:
            if start_ts:
                params["startTime"] = start_ts
            if end_ts:
                params["endTime"] = end_ts

            # Pilih salah satu base_url yang berhasil
            data = None
            for base_url in self.base_urls:
                url = f"{base_url}/api/v3/klines"
                try:
                    r = requests.get(url, params=params, timeout=self.timeout)
                    r.raise_for_status()
                    chunk = r.json()
                    if not chunk:
                        break
                    all_data.extend(chunk)
                    print(f"Fetched {len(chunk)} of {symbol} candles from {base_url}")
                    # Update start_ts ke close time terakhir + 1 ms
                    last_close = chunk[-1][6]
                    start_ts = last_close + 1
                    break
                except Exception as e:
                    print(f"⚠️ Failed on {base_url}: {e}")
                    continue

            if not chunk or len(chunk) < limit:
                break  # selesai, data sudah habis

            time.sleep(0.25)  # throttle aman, hindari rate limit

        if not all_data:
            raise ConnectionError("No data retrieved from any Binance endpoint.")

        # Format hasil seperti yfinance
        df = pd.DataFrame(
            all_data,
            columns=[
                "Open Time",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "Close Time",
                "Quote Asset Volume",
                "Number of Trades",
                "Taker Buy Base",
                "Taker Buy Quote",
                "Ignore",
            ],
        )

        df = df[["Open Time", "Open", "High", "Low", "Close", "Volume"]]
        df["Date"] = pd.to_datetime(df["Open Time"], unit="ms", utc=True)
        df = df.set_index("Date").drop(columns=["Open Time"])
        df = df.astype(float)

        symbol_name = symbol.replace("USDT", "").upper()
        return {symbol_name: df}
