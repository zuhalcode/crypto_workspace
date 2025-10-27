import pandas as pd
import requests

from datetime import datetime


class BinanceDataFetcher:
    def __init__(self, base_urls=None, timeout=10):
        # Daftar endpoint alternatif resmi Binance
        self.base_urls = base_urls or [
            "https://data-api.binance.vision",
            "https://api1.binance.com",
            "https://api2.binance.com",
            "https://api3.binance.com",
            "https://api4.binance.com",
            "https://api-gcp.binance.com",
        ]
        self.timeout = timeout

    def _to_timestamp(self, date_str):
        """Konversi YYYY-MM-DD menjadi timestamp (ms)."""
        return int(datetime.strptime(date_str, "%Y-%m-%d").timestamp() * 1000)

    def get_historical_klines(
        self, symbol, interval="1h", start_date=None, end_date=None, limit=500
    ):
        """Fetch historical candlestick data and return in yfinance-like dict format."""
        params = {"symbol": symbol, "interval": interval, "limit": limit}

        if start_date:
            params["startTime"] = self._to_timestamp(start_date)
        if end_date:
            params["endTime"] = self._to_timestamp(end_date)

        for base_url in self.base_urls:
            url = f"{base_url}/api/v3/klines"
            try:
                print(f"Fetching from {url} ...")
                r = requests.get(url, params=params, timeout=self.timeout)
                r.raise_for_status()
                data = r.json()
                print(f"✅ Success from {base_url}")
                break
            except Exception as e:
                print(f"⚠️ Failed on {base_url}: {e}")
                data = None
                continue

        if data is None:
            raise ConnectionError("All Binance endpoints failed to respond.")

        # Format hasil seperti yfinance
        df = pd.DataFrame(
            data,
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
        df = df.set_index("Date")
        df = df.drop(columns=["Open Time"])
        df = df.astype(float)

        # Bungkus jadi dict dengan key = simbol (tanpa USDT)
        symbol_name = symbol.replace("USDT", "").upper()
        return {symbol_name: df}
