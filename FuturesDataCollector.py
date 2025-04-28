import os
import time
import pandas as pd
import numpy as np
import datetime as dt
import pytz
from pybit.unified_trading import HTTP

class FuturesDataCollector:
    def __init__(self, api_key, api_secret, save_dir="futures_data", testnet=False, interval='60', limit=3600):
        self.api_key = api_key
        self.api_secret = api_secret
        self.save_dir = save_dir
        self.testnet = testnet
        self.interval = interval  # таймфрейм свечей
        self.limit = limit        # количество свечей
        self.session = HTTP(api_key=self.api_key, api_secret=self.api_secret, testnet=self.testnet)
        self.tickers = []

        os.makedirs(self.save_dir, exist_ok=True)
        print(f"📁 Папка для данных создана: {self.save_dir}")

    def get_usdt_futures_tickers(self):
        result = self.session.get_tickers(category="linear").get('result', {}).get('list', [])
        self.tickers = [asset['symbol'] for asset in result if asset['symbol'].endswith('USDT')]
        print(f"✅ Найдено {len(self.tickers)} тикеров: {self.tickers}")
        return self.tickers

    def format_data(self, response):
        data = response.get('list', [])
        if not data:
            return None

        df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
        moscow_tz = pytz.timezone('Europe/Moscow')
        df.index = df['timestamp'].apply(lambda x: dt.datetime.fromtimestamp(int(x) / 1000, moscow_tz).strftime('%Y-%m-%d %H:%M:%S'))
        return df[::-1].apply(pd.to_numeric)

    def get_last_timestamp(self, df):
        return int(df.timestamp[-1:].values[0])

    def collect_data_for_symbol(self, symbol):
        print(f"📈 Сбор данных для {symbol}")
        response = self.session.get_kline(category='linear', symbol=symbol, interval=self.interval, limit=self.limit).get('result')
        latest = self.format_data(response)

        if isinstance(latest, pd.DataFrame):
            file_path = os.path.join(self.save_dir, f"{symbol}_h1.csv")
            latest.to_csv(file_path)
            print(f"💾 Данные сохранены: {file_path}")
        else:
            print(f"⚠️ Нет данных для {symbol}")

    def collect_all_data(self):
        if not self.tickers:
            self.get_usdt_futures_tickers()

        for symbol in self.tickers:
            self.collect_data_for_symbol(symbol)

        print("🎯 Сбор всех данных завершен!")