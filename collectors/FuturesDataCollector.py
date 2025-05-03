import os
import pandas as pd
import datetime as dt
import pytz
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from pybit.unified_trading import HTTP

class FuturesDataCollector:
    """
    Класс для параллельного сбора исторических данных фьючерсов USDT с Bybit.
    """

    def __init__(self, api_key, api_secret,
                 save_dir="/Users/papaskakun/PycharmProjects/PythonProject/futures_data",
                 testnet=False, interval='60', limit=3600):
        self.api_key = api_key
        self.api_secret = api_secret
        self.save_dir = save_dir
        self.testnet = testnet
        self.interval = interval
        self.limit = limit
        self.session = HTTP(api_key=self.api_key, api_secret=self.api_secret, testnet=self.testnet)
        self.tickers = []

        os.makedirs(self.save_dir, exist_ok=True)
        print(f"📁 Папка для данных создана: {self.save_dir}")

    def get_usdt_futures_tickers(self):
        """
        Получает список тикеров фьючерсов, торгуемых к USDT.
        """
        result = self.session.get_tickers(category="linear").get('result', {}).get('list', [])
        self.tickers = [asset['symbol'] for asset in result if asset['symbol'].endswith('USDT')]
        print(f"✅ Найдено {len(self.tickers)} тикеров: {self.tickers}")
        return self.tickers

    def format_data(self, response):
        """
        Форматирует данные свечей в DataFrame с часовым поясом Москвы.
        """
        data = response.get('list', [])
        if not data:
            return None

        df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
        moscow_tz = pytz.timezone('Europe/Moscow')
        df.index = df['timestamp'].apply(
            lambda x: dt.datetime.fromtimestamp(int(x) / 1000, moscow_tz).strftime('%Y-%m-%d %H:%M:%S')
        )
        return df[::-1].apply(pd.to_numeric)

    def collect_data_for_symbol(self, symbol):
        """
        Скачивает и сохраняет исторические данные по указанному тикеру.
        Возвращает имя файла или сообщение об ошибке.
        """
        try:
            response = self.session.get_kline(
                category='linear',
                symbol=symbol,
                interval=self.interval,
                limit=self.limit
            ).get('result')

            latest = self.format_data(response)
            if isinstance(latest, pd.DataFrame):
                file_path = os.path.join(self.save_dir, f"{symbol}_h1.csv")
                latest.to_csv(file_path)
                return f"💾 {symbol} — сохранён"
            else:
                return f"⚠️ {symbol} — нет данных"
        except Exception as e:
            return f"❌ {symbol} — ошибка: {e}"

    def collect_all_data(self, max_workers=8):
        """
        Параллельно собирает исторические данные по всем тикерам.
        """
        if not self.tickers:
            self.get_usdt_futures_tickers()

        print("📊 Начинаем параллельный сбор данных...")

        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.collect_data_for_symbol, symbol): symbol for symbol in self.tickers}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Сбор данных"):
                result = future.result()
                results.append(result)

        # Выводим только ошибки и предупреждения после завершения
        print("\n📋 Результаты:")
        for r in results:
            if not r.startswith("💾"):
                print(r)

        print("🎯 Сбор всех данных завершен!")
