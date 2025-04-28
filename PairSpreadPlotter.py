import os
import pandas as pd
import matplotlib.pyplot as plt

class PairSpreadPlotter:
    def __init__(self, data_dir="futures_data"):
        self.data_dir = data_dir
        self.data = {}
        self.load_data()

    def load_data(self):
        """Загружает все csv-файлы из директории в память."""
        if not os.path.exists(self.data_dir):
            raise FileNotFoundError(f"❌ Папка {self.data_dir} не найдена.")

        for file in os.listdir(self.data_dir):
            if file.endswith(".csv"):
                file_path = os.path.join(self.data_dir, file)
                df = pd.read_csv(file_path)
                if 'timestamp' not in df.columns or 'close' not in df.columns:
                    continue
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                name = os.path.splitext(file)[0].replace('_h1', '')  # удаляем _h1 если есть
                self.data[name] = df[['timestamp', 'close']]

        print(f"✅ Загружено {len(self.data)} монет из папки '{self.data_dir}'.")

    def plot_pair_spread(self, coin_a, coin_b):
        """Строит график цен двух монет и их Z-score спреда."""
        # Автоматическая коррекция имен (если вдруг передали 'BTCUSDT_h1')
        coin_a = coin_a.replace('_h1', '')
        coin_b = coin_b.replace('_h1', '')

        if coin_a not in self.data or coin_b not in self.data:
            print(f"❗ Монеты {coin_a} или {coin_b} не найдены.")
            return

        df1 = self.data[coin_a].set_index('timestamp')
        df2 = self.data[coin_b].set_index('timestamp')
        merged = pd.merge(df1, df2, left_index=True, right_index=True, suffixes=('_a', '_b'))

        spread = merged['close_a'] - merged['close_b']
        zscore = (spread - spread.mean()) / spread.std()

        plt.figure(figsize=(14, 6))
        plt.subplot(2, 1, 1)
        plt.plot(merged.index, merged['close_a'], label=coin_a)
        plt.plot(merged.index, merged['close_b'], label=coin_b)
        plt.title(f'Цены пары: {coin_a}/{coin_b}')
        plt.legend()

        plt.subplot(2, 1, 2)
        plt.plot(merged.index, zscore, label='Z-score spread', color='purple')
        plt.axhline(0, linestyle='--', color='black')
        plt.axhline(2, linestyle='--', color='green')
        plt.axhline(-2, linestyle='--', color='red')
        plt.title(f'Z-score: {coin_a}/{coin_b}')
        plt.legend()
        plt.tight_layout()

        # Сохраняем в файл
        pair_name = f"{coin_a}_{coin_b}"
        plt.savefig(f"{pair_name}_spread.png")
        plt.close()

        print(f"✅ График сохранён: {pair_name}_spread.png")