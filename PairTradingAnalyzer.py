import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm

from FuturesDataCollector import FuturesDataCollector
from PairSpreadPlotter import PairSpreadPlotter


class PairTradingAnalyzer:
    def __init__(self, data_dir, corr_threshold=0.8, zscore_threshold=2, min_data_points=100):
        self.data_dir = data_dir
        self.corr_threshold = corr_threshold
        self.zscore_threshold = zscore_threshold
        self.min_data_points = min_data_points
        self.data = {}
        self.returns_df = pd.DataFrame()
        self.high_corr_pairs = []
        self.signals = []

    def load_all_data(self):
        data = {}
        for file in os.listdir(self.data_dir):
            if file.endswith(".csv"):
                df = pd.read_csv(os.path.join(self.data_dir, file))
                if 'timestamp' not in df.columns or 'close' not in df.columns:
                    continue
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.sort_values('timestamp')
                if len(df) < self.min_data_points:
                    continue
                name = os.path.splitext(file)[0]
                df = df[['timestamp', 'close']].dropna()
                data[name] = df
        self.data = data
        print(f"✅ Загружено {len(data)} монет.")

    def compute_log_returns(self):
        returns = {}
        for name, df in self.data.items():
            df = df.set_index('timestamp')
            df[f'{name}_logret'] = np.log(df['close'] / df['close'].shift(1))
            returns[name] = df[[f'{name}_logret']].dropna()
        self.returns_df = pd.concat(returns.values(), axis=1).dropna()
        print("📈 Лог-доходности посчитаны.")

    def find_high_corr_pairs(self):
        corr_matrix = self.returns_df.corr()
        pairs = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i + 1, len(corr_matrix.columns)):
                a, b = corr_matrix.columns[i], corr_matrix.columns[j]
                if corr_matrix.loc[a, b] > self.corr_threshold:
                    pairs.append((a.replace("_logret", ""), b.replace("_logret", "")))
        self.high_corr_pairs = pairs
        print(f"🔗 Найдено {len(pairs)} пар с корреляцией > {self.corr_threshold}")

    def compute_latest_zscore(self, series1, series2):
        spread = series1 - series2
        zscore = (spread - spread.mean()) / spread.std()
        return zscore.iloc[-1]

    def analyze_signals(self, limit=5000):
        signals = []
        for a, b in tqdm(self.high_corr_pairs[:limit], desc="🔍 Анализ пар"):
            df1 = self.data[a].set_index('timestamp')
            df2 = self.data[b].set_index('timestamp')
            merged = pd.merge(df1, df2, left_index=True, right_index=True, suffixes=('_a', '_b'))

            if len(merged) < 30:
                continue

            z = self.compute_latest_zscore(merged['close_a'], merged['close_b'])
            if abs(z) > self.zscore_threshold:
                signals.append({'pair': f'{a}/{b}', 'zscore': z})

        self.signals = signals
        print(f"📊 Найдено {len(signals)} сильных расхождений.")

    def save_signals(self, filename='signals.csv'):
        if self.signals:
            df_signals = pd.DataFrame(self.signals)
            df_signals['abs_zscore'] = df_signals['zscore'].abs()
            df_signals = df_signals.sort_values('abs_zscore', ascending=False)
            df_signals.to_csv(filename, index=False)
            print(f"✅ Сигналы сохранены в {filename}")
        else:
            print("🔍 Нет сигналов для сохранения.")

    def run_full_analysis(self):
        self.load_all_data()
        self.compute_log_returns()
        self.find_high_corr_pairs()
        self.analyze_signals()
        self.save_signals()


