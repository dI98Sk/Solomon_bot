import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score
from sklearn.utils import shuffle
from concurrent.futures import ThreadPoolExecutor, as_completed
import matplotlib.pyplot as plt

class PairPermutationTestEvaluator:
    """
    Этот оценщик прогоняет полученный датасет (От просчета коинтеграции)
    и проводит просчет Пермутационного теста
    В качесве дополнотильного этапа отбора самых значимых пар.
    """

    def __init__(self, pairs_csv, data_dir, output_csv='significant_pairs.csv',
                 r2_threshold=0.1, lag=1, max_workers=8):
        self.pairs_csv = pairs_csv
        self.data_dir = data_dir
        self.output_csv = output_csv
        self.r2_threshold = r2_threshold
        self.lag = lag
        self.max_workers = max_workers
        self.pairs_df = None
        self.results = []

    def load_asset_data(self, asset_name):
        path = os.path.join(self.data_dir, f"{asset_name}.csv")
        if not os.path.exists(path):
            print(f"⚠️ Файл не найден: {path}")
            return None
        df = pd.read_csv(path, parse_dates=['timestamp'])
        df = df[['timestamp', 'close']].rename(columns={'close': asset_name})
        return df.set_index('timestamp')

    def evaluate_pair(self, asset_a, asset_b):
        df_a = self.load_asset_data(asset_a)
        df_b = self.load_asset_data(asset_b)
        if df_a is None or df_b is None:
            return None

        df = df_a.join(df_b, how='inner').sort_index()
        df[f'{asset_a}_lag'] = df[asset_a].shift(self.lag)
        df = df.dropna()

        X = df[[f'{asset_a}_lag']]
        y = df[asset_b]

        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X, y)
        y_pred = model.predict(X)
        r2_real = r2_score(y, y_pred)

        X_shuffled = shuffle(X, random_state=42)
        model.fit(X_shuffled, y)
        y_pred_shuffled = model.predict(X_shuffled)
        r2_shuffled = r2_score(y, y_pred_shuffled)

        delta = r2_real - r2_shuffled
        return {
            'asset_a': asset_a,
            'asset_b': asset_b,
            'r2_real': r2_real,
            'r2_shuffled': r2_shuffled,
            'delta': delta
        }

    def filter_top_percent(self, top_percent=0.01):
        if not self.results:
            print("⚠️ Нет результатов для фильтрации.")
            return

        df = pd.DataFrame(self.results)
        df_sorted = df.sort_values(by='delta', ascending=False)
        top_n = max(1, int(len(df_sorted) * top_percent))  # минимум 1 результат

        df_top = df_sorted.head(top_n)
        filename = os.path.basename(self.output_csv)
        df_top.to_csv(f'top_{int(top_percent * 100)}pct_{filename}', index=False)
        print(f"🔎 Сохранено {top_n} самых значимых пар в файл: top_{int(top_percent * 100)}pct_{self.output_csv}")


    def plot_delta_distribution(self, bins=50):
        if not self.results:
            print("⚠️ Нет результатов для визуализации.")
            return

        df = pd.DataFrame(self.results)
        deltas = df['delta']

        plt.figure(figsize=(10, 6))
        plt.hist(deltas, bins=bins, color='skyblue', edgecolor='black')
        plt.axvline(np.percentile(deltas, 99), color='red', linestyle='--', label='1% порог')
        plt.title("Распределение значимости (delta R²)")
        plt.xlabel("Delta R² (R²_real - R²_shuffled)")
        plt.ylabel("Количество пар")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig("delta_distribution.png")
        plt.show()
        print("📊 График сохранён как: delta_distribution.png")

    def run_async_evaluation(self):
        self.pairs_df = pd.read_csv(self.pairs_csv)
        futures = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for _, row in self.pairs_df.iterrows():
                pair_str = row['pair']
                try:
                    asset_a, asset_b = pair_str.split('/')
                    future = executor.submit(self.evaluate_pair, asset_a, asset_b)
                    futures[future] = pair_str
                except Exception as e:
                    print(f"⚠️ Ошибка в паре {pair_str}: {e}")

            print("Выполняем Пермутационный тест")
            print("▶️ Обработка пар...")
            for future in tqdm(as_completed(futures), total=len(futures)):
                pair_str = futures[future]
                try:
                    result = future.result()
                    if result and result['delta'] > self.r2_threshold:
                        self.results.append(result)
                except Exception as e:
                    print(f"⚠️ Ошибка при обработке {pair_str}: {e}")

    def save_results(self):
        df = pd.DataFrame(self.results)
        df.to_csv(self.output_csv, index=False)
        print(f"✅ Результаты сохранены в: {self.output_csv}")

    def run(self):
        self.run_async_evaluation()
        self.save_results()


if __name__ == "__main__":
    evaluator = PairPermutationTestEvaluator(
        pairs_csv='cointegrated_pairs.csv',
        data_dir='/futures_data',
        output_csv='significant_pairs.csv',
        r2_threshold=0.1,
        lag=3,
        max_workers=10
    )
    evaluator.run()
    # Важный момент, благодаря этому этапу мы отсеем 99 процентов сигналов, и оставим 1 процент самых хначимых
    evaluator.filter_top_percent(top_percent=0.01)  # ← оставить только 1%

    # Этот этап позволяет "отрисовать"
    '''
    график распределения delta = R²_real - R²_shuffled даст наглядное понимание, как много пар действительно значимы,
     и где находится порог 1%.
    '''
    # Для получения отрисовки надо запустить код повторно , пайплайн длинный ( 30минут на выполнение по 10 потокам)
    # а эту фичу допилил после выполнения, в теории может быть баг,
    # логика зашита на .self (отдельно от выполнения основной логики не запустишь)
    evaluator.plot_delta_distribution()