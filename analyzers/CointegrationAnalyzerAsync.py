import os
import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import coint
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


class CointegrationAnalyzerAsync:
    """
       Анализатор коинтеграции с использованием асинхронной многопоточности.

       Цель:
       Найти пары активов, между которыми существует коинтеграционная связь, то есть
       их ценовые ряды имеют долгосрочную статистическую зависимость. Это часто используется
       в парном трейдинге и арбитраже.

       Алгоритм:
       1. Загружаются данные из CSV файлов, содержащих столбцы 'timestamp' и 'close'.
       2. Формируются все возможные пары активов.
       3. Каждая пара проверяется на коинтеграцию с использованием теста Энгла-Грейнджера.
       4. Проверка выполняется параллельно с помощью ThreadPoolExecutor.
       5. Пары с p-value ниже заданного порога считаются коинтегрированными.
       6. Результаты можно сохранить в файл.

       Параметры:
       - data_dir: путь к папке с CSV-файлами
       - min_data_points: минимальное количество точек данных для анализа
       - pvalue_threshold: пороговое значение p-value для коинтеграции
       - max_workers: количество потоков для параллельной обработки
       """

    def __init__(self, data_dir, min_data_points=100, pvalue_threshold=0.05, max_workers=8):
        self.data_dir = data_dir
        self.min_data_points = min_data_points
        self.pvalue_threshold = pvalue_threshold
        self.max_workers = max_workers
        self.data = self._load_data()
        self.results = []

    def _load_data(self):
        """
        Загружает и очищает CSV-файлы с данными.
        Оставляет только те, которые имеют нужный формат и достаточную длину.
        """
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
        return data

    def _check_cointegration(self, pair):
        """
                Проверяет коинтеграцию между двумя временными рядами (ценами активов).
                Возвращает словарь с парой и p-value, если найдено соответствие.
        """
        a, b = pair
        df1 = self.data[a].set_index('timestamp')
        df2 = self.data[b].set_index('timestamp')
        merged = pd.merge(df1, df2, left_index=True, right_index=True, suffixes=('_a', '_b'))

        if len(merged) < self.min_data_points:
            return None

        try:
            score, pvalue, _ = coint(merged['close_a'], merged['close_b'])
            if pvalue < self.pvalue_threshold:
                return {'pair': f'{a}/{b}', 'p-value': round(pvalue, 5)}
        except Exception:
            return None
        return None

    def run(self):
        """
                Запускает многопоточную проверку всех возможных пар на коинтеграцию.
        """
        symbols = list(self.data.keys())
        all_pairs = list(combinations(symbols, 2))
        print(f"🔍 Проверка {len(all_pairs)} пар на коинтеграцию...")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._check_cointegration, pair): pair for pair in all_pairs}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Обработка пар"):
                result = future.result()
                if result:
                    self.results.append(result)

        print(f"✅ Найдено {len(self.results)} коинтегрированных пар.")
        return self.results

    def save_results(self, filepath="/Users/papaskakun/PycharmProjects/PythonProject/cointegrated_pairs.csv"):
        """
                Сохраняет результаты анализа в CSV-файл.
        """
        if not self.results:
            print("⚠️ Нет результатов для сохранения.")
            return
        df = pd.DataFrame(self.results)
        df.sort_values(by="p-value", inplace=True)
        df.to_csv(filepath, index=False)
        print(f"💾 Сохранено в {filepath}")