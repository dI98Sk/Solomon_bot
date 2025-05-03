from analyzers.CointegrationAnalyzerAsync import CointegrationAnalyzerAsync
from collectors.FuturesDataCollector import FuturesDataCollector
from analyzers.CorrAnalyzer import CorrAnalyzer
from plotters.CorrSpreadPlotter import PairSpreadPlotter

if __name__ == "__main__":
    '''
    Что происходит в коде в целом?
    🔹 Это основа парного трейдинга (pair trading) — типа стратегии арбитража.
    '''

    analyzer_coin = CointegrationAnalyzerAsync(
        data_dir="/Users/papaskakun/PycharmProjects/PythonProject/futures_data",
        min_data_points=100,
        pvalue_threshold=0.05,
        max_workers=8  # адаптировано под M1 Pro
    )
    analyzer_coin.run()
    analyzer_coin.save_results("cointegrated_pairs.csv")



