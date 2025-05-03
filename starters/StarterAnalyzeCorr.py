from analyzers.CointegrationAnalyzerAsync import CointegrationAnalyzerAsync
from collectors.FuturesDataCollector import FuturesDataCollector
from analyzers.CorrAnalyzer import CorrAnalyzer

if __name__ == "__main__":
    '''
    Что происходит в коде в целом?

    🔹 Этот скрипт находит пары монет, которые обычно ведут себя похоже (высокая корреляция), 
                               но в данный момент сильно разошлись по цене (высокий Z-score).

    '''

    analyzer_corr = CorrAnalyzer(
        data_dir='/Users/papaskakun/PycharmProjects/PythonProject/futures_data',
        corr_threshold = 0.9,
        zscore_threshold = 2,
        min_data_points = 100
    )

    # Запуск анализатора
    analyzer_corr.run_full_analysis()




