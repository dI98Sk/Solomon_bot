from analyzers.CointegrationAnalyzerAsync import CointegrationAnalyzerAsync
from collectors.FuturesDataCollector import FuturesDataCollector
from analyzers.CorrAnalyzer import CorrAnalyzer

if __name__ == "__main__":
    '''
    Что происходит в коде в целом?

    🔹 Этот скрипт находит все доступные крипто фьючерсы, и собирает их в директорию futures_data
    '''

    collector = FuturesDataCollector(
        api_key='3HN7Kwu8UrqQ7uWLPZ',
        api_secret='4y5WFAJ4bFch0taGYYOlur0b3ikSO48S9oWM',
        save_dir="../futures_data",
        testnet=False,
        interval='60',
        limit=3600
    )

    # Запуск сбора информации

    collector.get_usdt_futures_tickers()
    collector.collect_all_data()

