from FuturesDataCollector import FuturesDataCollector
from PairSpreadPlotter import PairSpreadPlotter
from PairTradingAnalyzer import PairTradingAnalyzer
from PairTradingSimulator import PairTradingSimulator

if __name__ == "__main__":
    '''
    Что происходит в коде в целом?

    🔹 Этот скрипт находит пары монет, которые обычно ведут себя похоже (высокая корреляция), 
                               но в данный момент сильно разошлись по цене (высокий Z-score).
    🔹 Это основа парного трейдинга (pair trading) — типа стратегии арбитража.
    '''

    analyzer = PairTradingAnalyzer(
        data_dir=r"/Users/papaskakun/PycharmProjects/PythonProject/futures_data",
        corr_threshold=0.8,
        zscore_threshold=2,
        min_data_points=100
    )

    collector = FuturesDataCollector(
        api_key='3HN7Kwu8UrqQ7uWLPZ',
        api_secret='4y5WFAJ4bFch0taGYYOlur0b3ikSO48S9oWM',
        save_dir="futures_data",
        testnet=False,
        interval='60',
        limit=3600
    )

    # Запуск сбора информации и ее Анализ

    # collector.get_usdt_futures_tickers()
    # collector.collect_all_data()

    # Запуск анализатора
    # analyzer.run_full_analysis()

    # Можно нарисовать спред по паре:
    # !!!! Важно тут надо подавать название монеты именно без  _h1, так как словать хранил данные в формате :
    # Ключ : Значение , где ключ Название Монеты, а значение датасет
    # plotter = PairSpreadPlotter(data_dir="futures_data")

    # Построить спред по любой паре:
    # plotter.plot_pair_spread('APEUSDT', 'ACXUSDT')

    # Создаём экземпляр симулятора, указываем папку с данными
    simulator = PairTradingSimulator(data_dir="/Users/papaskakun/PycharmProjects/PythonProject/futures_data",
                                     save_dir="report_data"
                                     )

    # Запускаем симуляцию
    simulator.simulate(symbol1="BANDUSDT", symbol2="DYDXUSDT")

    # Печатаем красивый отчет
    simulator.report()


