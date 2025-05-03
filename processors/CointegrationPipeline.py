from analyzers.CointegrationAnalyzerAsync import CointegrationAnalyzerAsync
from collectors.FuturesDataCollector import FuturesDataCollector
from evaluators.PairPermutationTestEvaluator import PairPermutationTestEvaluator
from simulators.PairTradingSimulatorCoin import PairTradingSimulatorCoin

if __name__ == "__main__":
    '''
    Этот пайплайн нацелен на запуск всей ветви проекта из одного места, 
    это скрывает за собой много вычислений и перезаписи данных, 
    так что большей скорости и удобства запуска некоторых частей кода лучше воспользоваться точками входа из 
    директории starters
    '''

    collector = FuturesDataCollector(
        api_key='3HN7Kwu8UrqQ7uWLPZ',
        api_secret='4y5WFAJ4bFch0taGYYOlur0b3ikSO48S9oWM',
        save_dir="/Users/papaskakun/PycharmProjects/PythonProject/futures_data",
        testnet=False,
        interval='60',
        limit=3600
    )
    analyzer_coin = CointegrationAnalyzerAsync(
        data_dir="/Users/papaskakun/PycharmProjects/PythonProject/futures_data",
        min_data_points=100,
        pvalue_threshold=0.05,
        max_workers=8  # под M1 Pro
    )

    evaluator = PairPermutationTestEvaluator(
        pairs_csv='/Users/papaskakun/PycharmProjects/PythonProject/cointegrated_pairs.csv',
        data_dir='/Users/papaskakun/PycharmProjects/PythonProject/futures_data',
        output_csv='/Users/papaskakun/PycharmProjects/PythonProject/significant_pairs.csv',
        r2_threshold=0.1,
        lag=3,
        max_workers=10
    )

    sim = PairTradingSimulatorCoin(
        data_dir="/Users/papaskakun/PycharmProjects/PythonProject/futures_data",
        pairs_file="/Users/papaskakun/PycharmProjects/PythonProject/top_1pct_significant_pairs.csv"
    )

    # Запуск сбора информации
    # collector.get_usdt_futures_tickers()
    # collector.collect_all_data()

    # Запуск Анализатора на коинтеграцию с использованием теста Энгла-Грейнджера
    # analyzer_coin.run()
    # analyzer_coin.save_results("/Users/papaskakun/PycharmProjects/PythonProject/cointegrated_pairs.csv")



    # Запуск отборшика, фильтр на самые значимые пары
    # evaluator.run()
    # Важный момент, благодаря этому этапу мы отсеем 99 процентов сигналов, и оставим 1 процент самых значимых
    # evaluator.filter_top_percent(top_percent=0.01)  # ← оставить только 1%

    # Этот этап позволяет "отрисовать"
    '''
    график распределения delta = R²_real - R²_shuffled даст наглядное понимание, как много пар действительно значимы,
     и где находится порог 1%.
    '''
    # Для получения отрисовки надо запустить код повторно , пайплайн длинный ( 30минут на выполнение по 10 потокам)
    # а эту фичу допилил после выполнения, в теории может быть баг,
    # логика зашита на .self (отдельно от выполнения основной логики не запустишь)
    # evaluator.plot_delta_distribution()


    # Запуск симуляции
    sim.run_batch()




