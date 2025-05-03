import time
import logging
from datetime import datetime
from analyzers.CointegrationAnalyzerAsync import CointegrationAnalyzerAsync
from collectors.FuturesDataCollector import FuturesDataCollector
from evaluators.PairPermutationTestEvaluator import PairPermutationTestEvaluator
from simulators.PairTradingSimulatorCoin import PairTradingSimulatorCoin
from utils.telegram_notifier import TelegramNotifier
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "futures_data")
plot_path = os.path.join(BASE_DIR, "delta_distribution.png")


class MainPipelineRunner:
    '''
        Этот пайплайн нацелен на запуск всей ветви проекта из одного места,
        это скрывает за собой много вычислений и перезаписи данных,
        так что большей скорости и удобства запуска некоторых частей кода лучше воспользоваться точками входа из
        директории starters
        '''
    def __init__(self):
        self.notifier = TelegramNotifier(
            bot_token="AAE7vzbSwnUNWWszFsvUDSPKorR4jphXYOs",
            chat_id="-1002312259096"
        )

        self.collector = FuturesDataCollector(
            api_key='3HN7Kwu8UrqQ7uWLPZ',
            api_secret='4y5WFAJ4bFch0taGYYOlur0b3ikSO48S9oWM',
            save_dir=DATA_DIR,
            testnet=False,
            interval='60',
            limit=3600
        )

        self.analyzer = CointegrationAnalyzerAsync(
            data_dir=DATA_DIR,
            min_data_points=100,
            pvalue_threshold=0.05,
            max_workers=8
        )

        self.evaluator = PairPermutationTestEvaluator(
            pairs_csv='/Users/papaskakun/PycharmProjects/PythonProject/cointegrated_pairs.csv',
            data_dir='DATA_DIR',
            output_csv='/Users/papaskakun/PycharmProjects/PythonProject/significant_pairs.csv',
            r2_threshold=0.1,
            lag=3,
            max_workers=10
        )

        self.simulator = PairTradingSimulatorCoin(
            data_dir=DATA_DIR,
            pairs_file=os.path.join(BASE_DIR, "top_1pct_significant_pairs.csv")
        )

    def run(self):
        start_time = time.time()
        now = datetime.now()
        self.notifier.send_message(f"🚀 Запуск пайплайна: {now}")
        logging.info("Pipeline started")

        try:
            # Запуск сбора информации
            self.collector.collect_all_data()
            logging.info("Data collected.")
            self.notifier.send_message("📡 Сбор данных завершён.")

            # Запуск Анализатора на коинтеграцию с использованием теста Энгла-Грейнджера
            self.analyzer.run()
            self.analyzer.save_results("/Users/papaskakun/PycharmProjects/PythonProject/cointegrated_pairs.csv")
            logging.info("Cointegration analysis done.")
            self.notifier.send_message("🔍 Анализ завершён.")

            # Запуск отборшика, фильтр на самые значимые пары
            self.evaluator.run()
            # Важный момент, благодаря этому этапу мы отсеем 99 процентов сигналов, и оставим 1 процент самых значимых
            self.evaluator.filter_top_percent(top_percent=0.01)
            logging.info("Evaluation done.")
            self.notifier.send_message("📊 Оценка завершена.")

            # Этот этап позволяет "отрисовать"
            '''
                график распределения delta = R²_real - R²_shuffled даст наглядное понимание, как много пар действительно значимы,
                 и где находится порог 1%.
                '''
            # Для получения отрисовки надо запустить код повторно , пайплайн длинный ( 30минут на выполнение по 10 потокам)
            # а эту фичу допилил после выполнения, в теории может быть баг,
            # логика зашита на .self (отдельно от выполнения основной логики не запустишь)
            self.evaluator.plot_delta_distribution()
            graph_path = "/Users/papaskakun/PycharmProjects/PythonProject/delta_distribution.png"
            self.notifier.send_photo(graph_path, caption="📈 Распределение значимости пар")

            # Запуск симуляции
            self.simulator.run_batch()
            logging.info("Simulation complete.")
            self.notifier.send_message("🧪 Симуляция завершена.")

        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            self.notifier.send_message(f"❌ Ошибка: {e}")

        elapsed = time.time() - start_time
        self.notifier.send_message(f"⏱ Выполнение заняло: {elapsed / 60:.2f} минут")
        logging.info(f"Pipeline finished in {elapsed / 60:.2f} minutes.")