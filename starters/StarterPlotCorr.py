from analyzers.CointegrationAnalyzerAsync import CointegrationAnalyzerAsync
from collectors.FuturesDataCollector import FuturesDataCollector
from analyzers.CorrAnalyzer import CorrAnalyzer
from plotters.CorrSpreadPlotter import PairSpreadPlotter

if __name__ == "__main__":
    '''
    Что происходит в коде в целом?

    🔹 Этот скрипт находит пары монет, которые обычно ведут себя похоже (высокая корреляция), 
                               но в данный момент сильно разошлись по цене (высокий Z-score).
    🔹 Это основа парного трейдинга (pair trading) — типа стратегии арбитража.
    '''


    # Можно нарисовать спред по паре:
    # !!!! Важно тут надо подавать название монеты именно без  _h1, так как словать хранил данные в формате :
    # Ключ : Значение , где ключ Название Монеты, а значение датасет
    plotter = PairSpreadPlotter(data_dir="futures_data")

    # Построить спред по любой паре:
    plotter.plot_pair_spread('APEUSDT', 'ACXUSDT')

