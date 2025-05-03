import os
import pandas as pd
import matplotlib.pyplot as plt

class CoinReportPlotter:
    def __init__(self, results_csv,
                 output_dir="/Users/papaskakun/PycharmProjects/PythonProject/report_data_coin_method",
                 top_n=10
                 ):
        self.results_csv = results_csv
        self.output_dir = output_dir
        self.output_path = os.path.join(self.output_dir, "report_summary.png")
        self.top_n = top_n

        if not os.path.exists(self.results_csv):
            raise FileNotFoundError(f"Файл результатов не найден: {self.results_csv}")

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def generate(self):
        df = pd.read_csv(self.results_csv)

        if df.empty:
            print("❌ Файл результатов пуст.")
            return

        df = df[df['trades'] > 0]
        if df.empty:
            print("❗ Нет пар с выполненными сделками.")
            return

        df = df.sort_values('total_pnl', ascending=False)

        plt.figure(figsize=(16, 8))
        plt.bar(df['pair'], df['total_pnl'], color='skyblue')
        plt.axhline(y=df['total_pnl'].mean(), color='red', linestyle='--', label='Средняя доходность')
        plt.title("📈 Парный трейдинг: доходность по парам", fontsize=16)
        plt.xlabel("Пары", fontsize=12)
        plt.ylabel("Total PnL $", fontsize=12)
        plt.xticks(rotation=90, fontsize=8)
        plt.legend()

        for idx in df.head(self.top_n).index:
            plt.text(idx, df.loc[idx, 'total_pnl'], f"{df.loc[idx, 'total_pnl']:.2f}", ha='center', va='bottom', fontsize=8, color='green')
        for idx in df.tail(self.top_n).index:
            plt.text(idx, df.loc[idx, 'total_pnl'], f"{df.loc[idx, 'total_pnl']:.2f}", ha='center', va='top', fontsize=8, color='red')

        plt.tight_layout()
        plt.savefig(self.output_path)
        plt.close()
        print(f"✅ PNG-отчёт сохранён: {self.output_path}")



if __name__ == "__main__":
    '''
    Основной график: гистограмма доходности (Total PnL) по каждой паре
	•	X-ось — названия пар (asset_a_asset_b), например USDEUSDT_ETHBTCUSDT.
	•	Y-ось — суммарная прибыль или убыток по каждой паре (total_pnl), выраженная в долларах.
	•	Столбцы:
	•	Цвет: небесно-голубой (skyblue);
	•	Высота = суммарный PnL (результат всех совершённых сделок по этой паре).
	
	Горизонтальная линия: средняя доходность по всем парам
	•	Красная пунктирная линия (--);
	•	Показывает ориентир: выше неё — пары с выше-средней доходностью, ниже — хуже средней.

    Подписи к столбцам:
	•	Зелёным подписаны топ-N прибыльных пар;
	•	Красным подписаны топ-N убыточных пар;
	•	В подписи указано значение total_pnl в формате +12.45 или -8.22.
    '''
    csv_path = "/Users/papaskakun/PycharmProjects/PythonProject/report_data_coin_method/pair_trading_results.csv"
    report = CoinReportPlotter(csv_path)
    report.generate()