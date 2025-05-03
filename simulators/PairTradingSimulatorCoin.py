import os
import pandas as pd
from matplotlib import pyplot as plt
from sklearn.linear_model import LinearRegression
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

class PairTradingSimulatorCoin:
    def __init__(self, data_dir, pairs_file, save_dir="report_data_coin_method"):
        self.data_dir = data_dir
        self.pairs_file = pairs_file
        self.save_dir = save_dir
        os.makedirs(self.save_dir, exist_ok=True)
        print(f"📁 Папка для результатов: {self.save_dir}")

    def load_data(self, symbol1, symbol2):
        path1 = os.path.join(self.data_dir, f"{symbol1}_h1.csv")
        path2 = os.path.join(self.data_dir, f"{symbol2}_h1.csv")

        df1 = pd.read_csv(path1, parse_dates=['timestamp'])
        df2 = pd.read_csv(path2, parse_dates=['timestamp'])
        return df1, df2

    def simulate_pair(self, symbol1, symbol2, z_entry=-2, z_exit=0, z_window=30):
        try:
            df1, df2 = self.load_data(symbol1, symbol2)
            merged = pd.merge(
                df1[['timestamp', 'close']],
                df2[['timestamp', 'close']],
                on='timestamp',
                suffixes=(f'_{symbol1}', f'_{symbol2}')
            ).dropna().sort_values('timestamp')

            X = merged[[f'close_{symbol2}']]
            y = merged[f'close_{symbol1}']
            reg = LinearRegression().fit(X, y)
            beta = reg.coef_[0]

            merged['spread'] = y - beta * X.squeeze()
            merged['zscore'] = (merged['spread'] - merged['spread'].rolling(z_window).mean()) / merged['spread'].rolling(z_window).std()

            in_position = False
            entry_idx = None
            trades = []

            for i in range(z_window, len(merged)):
                z = merged['zscore'].iloc[i]
                if not in_position and z < z_entry:
                    in_position = True
                    entry_idx = i
                elif in_position and z >= z_exit:
                    exit_idx = i
                    a_entry = merged[f'close_{symbol1}'].iloc[entry_idx]
                    a_exit = merged[f'close_{symbol1}'].iloc[exit_idx]
                    b_entry = merged[f'close_{symbol2}'].iloc[entry_idx]
                    b_exit = merged[f'close_{symbol2}'].iloc[exit_idx]

                    a_pnl = a_exit - a_entry
                    b_pnl = -(b_exit - b_entry) * beta
                    total_pnl = a_pnl + b_pnl

                    trades.append({
                        "entry": merged['timestamp'].iloc[entry_idx],
                        "exit": merged['timestamp'].iloc[exit_idx],
                        "total_pnl": total_pnl
                    })
                    in_position = False

            if trades:
                total = sum(t['total_pnl'] for t in trades)
                avg = total / len(trades)
                win_rate = sum(t['total_pnl'] > 0 for t in trades) / len(trades)
                return {
                    "pair": f"{symbol1}_{symbol2}",
                    "trades": len(trades),
                    "total_pnl": round(total, 2),
                    "avg_pnl": round(avg, 2),
                    "win_rate": round(win_rate * 100, 2),
                    "beta": round(beta, 4)
                }
            else:
                return {"pair": f"{symbol1}_{symbol2}", "trades": 0, "total_pnl": 0.0, "avg_pnl": 0.0, "win_rate": 0.0, "beta": beta}

        except Exception as e:
            return {"pair": f"{symbol1}_{symbol2}", "error": str(e)}

    def run_batch(self):
        pairs_df = pd.read_csv(self.pairs_file)
        results = []

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(
                    self.simulate_pair,
                    row['asset_a'].replace('_h1', ''),
                    row['asset_b'].replace('_h1', '')
                )
                for _, row in pairs_df.iterrows()
            ]

            for future in tqdm(as_completed(futures), total=len(futures), desc="Simulating pairs"):
                results.append(future.result())

        # Исключаем результаты с ошибками
        filtered_results = [res for res in results if 'error' not in res and 'total_pnl' in res]

        if not filtered_results:
            print("⚠️ Нет корректных результатов. Проверь данные.")
            return

        results_df = pd.DataFrame(filtered_results)
        valid_results = results_df[results_df['trades'] > 0]

        if valid_results.empty:
            print("⚠️ Нет пар с выполненными сделками. Ничего не сохранено.")
            return

        avg_total_pnl = valid_results['total_pnl'].mean()
        print(f"📊 Средняя доходность по всем парам: {avg_total_pnl:.2f}")

        filtered_df = valid_results[valid_results['total_pnl'] >= avg_total_pnl]

        filtered_df.to_csv(os.path.join(self.save_dir, "pair_trading_results.csv"), index=False)
        print("✅ Отфильтрованные результаты сохранены в", os.path.join(self.save_dir, "pair_trading_results.csv"))

if __name__ == "__main__":
    sim = PairTradingSimulatorCoin(
        data_dir="/Users/papaskakun/PycharmProjects/PythonProject/futures_data",
        pairs_file="/Users/papaskakun/PycharmProjects/PythonProject/top_1pct_significant_pairs.csv"
    )
    sim.run_batch()
