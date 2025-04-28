import pandas as pd
from matplotlib import pyplot as plt
from sklearn.linear_model import LinearRegression
import os


class PairTradingSimulator:
    def __init__(self, data_dir, save_dir="report_data"):
        self.data_dir = data_dir
        self.save_dir = save_dir
        self.beta = None
        self.trades_df = None

        os.makedirs(self.save_dir, exist_ok=True)
        print(f"📁 Папка для данных создана: {self.save_dir}")

    def load_data(self, symbol1, symbol2):
        """Загрузка данных из директории по названию символов."""
        path1 = os.path.join(self.data_dir, f"{symbol1}_h1.csv")
        path2 = os.path.join(self.data_dir, f"{symbol2}_h1.csv")

        df1 = pd.read_csv(path1, parse_dates=['timestamp'])
        df2 = pd.read_csv(path2, parse_dates=['timestamp'])

        return df1, df2

    def simulate(self, symbol1, symbol2, z_entry=-2, z_exit=0, beta_window=60, z_window=30):
        """Основной метод запуска симуляции торговли."""
        self.symbol1 = symbol1
        self.symbol2 = symbol2
        df1, df2 = self.load_data(symbol1, symbol2)

        merged = pd.merge(
            df1[['timestamp', 'close']],
            df2[['timestamp', 'close']],
            on='timestamp',
            suffixes=(f'_{symbol1}', f'_{symbol2}')
        )
        merged = merged.sort_values('timestamp').dropna()

        self.merged_df = merged  # <-- ВАЖНО: сохраняем объединённый датафрейм

        X = merged[[f'close_{symbol2}']]
        y = merged[f'close_{symbol1}']
        reg = LinearRegression().fit(X, y)
        self.beta = reg.coef_[0]

        merged['spread'] = merged[f'close_{symbol1}'] - self.beta * merged[f'close_{symbol2}']
        merged['zscore'] = (merged['spread'] - merged['spread'].rolling(z_window).mean()) / merged['spread'].rolling(
            z_window).std()

        in_position = False
        positions = []
        entry_idx = None

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
                b_pnl = -(b_exit - b_entry) * self.beta
                total_pnl = a_pnl + b_pnl

                a_pnl_pct = a_pnl / a_entry * 100
                b_pnl_pct = b_pnl / (b_entry * self.beta) * 100
                total_pnl_pct = a_pnl_pct + b_pnl_pct

                spread_series = merged['spread'].iloc[entry_idx:exit_idx + 1]
                max_dd = (spread_series - spread_series.cummax()).min()

                positions.append({
                    "Entry Time": merged['timestamp'].iloc[entry_idx],
                    "Exit Time": merged['timestamp'].iloc[exit_idx],
                    "Duration (hours)": (merged['timestamp'].iloc[exit_idx] - merged['timestamp'].iloc[
                        entry_idx]).total_seconds() / 3600,
                    f"{symbol1} Entry": a_entry,
                    f"{symbol1} Exit": a_exit,
                    f"{symbol2} Entry": b_entry,
                    f"{symbol2} Exit": b_exit,
                    f"{symbol1} PnL %": round(a_pnl_pct, 2),
                    f"{symbol2} PnL %": round(b_pnl_pct, 2),
                    "Total PnL %": round(total_pnl_pct, 2),
                    "Total PnL $": round(total_pnl, 2),
                    "Max Drawdown Spread": round(max_dd, 2)
                })
                in_position = False

        self.trades_df = pd.DataFrame(positions)

    def report(self):
        """Человекочитаемый отчет + график торговли."""
        if self.trades_df is None or self.trades_df.empty:
            print("No trades executed.")
        else:
            report_lines = []

            report_lines.append("\n--- Pair Trading Simulation Report ---")
            print("\n--- Pair Trading Simulation Report ---")
            report_lines.append(self.trades_df.to_string(index=False))
            print(self.trades_df.to_string(index=False))

            total_return_dollars = self.trades_df["Total PnL $"].sum()
            avg_trade_return_pct = self.trades_df["Total PnL %"].mean()
            total_return_pct = self.trades_df["Total PnL %"].sum()
            win_rate_pct = (self.trades_df["Total PnL %"] > 0).mean() * 100

            report_lines.append(f"\nTotal Return ($): {total_return_dollars:.2f}")
            print(f"\nTotal Return ($): {total_return_dollars:.2f}")
            report_lines.append(f"Average Trade Return (%): {avg_trade_return_pct:.2f}")
            print(f"Average Trade Return (%): {avg_trade_return_pct:.2f}")
            report_lines.append(f"Total Return (%): {total_return_pct:.2f}")
            print(f"Total Return (%): {total_return_pct:.2f}")
            report_lines.append(f"Win Rate (% of profitable trades): {win_rate_pct:.2f}%")
            print(f"Win Rate (% of profitable trades): {win_rate_pct:.2f}%")
            report_lines.append(f"Beta (hedge ratio) used: {self.beta:.4f}")
            print(f"Beta (hedge ratio) used: {self.beta:.4f}")
            report_lines.append(f"Number of Trades: {len(self.trades_df)}")
            print(f"Number of Trades: {len(self.trades_df)}")

            # Объединяем все строки
            report_text = "\n".join(report_lines)

            # Сохраняем в файл
            report_filename = os.path.join(self.save_dir, f"report_{self.symbol1}_{self.symbol2}.txt")
            with open(report_filename, "w") as f:
                f.write(report_text)
            print(f"\nReport saved as {report_filename}")

            # --- График спреда и сделок ---
            if self.merged_df is not None:
                plt.figure(figsize=(14, 7))
                plt.plot(self.merged_df['timestamp'], self.merged_df['spread'], label="Spread", color='blue')
                plt.plot(self.merged_df['timestamp'], self.merged_df['spread'].rolling(30).mean(),
                         label="Spread Moving Avg", color='orange')

                # Рисуем точки входа/выхода
                for idx, trade in self.trades_df.iterrows():
                    plt.axvline(trade['Entry Time'], color='green', linestyle='--', alpha=0.6)
                    plt.axvline(trade['Exit Time'], color='red', linestyle='--', alpha=0.6)

                plt.title("Spread with Entry and Exit Points")
                plt.xlabel("Timestamp")
                plt.ylabel("Spread Value")
                plt.legend()
                plt.grid(True)
                # plt.show()

                # Сохраняем в файл
                pair_name = self.symbol1+self.symbol2
                graph_filename = os.path.join(self.save_dir,f"{pair_name}pair_trade_report.png")
                plt.savefig(graph_filename)
                plt.close()