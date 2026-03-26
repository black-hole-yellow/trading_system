import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from pathlib import Path

class TearsheetVisualizer:
    """
    Generates institutional-grade visual dashboards from backtest results.
    """
    @staticmethod
    def generate_dashboard(equity_df: pd.DataFrame, metrics: dict, hypothesis_name: str, save_path: Path):
        # 1. Calculate Drawdown Series
        rolling_max = equity_df['equity'].cummax()
        drawdown = (equity_df['equity'] - rolling_max) / rolling_max * 100

        # 2. Set up the plotting grid (2 rows, 1 column)
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})
        fig.suptitle(f"Backtest Tearsheet: {hypothesis_name}", fontsize=16, fontweight='bold')

        # --- Top Panel: Equity Curve ---
        ax1.plot(equity_df.index, equity_df['equity'], color='#2ca02c', linewidth=1.5, label='Portfolio Equity')
        ax1.fill_between(equity_df.index, equity_df['equity'], equity_df['equity'].iloc[0], 
                         where=(equity_df['equity'] >= equity_df['equity'].iloc[0]), color='#2ca02c', alpha=0.1)
        ax1.fill_between(equity_df.index, equity_df['equity'], equity_df['equity'].iloc[0], 
                         where=(equity_df['equity'] < equity_df['equity'].iloc[0]), color='#d62728', alpha=0.1)
        
        ax1.set_ylabel("Equity ($)", fontsize=12)
        ax1.grid(True, linestyle='--', alpha=0.6)
        ax1.legend(loc="upper left")

        # Overlay text metrics on the top chart
        metrics_text = "\n".join([f"{k}: {v}" for k, v in metrics.items()])
        props = dict(boxstyle='round', facecolor='white', alpha=0.8, edgecolor='gray')
        ax1.text(0.02, 0.05, metrics_text, transform=ax1.transAxes, fontsize=10,
                 verticalalignment='bottom', bbox=props, family='monospace')

        # --- Bottom Panel: Drawdown ---
        ax2.plot(drawdown.index, drawdown, color='#d62728', linewidth=1.2, label='Drawdown (%)')
        ax2.fill_between(drawdown.index, drawdown, 0, color='#d62728', alpha=0.3)
        ax2.set_ylabel("Drawdown (%)", fontsize=12)
        ax2.grid(True, linestyle='--', alpha=0.6)
        ax2.legend(loc="lower left")

        # Formatting dates on X-axis
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        fig.autofmt_xdate()

        # 3. Save the image
        plt.tight_layout()
        plt.savefig(save_path, dpi=300)
        plt.close()
        print(f"Visual dashboard saved to: {save_path}")