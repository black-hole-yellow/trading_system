import pandas as pd
import numpy as np
import time
from typing import Dict, Any
import pytz

from src.portfolio.portfolio_manager import PortfolioManager
from src.execution.execution_engine import ExecutionEngine

class BacktestEngine:
    """
    The Orchestrator (Pillar 5).
    Stitches together Alpha, Risk, and Execution over historical data.
    Generates the final institutional Tear Sheet.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.portfolio_manager = PortfolioManager(config)
        self.execution_engine = ExecutionEngine(config)
        self.initial_capital = self.execution_engine.initial_capital

    def run(self, df: pd.DataFrame, signals: pd.DataFrame, symbol: str):
        print(f"\n--- Starting Execution Simulation for {len(df)} bars ---")
        start_time = time.time()
        
        timestamps = df.index
        closes = df['close'].values
        target_positions = signals['target_position'].values
        # Grab SL and TP arrays
        sl_prices = signals['sl_price'].values
        tp_prices = signals['tp_price'].values
        
        for i in range(len(timestamps)):
            timestamp = timestamps[i]
            current_price = closes[i]
            
            current_prices = {symbol: current_price}
            signal_dict = {symbol: target_positions[i]}
            
            target_weights = self.portfolio_manager.generate_target_weights(signal_dict)
            
            # Pass sl and tp into process_weights
            self.execution_engine.process_weights(
                target_weights, current_prices, timestamp, sl_prices[i], tp_prices[i]
            )

        print(f"Simulation completed in {round(time.time() - start_time, 2)}s")
        return self._generate_tearsheet()

    def _generate_tearsheet(self):
        trades = pd.DataFrame(self.execution_engine.trade_log)
        equity = pd.DataFrame(self.execution_engine.equity_curve).set_index("timestamp")
        
        final_equity = equity['equity'].iloc[-1] if not equity.empty else self.initial_capital
        total_return_pct = ((final_equity / self.initial_capital) - 1) * 100
        
        rolling_max = equity['equity'].cummax()
        drawdown = (equity['equity'] - rolling_max) / rolling_max
        max_drawdown_pct = drawdown.min() * 100 if not drawdown.empty else 0.0

        # === ROUND TRIP TRADE BUILDER ===
        kyiv_tz = pytz.timezone('Europe/Kyiv')
        closed_trades = []
        
        if not trades.empty:
            # Pair every Entry (i-1) with its Exit (i)
            for i in range(1, len(trades), 2):
                if i < len(trades):
                    entry = trades.iloc[i-1]
                    exit_trade = trades.iloc[i]
                    
                    # Convert timestamps safely to Kyiv time for the CSV
                    def to_kyiv(ts):
                        if ts.tz is None: 
                            return ts.tz_localize('UTC').tz_convert(kyiv_tz)
                        return ts.tz_convert(kyiv_tz)
                        
                    entry_time = to_kyiv(entry['timestamp'])
                    exit_time = to_kyiv(exit_trade['timestamp'])
                    
                    if entry['action'] == 'BUY':
                        pnl = (exit_trade['fill_price'] - entry['fill_price']) * entry['units']
                        direction = 'Long'
                    else:
                        pnl = (entry['fill_price'] - exit_trade['fill_price']) * entry['units']
                        direction = 'Short'
                        
                    commission = entry['commission'] + exit_trade['commission']
                    net_pnl = pnl - commission
                    result = 'Win' if net_pnl > 0 else 'Loss'
                    
                    closed_trades.append({
                        'Entry Time (Kyiv)': entry_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'Exit Time (Kyiv)': exit_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'Symbol': entry['symbol'],
                        'Direction': direction,
                        'Units': entry['units'],
                        'Entry Price': entry['fill_price'],
                        'Exit Price': exit_trade['fill_price'],
                        'Stop Loss (SL)': entry.get('sl_price'),
                        'Take Profit (TP)': entry.get('tp_price'),
                        'Commission': commission,
                        'Net PnL': round(net_pnl, 2),
                        'Result': result
                    })
                    
            closed_trades_df = pd.DataFrame(closed_trades)
            
            winning_trades = len(closed_trades_df[closed_trades_df['Net PnL'] > 0])
            total_trades = len(closed_trades_df)
            win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
            
            gross_profit = closed_trades_df[closed_trades_df['Net PnL'] > 0]['Net PnL'].sum()
            gross_loss = abs(closed_trades_df[closed_trades_df['Net PnL'] <= 0]['Net PnL'].sum())
            profit_factor = gross_profit / gross_loss if gross_loss != 0 else float('inf')
        else:
            closed_trades_df = pd.DataFrame()
            total_trades = 0
            win_rate = 0.0
            profit_factor = 0.0

        tearsheet = {
            "Initial Capital": f"${self.initial_capital:,.2f}",
            "Final Capital": f"${final_equity:,.2f}",
            "Net Return": f"{total_return_pct:.2f}%",
            "Max Drawdown": f"{max_drawdown_pct:.2f}%",
            "Total Trades": total_trades,
            "Win Rate": f"{win_rate:.1f}%",
            "Profit Factor": f"{profit_factor:.2f}"
        }
        
        # We now return the cleanly formatted 'closed_trades_df' instead of the raw transactions
        return tearsheet, closed_trades_df, equity