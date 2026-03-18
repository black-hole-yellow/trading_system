import pandas as pd
import numpy as np

class EventDrivenBacktester:
    """
    The Master Clock. Steps through time chronologically, coordinating
    Data, Alpha, Risk, and Execution to prevent look-ahead bias.
    """
    def __init__(self, data: pd.DataFrame, strategy, portfolio_manager, execution_engine, broker):
        self.data = data
        self.strategy = strategy
        self.pm = portfolio_manager
        self.engine = execution_engine
        self.broker = broker
        self.equity_curve = []
        # Universe assumes a single asset for this specific test loop
        self.symbol = self.strategy.universe[0] 

    def run(self):
        print(f"Generating signals for {len(self.data)} bars...")
        # 1. Generate Alpha (Vectorized for speed, strictly uses past data)
        signals = self.strategy.generate_signals(self.data)
        
        # Extract to fast NumPy arrays for the event loop
        closes = self.data['close'].values
        target_positions = signals['target_position'].values
        timestamps = self.data.index
        
        print("Starting event-driven execution loop...")
        # 2. The Event Loop
        for i in range(len(timestamps)):
            current_price = closes[i]
            current_signal = target_positions[i]
            
            # Step A: Update Market Reality
            self.broker.update_market_data({self.symbol: current_price})
            
            # Step B: Apply Risk & Sizing Constraints
            target_weights = self.pm.generate_target_weights({self.symbol: current_signal})
            
            # Step C: Execute Trades to Match Target
            self.engine.synchronize_portfolio(target_weights)
            
            # Step D: Record State
            self.equity_curve.append({
                'timestamp': timestamps[i],
                'equity': self.broker.get_total_equity()
            })
            
        return self._generate_tear_sheet()

    def _generate_tear_sheet(self):
        """Calculates institutional risk-adjusted performance metrics."""
        df_equity = pd.DataFrame(self.equity_curve).set_index('timestamp')
        df_equity['returns'] = df_equity['equity'].pct_change().fillna(0)
        
        # 15m bars: 4/hr * 24hr * 252 trading days = ~24192 bars per year
        bars_per_year = 24192 
        returns = df_equity['returns']
        
        # Core Metrics
        mean_ret = returns.mean()
        std_ret = returns.std()
        
        sharpe = np.sqrt(bars_per_year) * (mean_ret / std_ret) if std_ret != 0 else 0
        
        downside_returns = returns[returns < 0]
        down_std = downside_returns.std()
        sortino = np.sqrt(bars_per_year) * (mean_ret / down_std) if down_std != 0 else 0
        
        rolling_max = df_equity['equity'].cummax()
        drawdown = df_equity['equity'] / rolling_max - 1.0
        max_dd = drawdown.min()
        
        total_ret = (df_equity['equity'].iloc[-1] / df_equity['equity'].iloc[0]) - 1.0
        
        print("\n=== INSTITUTIONAL TEAR SHEET ===")
        print(f"Total Return:   {total_ret:.2%}")
        print(f"Annual. Sharpe: {sharpe:.2f}")
        print(f"Sortino Ratio:  {sortino:.2f}")
        print(f"Max Drawdown:   {max_dd:.2%}")
        print("================================\n")
        
        return df_equity