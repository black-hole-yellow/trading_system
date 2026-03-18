import pandas as pd
from src.alpha.asian_breakout import AsianSessionBreakout
from src.portfolio.portfolio_manager import PortfolioManager
from src.execution.paper_broker import PaperBroker
from src.execution.execution_engine import ExecutionEngine
from src.backtest.event_driven_backtester import EventDrivenBacktester

def main():
    # Pillar 1: Data
    print("Loading Data...")
    df_15m = pd.read_parquet('data/processed/gbpusd_15m.parquet')
    
    # Configuration
    alpha_config = {"universe": ["GBPUSD"], "parameters": {"asian_start_hour": 23, "asian_end_hour": 7}}
    risk_config = {"max_gross_leverage": 3.0, "max_net_usd_exposure": 1.5, "usd_exposure_map": {"GBPUSD": -1}}
    
    # Initialize Pillars 2, 3, 4
    strategy = AsianSessionBreakout(alpha_config)
    pm = PortfolioManager(risk_config)
    
    # 1.0 bps slippage per trade
    broker = PaperBroker(initial_cash=100000.0, slippage_bps=1.0) 
    engine = ExecutionEngine(broker)
    
    # Pillar 5: Backtest
    backtester = EventDrivenBacktester(df_15m, strategy, pm, engine, broker)
    equity_df = backtester.run()

if __name__ == "__main__":
    main()