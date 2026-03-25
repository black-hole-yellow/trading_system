import pandas as pd
from pathlib import Path
import sys
import time

# This ensures Python can find the 'src' folder regardless of where you run the script
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.data.storage_engine import ParquetStorageEngine
from src.alpha.asian_breakout import AsianSessionBreakout

def run_alpha_test():
    print("--- Testing Pillar 2: Alpha Generation ---")
    start_time = time.time()

    # 1. Load Data (Testing Pillar 1 Integration)
    print("\n1. Loading 2010 Data from Partitioned Storage...")
    project_root = Path(__file__).resolve().parent.parent
    processed_dir = project_root / "data" / "processed"
    
    storage = ParquetStorageEngine(processed_dir=str(processed_dir))
    
    try:
        # We only load 2010 to keep the test lightning fast
        df = storage.load(symbol="GBPUSD", timeframe="15m", start_year=2010, end_year=2010)
        print(f"   -> Successfully loaded {len(df)} rows.")
    except Exception as e:
        print(f"   -> ERROR loading data: {e}")
        return

    # 2. Configure Strategy
    print("\n2. Initializing Strategy with MTF Trend Filter...")
    config = {
        "universe": ["GBPUSD"],
        "parameters": {
            "asian_start_hour": 23,
            "asian_end_hour": 7,
            "exit_hour": 21,
            "use_trend_filter": True,     # Turning ON the new 4H filter
            "trend_timeframe": "4h"
        }
    }
    strategy = AsianSessionBreakout(config)

    # 3. Generate Signals
    print("\n3. Generating Signals...")
    signals = strategy.generate_signals(df)

    # 4. Validate Output
    print("\n=== TEST RESULTS ===")
    print("Signal Distribution (Total 15m Bars):")
    print(signals['target_position'].value_counts())

    print("\nSample of Active Trades (Where system wants to be in the market):")
    active_trades = signals[signals['target_position'] != 0.0]
    
    if not active_trades.empty:
        # Show a slice where trades are happening
        print(active_trades.head(10))
    else:
        print("No trades generated in this period.")
        
    print(f"\nTest completed in {round(time.time() - start_time, 2)} seconds.")

if __name__ == "__main__":
    run_alpha_test()