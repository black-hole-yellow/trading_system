import json
import sys
import os
from pathlib import Path

# Force Python to recognize the project structure
root = Path(__file__).resolve().parent
sys.path.insert(0, str(root))

from src.data.storage_engine import ParquetStorageEngine
from src.alpha.weekly_continuation import WeeklyContinuationStrategy
from src.backtest.engine import BacktestEngine
from src.backtest.visualizer import TearsheetVisualizer 

def run():
    print("=== QUANT LAB ALPHA PIPELINE ===")
    
    # 1. Setup Output Directory
    output_dir = root / "outputs"
    output_dir.mkdir(exist_ok=True) # Creates the folder if it doesn't exist
    
    # 2. Load Hypothesis
    hypothesis_path = root / "config" / "hypotheses" / "weekly_level_continuation.json"
    with open(hypothesis_path, 'r') as f:
        config = json.load(f)
        
    symbol = config['universe']['instruments'][0]
    hypo_id = config['metadata']['hypothesis_id']
    hypo_name = config['metadata']['name']
    print(f"Loaded Hypothesis: {hypo_name}")

    # 3. Load Data
    print("\n[Pillar 1] Fetching Data...")
    processed_dir = root / "data" / "processed"
    storage = ParquetStorageEngine(processed_dir=str(processed_dir))
    df = storage.load(symbol=symbol, timeframe="15m", start_year=2000, end_year=2026)

    # 4. Generate Signals
    print("\n[Pillar 2] Generating Alpha Signals...")
    strategy = WeeklyContinuationStrategy(config)
    signals = strategy.generate_signals(df)

    # 5. Run Backtest (Now returns 3 items)
    print("\n[Pillar 3 & 4] Applying Risk & Execution...")
    engine = BacktestEngine(config)
    tearsheet, trades_df, equity_df = engine.run(df, signals, symbol)

    # 6. Print Final Results to Console
    print("\n======================================")
    print("        INSTITUTIONAL TEAR SHEET      ")
    print("======================================")
    for metric, value in tearsheet.items():
        print(f"{metric:<25}: {value}")
    print("======================================")

    # 7. EXPORT DATA & VISUALS
    print("\n[Exporting Results]...")
    
    # Export CSV
    csv_path = output_dir / f"{hypo_id}_trades.csv"
    if not trades_df.empty:
        trades_df.to_csv(csv_path, index=False)
        print(f"-> Trade log saved to: {csv_path}")
    else:
        print("-> No trades generated. CSV skipped.")

    # Export Dashboard
    png_path = output_dir / f"{hypo_id}_dashboard.png"
    TearsheetVisualizer.generate_dashboard(equity_df, tearsheet, hypo_name, png_path)

if __name__ == "__main__":
    run()