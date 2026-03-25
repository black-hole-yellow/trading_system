import pandas as pd
import time
import os
from pathlib import Path

# Import our Pillar 1 modules
from data_validator import DataValidator
from time_manager import TimeManager
from storage_engine import ParquetStorageEngine

def run_pipeline(raw_file_path: str, symbol: str, source_tz: str = 'US/Eastern'):
    print(f"--- Starting Data Pipeline for {symbol} ---")
    start_time = time.time()

    # 1. Extraction with custom Tab-Separated logic
    print(f"1. Loading raw data from {raw_file_path}...")
    try:
        # Your data has: No Header, Tab Separated, and No Volume column.
        df = pd.read_csv(
            raw_file_path, 
            sep='\t', 
            header=None, 
            names=['timestamp', 'open', 'high', 'low', 'close'],
            index_col='timestamp',
            parse_dates=True
        )
        
        # Add dummy volume column as our validator expects OHLCV
        if 'volume' not in df.columns:
            df['volume'] = 0
            
    except Exception as e:
        print(f"Error loading file: {e}")
        return

    print(f"   -> Found {len(df)} rows.")
    print(f"   -> Detected Date Range: {df.index.min()} to {df.index.max()}")

    # 2. Validation
    validator = DataValidator(max_pct_change=0.05, timeframe_minutes=15)
    clean_df, health_report = validator.validate(df, symbol=symbol)
    
    # 3. Time Normalization
    time_manager = TimeManager(target_tz='UTC')
    utc_df = time_manager.normalize(clean_df, source_tz=source_tz)

    # 4. Storage - Dynamically put it in the "processed" folder next to "raw"
    raw_path_obj = Path(raw_file_path)
    processed_dir = raw_path_obj.parent.parent / "processed"
    
    print(f"\n4. Saving to Partitioned Parquet Engine at: {processed_dir}")
    storage = ParquetStorageEngine(processed_dir=str(processed_dir))
    storage.store(utc_df, symbol=symbol, timeframe="15m")

    print(f"\n=== PIPELINE COMPLETE in {round(time.time() - start_time, 2)}s ===")

if __name__ == "__main__":
    import os
    
    print("Auto-searching for gbpusd_data.csv...")
    RAW_FILE = None
    
    # Start searching from the current working directory all the way down
    for root_dir, dirs, files in os.walk(os.getcwd()):
        if 'gbpusd_data.csv' in files:
            RAW_FILE = os.path.join(root_dir, 'gbpusd_data.csv')
            break
            
    # Fallback: Search from the script's directory upwards
    if not RAW_FILE:
        script_dir = Path(__file__).resolve().parent
        for root_dir, dirs, files in os.walk(script_dir.parent.parent.parent):
            if 'gbpusd_data.csv' in files:
                RAW_FILE = os.path.join(root_dir, 'gbpusd_data.csv')
                break

    if not RAW_FILE:
        print("CRITICAL ERROR: Could not find 'gbpusd_data.csv' anywhere.")
    else:
        print(f"SUCCESS: Found file at {RAW_FILE}")
        run_pipeline(raw_file_path=RAW_FILE, symbol="GBPUSD")