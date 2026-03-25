import pandas as pd
import os
from pathlib import Path
from typing import Optional

class ParquetStorageEngine:
    """
    Handles high-performance, partitioned storage of financial time-series data.
    Saves data partitioned by Year to allow for lazy-loading during backtests.
    """
    def __init__(self, processed_dir: str = "data/processed"):
        self.processed_dir = Path(processed_dir)
        # Ensure the base directory exists
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def store(self, df: pd.DataFrame, symbol: str, timeframe: str):
        """
        Saves a DataFrame to a partitioned Parquet dataset.
        Data is physically separated into folders like: data/processed/GBPUSD_15m/year=2010/
        """
        if df.empty:
            print(f"Warning: Attempted to store an empty DataFrame for {symbol}.")
            return

        # Ensure index is datetime to extract the year
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be a DatetimeIndex to partition by year.")

        store_df = df.copy()
        
        # Create a temporary 'year' column strictly for partitioning
        store_df['year'] = store_df.index.year

        # Define the specific dataset path (e.g., data/processed/GBPUSD_15m)
        dataset_path = self.processed_dir / f"{symbol}_{timeframe}"

        # Save to parquet using native pandas partitioning
        # engine='pyarrow' is required for partitioned datasets
        store_df.to_parquet(
            dataset_path, 
            engine='pyarrow', 
            partition_cols=['year'],
            index=True
        )
        print(f"Successfully partitioned and stored {symbol} {timeframe} data to {dataset_path}")

    def load(self, symbol: str, timeframe: str, 
             start_year: Optional[int] = None, 
             end_year: Optional[int] = None) -> pd.DataFrame:
        """
        Loads the partitioned data. If years are specified, it strictly reads 
        only those folders, massively saving RAM.
        """
        dataset_path = self.processed_dir / f"{symbol}_{timeframe}"

        if not dataset_path.exists():
            raise FileNotFoundError(f"No partitioned dataset found at {dataset_path}")

        # Build pyarrow filters for lazy loading specific years
        filters = []
        if start_year:
            filters.append(('year', '>=', start_year))
        if end_year:
            filters.append(('year', '<=', end_year))

        # Read the dataset
        if filters:
            df = pd.read_parquet(dataset_path, engine='pyarrow', filters=filters)
        else:
            df = pd.read_parquet(dataset_path, engine='pyarrow')

        # The 'year' column was loaded back from the folder names. 
        # We drop it to return the exact clean OHLCV schema you expect.
        if 'year' in df.columns:
            df.drop(columns=['year'], inplace=True)
            
        # Parquet partitioning sometimes messes with index sorting, so we enforce it
        df.sort_index(inplace=True)

        return df