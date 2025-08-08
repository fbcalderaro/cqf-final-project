# trading_system/utils/indicators.py

import pandas as pd
import pandas_ta as ta

def add_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a comprehensive set of technical indicators to the DataFrame.
    This function can be used by multiple strategies to pre-calculate indicators.
    
    Args:
        df (pd.DataFrame): DataFrame with OHLCV data.

    Returns:
        pd.DataFrame: DataFrame with added indicator columns.
    """
    # Use pandas-ta's strategy feature to append multiple indicators
    # This is a flexible way to add many indicators at once.
    # We can define custom strategies here as needed.
    CustomStrategy = ta.Strategy(
        name="All Indicators",
        description="A collection of common indicators",
        ta=[
            {"kind": "sma", "length": 20},
            {"kind": "sma", "length": 50},
            {"kind": "sma", "length": 200},
            {"kind": "rsi", "length": 14},
            {"kind": "bbands", "length": 20, "std": 2},
            {"kind": "atr", "length": 14},
            {"kind": "macd"},
            {"kind": "adx", "length": 14},
        ]
    )
    
    # Append the indicators to the DataFrame
    df.ta.strategy(CustomStrategy)
    
    # Rename columns for consistency if needed (pandas-ta has a standard naming)
    # Example: df.rename(columns={'SMA_20': 'sma_short'}, inplace=True)
    
    return df
