import polars as pl
import pandas as pd
import db_utils
from common import log
import config # Import the config module

def _calculate_supertrend_pandas(df: pd.DataFrame, period: int, multiplier: float) -> pd.Series:
    """
    Calculates a Supertrend series using pandas due to its iterative nature.
    """
    atr_col_name = f'st_atr_{period}'
    # Ensure input columns are float for calculation
    df['high_price'] = df['high_price'].astype(float)
    df['low_price'] = df['low_price'].astype(float)
    df['close_price'] = df['close_price'].astype(float)
    df['true_range'] = df['true_range'].astype(float)
    
    df[atr_col_name] = df['true_range'].ewm(alpha=1/period, adjust=False).mean()

    df['upper_band'] = (df['high_price'] + df['low_price']) / 2 + multiplier * df[atr_col_name]
    df['lower_band'] = (df['high_price'] + df['low_price']) / 2 - multiplier * df[atr_col_name]
    df['supertrend_dir'] = 1

    for i in range(1, len(df)):
        if df.loc[i-1, 'supertrend_dir'] == 1:
            if df.loc[i, 'close_price'] < df.loc[i-1, 'lower_band']:
                df.loc[i, 'supertrend_dir'] = -1
            else:
                df.loc[i, 'supertrend_dir'] = 1
                df.loc[i, 'lower_band'] = max(df.loc[i, 'lower_band'], df.loc[i-1, 'lower_band'])
        else:
            if df.loc[i, 'close_price'] > df.loc[i-1, 'upper_band']:
                df.loc[i, 'supertrend_dir'] = 1
            else:
                df.loc[i, 'supertrend_dir'] = -1
                df.loc[i, 'upper_band'] = min(df.loc[i, 'upper_band'], df.loc[i-1, 'upper_band'])
    
    return df['supertrend_dir']


def calculate_indicators(df: pl.DataFrame) -> pl.DataFrame:
    """Calculates technical analysis indicators using pure Polars expressions."""
    log.info("Calculating technical indicators...")

    # --- Use strategy parameters from config ---
    strategy_params = config.STRATEGY_CONFIG
    rsi_period = strategy_params.getint('rsi_period')
    atr_period = strategy_params.getint('atr_period')
    adx_period = strategy_params.getint('adx_period')
    st_period = strategy_params.getint('supertrend_period')
    st_multiplier = strategy_params.getfloat('supertrend_multiplier')
    
    alpha = 1 / adx_period

    df_with_indicators = df.with_columns(
        delta=pl.col("close_price").diff(),
        up=pl.col("high_price").diff(),
        down=-(pl.col("low_price").diff()),
    ).with_columns(
        gain=pl.when(pl.col("delta") > 0).then(pl.col("delta")).otherwise(0),
        loss=pl.when(pl.col("delta") < 0).then(-pl.col("delta")).otherwise(0),
    ).with_columns(
        avg_gain_rsi=pl.col("gain").ewm_mean(alpha=1/rsi_period, adjust=False),
        avg_loss_rsi=pl.col("loss").ewm_mean(alpha=1/rsi_period, adjust=False),
    ).with_columns(
        rs_rsi=pl.col("avg_gain_rsi") / pl.col("avg_loss_rsi"),
    ).with_columns(
        (pl.lit(100.0) - (pl.lit(100.0) / (pl.lit(1.0) + pl.col("rs_rsi")))).alias(f"rsi_{rsi_period}"),
    ).with_columns(
        true_range=pl.max_horizontal([
            (pl.col("high_price") - pl.col("low_price")),
            (pl.col("high_price") - pl.col("close_price").shift(1)).abs(),
            (pl.col("low_price") - pl.col("close_price").shift(1)).abs()
        ])
    ).with_columns(
        pl.col("true_range").ewm_mean(alpha=1/atr_period, adjust=False).alias(f"atr_{atr_period}"),
    ).with_columns(
        plus_dm=pl.when((pl.col("up") > pl.col("down")) & (pl.col("up") > 0)).then(pl.col("up")).otherwise(0),
        minus_dm=pl.when((pl.col("down") > pl.col("up")) & (pl.col("down") > 0)).then(pl.col("down")).otherwise(0),
    ).with_columns(
        plus_dm_smoothed=pl.col("plus_dm").ewm_mean(alpha=alpha, adjust=False),
        minus_dm_smoothed=pl.col("minus_dm").ewm_mean(alpha=alpha, adjust=False),
        tr_smoothed=pl.col("true_range").ewm_mean(alpha=alpha, adjust=False),
    ).with_columns(
        plus_di=pl.lit(100) * (pl.col("plus_dm_smoothed") / pl.col("tr_smoothed")),
        minus_di=pl.lit(100) * (pl.col("minus_dm_smoothed") / pl.col("tr_smoothed")),
    ).with_columns(
        dx=pl.when((pl.col("plus_di") + pl.col("minus_di")) == 0)
                 .then(0)
                 .otherwise(pl.lit(100) * (pl.col("plus_di") - pl.col("minus_di")).abs() / (pl.col("plus_di") + pl.col("minus_di"))),
    ).with_columns(
        pl.col("dx").ewm_mean(alpha=alpha, adjust=False).alias(f"adx_{adx_period}")
    )

    pandas_df = df_with_indicators.to_pandas()
    st_dir_col_name = f'supertrend_{st_period}_{str(st_multiplier).replace(".", "_")}_dir'
    st_dir = _calculate_supertrend_pandas(pandas_df.copy(), period=st_period, multiplier=st_multiplier)
    pandas_df[st_dir_col_name] = st_dir
    df_with_indicators = pl.from_pandas(pandas_df)
    
    # --- BUG FIX: Select the correct, dynamically named columns ---
    indicators_df = df_with_indicators.select(
        pl.col("open_time"), 
        pl.col(f"rsi_{rsi_period}"),
        pl.col(f"atr_{atr_period}"),
        pl.col(f"adx_{adx_period}"),
        pl.col("plus_di").alias(f"plus_di_{adx_period}"),
        pl.col("minus_di").alias(f"minus_di_{adx_period}"),
        pl.col(st_dir_col_name)
    )
    
    log.info("✅ Indicators calculated.")
    return indicators_df.drop_nulls()

def main():
    """Main function to run the indicator calculation and saving process."""
    log.info("--- Starting Indicator Calculation Process ---")
    
    candles_df = db_utils.fetch_candles_as_polars_df(config.CANDLES_TABLE_NAME, config.DEFAULT_START_DATE)

    if candles_df is None or candles_df.is_empty():
        log.error("Could not fetch data. Exiting.")
        return

    indicators_df = calculate_indicators(candles_df)
    log.info("--- Calculated Indicators (Sample) ---")
    print(indicators_df.tail())

    db_utils.save_indicators_to_db(indicators_df, config.INDICATORS_TABLE_NAME)

if __name__ == "__main__":
    main()
