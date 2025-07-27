import os
import psycopg2
import polars as pl
import db_utils
from datetime import datetime # <-- Added this import

# --- Table Configuration ---
CANDLES_TABLE_NAME = "btcusdt_1m_candles"
INDICATORS_TABLE_NAME = "btcusdt_1m_indicators"
START_DATE = datetime(2025, 1, 1, hour=0, minute=0, second=0, tzinfo=timezone.utc)

def calculate_indicators(df: pl.DataFrame) -> pl.DataFrame:
    """Calculates technical analysis indicators using pure Polars expressions."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Calculating technical indicators...")

    adx_period = 14
    alpha = 1 / adx_period

    df_with_indicators = df.with_columns(
        delta=pl.col("close_price").diff(),
    ).with_columns(
        gain=pl.when(pl.col("delta") > 0).then(pl.col("delta")).otherwise(0),
        loss=pl.when(pl.col("delta") < 0).then(-pl.col("delta")).otherwise(0),
    ).with_columns(
        avg_gain=pl.col("gain").ewm_mean(alpha=1/14, adjust=False),
        avg_loss=pl.col("loss").ewm_mean(alpha=1/14, adjust=False),
    ).with_columns(
        rs=pl.col("avg_gain") / pl.col("avg_loss"),
    ).with_columns(
        rsi_14=100.0 - (100.0 / (1.0 + pl.col("rs"))),
    ).with_columns(
        ema_fast=pl.col("close_price").ewm_mean(span=12, adjust=False),
        ema_slow=pl.col("close_price").ewm_mean(span=26, adjust=False),
    ).with_columns(
        macd=pl.col("ema_fast") - pl.col("ema_slow"),
    ).with_columns(
        macd_signal=pl.col("macd").ewm_mean(span=9, adjust=False),
    ).with_columns(
        macd_hist=pl.col("macd") - pl.col("macd_signal"),
    ).with_columns(
        bb_middle=pl.col("close_price").rolling_mean(window_size=20),
        bb_std=pl.col("close_price").rolling_std(window_size=20),
    ).with_columns(
        bb_upper=pl.col("bb_middle") + (pl.col("bb_std") * 2),
        bb_lower=pl.col("bb_middle") - (pl.col("bb_std") * 2),
    ).with_columns(
        ema_50=pl.col("close_price").ewm_mean(span=50, adjust=False),
    ).with_columns(
        prev_close=pl.col("close_price").shift(1),
        prev_high=pl.col("high_price").shift(1),
        prev_low=pl.col("low_price").shift(1),
    ).with_columns(
        tr_a=pl.col("high_price") - pl.col("low_price"),
        tr_b=(pl.col("high_price") - pl.col("prev_close")).abs(),
        tr_c=(pl.col("low_price") - pl.col("prev_close")).abs(),
    ).with_columns(
        true_range=pl.max_horizontal(["tr_a", "tr_b", "tr_c"])
    ).with_columns(
        up_move=pl.col("high_price") - pl.col("prev_high"),
        down_move=pl.col("prev_low") - pl.col("low_price"),
    ).with_columns(
        plus_dm=pl.when((pl.col("up_move") > pl.col("down_move")) & (pl.col("up_move") > 0)).then(pl.col("up_move")).otherwise(0),
        minus_dm=pl.when((pl.col("down_move") > pl.col("up_move")) & (pl.col("down_move") > 0)).then(pl.col("down_move")).otherwise(0),
    ).with_columns(
        atr_14=pl.col("true_range").ewm_mean(alpha=alpha, adjust=False),
        plus_dm_14=pl.col("plus_dm").ewm_mean(alpha=alpha, adjust=False),
        minus_dm_14=pl.col("minus_dm").ewm_mean(alpha=alpha, adjust=False),
    ).with_columns(
        plus_di_14=100 * (pl.col("plus_dm_14") / pl.col("atr_14")),
        minus_di_14=100 * (pl.col("minus_dm_14") / pl.col("atr_14")),
    ).with_columns(
        dx_14=(100 * (pl.col("plus_di_14") - pl.col("minus_di_14")).abs() / (pl.col("plus_di_14") + pl.col("minus_di_14"))),
    ).with_columns(
        adx_14=pl.col("dx_14").ewm_mean(alpha=alpha, adjust=False)
    )
    
    indicators_df = df_with_indicators.select(
        pl.col("open_time"), pl.col("rsi_14"), pl.col("macd"), pl.col("macd_signal"),
        pl.col("macd_hist"), pl.col("bb_lower"), pl.col("bb_middle"), pl.col("bb_upper"),
        pl.col("ema_50"), pl.col("adx_14"), pl.col("plus_di_14"), pl.col("minus_di_14"),
    )
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Indicators calculated.")
    return indicators_df.drop_nulls()

def main():
    """Main function to run the indicator calculation and saving process."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] --- Starting Indicator Calculation Process ---")
    
    # Step 1: Use db_utils to fetch data
    candles_df = db_utils.fetch_candles_as_polars_df(CANDLES_TABLE_NAME, START_TIME)

    if candles_df is None or candles_df.is_empty():
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Could not fetch data. Exiting.")
        return

    # Step 2: Calculate indicators
    indicators_df = calculate_indicators(candles_df)
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] --- Calculated Indicators (Sample) ---")
    print(indicators_df.tail())

    # Step 3: Save indicators to the database
    db_utils.save_indicators_to_db(indicators_df)

if __name__ == "__main__":
    main()