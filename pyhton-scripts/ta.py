import os
import psycopg2
import polars as pl
from sqlalchemy import create_engine

# --- Database Configuration ---
# Ensure these environment variables are set in your system
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']

# --- Table Configuration ---
SOURCE_TABLE_NAME = "btcusdt_1m_candles"
TARGET_TABLE_NAME = "btcusdt_1m_indicators"

# --- Time Configuration ---
# Define a start time for the data query. Format: 'YYYY-MM-DD HH:MM:SS'
# Set to a very old date to get all data, or change to a recent date for faster processing.
START_TIME = "2024-01-01 00:00:00"

def get_data_from_db():
    """
    Connects to the PostgreSQL database and fetches candle data starting from START_TIME,
    loading it into a Polars DataFrame.
    """
    print("Connecting to the database...")
    try:
        # Using SQLAlchemy engine for Polars' read_database for better performance
        db_uri = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_uri)
        
        # SQL query to fetch data starting from a specific time
        sql_query = f"""
        SELECT * FROM {SOURCE_TABLE_NAME}
        WHERE open_time >= '{START_TIME}'
        ORDER BY open_time ASC
        """

        print(f"Fetching data from '{SOURCE_TABLE_NAME}' starting from {START_TIME}...")
        df = pl.read_database_uri(query=sql_query, uri=db_uri)
        print(f"✅ Successfully loaded {len(df)} rows of data.")
        return df

    except Exception as e:
        print(f"❌ An error occurred while fetching data: {e}")
        return None

def calculate_indicators(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculates technical analysis indicators using pure Polars expressions.
    """
    print("Calculating technical indicators...")

    adx_period = 14
    alpha = 1 / adx_period

    # Use .with_columns to add new columns based on calculations
    df_with_indicators = df.with_columns(
        # RSI (Relative Strength Index)
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
        # MACD (Moving Average Convergence Divergence)
        ema_fast=pl.col("close_price").ewm_mean(span=12, adjust=False),
        ema_slow=pl.col("close_price").ewm_mean(span=26, adjust=False),
    ).with_columns(
        macd=pl.col("ema_fast") - pl.col("ema_slow"),
    ).with_columns(
        macd_signal=pl.col("macd").ewm_mean(span=9, adjust=False),
    ).with_columns(
        macd_hist=pl.col("macd") - pl.col("macd_signal"),
    ).with_columns(
        # Bollinger Bands
        bb_middle=pl.col("close_price").rolling_mean(window_size=20),
        bb_std=pl.col("close_price").rolling_std(window_size=20),
    ).with_columns(
        bb_upper=pl.col("bb_middle") + (pl.col("bb_std") * 2),
        bb_lower=pl.col("bb_middle") - (pl.col("bb_std") * 2),
    ).with_columns(
        # --- NEW: Exponential Moving Average (EMA) ---
        ema_50=pl.col("close_price").ewm_mean(span=50, adjust=False),
    ).with_columns(
        # --- NEW: Average Directional Index (ADX) Calculation ---
        # Step 1: Create prerequisite 'previous' columns first. This is the fix.
        prev_close=pl.col("close_price").shift(1),
        prev_high=pl.col("high_price").shift(1),
        prev_low=pl.col("low_price").shift(1),
    ).with_columns(
        # Step 2: Calculate True Range (TR)
        tr_a=pl.col("high_price") - pl.col("low_price"),
        tr_b=(pl.col("high_price") - pl.col("prev_close")).abs(),
        tr_c=(pl.col("low_price") - pl.col("prev_close")).abs(),
    ).with_columns(
        true_range=pl.max_horizontal(["tr_a", "tr_b", "tr_c"])
    ).with_columns(
        # Step 3: Calculate Directional Movement (+DM, -DM)
        up_move=pl.col("high_price") - pl.col("prev_high"),
        down_move=pl.col("prev_low") - pl.col("low_price"),
    ).with_columns(
        plus_dm=pl.when((pl.col("up_move") > pl.col("down_move")) & (pl.col("up_move") > 0)).then(pl.col("up_move")).otherwise(0),
        minus_dm=pl.when((pl.col("down_move") > pl.col("up_move")) & (pl.col("down_move") > 0)).then(pl.col("down_move")).otherwise(0),
    ).with_columns(
        # Step 4: Smoothed TR, +DM, -DM (using Wilder's smoothing)
        atr_14=pl.col("true_range").ewm_mean(alpha=alpha, adjust=False),
        plus_dm_14=pl.col("plus_dm").ewm_mean(alpha=alpha, adjust=False),
        minus_dm_14=pl.col("minus_dm").ewm_mean(alpha=alpha, adjust=False),
    ).with_columns(
        # Step 5: Directional Indicators (+DI, -DI)
        plus_di_14=100 * (pl.col("plus_dm_14") / pl.col("atr_14")),
        minus_di_14=100 * (pl.col("minus_dm_14") / pl.col("atr_14")),
    ).with_columns(
        # Step 6: Directional Movement Index (DX)
        dx_14=(100 * (pl.col("plus_di_14") - pl.col("minus_di_14")).abs() / (pl.col("plus_di_14") + pl.col("minus_di_14"))),
    ).with_columns(
        # Step 7: Average Directional Index (ADX)
        adx_14=pl.col("dx_14").ewm_mean(alpha=alpha, adjust=False)
    )

    # --- UPDATED: Select the columns we want to save ---
    # Added ema_50 and the ADX components to the final selection
    indicators_df = df_with_indicators.select(
        pl.col("open_time"),
        pl.col("rsi_14"),
        pl.col("macd"),
        pl.col("macd_signal"),
        pl.col("macd_hist"),
        pl.col("bb_lower"),
        pl.col("bb_middle"),
        pl.col("bb_upper"),
        pl.col("ema_50"),
        pl.col("adx_14"),
        pl.col("plus_di_14"),
        pl.col("minus_di_14"),
    )

    print("✅ Indicators calculated.")
    return indicators_df.drop_nulls()

def save_indicators_to_db(df: pl.DataFrame):
    """
    Saves the calculated indicators to a new table in the database.
    """
    if df.is_empty():
        print("No data to save.")
        return

    print(f"Saving {len(df)} records to '{TARGET_TABLE_NAME}'...")
    try:
        db_uri = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        
        # Use Polars' write_database method for efficient writing
        df.write_database(
            table_name=TARGET_TABLE_NAME,
            connection=db_uri,
            if_table_exists="replace"
        )
        
        # Set open_time as the primary key for the new table
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE {TARGET_TABLE_NAME} ADD PRIMARY KEY (open_time);")
            conn.commit()
        conn.close()

        print(f"✅ Successfully saved indicators to '{TARGET_TABLE_NAME}'.")

    except Exception as e:
        print(f"❌ An error occurred while saving data: {e}")

def main():
    """Main function to run the indicator calculation and saving process."""
    print("--- Starting Indicator Calculation Process ---")
    
    # Step 1: Fetch data from the database
    candles_df = get_data_from_db()

    if candles_df is None or candles_df.is_empty():
        print("Could not fetch data. Exiting.")
        return

    # Step 2: Calculate indicators
    indicators_df = calculate_indicators(candles_df)
    print("\n--- Calculated Indicators (Sample) ---")
    print(indicators_df.tail())

    # Step 3: Save indicators to the database
    save_indicators_to_db(indicators_df)

if __name__ == "__main__":
    main()
