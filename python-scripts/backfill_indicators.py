import polars as pl
from datetime import datetime, timezone, timedelta
import argparse
import db_utils 
from common import log
from ta import calculate_indicators
import config # --- NEW: Import the config module ---

# A sufficient lookback period to ensure indicator accuracy at the start of a chunk.
LOOKBACK_PERIOD_MINUTES = 200 

def backfill_indicators(start_dt, end_dt):
    """
    Fetches candle data in daily chunks for a given date range, calculates
    indicators, and upserts them into the database efficiently.
    """
    log.info(f"--- Starting Indicator Backfill from {start_dt.date()} to {end_dt.date()} ---")
    
    current_day = start_dt
    while current_day <= end_dt:
        chunk_start = current_day
        chunk_end = current_day + timedelta(days=1)
        fetch_start = chunk_start - timedelta(minutes=LOOKBACK_PERIOD_MINUTES)
        
        log.info(f"Processing data for {current_day.date()}...")

        # --- CHANGE: Use config variable for table name ---
        candles_df = db_utils.fetch_candles_for_range_as_polars_df(
            config.CANDLES_TABLE_NAME, 
            fetch_start, 
            chunk_end
        )

        if candles_df is None or candles_df.is_empty():
            log.warning(f"No candle data found for {current_day.date()}. Skipping.")
            current_day += timedelta(days=1)
            continue

        indicators_df = calculate_indicators(candles_df)

        indicators_to_save = indicators_df.filter(
            pl.col('open_time') >= chunk_start
        )

        if not indicators_to_save.is_empty():
            # --- CHANGE: Use config variable for table name ---
            db_utils.save_indicators_to_db(indicators_to_save, config.INDICATORS_TABLE_NAME)
        else:
            log.info(f"No new indicators generated for {current_day.date()}.")

        current_day += timedelta(days=1)

    log.info("--- Indicator Backfill Complete ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill technical indicators for a given date range.")
    parser.add_argument('--start', required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument('--end', required=True, help="End date in YYYY-MM-DD format.")
    args = parser.parse_args()

    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_date = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        backfill_indicators(start_date, end_date)
    except ValueError:
        print("Error: Please use the date format YYYY-MM-DD.")
