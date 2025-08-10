import sys
import os
import yaml
import requests
import time
from datetime import datetime, timezone, timedelta

# Add project root to Python's path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)

from trading_system.utils import db_utils
from trading_system.utils.common import log
from trading_system.data_ingestion import BINANCE_API_URL

CONFIG_PATH = os.path.join(PROJECT_ROOT, 'trading_system', 'config', 'config.yaml')

def fetch_and_fill_day(conn, asset: str, interval: str, day: datetime.date):
    """
    Fetches all data for a single day from Binance and inserts it into the database.
    Binance API limit is 1000 records, so we need two calls to get a full day (1440 minutes).
    """
    log.info(f"--- Fetching full day data for {asset} on {day} ---")
    
    table_name = f"{asset.replace('-', '').lower()}_{interval}_candles"
    all_day_data = []

    # Define the start of the target day in UTC
    start_of_day = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    
    # Batch 1: First 1000 minutes of the day (respecting Binance API limit)
    params1 = {
        'symbol': asset.replace('-', ''), 'interval': interval, 
        'startTime': int(start_of_day.timestamp() * 1000), 'limit': 1000
    }
    
    # Batch 2: Starts 1000 minutes after the beginning of the day
    start_of_batch2 = start_of_day + timedelta(minutes=1000)
    params2 = {
        'symbol': asset.replace('-', ''), 'interval': interval, 
        'startTime': int(start_of_batch2.timestamp() * 1000), 'limit': 440
    }

    try:
        log.info(f"⬇️  Fetching first batch of records...")
        response1 = requests.get(BINANCE_API_URL, params=params1)
        response1.raise_for_status()
        all_day_data.extend(response1.json())
        time.sleep(0.5) # Pause between requests

        log.info(f"⬇️  Fetching second batch of records...")
        response2 = requests.get(BINANCE_API_URL, params=params2)
        response2.raise_for_status()
        all_day_data.extend(response2.json())
        
        if all_day_data:
            # --- THIS IS THE CORRECTED LINE ---
            # Call the new upsert function instead of the old insert function
            inserted_count = db_utils.upsert_batch_data(conn, all_day_data, table_name)
            log.info(f"    ✅ Fetched {len(all_day_data)} records, 💾 Upserted/updated {inserted_count} records to fill gap.")
        else:
            log.warning("    No data returned from API for this day.")

    except requests.exceptions.RequestException as e:
        log.error(f"Error fetching full day data for {day}: {e}")

def find_gaps_by_daily_count(db_config: dict, asset: str, interval: str):
    """
    Analyzes candle data by counting records per day and triggers a full-day
    fetch for any days with missing records.
    """
    log.info(f"--- Starting Daily Count Integrity Check for asset: {asset} ---")
    
    table_name = f"{asset.replace('-', '').lower()}_{interval}_candles"
    THEORETICAL_MAX_PER_DAY = 1440
    
    conn = db_utils.get_db_connection(db_config)
    if conn is None: return

    try:
        daily_counts = db_utils.get_daily_candle_counts(db_config, table_name)
        if not daily_counts:
            log.warning(f"No daily data found for {asset}. Cannot check for gaps.")
            return

        gaps_found = False
        for day, count in daily_counts:
            # For the current day, it's normal to have less than 1440 records.
            if day == datetime.now(timezone.utc).date():
                continue

            if count < THEORETICAL_MAX_PER_DAY:
                gaps_found = True
                missing_count = THEORETICAL_MAX_PER_DAY - count
                log.warning(f"  -> DATA GAP DETECTED on {day}: Found {count}/{THEORETICAL_MAX_PER_DAY} records. ({missing_count} missing)")
                # Call the new function to fetch and fill the entire day
                #fetch_and_fill_day(conn, asset, interval, day)

        if not gaps_found:
            log.info(f"✅ No days with missing records found for {asset}.")

    finally:
        if conn: conn.close()


def main():
    """Main function to load config and run the gap-finding process."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        log.error(f"Configuration file not found at: {CONFIG_PATH}")
        return
    
    db_config = config['system']['database']
    ingestion_config = config['data_ingestion']
    
    for asset in ingestion_config['assets_to_track']:
        find_gaps_by_daily_count(db_config, asset, ingestion_config['base_interval'])
        log.info("-" * 50)

if __name__ == "__main__":
    main()