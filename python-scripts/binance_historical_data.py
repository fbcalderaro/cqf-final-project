import requests
import time
from datetime import datetime, timezone, timedelta
import db_utils # Import the new utility module

# --- Configuration ---
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTCUSDT"
INTERVAL = "1m"
TABLE_NAME = f"{SYMBOL.lower()}_{INTERVAL}_candles"
START_DATE = datetime(2020, 1, 1, hour=0, minute=0, second=0, tzinfo=timezone.utc)

def fetch_binance_data(symbol, interval, start_time_dt, limit=1000):
    """Fetches k-line data from the Binance API."""
    start_time_ms = int(start_time_dt.timestamp() * 1000)
    params = {'symbol': symbol, 'interval': interval, 'startTime': start_time_ms, 'limit': limit}
    try:
        common.log(f" ⬇️  Fetching {limit} records from {start_time_dt.strftime('%Y-%m-%d %H:%M:%S')}...")
        response = requests.get(BINANCE_API_URL, params=params)
        response.raise_for_status()
        common.log(f"   ✅ Fetched {len(response.json())} records.")
        return response.json()
    except requests.exceptions.RequestException as e:
        common.log(f" ❌ Error fetching data: {e}")
        return []

def main():
    """Main function to backfill historical data."""
    common.log(" --- Starting Historical Data Backfiller ---")
    conn = db_utils.get_db_connection()
    if conn is None: return

    db_utils.create_candles_table(conn, TABLE_NAME)
    
    # Use the specific get_oldest_timestamp function from db_utils
    oldest_ts_in_db = db_utils.get_oldest_timestamp(conn, TABLE_NAME, start_date=START_DATE)

    if oldest_ts_in_db is None:
        common.log("❌ No data found after the start date. Please run the real-time script first.")
        conn.close()
        return

    common.log(f"Oldest relevant record is from: {oldest_ts_in_db.strftime('%Y-%m-%d %H:%M:%S')}")
    start_of_gap = START_DATE
    end_of_gap = oldest_ts_in_db

    if start_of_gap >= end_of_gap:
        common.log("✅ No historical gap to fill.")
        conn.close()
        return

    common.log(f"--- Backfilling data from {start_of_gap.strftime('%Y-%m-%d')} to {end_of_gap.strftime('%Y-%m-%d')} ---")
    current_dt = start_of_gap
    while current_dt < end_of_gap:
        data = fetch_binance_data(SYMBOL, INTERVAL, current_dt)
        if not data:
            common.log("⏹️ No more data from API. Stopping.")
            break
        
        db_utils.insert_batch_data(conn, data, TABLE_NAME)
        last_ts = datetime.fromtimestamp(data[-1][0]/1000, tz=timezone.utc)
        current_dt = last_ts + timedelta(minutes=1)
        time.sleep(0.5)

    conn.close()
    common.log("--- Backfill complete. Connection closed. ---")

if __name__ == "__main__":
    main()