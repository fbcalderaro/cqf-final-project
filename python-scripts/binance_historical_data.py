import os
import psycopg2
from psycopg2 import extras
import requests
import time
from datetime import datetime, timezone, timedelta

# --- Configuration ---
DB_NAME = os.environ.get('DB_NAME', 'crypto_trading')
DB_USER = os.environ.get('DB_USER', 'testuser')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'testpass')
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = os.environ.get('DB_PORT', '5432')

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTCUSDT"
INTERVAL = "1m"
TABLE_NAME = f"{SYMBOL.lower()}_{INTERVAL}_candles"

# The earliest date you want data from.
START_DATE = datetime(2020, 1, 1, 
                      hour=0, minute=0, second=0, 
                      tzinfo=timezone(timedelta(hours=-3)))  # Adjusted for UTC-3 timezone
#START_DATE = None

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        print("✅ Database connection successful.")
        return conn
    except Exception as e:
        print(f"❌ Could not connect to database: {e}")
        return None

def create_candles_table(conn):
    """Creates the candles table if it does not already exist."""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        open_time TIMESTAMPTZ PRIMARY KEY,
        open_price NUMERIC,
        high_price NUMERIC,
        low_price NUMERIC,
        close_price NUMERIC,
        volume NUMERIC,
        close_time TIMESTAMPTZ,
        quote_asset_volume NUMERIC,
        number_of_trades BIGINT,
        taker_buy_base_asset_volume NUMERIC,
        taker_buy_quote_asset_volume NUMERIC,
        ignore TEXT
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
        print(f"✅ Table '{TABLE_NAME}' is ready.")

def get_oldest_timestamp(conn, start_date=None):
    """
    Gets the earliest (oldest) timestamp from the database.

    If start_date is provided, it finds the oldest timestamp strictly greater
    than that date. Otherwise, it finds the absolute oldest timestamp.
    """
    params = []
    sql_query = f"SELECT MIN(open_time) FROM {TABLE_NAME}"

    if start_date:
        sql_query += " WHERE open_time > %s"
        params.append(start_date)
    
    with conn.cursor() as cur:
        cur.execute(sql_query, params)
        result = cur.fetchone()

        # The result could be None or contain a None value, so we check both.
        if result and result[0] is not None:
            return result[0]
        return None

def fetch_binance_data(symbol, interval, start_time_dt, limit=1000):
    """Fetches k-line data from the Binance API."""
    start_time_ms = int(start_time_dt.timestamp() * 1000)
    params = {'symbol': symbol, 'interval': interval, 'startTime': start_time_ms, 'limit': limit}

    try:
        log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{log_time}] ⬇️  Fetching {limit} records from Binance starting at {start_time_dt.strftime('%Y-%m-%d %H:%M:%S')}...")

        response = requests.get(BINANCE_API_URL, params=params)
        response.raise_for_status()

        log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{log_time}]    ✅ Fetched {len(response.json())} records.")
        return response.json()
    except requests.exceptions.RequestException as e:
        log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{log_time}] ❌ Error fetching data from Binance: {e}")
        return []

def insert_data_to_db(conn, data):
    """Converts timestamps and inserts a batch of candle data into the database."""
    if not data:
        return 0

    transformed_data = [
        (
            datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc), row[1], row[2], row[3], row[4], row[5],
            datetime.fromtimestamp(row[6] / 1000, tz=timezone.utc), row[7], row[8], row[9], row[10], row[11]
        ) for row in data
    ]

    insert_query = f"""
    INSERT INTO {TABLE_NAME} (
        open_time, open_price, high_price, low_price, close_price, volume,
        close_time, quote_asset_volume, number_of_trades,
        taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (open_time) DO NOTHING;
    """
    with conn.cursor() as cur:
        extras.execute_batch(cur, insert_query, transformed_data)
        conn.commit()
        log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{log_time}]    💾 Inserted {cur.rowcount} new records into the database.")
        return cur.rowcount

def main():
    """Main function to backfill historical data."""
    print("--- Starting Historical Data Backfiller ---")

    conn = get_db_connection()
    if conn is None:
        return

    # Ensure the table exists before we try to query it
    create_candles_table(conn)

    # Find the oldest record we have to define the end of the backfill period
    oldest_ts_in_db = get_oldest_timestamp(conn, start_date=START_DATE)

    if oldest_ts_in_db is None:
        print("❌ Database table is empty. This script is for backfilling gaps.")
        print("   Please run a script to fetch recent data first.")
        conn.close()
        return

    print(f"Oldest record in database is from: {oldest_ts_in_db.strftime('%Y-%m-%d %H:%M:%S')}")

    # The period to fill is from our desired START_DATE up to the oldest record we have.
    # We use this oldest record as the end point for the backfill.
    start_of_gap = START_DATE
    end_of_gap = oldest_ts_in_db

    if start_of_gap >= end_of_gap:
        print("✅ No historical gap to fill. Data is complete back to the start date.")
        conn.close()
        return

    print(f"--- Backfilling data from {start_of_gap.strftime('%Y-%m-%d')} to {end_of_gap.strftime('%Y-%m-%d')} ---")

    current_dt = start_of_gap
    while current_dt < end_of_gap:
        data = fetch_binance_data(SYMBOL, INTERVAL, current_dt)
        if not data:
            print("⏹️ No more data returned from API. Stopping.")
            break

        insert_data_to_db(conn, data)

        # Move the start time for the next fetch to be 1 minute after the last record we received
        last_record_time_ms = data[-1][0]
        current_dt = datetime.fromtimestamp(last_record_time_ms / 1000, tz=timezone.utc) + timedelta(minutes=1)
        
        # A short delay to be respectful to the API
        time.sleep(0.5)

    conn.close()
    print("\n--- Backfill complete. Connection closed. ---")

if __name__ == "__main__":
    main()