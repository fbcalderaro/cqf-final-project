import os
import psycopg2
from psycopg2 import extras
import requests
import time
from datetime import datetime, timezone, timedelta
import websocket
import json
import threading

# --- Configuration ---
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
SYMBOL_REST = "BTCUSDT"
SYMBOL_WS = "btcusdt"
INTERVAL = "1m"
TABLE_NAME = f"{SYMBOL_WS}_{INTERVAL}_candles"
SOCKET = f"wss://stream.binance.com:9443/ws/{SYMBOL_WS}@kline_{INTERVAL}"

# --- Global Database Connection ---
# This connection will be shared by both historical and real-time functions
db_connection = None

def get_db_connection():
    """Establishes a global connection to the PostgreSQL database."""
    global db_connection
    try:
        db_connection = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        print("✅ Database connection successful.")
    except Exception as e:
        print(f"❌ Could not connect to database: {e}")
        db_connection = None

# --- Historical Data Functions ---

def get_latest_timestamp():
    """Gets the most recent timestamp from the database."""
    with db_connection.cursor() as cur:
        cur.execute(f"SELECT MAX(open_time) FROM {TABLE_NAME};")
        result = cur.fetchone()[0]
        return result

def fetch_historical_data(start_dt):
    """Fetches and inserts historical data from a start date until now."""
    print("\n--- Starting Historical Data Backfill ---")
    end_dt = datetime.now(timezone.utc)
    current_dt = start_dt

    while current_dt < end_dt:
        params = {
            'symbol': SYMBOL_REST,
            'interval': INTERVAL,
            'startTime': int(current_dt.timestamp() * 1000),
            'limit': 1000
        }
        try:
            print(f"⬇️  Fetching records from {current_dt.strftime('%Y-%m-%d %H:%M:%S')}...")
            response = requests.get(BINANCE_API_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                print("   No more historical data to fetch.")
                break

            print(f"   ✅ Fetched {len(data)} records.")
            insert_batch_data(data)
            
            last_record_time_ms = data[-1][0]
            current_dt = datetime.fromtimestamp(last_record_time_ms / 1000, tz=timezone.utc) + timedelta(minutes=1)
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching data from Binance: {e}")
            break
    
    print("--- Historical Data Backfill Complete ---")

def insert_batch_data(data):
    """Inserts a batch of historical candle data into the database."""
    if not data:
        return 0

    transformed_data = [
        (
            datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc), row[1], row[2], row[3], row[4], row[5],
            datetime.fromtimestamp(row[6] / 1000, tz=timezone.utc), row[7], row[8], row[9], row[10], 'historical'
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
    with db_connection.cursor() as cur:
        extras.execute_batch(cur, insert_query, transformed_data)
        db_connection.commit()
        print(f"   💾 Inserted {cur.rowcount} new historical records.")

# --- Real-time Data Functions ---

def insert_realtime_candle(candle_data):
    """Inserts a single real-time candle data into the database."""
    k = candle_data['k']
    if not k['x']: # Only process closed candles
        return

    print(f"🕯️  New closed candle received: {datetime.fromtimestamp(k['t']/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")

    insert_query = f"""
    INSERT INTO {TABLE_NAME} (
        open_time, open_price, high_price, low_price, close_price, volume,
        close_time, quote_asset_volume, number_of_trades,
        taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (open_time) DO UPDATE SET
        close_price = EXCLUDED.close_price, high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price, volume = EXCLUDED.volume,
        number_of_trades = EXCLUDED.number_of_trades;
    """
    data_tuple = (
        datetime.fromtimestamp(k['t'] / 1000, tz=timezone.utc), k['o'], k['h'], k['l'], k['c'], k['v'],
        datetime.fromtimestamp(k['T'] / 1000, tz=timezone.utc), k['q'], k['n'], k['V'], k['Q'], 'realtime'
    )

    try:
        with db_connection.cursor() as cur:
            cur.execute(insert_query, data_tuple)
            db_connection.commit()
            print("   💾 Record inserted/updated successfully.")
    except Exception as e:
        print(f"❌ Error inserting real-time data: {e}")

def on_message(ws, message):
    json_message = json.loads(message)
    insert_realtime_candle(json_message)

def on_error(ws, error):
    print(f"--- WebSocket Error: {error} ---")

def on_close(ws, close_status_code, close_msg):
    print("--- WebSocket Closed ---")

def on_open(ws):
    print("--- WebSocket Connection Opened ---")
    print(f"--- Subscribed to {SYMBOL_WS}@{INTERVAL} klines ---")

def start_websocket():
    """Initializes and starts the WebSocket client."""
    print("\n--- Starting Real-time Data Stream ---")
    ws = websocket.WebSocketApp(SOCKET,
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    ws.run_forever()

# --- Main Execution ---

if __name__ == "__main__":
    get_db_connection()
    if db_connection is None:
        exit()

    # Step 1: Backfill historical data
    try:
        latest_ts = get_latest_timestamp()
        if latest_ts:
            start_date = latest_ts + timedelta(minutes=1)
            print(f"Database contains data up to {latest_ts.strftime('%Y-%m-%d %H:%M:%S')}.")
            fetch_historical_data(start_date)
        else:
            print("Database is empty. Starting historical download from scratch.")
            # Define a default start date if the DB is empty
            start_date = datetime(2022, 1, 1, tzinfo=timezone.utc)
            fetch_historical_data(start_date)
    except Exception as e:
        print(f"An error occurred during historical backfill: {e}")

    # Step 2: Start the real-time data stream
    # This will only run after the historical backfill is complete
    start_websocket()

    # Clean up the connection when the script is stopped
    if db_connection:
        db_connection.close()
        print("Database connection closed.")
