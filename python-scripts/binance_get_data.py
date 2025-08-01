import requests
import time
from datetime import datetime, timezone, timedelta
import websocket
import json
import db_utils 

# --- Configuration ---
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
SYMBOL_REST = "BTCUSDT"
SYMBOL_WS = "btcusdt"
INTERVAL = "1m"
TABLE_NAME = f"{SYMBOL_WS}_{INTERVAL}_candles"
SOCKET = f"wss://stream.binance.com:9443/ws/{SYMBOL_WS}@kline_{INTERVAL}"

# Default start date for historical data if the database is empty
DEFAULT_START_DATE = datetime(2022, 1, 1, tzinfo=timezone.utc)

db_connection = None # Global connection for this script

# --- Historical Data Functions ---

def fetch_historical_data(start_dt):
    """Fetches and inserts historical data from a start date until now."""
    log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n[{log_time}] --- Starting Historical Data Backfill ---")
    end_dt = datetime.now(timezone.utc)
    current_dt = start_dt

    while current_dt < end_dt:
        params = {
            'symbol': SYMBOL_REST, 'interval': INTERVAL,
            'startTime': int(current_dt.timestamp() * 1000), 'limit': 1000
        }
        try:
            log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{log_time}] ⬇️  Fetching records from {current_dt.strftime('%Y-%m-%d %H:%M:%S')}...")
            response = requests.get(BINANCE_API_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print(f"[{log_time}]    No more historical data to fetch.")
                break

            log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{log_time}]    ✅ Fetched {len(data)} records.")
            db_utils.insert_batch_data(db_connection, data, TABLE_NAME)
            
            last_record_time_ms = data[-1][0]
            current_dt = datetime.fromtimestamp(last_record_time_ms / 1000, tz=timezone.utc) + timedelta(minutes=1)
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{log_time}] ❌ Error fetching data from Binance: {e}")
            break
    
    log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{log_time}] --- Historical Data Backfill Complete ---")

# --- Real-time Data Functions ---

def on_message(ws, message):
    json_message = json.loads(message)
    k = json_message['k']
    if not k['x']: # Only process closed candles
        return

    log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{log_time}] 🕯️  New closed candle received: {datetime.fromtimestamp(k['t']/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    
    insert_query = f"""
    INSERT INTO {TABLE_NAME} (open_time, open_price, high_price, low_price, close_price, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (open_time) DO UPDATE SET
        close_price = EXCLUDED.close_price, high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price, volume = EXCLUDED.volume,
        number_of_trades = EXCLUDED.number_of_trades;
    """
    data_tuple = (
        datetime.fromtimestamp(k['t'] / 1000, tz=timezone.utc), k['o'], k['h'], k['l'], k['c'], k['v'],
        datetime.fromtimestamp(k['T'] / 1000, tz=timezone.utc), k['q'], k['n'], k['V'], k['Q'], 'realtime'
    )
    with db_connection.cursor() as cur:
        cur.execute(insert_query, data_tuple)
        db_connection.commit()
    
    log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{log_time}]    💾 Record inserted/updated successfully.")

def on_error(ws, error):
    log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{log_time}] --- WebSocket Error: {error} ---")

def on_close(ws, close_status_code, close_msg):
    log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{log_time}] --- WebSocket Closed ---")

def on_open(ws):
    log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{log_time}] --- WebSocket Connection Opened ---")
    print(f"[{log_time}] --- Subscribed to {SYMBOL_WS}@{INTERVAL} klines ---")

def start_websocket():
    """Initializes and starts the WebSocket client."""
    log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n[{log_time}] --- Starting Real-time Data Stream ---")
    ws = websocket.WebSocketApp(SOCKET, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.run_forever()

# --- Main Execution ---

if __name__ == "__main__":
    db_connection = db_utils.get_db_connection()
    if db_connection is None:
        exit()
    
    db_utils.create_candles_table(db_connection, TABLE_NAME)

    try:
        latest_ts = db_utils.get_latest_timestamp(db_connection, TABLE_NAME)
        if latest_ts:
            start_date = latest_ts + timedelta(minutes=1)
            log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{log_time}] Database contains data up to {latest_ts.strftime('%Y-%m-%d %H:%M:%S')}.")
            fetch_historical_data(start_date)
        else:
            log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{log_time}] Database is empty. Starting historical download from scratch.")
            fetch_historical_data(DEFAULT_START_DATE)
    except Exception as e:
        log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{log_time}] An error occurred during historical backfill: {e}")

    start_websocket()

    if db_connection:
        db_connection.close()
        log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{log_time}] Database connection closed.")