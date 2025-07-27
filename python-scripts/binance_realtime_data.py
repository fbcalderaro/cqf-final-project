import os
import websocket
import json
import psycopg2
from datetime import datetime, timezone

# --- Database Configuration ---
# Ensure these environment variables are set in your compose.yaml or system
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']

TABLE_NAME = "btcusdt_1m_candles"

# --- Binance WebSocket Configuration ---
SYMBOL = "btcusdt"
INTERVAL = "1m"
SOCKET = f"wss://stream.binance.com:9443/ws/{SYMBOL}@kline_{INTERVAL}"

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL/TimescaleDB database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True # Set autocommit to True for real-time inserts
        print("✅ Database connection successful.")
        return conn
    except Exception as e:
        print(f"❌ Could not connect to the database: {e}")
        return None

def insert_candle_to_db(conn, candle_data):
    """Inserts a single candle data into the database."""
    k = candle_data['k']
    print(f"Received candle data: {k}")
    
    # We only want to insert closed candles
    if not k['x']:
        # Candle is not closed yet, do nothing.
        # You could print a message here for debugging if you want.
        # print(f"-> Received update for open candle at {datetime.fromtimestamp(k['t']/1000, tz=timezone.utc).strftime('%H:%M:%S')}")
        return

    print(f"🕯️  New closed candle received for {datetime.fromtimestamp(k['t']/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")

    # This query robustly handles inserts. If a record for that minute already exists,
    # it updates it. This is useful for ensuring the final candle data is correct.
    insert_query = f"""
    INSERT INTO {TABLE_NAME} (
        open_time, open_price, high_price, low_price, close_price, volume,
        close_time, quote_asset_volume, number_of_trades,
        taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (open_time) DO UPDATE SET
        close_price = EXCLUDED.close_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        volume = EXCLUDED.volume,
        number_of_trades = EXCLUDED.number_of_trades;
    """
    
    # Prepare data for insertion
    data_tuple = (
        datetime.fromtimestamp(k['t'] / 1000, tz=timezone.utc),  # open_time
        k['o'],  # open_price
        k['h'],  # high_price
        k['l'],  # low_price
        k['c'],  # close_price
        k['v'],  # volume
        datetime.fromtimestamp(k['T'] / 1000, tz=timezone.utc),  # close_time
        k['q'],  # quote_asset_volume
        k['n'],  # number_of_trades
        k['V'],  # taker_buy_base_asset_volume
        k['Q'],  # taker_buy_quote_asset_volume
        'realtime' # Using 'ignore' field to mark as realtime data
    )

    try:
        with conn.cursor() as cur:
            cur.execute(insert_query, data_tuple)
            print("   💾 Record inserted/updated successfully.")
    except Exception as e:
        print(f"❌ Error inserting data: {e}")


def on_message(ws, message):
    """Callback function to handle incoming messages from the WebSocket."""
    json_message = json.loads(message)
    insert_candle_to_db(db_connection, json_message)

def on_error(ws, error):
    """Callback function to handle errors."""
    print(f"--- WebSocket Error: {error} ---")

def on_close(ws, close_status_code, close_msg):
    """Callback function to handle WebSocket closing."""
    print("--- WebSocket Closed ---")

def on_open(ws):
    """Callback function when the WebSocket connection is opened."""
    print("--- WebSocket Connection Opened ---")
    print(f"--- Subscribed to {SYMBOL} {INTERVAL} klines ---")

if __name__ == "__main__":
    # First, establish a persistent connection to the database
    db_connection = get_db_connection()
    if db_connection is None:
        exit()

    # Install websocket-client: pip install websocket-client
    ws = websocket.WebSocketApp(SOCKET,
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)

    # Start the WebSocket client and run forever
    ws.run_forever()

    # This part will only be reached if the script is stopped (e.g., with Ctrl+C)
    if db_connection:
        db_connection.close()
        print("Database connection closed.")
