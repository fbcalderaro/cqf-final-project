import websocket
import json
import db_utils 

# --- Configuration ---
TABLE_NAME = "btcusdt_1m_candles"
SYMBOL = "btcusdt"
INTERVAL = "1m"
SOCKET = f"wss://stream.binance.com:9443/ws/{SYMBOL}@kline_{INTERVAL}"

db_connection = None # Global connection for this script

def on_message(ws, message):
    """Callback to handle incoming WebSocket messages."""
    json_message = json.loads(message)
    db_utils.upsert_realtime_candle(db_connection, json_message, TABLE_NAME)

def on_error(ws, error): print(f"--- WebSocket Error: {error} ---")
def on_close(ws, close_status_code, close_msg): print("--- WebSocket Closed ---")
def on_open(ws): print(f"--- WebSocket Opened ---\n--- Subscribed to {SYMBOL} {INTERVAL} klines ---")

if __name__ == "__main__":
    db_connection = db_utils.get_db_connection()
    if db_connection is None: exit()
    
    # Ensure the table exists before starting
    db_utils.create_candles_table(db_connection, TABLE_NAME)

    ws = websocket.WebSocketApp(SOCKET, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.run_forever()

    if db_connection:
        db_connection.close()
        print("Database connection closed.")