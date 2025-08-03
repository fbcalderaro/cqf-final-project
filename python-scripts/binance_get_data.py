import requests
import time
from datetime import datetime, timezone, timedelta
import websocket
import json
import db_utils 
from common import log
import polars as pl
from ta import calculate_indicators
import config 

# --- Globals ---
db_connection = None
ws_app = None

# --- Historical Data and Indicator Calculation ---
def fetch_historical_data(conn, start_dt):
    """
    Fetches historical candle data, saves it, calculates indicators, and saves them.
    """
    log.info("--- Starting Historical Data Backfill ---")
    end_dt = datetime.now(timezone.utc)
    current_dt = start_dt

    while current_dt < end_dt:
        params = {
            'symbol': config.SYMBOL, 'interval': config.STREAM_INTERVAL,
            'startTime': int(current_dt.timestamp() * 1000), 'limit': 1000
        }
        try:
            log.info(f"⬇️  Fetching records from {current_dt.strftime('%Y-%m-%d %H:%M:%S')}...")
            response = requests.get(config.API_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                log.info("No more historical data to fetch.")
                break

            log.info(f"✅ Fetched {len(data)} records.")
            db_utils.insert_batch_data(conn, data, config.CANDLES_TABLE_NAME)
            
            first_new_candle_time = datetime.fromtimestamp(data[0][0] / 1000, tz=timezone.utc)
            fetch_end_time = datetime.fromtimestamp(data[-1][6] / 1000, tz=timezone.utc)
            lookback_start_time = first_new_candle_time - timedelta(minutes=200)

            historical_context_df = db_utils.fetch_candles_for_range_as_polars_df(config.CANDLES_TABLE_NAME, lookback_start_time, fetch_end_time)
            
            if historical_context_df is not None and not historical_context_df.is_empty():
                indicators_df = calculate_indicators(historical_context_df)
                indicators_to_save = indicators_df.filter(pl.col("open_time") >= first_new_candle_time)
                if not indicators_to_save.is_empty():
                    db_utils.save_indicators_to_db(indicators_to_save, config.INDICATORS_TABLE_NAME)
            
            last_record_time_ms = data[-1][0]
            current_dt = datetime.fromtimestamp(last_record_time_ms / 1000, tz=timezone.utc) + timedelta(minutes=1)
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            log.error(f"❌ Error fetching data from Binance: {e}")
            break
        except Exception as e:
            log.error(f"❌ An error occurred during historical processing: {e}", exc_info=True)
            break
    
    log.info("--- Historical Data Backfill Complete ---")
    
# --- Real-time Data Functions ---
def on_message(ws, message):
    """Callback for WebSocket messages. Inserts candle AND triggers indicator update."""
    try:
        json_message = json.loads(message)
        k = json_message.get('k')
        if not k or not k.get('x'):
            return

        db_utils.upsert_realtime_candle(db_connection, json_message, config.CANDLES_TABLE_NAME)

        lookback_start_time = datetime.now(timezone.utc) - timedelta(minutes=200)
        candles_df = db_utils.fetch_candles_for_range_as_polars_df(config.CANDLES_TABLE_NAME, lookback_start_time, datetime.now(timezone.utc))
        
        if candles_df is None or candles_df.is_empty():
            return

        indicators_df = calculate_indicators(candles_df)
        latest_indicators_row = indicators_df.tail(1)
        
        if not latest_indicators_row.is_empty():
            db_utils.save_indicators_to_db(latest_indicators_row, config.INDICATORS_TABLE_NAME)
    except Exception as e:
        log.error(f"Error in on_message callback: {e}", exc_info=True)

def on_error(ws, error):
    log.error(f"--- WebSocket Error: {error} ---")

def on_close(ws, close_status_code, close_msg):
    log.warning(f"--- WebSocket Closed --- Code: {close_status_code}, Msg: {close_msg}")

def on_open(ws):
    log.info("--- WebSocket Connection Opened ---")
    log.info(f"--- Subscribed to {config.SYMBOL_LOWER}@{config.STREAM_INTERVAL} klines ---")

def start_websocket():
    """Initializes and starts the WebSocket client. This function will block until the connection closes."""
    global ws_app
    log.info("--- Starting Real-time Data Stream ---")
    ws_app = websocket.WebSocketApp(config.SOCKET_URL, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
    ws_app.run_forever()

# --- Main Execution ---
if __name__ == "__main__":
    try:
        db_connection = db_utils.get_db_connection()
        if db_connection is None:
            log.critical("Exiting: Database connection could not be established.")
            exit()
        
        db_utils.create_candles_table(db_connection, config.CANDLES_TABLE_NAME)
        
        latest_ts = db_utils.get_latest_timestamp(db_connection, config.CANDLES_TABLE_NAME)
        if latest_ts:
            start_date = latest_ts + timedelta(minutes=1)
            log.info(f"Database has data up to {latest_ts.strftime('%Y-%m-%d %H:%M:%S')}. Resuming download.")
            fetch_historical_data(db_connection, start_date)
        else:
            log.info("Database is empty. Starting historical download from scratch.")
            fetch_historical_data(db_connection, config.DEFAULT_START_DATE)

        # --- NEW: Resilience and Reconnection Loop ---
        reconnect_delay = 5  # Initial delay in seconds
        while True:
            try:
                start_websocket()  # This blocks until the connection is closed or fails
                log.warning("WebSocket connection closed cleanly.")
                reconnect_delay = 5 # Reset delay after a clean closure
            except Exception as e:
                log.error(f"An error occurred in the WebSocket loop: {e}", exc_info=True)
            
            log.info(f"Attempting to reconnect in {reconnect_delay} seconds...")
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60) # Exponential backoff, max 60 seconds

    except KeyboardInterrupt:
        log.info("--- Shutdown signal received (KeyboardInterrupt) ---")
    except Exception as e:
        log.error(f"An unexpected error occurred in the main block: {e}", exc_info=True)
    finally:
        if ws_app:
            log.info("Closing WebSocket connection...")
            ws_app.close()
        if db_connection:
            log.info("Closing database connection...")
            db_connection.close()
        log.info("--- Script Shutdown Complete ---")
