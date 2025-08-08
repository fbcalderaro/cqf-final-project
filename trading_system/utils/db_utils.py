# trading_system/utils/db_utils.py

import os
import pandas as pd
import psycopg2
from psycopg2 import extras
from datetime import datetime, timezone
from trading_system.utils.common import log

# --- Connection ---
def get_db_connection(db_config: dict):
    try:
        conn_details = {
            'dbname': os.environ.get('DB_NAME', db_config.get('name')),
            'user': os.environ.get('DB_USER', db_config.get('user')),
            'password': os.environ.get('DB_PASSWORD', db_config.get('password')),
            'host': os.environ.get('DB_HOST', db_config.get('host')),
            'port': os.environ.get('DB_PORT', db_config.get('port'))
        }
        conn = psycopg2.connect(**conn_details)
        return conn
    except Exception as e:
        log.error(f"❌ Could not connect to the database: {e}")
        return None

# --- Read Operations ---
def fetch_candles_for_range(db_config: dict, asset: str, start_dt, end_dt, interval: str = '1m') -> pd.DataFrame | None:
    table_name = f"{asset.replace('-', '').lower()}_{interval}_candles" 
    log.info(f"Attempting to fetch data from table: '{table_name}'")
    query = f'SELECT open_time, open_price, high_price, low_price, close_price, volume FROM "{table_name}" WHERE open_time >= %s AND open_time < %s ORDER BY open_time ASC;'
    conn = get_db_connection(db_config)
    if not conn: return None
    try:
        df = pd.read_sql_query(query, conn, params=(start_dt, end_dt), index_col='open_time')
        if df.empty:
            log.warning(f"No data found in table '{table_name}' for the specified date range.")
        else:
            log.info(f"Successfully fetched {len(df)} records from '{table_name}'.")
        return df
    except psycopg2.Error as e:
        log.error(f"Error fetching data from table '{table_name}': {e}")
        return None
    finally:
        if conn: conn.close()

def get_latest_timestamp(conn, table_name: str) -> datetime | None:
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT MAX(open_time) FROM "{table_name}";')
            result = cur.fetchone()[0]
            return result
    except psycopg2.errors.UndefinedTable:
        return None
    except Exception as e:
        log.error(f"Error getting latest timestamp from '{table_name}': {e}")
        return None

# --- Write Operations ---
def create_candles_table(conn, table_name: str):
    query = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        open_time TIMESTAMPTZ PRIMARY KEY, open_price NUMERIC, high_price NUMERIC,
        low_price NUMERIC, close_price NUMERIC, volume NUMERIC, close_time TIMESTAMPTZ,
        quote_asset_volume NUMERIC, number_of_trades BIGINT, taker_buy_base_asset_volume NUMERIC,
        taker_buy_quote_asset_volume NUMERIC, ignore TEXT
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
        log.info(f"Table '{table_name}' is ready.")
    except Exception as e:
        log.error(f"Error creating table '{table_name}': {e}")
        conn.rollback()

def insert_batch_data(conn, data: list, table_name: str) -> int:
    """Inserts a batch of historical candle data, ignoring conflicts."""
    if not data: return 0
    transformed_data = [(datetime.fromtimestamp(row[0]/1000, tz=timezone.utc), row[1], row[2], row[3], row[4], row[5], datetime.fromtimestamp(row[6]/1000, tz=timezone.utc), row[7], row[8], row[9], row[10], 'historical') for row in data]
    query = f'INSERT INTO "{table_name}" (open_time, open_price, high_price, low_price, close_price, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore) VALUES %s ON CONFLICT (open_time) DO NOTHING;'
    try:
        with conn.cursor() as cur:
            extras.execute_values(cur, query, transformed_data)
            inserted_count = cur.rowcount
            conn.commit()
        # --- REMOVED LOGGING FROM HERE ---
        return inserted_count
    except Exception as e:
        log.error(f"Error inserting batch data into '{table_name}': {e}")
        conn.rollback()
        return 0

def upsert_realtime_candle(conn, candle_data: dict, table_name: str):
    k = candle_data.get('k', {})
    if not k.get('x'): return
    log.info(f"🕯️  New closed candle received for {table_name}: {datetime.fromtimestamp(k['t']/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    query = f"""
    INSERT INTO "{table_name}" (open_time, open_price, high_price, low_price, close_price, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
    ON CONFLICT (open_time) 
    DO UPDATE SET 
        close_price = EXCLUDED.close_price, high_price = EXCLUDED.high_price, 
        low_price = EXCLUDED.low_price, volume = EXCLUDED.volume, 
        number_of_trades = EXCLUDED.number_of_trades;
    """
    data_tuple = (datetime.fromtimestamp(k['t']/1000, tz=timezone.utc), k['o'], k['h'], k['l'], k['c'], k['v'], datetime.fromtimestamp(k['T']/1000, tz=timezone.utc), k['q'], k['n'], k['V'], k['Q'], 'realtime')
    try:
        with conn.cursor() as cur:
            cur.execute(query, data_tuple)
            conn.commit()
        log.info("    💾 Record inserted/updated successfully.")
    except Exception as e:
        log.error(f"Error upserting candle into '{table_name}': {e}")
        conn.rollback()
