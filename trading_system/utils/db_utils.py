"""
Database Utility Module (db_utils.py)

This module provides a centralized set of functions for interacting with the
PostgreSQL database. It handles connection management, table creation,
and various read/write operations for candlestick data.

The functions are designed to be used by other parts of the system, such as
the data ingestion engine, backtester, and live trader.
"""

import os
import pandas as pd
import psycopg2
from psycopg2 import extras
from datetime import datetime, timezone
from trading_system.utils.common import log

# --- Connection ---
def get_db_connection(db_config: dict):
    """
    Establishes and returns a connection to the PostgreSQL database.
    It prioritizes environment variables for connection details, falling back
    to the provided config dictionary. This is useful for Docker environments.

    Args:
        db_config (dict): A dictionary containing database connection parameters
                          (name, user, password, host, port).

    Returns:
        psycopg2.connection: A database connection object, or None if connection fails.
    """
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

def get_latest_timestamp(conn, table_name: str) -> datetime | None:
    """
    Retrieves the most recent 'open_time' from a specified table.
    This is crucial for the backfill process to know where to resume fetching data.

    Args:
        conn (psycopg2.connection): An active database connection.
        table_name (str): The name of the table to query.

    Returns:
        datetime: The latest timestamp found, or None if the table is empty or doesn't exist.
    """
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

def get_daily_candle_counts(db_config: dict, table_name: str) -> list[tuple] | None:
    """
    Counts the number of 1-minute candles per day in the database for data integrity checks.
    This helps identify days with missing data that need to be backfilled.

    Args:
        db_config (dict): Database connection configuration.
        table_name (str): The name of the table to analyze.

    Returns:
        list[tuple] | None: A list of (date, count) tuples, or None on error.
    """
    log.info(f"Counting daily records for '{table_name}'...")
    conn = get_db_connection(db_config)
    if not conn: return None
    try:
        # This SQL query casts the timestamp to a date and groups by that day
        query = f"""
        SELECT 
            DATE(open_time AT TIME ZONE 'UTC') as candle_date, 
            COUNT(1) as candle_count
        FROM "{table_name}"
        GROUP BY candle_date
        ORDER BY candle_date ASC;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()
        log.info(f"Successfully analyzed daily counts for {len(results)} days.")
        return results
    except Exception as e:
        log.error(f"Error counting daily candles for '{table_name}': {e}")
        return None
    finally:
        if conn: conn.close()

def fetch_candles_for_range(db_config: dict, asset: str, start_dt, end_dt, interval: str = '1m') -> pd.DataFrame | None:
    """
    Fetches raw candle data for a specific asset and date range into a pandas DataFrame,
    which is the primary data source for backtesting and pre-loading strategies.

    Args:
        db_config (dict): Database connection configuration.
        asset (str): The asset symbol (e.g., 'BTC-USDT').
        start_dt (datetime): The start of the date range (inclusive).
        end_dt (datetime): The end of the date range (exclusive).
        interval (str, optional): The candle interval. Defaults to '1m'.

    Returns:
        pd.DataFrame | None: A DataFrame with candle data, indexed by 'open_time', or None on error.
    """
    table_name = f"{asset.replace('-', '').lower()}_{interval}_candles" 
    log.info(f"Fetching candle data from table: '{table_name}'")
    query = f'SELECT open_time, open_price, high_price, low_price, close_price, volume FROM "{table_name}" WHERE open_time >= %s AND open_time < %s ORDER BY open_time ASC;'
    conn = get_db_connection(db_config)
    if not conn: return None
    try:
        df = pd.read_sql_query(query, conn, params=(start_dt, end_dt), index_col='open_time')
        # Ensure numeric columns are of the correct type for calculations.
        numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df.dropna(inplace=True)
        log.info(f"Successfully fetched {len(df)} records from '{table_name}'.")
        return df
    except Exception as e:
        log.error(f"Error fetching candle data: {e}")
        return None
    finally:
        if conn: conn.close()

# --- Write Operations ---
def create_candles_table(conn, table_name: str):
    """
    Creates a new table for storing candle data if it doesn't already exist.
    The 'open_time' is set as the PRIMARY KEY to enforce uniqueness.

    Args:
        conn (psycopg2.connection): An active database connection.
        table_name (str): The name of the table to create.
    """
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
    """
    Inserts a batch of historical candle data from the Binance API.
    It uses 'ON CONFLICT (open_time) DO NOTHING' to efficiently skip records
    that already exist in the database, which is ideal for historical backfills.

    Args:
        conn (psycopg2.connection): An active database connection.
        data (list): A list of candle data rows from the Binance API.
        table_name (str): The name of the table to insert into.

    Returns:
        int: The number of new rows inserted.
    """
    if not data: return 0
    transformed_data = [(datetime.fromtimestamp(row[0]/1000, tz=timezone.utc), row[1], row[2], row[3], row[4], row[5], datetime.fromtimestamp(row[6]/1000, tz=timezone.utc), row[7], row[8], row[9], row[10], 'historical') for row in data]
    query = f'INSERT INTO "{table_name}" (open_time, open_price, high_price, low_price, close_price, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore) VALUES %s ON CONFLICT (open_time) DO NOTHING;'
    
    try:
        with conn.cursor() as cur:
            extras.execute_values(cur, query, transformed_data)
            inserted_count = cur.rowcount
            conn.commit()
        return inserted_count
    except Exception as e:
        log.error(f"Error inserting batch data into '{table_name}': {e}")
        conn.rollback()
        return 0

def upsert_realtime_candle(conn, candle_data: dict, table_name: str):
    """
    Inserts or updates a single candle from a real-time WebSocket stream.
    'ON CONFLICT...DO UPDATE' ensures that if a candle for a given timestamp
    already exists, it gets updated with the latest data. This is useful for
    handling potential data corrections from the stream.

    Args:
        conn (psycopg2.connection): An active database connection.
        candle_data (dict): A dictionary representing a single candle from the WebSocket.
        table_name (str): The name of the table to upsert into.
    """
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

def upsert_batch_data(conn, data: list, table_name: str) -> int:
    """
    De-duplicates and then "upserts" (inserts or updates) a batch of candle data.
    This is used by the data integrity checker to fill gaps, ensuring that existing
    records are updated if necessary.

    Args:
        conn (psycopg2.connection): An active database connection.
        data (list): A list of candle data rows from the Binance API.
        table_name (str): The name of the table to upsert into.

    Returns:
        int: The number of rows affected (inserted or updated).
    """
    if not data: return 0

    # --- De-duplication Logic ---
    # The API can sometimes return duplicate timestamps. We must ensure the data is unique
    # before sending it to the database to avoid the "cannot affect row a second time" error.
    unique_data = {}
    for row in data:
        # Use the open_time (the first element) as the unique key
        unique_data[row[0]] = row
    
    # Get the de-duplicated list of records
    deduplicated_data = list(unique_data.values())
    if len(data) != len(deduplicated_data):
        log.warning(f"Removed {len(data) - len(deduplicated_data)} duplicate records from API response.")

    # Transform raw API data into the format expected by the database table.
    transformed_data = [(datetime.fromtimestamp(row[0]/1000, tz=timezone.utc), row[1], row[2], row[3], row[4], row[5], datetime.fromtimestamp(row[6]/1000, tz=timezone.utc), row[7], row[8], row[9], row[10], 'historical_fill') for row in deduplicated_data]
    
    query = f"""
    INSERT INTO "{table_name}" (open_time, open_price, high_price, low_price, close_price, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore) 
    VALUES %s 
    ON CONFLICT (open_time) 
    DO UPDATE SET 
        close_price = EXCLUDED.close_price, high_price = EXCLUDED.high_price, 
        low_price = EXCLUDED.low_price, volume = EXCLUDED.volume, 
        number_of_trades = EXCLUDED.number_of_trades;
    """
    try:
        with conn.cursor() as cur:
            extras.execute_values(cur, query, transformed_data)
            inserted_count = cur.rowcount
            conn.commit()
        return inserted_count
    except Exception as e:
        log.error(f"Error upserting batch data into '{table_name}': {e}")
        conn.rollback()
        return 0