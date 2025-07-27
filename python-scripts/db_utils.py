import os
import psycopg2
from psycopg2 import extras
from datetime import datetime, timezone

# --- Database Configuration ---
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        print("✅ Database connection successful.")
        return conn
    except Exception as e:
        print(f"❌ Could not connect to the database: {e}")
        return None

def create_candles_table(conn, table_name):
    """Creates the specified candles table if it does not already exist."""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
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
        print(f"✅ Table '{table_name}' is ready.")

def insert_batch_data(conn, data, table_name):
    """Inserts a batch of historical candle data into the specified table."""
    if not data:
        return 0

    transformed_data = [
        (
            datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc), row[1], row[2], row[3], row[4], row[5],
            datetime.fromtimestamp(row[6] / 1000, tz=timezone.utc), row[7], row[8], row[9], row[10], 'historical'
        ) for row in data
    ]

    insert_query = f"""
    INSERT INTO {table_name} (
        open_time, open_price, high_price, low_price, close_price, volume,
        close_time, quote_asset_volume, number_of_trades,
        taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (open_time) DO NOTHING;
    """
    with conn.cursor() as cur:
        extras.execute_batch(cur, insert_query, transformed_data)
        conn.commit()
        print(f"   💾 Inserted {cur.rowcount} new historical records.")
        return cur.rowcount

def get_latest_timestamp(conn, table_name):
    """Gets the most recent timestamp from the specified table."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(open_time) FROM {table_name};")
        result = cur.fetchone()[0]
        return result