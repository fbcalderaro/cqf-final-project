import pandas as pd
import polars as pl
import psycopg2
from psycopg2 import extras
from datetime import datetime, timezone
import common
import config # Import the config module

# --- Common ---

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=config.DB_NAME, user=config.DB_USER, password=config.DB_PASSWORD, 
            host=config.DB_HOST, port=config.DB_PORT
        )
        return conn
    except Exception as e:
        common.log.error(f"❌ Could not connect to the database: {e}")
        return None

# --- Candles ---

def create_candles_table(conn, table_name):
    """Creates the specified candles table if it does not already exist."""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        open_time TIMESTAMPTZ PRIMARY KEY, open_price NUMERIC, high_price NUMERIC,
        low_price NUMERIC, close_price NUMERIC, volume NUMERIC, close_time TIMESTAMPTZ,
        quote_asset_volume NUMERIC, number_of_trades BIGINT, taker_buy_base_asset_volume NUMERIC,
        taker_buy_quote_asset_volume NUMERIC, ignore TEXT
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
    common.log.info(f"✅ Table '{table_name}' is ready.")

def insert_batch_data(conn, data, table_name):
    if not data: return 0
    transformed_data = [(datetime.fromtimestamp(row[0]/1000, tz=timezone.utc), row[1], row[2], row[3], row[4], row[5], datetime.fromtimestamp(row[6]/1000, tz=timezone.utc), row[7], row[8], row[9], row[10], 'historical') for row in data]
    insert_query = f"INSERT INTO {table_name} (open_time, open_price, high_price, low_price, close_price, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore) VALUES %s ON CONFLICT (open_time) DO NOTHING;"
    with conn.cursor() as cur:
        extras.execute_values(cur, insert_query, transformed_data)
        inserted_count = cur.rowcount
        conn.commit()
    common.log.info(f"    💾 Inserted {inserted_count} new historical records.")
    return inserted_count

def get_latest_timestamp(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(open_time) FROM {table_name};")
        return cur.fetchone()[0]

def upsert_realtime_candle(conn, candle_data, table_name):
    k = candle_data['k']
    if not k['x']: return
    common.log.info(f"🕯️  New closed candle received: {datetime.fromtimestamp(k['t']/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    query = f"INSERT INTO {table_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (open_time) DO UPDATE SET close_price = EXCLUDED.close_price, high_price = EXCLUDED.high_price, low_price = EXCLUDED.low_price, volume = EXCLUDED.volume, number_of_trades = EXCLUDED.number_of_trades;"
    data = (datetime.fromtimestamp(k['t']/1000, tz=timezone.utc), k['o'], k['h'], k['l'], k['c'], k['v'], datetime.fromtimestamp(k['T']/1000, tz=timezone.utc), k['q'], k['n'], k['V'], k['Q'], 'realtime')
    with conn.cursor() as cur:
        cur.execute(query, data)
        conn.commit()
    common.log.info("    💾 Record inserted/updated successfully.")

def fetch_candles_as_polars_df(table_name, start_time):
    common.log.info(f"Fetching data from '{table_name}' starting from {start_time}...")
    conn = get_db_connection()
    if not conn: return None
    try:
        sql_query = f"SELECT * FROM {table_name} WHERE open_time >= %s ORDER BY open_time ASC"
        with conn.cursor() as cur:
            cur.execute(sql_query, (start_time,))
            rows = cur.fetchall()
            if not rows: return pl.DataFrame()
            columns = [desc[0] for desc in cur.description]
            df = pl.DataFrame(rows, schema=columns)
        numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))
        common.log.info(f"✅ Successfully loaded and casted {len(df)} rows of data.")
        return df
    except Exception as e:
        common.log.error(f"❌ An error occurred while fetching Polars data: {e}")
        return None
    finally:
        if conn: conn.close()

def fetch_candles_for_range_as_polars_df(table_name, start_dt, end_dt):
    common.log.info(f"Fetching candles from {start_dt} to {end_dt}...")
    conn = get_db_connection()
    if not conn: return None
    try:
        sql_query = f"SELECT * FROM {table_name} WHERE open_time >= %s AND open_time < %s ORDER BY open_time ASC"
        with conn.cursor() as cur:
            cur.execute(sql_query, (start_dt, end_dt))
            rows = cur.fetchall()
            if not rows: return pl.DataFrame()
            columns = [desc[0] for desc in cur.description]
            df = pl.DataFrame(rows, schema=columns, orient="row")
        numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))
        common.log.info(f"✅ Loaded and casted {len(df)} rows.")
        return df
    except Exception as e:
        common.log.error(f"❌ An error occurred while fetching polars data for range: {e}")
        return None
    finally:
        if conn: conn.close()

# --- Indicators ---

def sync_indicators_schema(conn, table_name, expected_columns):
    """
    Checks if the indicators table has all expected columns and adds any that are missing.
    This makes the schema robust to changes in ta.py.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s;", (table_name,))
            existing_columns = {row[0] for row in cur.fetchall()}
            missing_columns = expected_columns - existing_columns
            for col in missing_columns:
                common.log.warning(f"⚠️ Missing column '{col}' in table '{table_name}'. Adding it...")
                # Use double quotes to handle special characters in column names
                cur.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" NUMERIC;')
        conn.commit()
    except Exception as e:
        common.log.error(f"❌ Failed to sync schema for table '{table_name}': {e}")
        conn.rollback()


def create_indicators_table(conn, table_name):
    """
    Creates the indicators table if it doesn't exist, and syncs its schema
    to ensure all necessary columns are present.
    """
    strategy_params = config.STRATEGY_CONFIG
    rsi_period = strategy_params.getint('rsi_period')
    atr_period = strategy_params.getint('atr_period')
    adx_period = strategy_params.getint('adx_period')
    st_period = strategy_params.getint('supertrend_period')
    st_multiplier = strategy_params.getfloat('supertrend_multiplier')
    st_dir_col_name = f'supertrend_{st_period}_{str(st_multiplier).replace(".", "_")}_dir'

    # Base CREATE TABLE query
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        open_time TIMESTAMPTZ PRIMARY KEY
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_query)
    conn.commit()

    # Define all columns the application expects to exist
    expected_cols = {
        f'rsi_{rsi_period}', f'atr_{atr_period}', f'adx_{adx_period}',
        f'plus_di_{adx_period}', f'minus_di_{adx_period}', st_dir_col_name
    }
    # Sync schema to add any missing indicator columns
    sync_indicators_schema(conn, table_name, expected_cols)
    
    common.log.info(f"✅ Table '{table_name}' is ready and schema is synced.")


def save_indicators_to_db(df: pl.DataFrame, table_name):
    """
    Upserts indicator data into the database.
    """
    if df.is_empty():
        common.log.warning("No indicator data to save.")
        return
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Could not establish database connection.")
        create_indicators_table(conn, table_name)
        common.log.info(f"Upserting {len(df)} indicator records into '{table_name}'...")
        data = list(df.iter_rows())
        cols = df.columns
        update_cols = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in cols if col != 'open_time'])
        query = f'INSERT INTO {table_name} ("' + '", "'.join(cols) + '") VALUES %s ON CONFLICT (open_time) DO UPDATE SET ' + update_cols + ';'
        with conn.cursor() as cur:
            extras.execute_values(cur, query, data)
            conn.commit()
        common.log.info(f"✅ Successfully upserted indicators into '{table_name}'.")
    except Exception as e:
        common.log.error(f"❌ An error occurred while saving indicators: {e}")
    finally:
        if conn:
            conn.close()

def fetch_resampled_candles_and_indicators(source_candles_table, source_indicators_table, interval='15min', limit=None):
    """
    Fetches 1-minute candle and indicator data and resamples it to a higher
    timeframe in memory using pandas.
    """
    common.log.info(f"Fetching 1-minute data to resample to '{interval}'...")
    conn = get_db_connection()
    if not conn:
        return None
    try:
        strategy_params = config.STRATEGY_CONFIG
        rsi_col = f"rsi_{strategy_params.getint('rsi_period')}"
        atr_col = f"atr_{strategy_params.getint('atr_period')}"
        adx_col = f"adx_{strategy_params.getint('adx_period')}"
        plus_di_col = f"plus_di_{strategy_params.getint('adx_period')}"
        minus_di_col = f"minus_di_{strategy_params.getint('adx_period')}"
        st_dir_col = f"supertrend_{strategy_params.getint('supertrend_period')}_{str(strategy_params.getfloat('supertrend_multiplier')).replace('.', '_')}_dir"

        source_limit = None
        if limit:
            minutes_in_interval = int(''.join(filter(str.isdigit, interval)))
            source_limit = (limit * minutes_in_interval) + 500
        
        query = f"""
        SELECT c.open_time, c.open_price, c.high_price, c.low_price, c.close_price, c.volume,
               i."{rsi_col}", i."{atr_col}", i."{st_dir_col}" AS supertrend_direction,
               i."{adx_col}", i."{plus_di_col}", i."{minus_di_col}"
        FROM "{source_candles_table}" c
        INNER JOIN "{source_indicators_table}" i ON c.open_time = i.open_time
        ORDER BY c.open_time DESC
        """
        if source_limit:
             query += f" LIMIT {int(source_limit)}"
        query += ";"

        df = pd.read_sql_query(query, conn, index_col='open_time')
        df.sort_index(ascending=True, inplace=True)

        if df.empty:
            common.log.warning("No source data found to resample.")
            return None

        common.log.info(f"✅ Loaded {len(df)} 1-min records. Resampling...")
        agg_rules = {
            'open_price': 'first', 'high_price': 'max', 'low_price': 'min', 'close_price': 'last', 'volume': 'sum',
            rsi_col: 'last', atr_col: 'last', 'supertrend_direction': 'last',
            adx_col: 'last', plus_di_col: 'last', minus_di_col: 'last'
        }
        resampled_df = df.resample(interval).agg(agg_rules)
        resampled_df.dropna(inplace=True)
        resampled_df.rename(columns={
            rsi_col: 'rsi_9', atr_col: 'atr_14', adx_col: 'adx_14',
            plus_di_col: 'plus_di_14', minus_di_col: 'minus_di_14'
        }, inplace=True)

        for col in resampled_df.columns:
            resampled_df[col] = pd.to_numeric(resampled_df[col], errors='coerce')
        resampled_df.dropna(inplace=True)
        common.log.info(f"✅ Resampled to {len(resampled_df)} '{interval}' bars.")
        if limit:
            resampled_df = resampled_df.tail(limit)
        return resampled_df
    except Exception as e:
        common.log.error(f"❌ Error during fetch/resample: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()

# --- Charting ---

def fetch_candles_and_indicators_for_range(start_dt, end_dt):
    """
    Fetches raw 1-minute candle and indicator data for a specific date range,
    returning a pandas DataFrame ready for plotting.
    """
    common.log.info(f"Fetching full 1-min data from {start_dt.date()} to {end_dt.date()}...")
    conn = get_db_connection()
    if not conn: return None
    try:
        strategy_params = config.STRATEGY_CONFIG
        rsi_col = f"rsi_{strategy_params.getint('rsi_period')}"
        adx_col = f"adx_{strategy_params.getint('adx_period')}"
        plus_di_col = f"plus_di_{strategy_params.getint('adx_period')}"
        minus_di_col = f"minus_di_{strategy_params.getint('adx_period')}"
        st_dir_col = f"supertrend_{strategy_params.getint('supertrend_period')}_{str(strategy_params.getfloat('supertrend_multiplier')).replace('.', '_')}_dir"

        query = f"""
        SELECT 
            c.open_time, c.open_price, c.high_price, c.low_price, c.close_price, c.volume,
            i."{rsi_col}", i."{adx_col}", i."{plus_di_col}", i."{minus_di_col}", i."{st_dir_col}"
        FROM "{config.CANDLES_TABLE_NAME}" c
        INNER JOIN "{config.INDICATORS_TABLE_NAME}" i ON c.open_time = i.open_time
        WHERE c.open_time >= %s AND c.open_time < %s
        ORDER BY c.open_time ASC;
        """
        df = pd.read_sql_query(query, conn, params=(start_dt, end_dt), index_col='open_time')
        if df.empty:
            common.log.warning("No data found for the specified date range.")
            return None
        
        # Rename columns for easier access in the plotting script
        df.rename(columns={
            rsi_col: 'rsi', adx_col: 'adx', plus_di_col: 'plus_di',
            minus_di_col: 'minus_di', st_dir_col: 'supertrend_direction'
        }, inplace=True)
        
        # Ensure correct data types
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df.dropna(inplace=True)

        common.log.info(f"✅ Loaded {len(df)} 1-min records for charting.")
        return df
    except Exception as e:
        common.log.error(f"❌ Error fetching data for chart: {e}", exc_info=True)
        return None
    finally:
        if conn: conn.close()