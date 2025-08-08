# trading_system/data_ingestion.py

import sys
import os
import yaml
import requests
import time
import argparse
import asyncio
import json
import websocket
from datetime import datetime, timezone, timedelta

# Add project root to Python's path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)

from trading_system.utils import db_utils
from trading_system.utils.common import log

CONFIG_PATH = os.path.join(PROJECT_ROOT, 'trading_system', 'config', 'config.yaml')
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"

class DataIngestor:
    """
    A unified class for ingesting cryptocurrency data from Binance.
    """
    def __init__(self, config):
        self.config = config
        self.db_config = config['system']['database']
        self.ingestion_config = config['data_ingestion']
        self.assets = self.ingestion_config['assets_to_track']
        self.interval = self.ingestion_config['base_interval']

    def _fetch_and_store(self, conn, asset, table_name, start_dt):
        current_dt = start_dt
        while current_dt < datetime.now(timezone.utc):
            params = {'symbol': asset.replace('-', ''), 'interval': self.interval, 'startTime': int(current_dt.timestamp() * 1000), 'limit': 1000}
            try:
                log.info(f"⬇️  Fetching up to 1000 records for {asset} from {current_dt.strftime('%Y-%m-%d %H:%M:%S')}...")
                response = requests.get(BINANCE_API_URL, params=params)
                response.raise_for_status()
                data = response.json()
                if not data: break
                fetched_count = len(data)
                inserted_count = db_utils.insert_batch_data(conn, data, table_name)
                log.info(f"    ✅ Fetched {fetched_count} records, 💾 Inserted {inserted_count} new records.")
                if fetched_count < 1000:
                    log.info(f"API returned fewer than 1000 records. Backfill for {asset} is complete.")
                    break
                last_ts_ms = data[-1][0]
                current_dt = datetime.fromtimestamp(last_ts_ms / 1000, tz=timezone.utc) + timedelta(minutes=1)
                time.sleep(0.5)
            except requests.exceptions.RequestException as e:
                log.error(f"Error fetching data from Binance API: {e}")
                time.sleep(10)
            except Exception as e:
                log.error(f"An unexpected error occurred during fetch/store: {e}", exc_info=True)
                break

    def run_backfill(self):
        log.info("--- Starting Historical Data Backfill Process ---")
        for asset in self.assets:
            self.backfill_asset(asset)
        log.info("--- Historical Data Backfill Complete ---")

    def backfill_asset(self, asset: str):
        log.info(f"--- Processing asset: {asset} ---")
        table_name = f"{asset.replace('-', '').lower()}_{self.interval}_candles"
        conn = db_utils.get_db_connection(self.db_config)
        if not conn: return
        try:
            db_utils.create_candles_table(conn, table_name)
            latest_ts = db_utils.get_latest_timestamp(conn, table_name)
            start_dt = (latest_ts + timedelta(minutes=1)) if latest_ts else datetime.strptime(self.ingestion_config['historical_start_date'], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            log.info(f"Starting backfill for {asset} from {start_dt}.")
            self._fetch_and_store(conn, asset, table_name, start_dt)
        finally:
            if conn: conn.close()

    async def run_live(self):
        log.info("--- Starting Live Data Ingestion Process ---")
        tasks = [self.listen_to_asset(asset) for asset in self.assets]
        await asyncio.gather(*tasks)

    async def listen_to_asset(self, asset: str):
        """
        Creates and manages a WebSocket connection for a single asset,
        running the blocking `run_forever` call in a separate thread.
        """
        table_name = f"{asset.replace('-', '').lower()}_{self.interval}_candles"
        socket_url = f"wss://stream.binance.com:9443/ws/{asset.replace('-', '').lower()}@kline_{self.interval}"
        
        conn = db_utils.get_db_connection(self.db_config)
        if not conn:
            log.error(f"Cannot start listener for {asset}, DB connection failed.")
            return

        def on_message(ws, message):
            json_message = json.loads(message)
            db_utils.upsert_realtime_candle(conn, json_message, table_name)

        ws_app = websocket.WebSocketApp(socket_url, on_message=on_message, on_error=lambda ws, err: log.error(f"WS Error for {asset}: {err}"))
        
        # --- CORRECTED CONCURRENCY LOGIC ---
        # Get the current asyncio event loop
        loop = asyncio.get_event_loop()
        log.info(f"Starting WebSocket listener for {asset} in a background thread...")
        # Use run_in_executor to run the blocking call without blocking the event loop
        await loop.run_in_executor(None, ws_app.run_forever)
        # ---

        # This part will only be reached if run_forever exits
        conn.close()

    def run_sync(self):
        log.info("--- Starting Data Sync Process (Backfill + Live) ---")
        self.run_backfill()
        try:
            log.info("--- Backfill complete. Transitioning to live data ingestion... ---")
            asyncio.run(self.run_live())
        except KeyboardInterrupt:
            log.info("--- Shutdown signal received during live ingestion. ---")
        except Exception as e:
            log.error(f"A critical error occurred during the live ingestion phase: {e}", exc_info=True)

def main():
    parser = argparse.ArgumentParser(description="Data Ingestion Engine for the Trading System.")
    parser.add_argument(
        '--mode', 
        type=str, 
        default='sync', 
        choices=['backfill', 'live', 'sync'], 
        help="The mode to run the script in. 'sync' (default) runs backfill then live."
    )
    args = parser.parse_args()

    try:
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        log.error(f"Configuration file not found at: {CONFIG_PATH}")
        return
    
    ingestor = DataIngestor(config)

    if args.mode == 'backfill':
        ingestor.run_backfill()
    elif args.mode == 'live':
        asyncio.run(ingestor.run_live())
    elif args.mode == 'sync':
        ingestor.run_sync()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("--- Shutdown signal received ---")
