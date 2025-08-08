# trading_system/trader.py

import sys
import os
import yaml
import importlib
import asyncio
import json
import websocket
import pandas as pd

# Add project root to Python's path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)

from trading_system.utils.common import log
from trading_system.utils import db_utils
from trading_system.engine.execution_handler import ExecutionHandler
from trading_system.engine.portfolio_manager import PortfolioManager

CONFIG_PATH = os.path.join(PROJECT_ROOT, 'trading_system', 'config', 'config.yaml')

# --- Global State ---
STRATEGY_STATES = {}

class TradingState:
    SEARCHING = "SEARCHING"
    IN_POSITION = "IN_POSITION"

# --- Strategy Loader ---
def load_strategies_from_config(config_path: str) -> list:
    log.info(f"Loading strategies from config: {config_path}")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    initialized_strategies = [] # <-- FIXED
    for sc in config.get('strategies', []): # <-- FIXED
        try:
            module_path = "trading_system." + sc['module']
            class_name = sc['class']
            log.info(f"  -> Loading strategy '{class_name}' from '{module_path}'")
            
            module = importlib.import_module(module_path)
            StrategyClass = getattr(module, class_name)
            instance = StrategyClass()
            instance.initialize(sc)
            initialized_strategies.append((instance, sc))
        except Exception as e:
            log.error(f"Failed to load strategy '{sc.get('name', 'N/A')}': {e}", exc_info=True)

    return initialized_strategies, config.get('system', {})

# --- Data Pre-loading ---
def preload_historical_data(asset: str, timeframe: str, db_config: dict) -> pd.DataFrame:
    log.info(f"Pre-loading historical data for {asset} on {timeframe} timeframe...")
    try:
        from datetime import datetime, timedelta
        lookback_days = 10
        if 'h' in timeframe: lookback_days = int(timeframe.replace('h', '')) * 200 / 24 + 2
        elif 'min' in timeframe: lookback_days = int(timeframe.replace('min', '')) * 200 / 1440 + 2

        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(days=lookback_days)
        
        df_1m = db_utils.fetch_candles_for_range(db_config, asset, start_dt, end_dt)
        if df_1m is None or df_1m.empty:
            log.warning(f"Could not preload historical data for {asset}.")
            return pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])

        df_1m.rename(columns={'open_price': 'Open', 'high_price': 'High', 'low_price': 'Low', 'close_price': 'Close', 'volume': 'Volume'}, inplace=True)
        agg_rules = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
        df_resampled = df_1m.resample(timeframe).agg(agg_rules).dropna()
        log.info(f"Successfully pre-loaded {len(df_resampled)} bars for {asset}.")
        return df_resampled
    except Exception as e:
        log.error(f"Failed to preload data for {asset}: {e}", exc_info=True)
        return pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])

# --- Live Trading Logic ---
async def strategy_runner(strategy, config, execution_handler, portfolio_manager, db_config):
    strategy_name = strategy.name
    asset = config['asset']
    timeframe = config.get('timeframe', '1h')
    
    historical_data = preload_historical_data(asset, timeframe, db_config)
    
    STRATEGY_STATES[strategy_name] = {
        'state': TradingState.SEARCHING,
        'data': historical_data,
        'last_processed_timestamp': historical_data.index[-1] if not historical_data.empty else None
    }
    log.info(f"[{strategy_name}] Initialized. State: SEARCHING. Asset: {asset}. Timeframe: {timeframe}.")

    socket_url = f"wss://stream.binance.com:9443/ws/{asset.replace('-', '').lower()}@kline_1m"
    
    def on_message(ws, message):
        json_message = json.loads(message)
        candle = json_message.get('k')
        if candle:
            portfolio_manager.update_market_value(asset, float(candle['c']))
            if candle['x']:
                handle_closed_candle(candle)

    def handle_closed_candle(candle):
        new_row = {'Open': float(candle['o']), 'High': float(candle['h']), 'Low': float(candle['l']), 'Close': float(candle['c']), 'Volume': float(candle['v'])}
        timestamp = pd.to_datetime(candle['t'], unit='ms', utc=True)
        
        current_data = STRATEGY_STATES[strategy_name]['data']
        new_df = pd.DataFrame([new_row], index=[timestamp])
        STRATEGY_STATES[strategy_name]['data'] = pd.concat([current_data, new_df])

        resampler = STRATEGY_STATES[strategy_name]['data'].resample(timeframe)
        if len(resampler) > 0:
            last_ts = resampler.last().index[-1]
            if STRATEGY_STATES[strategy_name]['last_processed_timestamp'] is None or last_ts > STRATEGY_STATES[strategy_name]['last_processed_timestamp']:
                process_new_bar(last_ts, resampler, float(candle['c']))
                STRATEGY_STATES[strategy_name]['last_processed_timestamp'] = last_ts

    def process_new_bar(timestamp, resampler, current_price):
        log.info(f"[{strategy_name}] New '{timeframe}' bar closed at {timestamp}")
        agg_rules = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
        resampled_df = resampler.agg(agg_rules).dropna()
        
        signals_df = strategy.generate_signals(resampled_df.copy())
        latest_signal = signals_df['signal'].iloc[-1]

        current_state = STRATEGY_STATES[strategy_name]['state']
        if current_state == TradingState.SEARCHING and latest_signal == 1:
            log.info(f"[{strategy_name}] 🟢 BUY SIGNAL DETECTED 🟢")
            risk_amount = portfolio_manager.calculate_position_size(asset)
            if risk_amount > 0:
                quantity = risk_amount / current_price
                execution_handler.place_order(asset, 'MARKET', quantity, 'BUY')
                portfolio_manager.on_fill(asset, quantity, current_price, 'BUY')
                STRATEGY_STATES[strategy_name]['state'] = TradingState.IN_POSITION

        elif current_state == TradingState.IN_POSITION and latest_signal == -1:
            log.info(f"[{strategy_name}] 🔴 SELL SIGNAL DETECTED 🔴")
            quantity_to_sell = portfolio_manager.positions.get(asset, 0)
            if quantity_to_sell > 0:
                execution_handler.place_order(asset, 'MARKET', quantity_to_sell, 'SELL')
                portfolio_manager.on_fill(asset, quantity_to_sell, current_price, 'SELL')
                STRATEGY_STATES[strategy_name]['state'] = TradingState.SEARCHING

    ws = websocket.WebSocketApp(socket_url, on_message=on_message)
    ws.run_forever()


async def main():
    log.info("--- Starting Multi-Strategy Live Trading Engine ---")
    
    strategies, system_config = load_strategies_from_config(CONFIG_PATH)
    if not strategies: return

    db_config = system_config.get('database')
    if not db_config:
        log.error("Database configuration missing from config.yaml.")
        return

    portfolio_manager = PortfolioManager(system_config)
    execution_handler = ExecutionHandler(system_config)
    
    tasks = [] # <-- FIXED
    for strategy_instance, config in strategies:
        task = asyncio.create_task(
            strategy_runner(strategy_instance, config, execution_handler, portfolio_manager, db_config)
        )
        tasks.append(task)

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("--- Shutdown signal received ---")
