# trading_system/trader.py

import sys
import os
import yaml
import importlib
import asyncio
import json
import random
import websocket
import pandas as pd
import threading
import time 
from datetime import datetime, timedelta, timezone

# Add project root to Python's path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)

from trading_system.utils.common import log
from trading_system.utils import db_utils
from trading_system.engine.execution_handler import MockExecutionHandler, BinanceExecutionHandler
from trading_system.engine.portfolio_manager import PortfolioManager
from trading_system.engine.strategy_monitor import StrategyMonitor

CONFIG_PATH = os.path.join(PROJECT_ROOT, 'trading_system', 'config', 'config.yaml')

class TradingState:
    SEARCHING = "SEARCHING"
    IN_POSITION = "IN_POSITION"

# --- NEW HELPER FUNCTION ---
def validate_total_cash_allocation(config_path: str):
    """
    Loads all strategy configurations and validates that the sum of
    'cash_allocation_pct' does not exceed 100%.
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    all_strategies = config.get('strategies', [])
    if not all_strategies:
        log.warning("No strategies found in config to validate cash allocation.")
        return

    total_allocation = sum(sc.get('cash_allocation_pct', 0) for sc in all_strategies)

    log.info(f"Validating cash allocation... Total allocated across all strategies: {total_allocation:.2f}%")
    if total_allocation > 100.0:
        log.error(f"FATAL: Total cash allocation across all strategies is {total_allocation:.2f}%, which exceeds 100%.")
        log.error("Please adjust 'cash_allocation_pct' in your config.yaml file. Exiting.")
        sys.exit(1)
    elif total_allocation < 99.9: # Use a small buffer for float precision
        log.warning(f"Total cash allocation is {total_allocation:.2f}%. Note that {100 - total_allocation:.2f}% of capital is unallocated and will not be used.")

# --- Reconciliation Loop ---
def reconciliation_loop(portfolio_manager, execution_handler, interval: int, cash_allocation_pct: float):
    """
    Periodically checks the broker's account status and reconciles
    the internal portfolio manager state.
    """
    while True:
        try:
            log.info(f"--- [Reconciler] Waking up (Allocation: {cash_allocation_pct}%) ---")
            actual_status = execution_handler.get_account_status()
            
            # --- NEW: Calculate the allocated portion of cash for reconciliation ---
            total_actual_cash = actual_status.get('cash', 0.0)
            allocated_actual_cash = total_actual_cash * (cash_allocation_pct / 100.0)
            
            portfolio_manager.reconcile(actual_status['positions'], allocated_actual_cash)
        except Exception as e:
            log.error(f"[Reconciler] Error during reconciliation: {e}", exc_info=True)
        time.sleep(interval)

# --- Strategy Runner ---
async def strategy_runner(strategy, config, execution_handler, portfolio_manager, db_config, strategy_monitor):
    strategy_name = strategy.name
    asset = config['asset']
    timeframe = config.get('timeframe', '1h')
    
    historical_data = preload_historical_data(asset, timeframe, db_config)
    
    strategy_state = {
        'state': TradingState.SEARCHING,
        'data': historical_data,
        'last_processed_timestamp': historical_data.index[-1] if not historical_data.empty else None,
        'last_ws_message_time': time.time(),
        'config': config,
        'reconnect_attempts': 0
    }
    log.info(f"[{strategy_name}] Initialized. State: {strategy_state['state']}.")

    socket_url = f"wss://stream.binance.com:9443/ws/{asset.replace('-', '').lower()}@kline_1m"

    ws = None
    try:
        while True:
            try:
                log.info(f"[{strategy_name}] Attempting to connect to websocket: {socket_url}")
                ws = websocket.WebSocketApp(
                    socket_url,
                    on_open=lambda ws: on_open(ws, strategy_name, strategy_state),
                    on_message=lambda ws, msg: on_message(ws, msg, strategy, strategy_state, portfolio_manager, execution_handler, strategy_monitor),
                    on_error=lambda ws, err: on_error(ws, err, strategy_name),
                    on_close=lambda ws, code, msg: on_close(ws, code, msg, strategy_name)
                )
                ws.run_forever()
                # If run_forever exits cleanly (e.g., server disconnect), we treat it as an
                # error to trigger the exponential backoff and reconnect logic.
                raise ConnectionAbortedError("Websocket connection closed unexpectedly. Reconnecting...")

            except Exception as e:
                strategy_state['reconnect_attempts'] += 1
                base_delay = 5; max_delay = 60
                backoff_time = min(max_delay, base_delay * (2 ** strategy_state['reconnect_attempts']))
                sleep_duration = backoff_time + random.uniform(0, 1)
                log.error(f"[{strategy_name}] Websocket connection error: {e}. Reconnecting in {sleep_duration:.2f} seconds... (Attempt {strategy_state['reconnect_attempts']})")
                time.sleep(sleep_duration)
    except KeyboardInterrupt:
        log.info(f"\n[{strategy_name}] Shutdown signal received. Exiting strategy runner.")
    finally:
        if ws and ws.sock and ws.sock.connected:
            ws.close()
        log.info(f"[{strategy_name}] Strategy runner has terminated.")

def on_open(ws, strategy_name: str, strategy_state: dict):
    if strategy_state['reconnect_attempts'] > 0:
        log.info(f"[{strategy_name}] ✅ Successfully reconnected to websocket.")
        strategy_state['reconnect_attempts'] = 0
    else:
        log.info(f"[{strategy_name}] Websocket connection opened.")

def on_error(ws, error, strategy_name):
    log.error(f"[{strategy_name}] Websocket error: {error}")

def on_close(ws, close_status_code, close_msg, strategy_name):
    log.warning(f"[{strategy_name}] Websocket connection closed. Code: {close_status_code}, Msg: {close_msg}")

def on_message(ws, message, strategy, strategy_state, portfolio_manager, execution_handler, strategy_monitor):
    try:
        strategy_state['last_ws_message_time'] = time.time()
        json_message = json.loads(message)
        candle = json_message.get('k')
        if candle and candle['x']:
            handle_closed_candle(candle, strategy, strategy_state, portfolio_manager, execution_handler, strategy_monitor)
    except Exception as e:
        log.error(f"[{strategy.name}] Error processing message: {e}", exc_info=True)

def handle_closed_candle(candle, strategy, strategy_state, portfolio_manager, execution_handler, strategy_monitor):
    timeframe = strategy_state['config']['timeframe']
    
    timestamp = pd.to_datetime(candle['t'], unit='ms', utc=True)
    # log.info(f"[{strategy.name}] Received closed 1m candle. Time: {timestamp.strftime('%H:%M:%S')}, Price: {candle['c']}")
    
    new_row = {'Open': float(candle['o']), 'High': float(candle['h']), 'Low': float(candle['l']), 'Close': float(candle['c']), 'Volume': float(candle['v'])}
    new_df = pd.DataFrame([new_row], index=[timestamp])
    strategy_state['data'] = pd.concat([strategy_state['data'], new_df])

    resample_freq = timeframe.replace('m', 'min').replace('h', 'H')
    resampler = strategy_state['data'].resample(resample_freq)
    if not resampler.groups: return

    last_resampled_ts = resampler.last().index[-1]
    if strategy_state['last_processed_timestamp'] is None or last_resampled_ts > strategy_state['last_processed_timestamp']:
        # log.info(f"[{strategy.name}] New '{timeframe}' bar detected. Timestamp: {last_resampled_ts}")
        process_new_bar(strategy, strategy_state, resampler, float(candle['c']), portfolio_manager, execution_handler, strategy_monitor)
        strategy_state['last_processed_timestamp'] = last_resampled_ts

def process_new_bar(strategy, strategy_state, resampler, current_price, portfolio_manager, execution_handler, strategy_monitor):
    asset = strategy_state['config']['asset']
    
    agg_rules = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
    resampled_df = resampler.agg(agg_rules).dropna()
    
    signals_df = strategy.generate_signals(resampled_df.copy())
    latest_signal = signals_df['signal'].iloc[-1]
    current_state = strategy_state['state']
    
    # log.info(f"[{strategy.name}] Analyzing bar. State: {current_state}, Signal: {latest_signal}")
    # log.info(f"    -> Current Cash: ${portfolio_manager.cash:,.2f}")
    
    # --- FIX: Log only the relevant position for this strategy ---
    base_asset = asset.split('-')[0]
    position_qty = portfolio_manager.positions.get(asset, 0.0)
    # log.info(f"    -> Current Position ({base_asset}): {position_qty}")
    
    if current_state == TradingState.SEARCHING and latest_signal == 1:
        log.info(f"[{strategy.name}] ---> Decision: BUY SIGNAL DETECTED <---")
        risk_amount = portfolio_manager.calculate_position_size(asset)
        if risk_amount > 0:
            quantity = risk_amount / current_price
            order_response = execution_handler.place_order(asset, 'MARKET', quantity, 'BUY', current_price)
            if order_response and order_response['status'] == 'FILLED':
                log.info(f"[{strategy.name}] Order fill confirmed by server.")
                portfolio_manager.on_fill(datetime.now(timezone.utc), asset, order_response['filled_quantity'], order_response['fill_price'], 'BUY')
                strategy_state['state'] = TradingState.IN_POSITION
            else:
                log.error(f"[{strategy.name}] BUY order failed! Response: {order_response}")

    elif current_state == TradingState.IN_POSITION and latest_signal == -1:
        log.info(f"[{strategy.name}] ---> Decision: SELL SIGNAL DETECTED <---")
        quantity_to_sell = portfolio_manager.positions.get(asset, 0)
        if quantity_to_sell > 0:
            order_response = execution_handler.place_order(asset, 'MARKET', quantity_to_sell, 'SELL', current_price)
            if order_response and order_response['status'] == 'FILLED':
                log.info(f"[{strategy.name}] Order fill confirmed by server.")
                portfolio_manager.on_fill(datetime.now(timezone.utc), asset, order_response['filled_quantity'], order_response['fill_price'], 'SELL')
                strategy_state['state'] = TradingState.SEARCHING
            else:
                log.error(f"[{strategy.name}] SELL order failed! Response: {order_response}")
    else:
        log.info(f"[{strategy.name}] ---> Decision: No action. Holding state: {current_state}")

    # --- Filter data for the monitor to show only the current live session ---
    # This prevents the chart from being cluttered with historical pre-load data.
    monitor_price_data = resampled_df[resampled_df.index >= strategy_monitor.start_time]

    # --- Update the live monitor report with filtered data ---
    strategy_monitor.generate_report(
        strategy_state=strategy_state['state'],
        latest_signal=int(latest_signal),
        current_price=current_price,
        price_data=monitor_price_data
    )


# --- Main Application ---
async def main(strategy_to_run: str):
    log.info(f"--- Starting Single-Strategy Live Trading Engine for: {strategy_to_run} ---")
    
    # --- NEW: Validate total allocation before proceeding ---
    validate_total_cash_allocation(CONFIG_PATH)

    strategy_instance, config, system_config = load_single_strategy_from_config(CONFIG_PATH, strategy_to_run)
    if not strategy_instance:
        log.error(f"Could not load strategy '{strategy_to_run}'. Exiting.")
        return

    asset_being_traded = config.get('asset')

    # --- NEW: Get cash allocation for this specific strategy ---
    cash_allocation_pct = config.get('cash_allocation_pct')
    if cash_allocation_pct is None:
        log.error(f"Strategy '{strategy_to_run}' is missing the required 'cash_allocation_pct' parameter in config.yaml.")
        log.error("Please add this parameter to the strategy configuration. Exiting.")
        sys.exit(1)
    log.info(f"Strategy '{strategy_to_run}' is allocated {cash_allocation_pct}% of the total capital.")
    
    trading_mode = system_config.get('trading_mode', 'paper').lower()
    
    if trading_mode == 'live':
        execution_handler = BinanceExecutionHandler(system_config)
        log.info("Live mode detected. Fetching initial account status from broker...")
        initial_status = execution_handler.get_account_status()
        total_broker_cash = initial_status.get('cash')
        initial_positions = initial_status.get('positions')
        
        if total_broker_cash is None:
            log.error("Could not fetch initial cash from broker. Exiting.")
            return
        
        # --- NEW: Calculate allocated cash for this strategy instance ---
        allocated_cash = total_broker_cash * (cash_allocation_pct / 100.0)
        portfolio_manager = PortfolioManager(system_config, initial_cash=allocated_cash, initial_positions=initial_positions, traded_asset=asset_being_traded)
    else: # Paper trading mode
        execution_handler = MockExecutionHandler(system_config)
        total_paper_cash = system_config.get('initial_cash', 100000.0)
        allocated_cash = total_paper_cash * (cash_allocation_pct / 100.0)
        portfolio_manager = PortfolioManager(system_config, initial_cash=allocated_cash, traded_asset=asset_being_traded)
    
    # --- NEW: Initialize the Strategy Monitor ---
    strategy_monitor = StrategyMonitor(
        strategy=strategy_instance,
        portfolio_manager=portfolio_manager,
        asset=asset_being_traded,
        timeframe=config.get('timeframe')
    )
    
    db_config = system_config.get('database')
    recon_interval = system_config.get('reconciliation_interval_seconds', 60)
    log.info(f"Portfolio reconciliation interval set to {recon_interval} seconds.")
    recon_thread = threading.Thread(target=reconciliation_loop, args=(portfolio_manager, execution_handler, recon_interval, cash_allocation_pct), daemon=True)
    recon_thread.start()
    log.info("Portfolio reconciliation thread started.")
    
    await strategy_runner(strategy_instance, config, execution_handler, portfolio_manager, db_config, strategy_monitor)

def load_single_strategy_from_config(config_path: str, strategy_name: str):
    log.info(f"Loading configuration for strategy: {strategy_name}")
    with open(config_path, 'r') as f: config = yaml.safe_load(f)
    
    strategy_config = next((sc for sc in config.get('strategies', []) if sc.get('name') == strategy_name), None)
    
    if not strategy_config:
        return None, None, None

    try:
        module_path = f"trading_system.{strategy_config['module']}"
        class_name = strategy_config['class']
        log.info(f"  -> Loading class '{class_name}' from '{module_path}'")
        module = importlib.import_module(module_path)
        StrategyClass = getattr(module, class_name)
        instance = StrategyClass()
        instance.initialize(strategy_config)
        return instance, strategy_config, config.get('system', {})
    except Exception as e:
        log.error(f"Failed to load strategy '{strategy_name}': {e}", exc_info=True)
        return None, None, None

def preload_historical_data(asset: str, timeframe: str, db_config: dict):
    log.info(f"Pre-loading historical data for {asset} on {timeframe} timeframe...")
    try:
        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(days=30)
        df_1m = db_utils.fetch_candles_for_range(db_config, asset, start_dt, end_dt)
        if df_1m is None or df_1m.empty: return pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])
        df_1m.rename(columns={'open_price': 'Open', 'high_price': 'High', 'low_price': 'Low', 'close_price': 'Close', 'volume': 'Volume'}, inplace=True)
        resample_freq = timeframe.replace('m', 'min').replace('h', 'H')
        agg_rules = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
        df_resampled = df_1m.resample(resample_freq).agg(agg_rules).dropna()
        log.info(f"Successfully pre-loaded {len(df_resampled)} bars for {asset}.")
        return df_resampled
    except Exception as e:
        log.error(f"Failed to preload data for {asset}: {e}", exc_info=True)
        return pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])

if __name__ == "__main__":
    if len(sys.argv) < 2:
        log.error("Usage: python trader.py <strategy_name>")
        log.error("Please provide the 'name' of the strategy from your config.yaml as an argument.")
        sys.exit(1)
        
    strategy_name_to_run = sys.argv[1]
    
    try:
        asyncio.run(main(strategy_name_to_run))
    except KeyboardInterrupt:
        log.info("\n--- Shutdown signal received ---")
