"""
Main Trading Application (trader.py)

This script is the central orchestrator for the live/paper trading engine.
It loads strategies from the configuration file, initializes the portfolio manager,
and runs each strategy in its own asynchronous task, connecting to a real-time data stream.
"""

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
from trading_system.engine.strategy_portfolio import StrategyPortfolio
from trading_system.engine.strategy_monitor import StrategyMonitor
from trading_system.dashboard_generator import main as generate_dashboard

CONFIG_PATH = os.path.join(PROJECT_ROOT, 'trading_system', 'config', 'config.yaml')
MONITOR_DIR = os.path.join(PROJECT_ROOT, 'output', 'live_monitoring')

class TradingState:
    SEARCHING = "SEARCHING"
    IN_POSITION = "IN_POSITION"

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
def reconciliation_loop(portfolio_manager: PortfolioManager, execution_handler, interval: int):
    """
    Periodically checks the broker's account status and reconciles the
    master portfolio manager state.
    """
    # This loop runs in a separate thread to avoid blocking the main async event loop.
    # It ensures that the system's internal state doesn't drift from the broker's reality.
    while True:
        try:
            log.info(f"--- [Reconciler] Waking up ---")
            actual_status = execution_handler.get_account_status()
            portfolio_manager.reconcile(actual_status.get('cash', 0.0), actual_status.get('positions', {}))
        except Exception as e:
            log.error(f"[Reconciler] Error during reconciliation: {e}", exc_info=True)
        time.sleep(interval)

# --- Master Portfolio Monitor Loop ---
def save_master_portfolio_summary(portfolio_manager: PortfolioManager):
    """Saves a JSON summary of the master portfolio's state for the dashboard."""
    os.makedirs(MONITOR_DIR, exist_ok=True)
    summary_filepath = os.path.join(MONITOR_DIR, 'master_summary.json')

    equity_curve_data = []
    if portfolio_manager.equity_curve:
        # Convert the list of (timestamp, equity) tuples to a DataFrame for easy manipulation.
        equity_df = pd.DataFrame(portfolio_manager.equity_curve, columns=['Timestamp', 'Equity'])
        # Format timestamp to a standard string format for JSON serialization.
        equity_df['Timestamp'] = equity_df['Timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        equity_curve_data = equity_df.to_dict(orient='records')

    total_equity = portfolio_manager.get_total_equity() # This is a snapshot value
    pnl = total_equity - portfolio_manager.initial_cash
    pnl_pct = (pnl / portfolio_manager.initial_cash) * 100 if portfolio_manager.initial_cash > 0 else 0.0

    summary_data = {
        'portfolio_name': 'Master Account',
        'last_update': datetime.now(timezone.utc).isoformat(),
        'total_equity': total_equity,
        'pnl': pnl,
        'pnl_pct': pnl_pct,
        'equity_curve': equity_curve_data
    }

    try:
        with open(summary_filepath, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f)
    except Exception as e:
        log.error(f"Error writing master portfolio summary: {e}", exc_info=True)

def master_monitor_loop(portfolio_manager: PortfolioManager, interval: int):
    """Periodically saves the master portfolio summary."""
    while True:
        save_master_portfolio_summary(portfolio_manager)
        time.sleep(interval)

def dashboard_generator_loop(interval: int):
    """Periodically generates the main dashboard HTML file."""
    while True:
        try:
            # The generator script logs its own start/finish messages,
            # so we don't need extra logging here.
            generate_dashboard()
        except Exception as e:
            log.error(f"[Dashboard] Error during generation: {e}", exc_info=True)
        time.sleep(interval)

# --- Strategy Runner ---
async def strategy_runner(strategy, config, execution_handler, portfolio_manager, db_config, strategy_monitor):
    """
    The main asynchronous task for running a single trading strategy.

    This function manages the lifecycle of one strategy, including:
    - Pre-loading historical data.
    - Establishing and maintaining a WebSocket connection for real-time data.
    - Handling connection errors with exponential backoff.
    - Passing incoming data to the appropriate handlers.
    """
    strategy_name = strategy.name
    asset = config['asset']
    timeframe = config.get('timeframe', '1h')
    
    historical_data = preload_historical_data(asset, timeframe, db_config)
    
    # This dictionary holds the dynamic state for this specific strategy instance.
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
    loop = asyncio.get_event_loop()
    try:
        # This outer loop handles reconnection logic.
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
                # The `run_forever()` method is a blocking call. To prevent it from
                # halting the entire asyncio event loop (which would stop all other
                # strategies), we run it in a separate thread managed by the event loop's executor.
                await loop.run_in_executor(None, ws.run_forever)
                
                # If `run_forever` exits (e.g., due to a server disconnect), we raise an
                # exception to trigger the reconnect logic below.
                raise ConnectionAbortedError("Websocket connection closed unexpectedly. Reconnecting...")

            except Exception as e:
                # Exponential backoff for reconnection attempts to avoid spamming the server.
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
    """Callback executed when the WebSocket connection is successfully opened."""
    if strategy_state['reconnect_attempts'] > 0:
        log.info(f"[{strategy_name}] ✅ Successfully reconnected to websocket.")
        strategy_state['reconnect_attempts'] = 0
    else:
        log.info(f"[{strategy_name}] Websocket connection opened.")

def on_error(ws, error, strategy_name):
    """Callback executed when a WebSocket error occurs."""
    log.error(f"[{strategy_name}] Websocket error: {error}")

def on_close(ws, close_status_code, close_msg, strategy_name):
    """Callback executed when the WebSocket connection is closed."""
    log.warning(f"[{strategy_name}] Websocket connection closed. Code: {close_status_code}, Msg: {close_msg}")

def on_message(ws, message, strategy, strategy_state, portfolio_manager, execution_handler, strategy_monitor):
    """
    Callback for processing incoming WebSocket messages.
    This is the entry point for all real-time market data.
    """
    try:
        strategy_state['last_ws_message_time'] = time.time()
        json_message = json.loads(message)
        candle = json_message.get('k')
        # We only care about candles that have officially closed.
        if candle and candle['x']:
            handle_closed_candle(candle, strategy, strategy_state, portfolio_manager, execution_handler, strategy_monitor)
    except Exception as e:
        log.error(f"[{strategy.name}] Error processing message: {e}", exc_info=True)

def handle_closed_candle(candle, strategy, strategy_state, portfolio_manager, execution_handler, strategy_monitor):
    """
    Processes a single closed 1-minute candle from the WebSocket stream.
    It appends the new data and checks if a new bar for the strategy's timeframe has formed.
    """
    timeframe = strategy_state['config']['timeframe']
    
    timestamp = pd.to_datetime(candle['t'], unit='ms', utc=True)
    
    # Create a new DataFrame row for the incoming 1-minute candle.
    new_row = {'Open': float(candle['o']), 'High': float(candle['h']), 'Low': float(candle['l']), 'Close': float(candle['c']), 'Volume': float(candle['v'])}
    new_df = pd.DataFrame([new_row], index=[timestamp])
    # Append it to the strategy's historical data.
    strategy_state['data'] = pd.concat([strategy_state['data'], new_df])

    # Resample the accumulated 1-minute data into the strategy's required timeframe (e.g., '15m', '1h').
    resample_freq = timeframe.replace('m', 'min').replace('h', 'H')
    resampler = strategy_state['data'].resample(resample_freq)
    if not resampler.groups: return

    # Check if the latest resampled bar is newer than the last one we processed.
    last_resampled_ts = resampler.last().index[-1]
    if strategy_state['last_processed_timestamp'] is None or last_resampled_ts > strategy_state['last_processed_timestamp']:
        # A new bar has formed, time to make a decision.
        process_new_bar(strategy, strategy_state, resampler, float(candle['c']), portfolio_manager, execution_handler, strategy_monitor)
        strategy_state['last_processed_timestamp'] = last_resampled_ts

def process_new_bar(strategy, strategy_state, resampler, current_price, portfolio_manager, execution_handler, strategy_monitor):
    """
    The core decision-making function. Called when a new bar for the strategy's
    timeframe is completed.

    It performs these steps:
    1. Aggregates the 1-minute data into the final resampled DataFrame.
    2. Calls the strategy's `generate_signals` method.
    3. Retrieves the strategy's dedicated sub-portfolio.
    4. Checks the latest signal and current position state to decide on an action (BUY, SELL, HOLD).
    5. Executes orders through the execution handler.
    6. Updates the monitoring report.
    """
    asset = strategy_state['config']['asset']
    
    # 1. Aggregate the 1-minute data into the final OHLCV bars for the strategy's timeframe.
    agg_rules = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
    resampled_df = resampler.agg(agg_rules).dropna()
    
    # 2. Generate signals using the strategy's logic.
    signals_df = strategy.generate_signals(resampled_df.copy())
    latest_signal = signals_df['signal'].iloc[-1]
    
    # 3. Get the sub-portfolio dedicated to this specific strategy.
    strategy_portfolio = portfolio_manager.get_strategy_portfolio(strategy.name)
    if not strategy_portfolio:
        log.error(f"[{strategy.name}] Could not find its sub-portfolio. Skipping bar processing.")
        return

    # Update the sub-portfolio's market value before making decisions
    strategy_portfolio.update_market_value(current_price)
    current_state = strategy_state['state']

    # 4. Decision logic based on signal and current state.
    if current_state == TradingState.SEARCHING and latest_signal == 1:
        log.info(f"[{strategy.name}] ---> Decision: BUY SIGNAL DETECTED <---")
        # Position size is calculated based on the strategy's *own* allocated equity, not the master account.
        risk_amount = strategy_portfolio.calculate_position_size()
        if risk_amount > 0:
            quantity = risk_amount / current_price
            order_response = execution_handler.place_order(asset, 'MARKET', quantity, 'BUY', current_price)
            if order_response and order_response.get('success'):
                fill_data = order_response['data']
                # The master portfolio manager handles the state update for both the master account
                # and the specific strategy's sub-portfolio.
                portfolio_manager.on_fill(
                    strategy_name=strategy.name,
                    timestamp=datetime.now(timezone.utc), 
                    asset=asset, 
                    quantity=fill_data['filled_quantity'], 
                    fill_price=fill_data['fill_price'], 
                    direction='BUY',
                    trade_value_quote=fill_data['trade_value_quote']
                )
                strategy_state['state'] = TradingState.IN_POSITION
            else:
                log.error(f"[{strategy.name}] BUY order failed or was not confirmed! Reason: {order_response.get('error', 'Unknown')}")

    elif current_state == TradingState.IN_POSITION and latest_signal == -1:
        log.info(f"[{strategy.name}] ---> Decision: SELL SIGNAL DETECTED <---")
        # When selling, we sell the entire quantity held by this specific strategy's sub-portfolio.
        quantity_to_sell = strategy_portfolio.positions.get(asset, 0)
        if quantity_to_sell > 0:
            order_response = execution_handler.place_order(asset, 'MARKET', quantity_to_sell, 'SELL', current_price)
            if order_response and order_response.get('success'):
                fill_data = order_response['data']
                log.info(f"[{strategy.name}] SELL order fill confirmed by execution handler.")
                portfolio_manager.on_fill(
                    strategy_name=strategy.name,
                    timestamp=datetime.now(timezone.utc),
                    asset=asset,
                    quantity=fill_data['filled_quantity'],
                    fill_price=fill_data['fill_price'],
                    direction='SELL',
                    trade_value_quote=fill_data['trade_value_quote']
                )
                strategy_state['state'] = TradingState.SEARCHING
            else:
                log.error(f"[{strategy.name}] SELL order failed or was not confirmed! Reason: {order_response.get('error', 'Unknown')}")
    else:
        log.info(f"[{strategy.name}] ---> Decision: No action. Holding state: {current_state}")

    # 5. Generate the updated HTML and JSON monitoring files for this strategy.
    monitor_price_data = resampled_df[resampled_df.index >= strategy_monitor.start_time]
    strategy_monitor.generate_report(
        strategy_state=strategy_state['state'],
        latest_signal=int(latest_signal),
        current_price=current_price,
        price_data=monitor_price_data
    )


# --- Main Application ---
async def main():
    """The main entry point for the trading application."""
    log.info("--- Starting Multi-Strategy Live Trading Engine ---")
    
    # --- 1. Configuration Loading & Validation ---
    validate_total_cash_allocation(CONFIG_PATH)

    all_strategy_configs, system_config = load_all_strategies_from_config(CONFIG_PATH)
    if not all_strategy_configs:
        log.error("No strategies loaded from config. Exiting.")
        return

    # --- 2. Initialize Execution Handler (Live or Paper) ---
    trading_mode = system_config.get('trading_mode', 'paper').lower()
    
    if trading_mode == 'live':
        # For live trading, connect to the actual broker.
        execution_handler = BinanceExecutionHandler(system_config)
        log.info("Fetching initial account status from broker...")
        
        # Get the real account state before starting.
        initial_status = execution_handler.get_account_status()
        total_broker_cash = initial_status.get('cash')
        initial_positions = initial_status.get('positions')
        
        if total_broker_cash is None:
            log.error("FATAL: Could not fetch initial cash from broker. Exiting.")
            sys.exit(1)
        
        # Pre-run safety check: If we have existing positions in assets the system
        # is about to trade, flatten them to start from a clean, known state.
        assets_to_trade = {sc['asset'] for sc in all_strategy_configs}
        for asset in assets_to_trade:
            if asset in initial_positions and initial_positions[asset] > 0:
                position_qty = initial_positions[asset]
                log.warning(f"Found existing position for {asset} of {position_qty}. System will flatten this position before starting.")
                close_order_response = execution_handler.place_order(asset, 'MARKET', position_qty, 'SELL', price=0)
                
                if not (close_order_response and close_order_response.get('success')):
                    log.error(f"FATAL: Failed to close existing position for {asset}. Reason: {close_order_response.get('error', 'Unknown')}")
                    log.error("Cannot start in an unknown position state. Exiting.")
                    sys.exit(1)

        # Re-fetch the final state after all flattening is done to get the precise starting cash.
        log.info("Re-fetching final account status after flattening...")
        time.sleep(system_config.get('order_verify_delay_seconds', 2))
        final_status = execution_handler.get_account_status()
        total_broker_cash = final_status.get('cash')
        initial_positions = final_status.get('positions')

        portfolio_manager = PortfolioManager(system_config, initial_cash=total_broker_cash, initial_positions=initial_positions, relevant_assets=assets_to_trade)
    else: # Paper trading mode
        # For paper trading, use the mock handler that simulates fills.
        execution_handler = MockExecutionHandler(system_config)
        total_paper_cash = system_config.get('initial_cash', 100000.0)
        assets_to_trade = {sc['asset'] for sc in all_strategy_configs}
        portfolio_manager = PortfolioManager(system_config, initial_cash=total_paper_cash, initial_positions={}, relevant_assets=assets_to_trade)
        total_broker_cash = total_paper_cash # for allocation calculation

    # --- 3. Initialize and Register All Strategies ---
    strategy_tasks = []
    db_config = system_config.get('database')

    for config in all_strategy_configs:
        # Load the strategy's class from its module.
        strategy_instance = load_strategy_instance(config)
        if not strategy_instance: continue

        # Calculate the cash to allocate to this strategy based on the config percentage.
        cash_allocation_pct = config.get('cash_allocation_pct', 0)
        allocated_equity = total_broker_cash * (cash_allocation_pct / 100.0)
        
        # Register the strategy with the master portfolio manager, which creates
        # a dedicated sub-portfolio for it.
        portfolio_manager.register_strategy(strategy_instance.name, config, allocated_equity)
        strategy_portfolio = portfolio_manager.get_strategy_portfolio(strategy_instance.name)

        strategy_monitor = StrategyMonitor(
            strategy=strategy_instance,
            strategy_portfolio=strategy_portfolio, # Pass the sub-portfolio
            asset=config['asset'],
            timeframe=config.get('timeframe')
        )

        # Create an asyncio task for each strategy. This is what allows them to run concurrently.
        task = asyncio.create_task(strategy_runner(strategy_instance, config, execution_handler, portfolio_manager, db_config, strategy_monitor))
        strategy_tasks.append(task)
    
    # --- 4. Start Background Monitoring Threads ---
    # These threads run alongside the main async loop for periodic tasks.
    recon_interval = system_config.get('reconciliation_interval_seconds', 60)
    log.info(f"Portfolio reconciliation interval set to {recon_interval} seconds.")
    recon_thread = threading.Thread(target=reconciliation_loop, args=(portfolio_manager, execution_handler, recon_interval), daemon=True)
    recon_thread.start()
    log.info("Portfolio reconciliation thread started.")

    master_monitor_interval = system_config.get('master_monitor_interval_seconds', 60)
    master_monitor_thread = threading.Thread(target=master_monitor_loop, args=(portfolio_manager, master_monitor_interval), daemon=True)
    master_monitor_thread.start()
    log.info(f"Master portfolio monitor thread started (updates every {master_monitor_interval}s).")
    
    dashboard_interval = system_config.get('dashboard_interval_seconds', 60)
    dashboard_thread = threading.Thread(target=dashboard_generator_loop, args=(dashboard_interval,), daemon=True)
    dashboard_thread.start()
    log.info(f"Dashboard generator thread started (updates every {dashboard_interval}s).")

    # --- 5. Run All Strategy Tasks Concurrently ---
    if strategy_tasks:
        log.info(f"--- Starting {len(strategy_tasks)} strategies concurrently ---")
        await asyncio.gather(*strategy_tasks)
    else:
        log.warning("No strategies were successfully started.")

def load_all_strategies_from_config(config_path: str) -> tuple:
    """Loads the 'strategies' and 'system' sections from the main YAML config file."""
    log.info("Loading all strategy configurations...")
    with open(config_path, 'r') as f: config = yaml.safe_load(f)
    return config.get('strategies', []), config.get('system', {})

def load_strategy_instance(config: dict):
    """Loads a single strategy class instance from its config."""
    try:
        strategy_name = config['name']
        module_path = f"trading_system.{config['module']}"
        class_name = config['class']
        log.info(f"  -> Loading strategy '{strategy_name}' (Class: {class_name})")
        module = importlib.import_module(module_path)
        StrategyClass = getattr(module, class_name)
        instance = StrategyClass()
        instance.initialize(config)
        return instance
    except Exception as e:
        log.error(f"Failed to load strategy instance for '{config.get('name', 'N/A')}': {e}", exc_info=True)
        return None

def preload_historical_data(asset: str, timeframe: str, db_config: dict):
    """
    Fetches the initial chunk of historical data needed to warm up the strategy's indicators.
    """
    log.info(f"Pre-loading historical data for {asset} on {timeframe} timeframe...")
    try:
        # Fetch the last 30 days of 1-minute data to ensure we have enough to
        # calculate indicators even for long lookback periods (e.g., 200-period MA on a 1h chart).
        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(days=30)
        df_1m = db_utils.fetch_candles_for_range(db_config, asset, start_dt, end_dt)
        
        if df_1m is None or df_1m.empty:
            log.warning(f"No historical data found for {asset} in the last 30 days. Starting with an empty DataFrame.")
            return pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])
        
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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("\n--- Shutdown signal received ---")
        log.info("--- Engine shutting down. ---")
