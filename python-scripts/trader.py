import time
import websocket
import json
from datetime import datetime, timezone, timedelta
import config
from common import log
import db_utils
from ta import calculate_indicators
from broker import Broker

# --- State Management ---
class TradingState:
    SEARCHING = "SEARCHING"
    IN_POSITION = "IN_POSITION"

class Trader:
    """
    The main class for the live trading bot. Manages state, data, and strategy execution.
    """
    def __init__(self):
        self.state = TradingState.SEARCHING
        self.broker = Broker()
        self.ws_app = None
        # Sync state with the exchange on startup
        self.sync_state()

    def sync_state(self):
        """
        Checks the broker for open positions to determine the initial state.
        This makes the bot resilient to restarts.
        """
        log.info("--- Syncing State with Exchange ---")
        position_size = self.broker.get_open_positions(config.SYMBOL)
        
        if position_size > 0:
            self.state = TradingState.IN_POSITION
            log.info(f"✅ Found existing position of {position_size} {config.SYMBOL}. State set to IN_POSITION.")
        else:
            self.state = TradingState.SEARCHING
            log.info("✅ No existing position found. State set to SEARCHING.")

    def on_message(self, ws, message):
        """
        The core logic of the bot, executed on each new candle from the WebSocket.
        """
        try:
            json_message = json.loads(message)
            k = json_message.get('k')
            if not k or not k.get('x'): # Only process closed candles
                return

            log.info(f"🕯️  New Candle Received: Close Price = {k['c']}")

            # --- 1. Fetch Data & Calculate Indicators ---
            # We need the last ~200 candles for accurate indicator calculation
            lookback_start_time = datetime.now(timezone.utc) - timedelta(minutes=200)
            candles_df = db_utils.fetch_candles_for_range_as_polars_df(config.CANDLES_TABLE_NAME, lookback_start_time, datetime.now(timezone.utc))
            
            if candles_df is None or candles_df.is_empty() or len(candles_df) < 50:
                log.warning("Not enough data to calculate indicators. Skipping.")
                return

            indicators_df = calculate_indicators(candles_df)
            latest_indicators = indicators_df.to_dicts()[-1]

            # --- 2. Apply Strategy Logic Based on State ---
            log.info(f"Current State: {self.state}")
            self.execute_strategy(latest_indicators)

        except Exception as e:
            log.error(f"Error in on_message callback: {e}", exc_info=True)

    def execute_strategy(self, indicators):
        """
        Contains the actual trading logic based on the strategy rules.
        """
        # --- Strategy Parameters ---
        is_uptrend = indicators[f'supertrend_{config.STRATEGY_CONFIG.getint("supertrend_period")}_{str(config.STRATEGY_CONFIG.getfloat("supertrend_multiplier")).replace(".", "_")}_dir'] == 1
        is_trending = indicators[f'adx_{config.STRATEGY_CONFIG.getint("adx_period")}'] > config.ADX_THRESHOLD
        is_bullish = indicators[f'plus_di_{config.STRATEGY_CONFIG.getint("adx_period")}'] > indicators[f'minus_di_{config.STRATEGY_CONFIG.getint("adx_period")}']
        rsi_value = indicators[f'rsi_{config.STRATEGY_CONFIG.getint("rsi_period")}']
        in_buy_zone = config.RSI_BUY_ZONE[0] <= rsi_value <= config.RSI_BUY_ZONE[1]
        
        log.info(f"Signal Check: Uptrend={is_uptrend}, Trending={is_trending}, Bullish={is_bullish}, RSI={rsi_value:.2f}, InBuyZone={in_buy_zone}")

        # --- State Machine Logic ---
        if self.state == TradingState.SEARCHING:
            # --- Entry Condition ---
            if is_uptrend and is_trending and is_bullish and in_buy_zone:
                log.warning("🚀 BUY SIGNAL DETECTED 🚀")
                
                # Calculate position size
                usdt_balance = self.broker.get_asset_balance('USDT')
                if usdt_balance < 10: # Minimum trade size check
                    log.error("Insufficient USDT balance to place trade.")
                    return
                
                # For simplicity, we'll use a fixed percentage of our USDT balance
                trade_size_usdt = usdt_balance * config.RISK_PER_TRADE_PCT
                current_price = float(indicators['close_price']) # Assuming we can get close price
                quantity_to_buy = round(trade_size_usdt / current_price, 5) # Round to a valid precision for BTC

                # Place the buy order
                order = self.broker.place_market_order(config.SYMBOL, 'BUY', quantity_to_buy)
                if order:
                    log.info("✅ BUY order placed successfully.")
                    self.state = TradingState.IN_POSITION # IMPORTANT: Change state after successful order
                else:
                    log.error("❌ BUY order failed to place.")
        
        elif self.state == TradingState.IN_POSITION:
            # --- Exit Condition ---
            if not is_uptrend:
                log.warning("🛑 SELL SIGNAL DETECTED (Supertrend flipped) 🛑")
                
                # Get the size of our current position
                position_size = self.broker.get_open_positions(config.SYMBOL)
                if position_size > 0:
                    # Place the sell order
                    order = self.broker.place_market_order(config.SYMBOL, 'SELL', position_size)
                    if order:
                        log.info("✅ SELL order placed successfully.")
                        self.state = TradingState.SEARCHING # IMPORTANT: Change state after successful order
                    else:
                        log.error("❌ SELL order failed to place.")
                else:
                    log.warning("Sell signal detected, but no position found. Resetting state.")
                    self.state = TradingState.SEARCHING


    def start(self):
        """Initializes and starts the WebSocket client in a resilient loop."""
        reconnect_delay = 5
        while True:
            try:
                log.info("--- Starting Trader WebSocket Stream ---")
                self.ws_app = websocket.WebSocketApp(
                    config.SOCKET_URL,
                    on_open=lambda ws: log.info(f"--- WebSocket Connection Opened for Trader ---"),
                    on_message=self.on_message,
                    on_error=lambda ws, err: log.error(f"--- Trader WebSocket Error: {err} ---"),
                    on_close=lambda ws, code, msg: log.warning(f"--- Trader WebSocket Closed ---")
                )
                self.ws_app.run_forever()
                log.warning("WebSocket connection closed cleanly.")
                reconnect_delay = 5
            except Exception as e:
                log.error(f"An error occurred in the Trader WebSocket loop: {e}", exc_info=True)
            
            log.info(f"Attempting to reconnect in {reconnect_delay} seconds...")
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

if __name__ == '__main__':
    try:
        trader = Trader()
        trader.start()
    except KeyboardInterrupt:
        log.info("--- Shutdown signal received ---")
    except Exception as e:
        log.critical(f"A critical error occurred in the main trader block: {e}", exc_info=True)
    finally:
        log.info("--- Trader Shutdown Complete ---")
