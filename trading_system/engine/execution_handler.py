"""
Execution Handler Module (execution_handler.py)

This module defines the interface for executing trades. It provides a
`MockExecutionHandler` for paper trading and a `BinanceExecutionHandler`
for live trading on the Binance Testnet. This abstraction allows the core
trading logic to remain agnostic of the execution environment.
"""

import os
import time
import math
from trading_system.utils.common import log
import random
from binance.client import Client
from binance.exceptions import BinanceAPIException

class MockExecutionHandler:
    """
    Simulates a broker's execution for paper trading.

    It mimics order fills, calculates slippage and commissions, and maintains
    a virtual account state (cash and positions) without interacting with a
    real exchange.
    """
    def __init__(self, system_config: dict):
        """Initializes the mock handler with paper trading parameters."""
        self._mock_broker_cash = system_config.get('initial_cash', 100000.0)
        self._mock_broker_positions = {}
        self.commission_pct = system_config.get('commission_pct', 0.001)
        self.slippage_pct = system_config.get('paper_slippage_pct', 0.0005) # 0.05%
        log.info("Execution Handler initialized (PAPER TRADING MODE).")

    def place_order(self, asset: str, order_type: str, quantity: float, direction: str, price: float):
        """
        Simulates placing and filling an order.

        Returns:
            dict: A response dictionary mimicking the structure of the live handler,
                  containing details of the simulated fill.
        """
        log.info(f"--- MOCK ORDER PLACED ---")
        log.info(f"    Asset: {asset}, Type: {order_type}, Dir: {direction.upper()}, Qty: {quantity:.6f}")
        
        # --- NEW: Simulate slippage for more realistic paper trading ---
        slippage_factor = self.slippage_pct + random.uniform(0, 0.0002)
        fill_price = price * (1 + slippage_factor) if direction.upper() == 'BUY' else price * (1 - slippage_factor)
        log.info(f"    Simulating slippage. Target Price: {price:.4f}, Fill Price: {fill_price:.4f}")

        trade_value = quantity * fill_price
        commission = trade_value * self.commission_pct

        # The total quote value reflects the cash change, including commission.
        trade_value_quote = trade_value + commission if direction.upper() == 'BUY' else trade_value - commission

        # Update the internal mock broker state.
        if direction.upper() == 'BUY':
            self._mock_broker_cash -= trade_value_quote
            self._mock_broker_positions[asset] = self._mock_broker_positions.get(asset, 0) + quantity
        elif direction.upper() == 'SELL':
            self._mock_broker_cash += trade_value_quote
            self._mock_broker_positions[asset] = self._mock_broker_positions.get(asset, 0) - quantity
            if self._mock_broker_positions[asset] <= 1e-9:
                del self._mock_broker_positions[asset]

        # Return a structure that is identical to the live handler's success response.
        response = { "success": True, "data": {
                "status": "FILLED", "order_id": f"mock_{random.randint(10000, 99999)}",
                "asset": asset, "filled_quantity": quantity, "fill_price": fill_price,
                "trade_value_quote": trade_value_quote }
        }
        log.info(f"--- MOCK ORDER RESPONSE: {response['data']} ---")
        return response

    def get_account_status(self) -> dict:
        """Returns the current state of the mock broker account."""
        log.info("Mock execution handler received request for account status.")
        return {"cash": self._mock_broker_cash, "positions": self._mock_broker_positions.copy()}

class BinanceExecutionHandler:
    """
    Handles live order execution and account management via the Binance API.

    This class connects to the Binance Testnet, places market orders,
    and includes pre-trade (liquidity) and post-trade (fill verification)
    checks to ensure robust execution.
    """
    def __init__(self, system_config: dict):
        """Initializes the Binance client and sets execution parameters."""
        api_key = os.environ.get('BINANCE_KEY_TEST')
        api_secret = os.environ.get('BINANCE_SECRET_TEST')
        
        # --- NEW: Execution Control Parameters ---
        self.max_spread_pct = system_config.get('max_spread_pct', 0.15)
        self.order_verify_retries = system_config.get('order_verify_retries', 3)
        self.order_verify_delay_seconds = system_config.get('order_verify_delay_seconds', 2)

        if not api_key or not api_secret:
            raise ValueError("BINANCE_KEY_TEST and BINANCE_SECRET_TEST must be set in your .env file.")

        try:
            log.info("Connecting to Binance Testnet using API keys from environment...")
            self.client = Client(api_key, api_secret, testnet=True)
            self.client.ping()
            log.info("✅ Successfully connected to Binance Testnet.")
            log.info("Execution Handler initialized (LIVE TRADING MODE).")
        except BinanceAPIException as e:
            log.error(f"❌ Binance API Exception during initialization: {e}")
            raise

    def _is_liquid(self, symbol: str) -> bool:
        """
        Pre-trade check to ensure the bid-ask spread is within an acceptable range.
        This prevents entering trades in illiquid market conditions where slippage
        could be excessively high.

        Returns:
            bool: True if the market is liquid enough, False otherwise.
        """
        log.info(f"    Checking liquidity for {symbol}...")
        try:
            ticker = self.client.get_orderbook_ticker(symbol=symbol)
            bid_price = float(ticker['bidPrice'])
            ask_price = float(ticker['askPrice'])
            
            if ask_price == 0: return False # Avoid division by zero
            
            spread_pct = ((ask_price - bid_price) / ask_price) * 100
            log.info(f"    Current Spread: {spread_pct:.4f}%")
            
            if spread_pct > self.max_spread_pct:
                log.warning(f"    LIQUIDITY CHECK FAILED: Spread {spread_pct:.4f}% > Max {self.max_spread_pct:.2f}%. Aborting order.")
                return False
            
            log.info(f"    ✅ Liquidity check passed.")
            return True
        except BinanceAPIException as e:
            log.error(f"    ❌ API Error during liquidity check for {symbol}: {e}")
            return False # Fail safe

    def _verify_order_fill(self, symbol: str, order_id: int) -> dict:
        """
        Post-trade check to verify if an order was actually filled.
        It polls the exchange a few times to account for potential API or
        matching engine delays.

        Returns:
            dict: A success or failure dictionary containing the final fill details
                  or an error message.
        """
        log.info(f"    Verifying fill for Order ID {order_id}...")
        for i in range(self.order_verify_retries):
            try:
                order_status = self.client.get_order(symbol=symbol, orderId=order_id)
                if order_status['status'] == 'FILLED':
                    log.info(f"    ✅ Verification successful. Order {order_id} is FILLED.")
                    
                    # Extract precise fill details from the confirmed order.
                    executed_qty = float(order_status['executedQty'])
                    cummulative_quote_qty = float(order_status['cummulativeQuoteQty'])
                    
                    fill_price = 0
                    if executed_qty > 0:
                        fill_price = cummulative_quote_qty / executed_qty

                    return {
                        "success": True,
                        "data": {
                            "status": "FILLED", "order_id": order_id, "asset": symbol.replace('USDT', '-USDT'),
                            "filled_quantity": executed_qty,
                            "fill_price": fill_price,
                            "trade_value_quote": cummulative_quote_qty
                        }
                    }
                log.warning(f"    Order {order_id} status is '{order_status['status']}'. Retrying verification... ({i+1}/{self.order_verify_retries})")
                time.sleep(self.order_verify_delay_seconds)
            except BinanceAPIException as e:
                log.error(f"    ❌ API Error during order verification for {order_id}: {e}")
                time.sleep(self.order_verify_delay_seconds)
        log.error(f"    Verification FAILED for order {order_id} after {self.order_verify_retries} retries.")
        return {"success": False, "error": f"Order {order_id} could not be confirmed as FILLED."}

    def place_order(self, asset: str, order_type: str, quantity: float, direction: str, price: float):
        """
        Places a market order on Binance.

        The process is:
        1. Format the quantity to match the symbol's required precision.
        2. Perform a pre-trade liquidity check.
        3. Submit the market order.
        4. Perform a post-trade verification to confirm the fill.
        """
        symbol = asset.replace('-', '')
        side = Client.SIDE_BUY if direction.upper() == 'BUY' else Client.SIDE_SELL
        
        try:
            # Fetch symbol info to correctly format the order quantity.
            info = self.client.get_symbol_info(symbol)
            step_size = float(next(f['stepSize'] for f in info['filters'] if f['filterType'] == 'LOT_SIZE'))
            precision = int(round(-math.log(step_size, 10), 0))
            formatted_quantity = f"{quantity:.{precision}f}"
        except (StopIteration, KeyError) as e:
            log.error(f"Could not determine quantity precision for {symbol}. Error: {e}", exc_info=True)
            return {"success": False, "error": "Could not determine quantity precision."}

        # --- 1. Pre-trade liquidity check ---
        if not self._is_liquid(symbol):
            return {"success": False, "error": "Market is illiquid (spread too wide)."}

        log.info(f"--- PLACING LIVE ORDER ---")
        log.info(f"    Asset: {asset}, Type: {order_type}, Dir: {direction.upper()}, Qty: {formatted_quantity}")
        try:
            # --- 2. Place the actual order ---
            order = self.client.create_order(
                symbol=symbol, side=side, type=Client.ORDER_TYPE_MARKET, quantity=formatted_quantity
            )
            log.info(f"    Initial order submission response received. Order ID: {order['orderId']}")
            
            # --- 3. Post-trade verification ---
            # This is the crucial step to confirm the fill.
            return self._verify_order_fill(symbol, order['orderId'])

        except BinanceAPIException as e:
            log.error(f"❌ Binance API Exception while placing order: {e}")
            return {"success": False, "error": str(e)}

    def get_account_status(self) -> dict:
        """Fetches the current cash and positions from the Binance account."""
        log.info("Fetching account status from Binance...")
        try:
            account_info = self.client.get_account()
            
            usdt_balance = 0.0
            cash_item = next((item for item in account_info['balances'] if item["asset"] == 'USDT'), None)
            if cash_item:
                usdt_balance = float(cash_item['free'])

            positions = {}
            for item in account_info['balances']:
                asset_name = item['asset']
                free_balance = float(item['free'])
                if free_balance > 0.00001 and asset_name != 'USDT':
                    positions[f"{asset_name}-USDT"] = free_balance
            
            return {"cash": usdt_balance, "positions": positions}
        except BinanceAPIException as e:
            log.error(f"❌ Binance API Exception while fetching account status: {e}")
            return {"cash": 0.0, "positions": {}}
