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
        
        # Simulate slippage
        slippage_factor = self.slippage_pct + random.uniform(0, 0.0002)
        fill_price = price * (1 + slippage_factor) if direction.upper() == 'BUY' else price * (1 - slippage_factor)
        log.info(f"    Simulating slippage. Target Price: {price:.4f}, Fill Price: {fill_price:.4f}")

        # --- NEW: Simulate partial fills for more realistic paper trading ---
        # For larger orders, simulate that not all of it might get filled.
        fill_ratio = 1.0
        if quantity * price > 5000: # Arbitrary threshold for a "large" order ($5k)
            fill_ratio = random.uniform(0.85, 1.0) # Fill between 85% and 100%
        
        filled_quantity = quantity * fill_ratio
        trade_value = filled_quantity * fill_price
        commission = trade_value * self.commission_pct

        # The total quote value reflects the cash change, including commission
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
        response = {"success": True, "data": {
                "status": "FILLED", "order_id": f"mock_{random.randint(10000, 99999)}", "asset": asset,
                "filled_quantity": filled_quantity, "fill_price": fill_price,
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
        
        # --- Execution Control Parameters ---
        self.order_verify_retries = system_config.get('order_verify_retries', 3)
        self.order_verify_delay_seconds = system_config.get('order_verify_delay_seconds', 2)
        self.max_impact_slippage_pct = system_config.get('max_impact_slippage_pct', 0.2)

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

    def _check_order_book_depth(self, symbol: str, quantity: float, direction: str) -> dict:
        """
        Pre-trade check to analyze order book depth and estimate slippage.
        This is more robust than a simple spread check for large orders.

        Returns:
            A dictionary containing success status and either the top-of-book price or an error.
        """
        log.info(f"    Checking order book depth for {quantity:.6f} of {symbol}...")
        try:
            depth = self.client.get_order_book(symbol=symbol, limit=100)
            book_side = depth['asks'] if direction.upper() == 'BUY' else depth['bids']
            top_price = float(book_side[0][0])
            
            qty_to_fill = quantity
            total_cost = 0
            
            for level in book_side:
                level_price, level_qty = float(level[0]), float(level[1])
                
                if qty_to_fill <= level_qty:
                    total_cost += qty_to_fill * level_price
                    qty_to_fill = 0
                    break
                else:
                    total_cost += level_qty * level_price
                    qty_to_fill -= level_qty
            
            if qty_to_fill > 1e-9: # Use a small epsilon for float comparison
                log.warning(f"    LIQUIDITY CHECK FAILED: Not enough depth to fill {quantity:.6f} {symbol}. Only {quantity - qty_to_fill:.6f} available in top 100 levels.")
                return {"success": False, "error": "Insufficient order book depth."}
                
            avg_fill_price = total_cost / quantity
            slippage_pct = (abs(avg_fill_price - top_price) / top_price) * 100
            log.info(f"    Estimated Impact: Top Price: {top_price:.4f}, Avg Fill Price: {avg_fill_price:.4f}, Slippage: {slippage_pct:.4f}%")
            
            if slippage_pct > self.max_impact_slippage_pct:
                log.warning(f"    LIQUIDITY CHECK FAILED: Estimated slippage {slippage_pct:.4f}% > Max {self.max_impact_slippage_pct:.2f}%. Aborting order.")
                return {"success": False, "error": f"Estimated slippage ({slippage_pct:.4f}%) exceeds threshold."}
            
            log.info(f"    ✅ Order book depth check passed.")
            return {"success": True, "limit_price": top_price}
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
                    side = order_status['side']
                    
                    # --- Fetch associated trades to get accurate commission data ---
                    # This is crucial as the get_order endpoint does not provide commission details.
                    try:
                        trades = self.client.get_my_trades(symbol=symbol, orderId=order_id)
                        total_commission = sum(float(trade['commission']) for trade in trades)
                        
                        # Check if commission was paid in a non-quote asset (e.g., BNB)
                        if trades and trades[0]['commissionAsset'] != 'USDT':
                            log.warning(f"    Commission for order {order_id} was paid in {trades[0]['commissionAsset']}. P&L calculations might be slightly off if its value is not factored in USDT.")
                            # A more advanced implementation would fetch the price of the commission asset.

                    except BinanceAPIException as e:
                        log.error(f"    Could not fetch trades for order {order_id} to get commission. Commission will be assumed as 0. Error: {e}")
                        total_commission = 0.0

                    # Calculate the final cash impact (trade value in quote currency).
                    # This is the value that the portfolio manager uses to calculate the commission paid.
                    final_trade_value_quote = 0
                    if side == 'BUY':
                        final_trade_value_quote = cummulative_quote_qty + total_commission
                    elif side == 'SELL':
                        final_trade_value_quote = cummulative_quote_qty - total_commission

                    fill_price = 0
                    if executed_qty > 0:
                        # This is the average price *before* commission.
                        fill_price = cummulative_quote_qty / executed_qty

                    return {
                        "success": True,
                        "data": {
                            "status": "FILLED", "order_id": order_id, "asset": symbol.replace('USDT', '-USDT'),
                            "filled_quantity": executed_qty,
                            "fill_price": fill_price,
                            "trade_value_quote": final_trade_value_quote
                        }
                    }
                elif order_status['status'] == 'EXPIRED':
                    # This happens for an IOC order that could not be filled at all.
                    log.warning(f"    Verification FAILED. Order {order_id} expired (IOC). No fill.")
                    return {"success": False, "error": f"Order {order_id} expired (IOC). No fill."}
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

        The process is now:
        1. Format the quantity to match the symbol's required precision.
        2. Perform a pre-trade order book depth check to estimate slippage.
        3. Submit a LIMIT IOC (Immediate-Or-Cancel) order to prevent sweeping the book.
        4. Perform a post-trade verification to confirm the fill.
        """
        symbol = asset.replace('-', '')
        side = Client.SIDE_BUY if direction.upper() == 'BUY' else Client.SIDE_SELL
        
        try:
            # Fetch symbol info to correctly format the order quantity.
            info = self.client.get_symbol_info(symbol)
            lot_size_filter = next(f for f in info['filters'] if f['filterType'] == 'LOT_SIZE')
            step_size = float(lot_size_filter['stepSize'])
            quantity_precision = int(round(-math.log(step_size, 10), 0))
            formatted_quantity = f"{quantity:.{quantity_precision}f}"

            price_filter = next(f for f in info['filters'] if f['filterType'] == 'PRICE_FILTER')
            tick_size = float(price_filter['tickSize'])
            price_precision = int(round(-math.log(tick_size, 10), 0))

        except (StopIteration, KeyError) as e:
            log.error(f"Could not determine quantity precision for {symbol}. Error: {e}", exc_info=True)
            return {"success": False, "error": "Could not determine quantity precision."}

        # --- 1. Pre-trade depth check ---
        depth_check = self._check_order_book_depth(symbol, quantity, direction)
        if not depth_check['success']:
            return {"success": False, "error": depth_check['error']}
        
        limit_price = depth_check['limit_price']

        log.info(f"--- PLACING LIVE ORDER ---")
        log.info(f"    Asset: {asset}, Type: LIMIT IOC, Dir: {direction.upper()}, Qty: {formatted_quantity}, Price: {limit_price}")
        try:
            # --- 2. Place the actual order ---
            order = self.client.create_order(
                symbol=symbol, side=side, type=Client.ORDER_TYPE_LIMIT, timeInForce=Client.TIME_IN_FORCE_IOC,
                quantity=formatted_quantity, price=f"{limit_price:.{price_precision}f}"
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
