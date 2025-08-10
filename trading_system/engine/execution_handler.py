# trading_system/engine/execution_handler.py

import os
import math
from trading_system.utils.common import log
import random
from binance.client import Client
from binance.exceptions import BinanceAPIException

class MockExecutionHandler:
    """
    A placeholder for a real ExecutionHandler that simulates broker behavior
    for paper trading.
    """
    def __init__(self, system_config: dict):
        self._mock_broker_cash = system_config.get('initial_cash', 100000.0)
        self._mock_broker_positions = {}
        self.commission_pct = system_config.get('commission_pct', 0.0)
        log.info("Execution Handler initialized (PAPER TRADING MODE).")

    def place_order(self, asset: str, order_type: str, quantity: float, direction: str, price: float):
        log.info(f"--- MOCK ORDER PLACED ---")
        log.info(f"    Asset: {asset}, Type: {order_type}, Dir: {direction.upper()}, Qty: {quantity:.6f}")
        
        trade_value = quantity * price
        commission = trade_value * self.commission_pct
        
        if direction.upper() == 'BUY':
            self._mock_broker_cash -= (trade_value + commission)
            self._mock_broker_positions[asset] = self._mock_broker_positions.get(asset, 0) + quantity
        elif direction.upper() == 'SELL':
            self._mock_broker_cash += (trade_value - commission)
            self._mock_broker_positions[asset] = self._mock_broker_positions.get(asset, 0) - quantity
            if self._mock_broker_positions[asset] <= 1e-9:
                del self._mock_broker_positions[asset]

        response = {
            "status": "FILLED", "order_id": f"mock_{random.randint(10000, 99999)}",
            "asset": asset, "filled_quantity": quantity, "fill_price": price
        }
        log.info(f"--- MOCK ORDER RESPONSE: {response} ---")
        return response

    def get_account_status(self) -> dict:
        log.info("Mock execution handler received request for account status.")
        return {"cash": self._mock_broker_cash, "positions": self._mock_broker_positions.copy()}

class BinanceExecutionHandler:
    """
    Handles all interactions with the Binance API for order execution and
    account management on the Testnet by reading keys from environment variables.
    """
    def __init__(self, system_config: dict):
        # --- NEW: Read API keys from environment variables ---
        api_key = os.environ.get('BINANCE_KEY_TEST')
        api_secret = os.environ.get('BINANCE_SECRET_TEST')

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

    def place_order(self, asset: str, order_type: str, quantity: float, direction: str, price: float):
        symbol = asset.replace('-', '')
        side = Client.SIDE_BUY if direction.upper() == 'BUY' else Client.SIDE_SELL
        
        try:
            info = self.client.get_symbol_info(symbol)
            step_size = float(next(f['stepSize'] for f in info['filters'] if f['filterType'] == 'LOT_SIZE'))
            precision = int(round(-math.log(step_size, 10), 0))
            formatted_quantity = f"{quantity:.{precision}f}"
        except (StopIteration, KeyError) as e:
            log.error(f"Could not determine quantity precision for {symbol}. Error: {e}")
            return {"status": "FAILED", "error": "Could not determine quantity precision."}


        log.info(f"Attempting to place a {direction} market order for {formatted_quantity} {symbol}...")
        try:
            order = self.client.create_order(
                symbol=symbol, side=side, type=Client.ORDER_TYPE_MARKET, quantity=formatted_quantity
            )
            log.info(f"✅ Market order placed successfully: {order}")
            
            return {
                "status": order['status'], "order_id": order['orderId'],
                "asset": asset, "filled_quantity": float(order['executedQty']),
                "fill_price": float(order['fills'][0]['price']) if order['fills'] else price
            }
        except BinanceAPIException as e:
            log.error(f"❌ Binance API Exception while placing order: {e}")
            return {"status": "FAILED", "error": str(e)}

    def get_account_status(self) -> dict:
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
