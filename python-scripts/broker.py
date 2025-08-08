from binance.client import Client
from binance.exceptions import BinanceAPIException
import config
from common import log

class Broker:
    """
    Handles all interactions with the Binance API for order execution and account management.
    """
    def __init__(self):
        """
        Initializes the Binance client using API keys from the configuration.
        """
        log.info("Initializing Broker...")
        if not config.API_KEY or not config.API_SECRET:
            raise ValueError("Binance API keys must be set in the environment variables.")
        
        masked_key = f"{config.API_KEY[:5]}...{config.API_KEY[-5:]}"
        log.info(f"Attempting to connect with API Key: {masked_key}")

        try:
            # --- PERMANENT FIX: Use the simple, direct connection from your working broker.py ---
            # This is the proven method.
            log.info("Attempting to connect to Binance Testnet...")
            self.client = Client(config.API_KEY, config.API_SECRET, testnet=True)

            # Test connection
            self.client.ping()
            server_time = self.client.get_server_time()
            log.info(f"✅ Successfully connected to Binance Testnet. Server time: {server_time}")
            log.info(f"   Client is connected to API endpoint: {self.client.API_URL}")

        except BinanceAPIException as e:
            log.error(f"❌ Binance API Exception during initialization: {e}")
            log.error("    Please check that your TESTNET API keys are correct and have the right permissions.")
            raise
        except Exception as e:
            log.error(f"❌ An unexpected error occurred during broker initialization: {e}")
            raise

    def get_asset_balance(self, asset):
        """
        Retrieves the free balance for a specific asset.
        """
        try:
            balance = self.client.get_asset_balance(asset=asset)
            return float(balance['free']) if balance else 0.0
        except BinanceAPIException as e:
            log.error(f"❌ Error fetching balance for {asset}: {e}")
            return 0.0

    def get_open_positions(self, symbol):
        """
        Checks for an open position for a given symbol.
        """
        base_asset = symbol.replace('USDT', '')
        min_position_amount = 0.0001 
        balance = self.get_asset_balance(base_asset)
        return balance if balance > min_position_amount else 0.0

    def place_market_order(self, symbol, side, quantity):
        """
        Places a market order on Binance.
        """
        log.info(f"Attempting to place a {side} market order for {quantity} {symbol}...")
        try:
            order = self.client.create_order(
                symbol=symbol,
                side=side,
                type=Client.ORDER_TYPE_MARKET,
                quantity=quantity
            )
            log.info(f"✅ Market order placed successfully: {order}")
            return order
        except BinanceAPIException as e:
            log.error(f"❌ Binance API Exception while placing order: {e}")
            return None
        except Exception as e:
            log.error(f"❌ An unexpected error occurred while placing order: {e}")
            return None

    def place_stop_loss_order(self, symbol, side, quantity, stop_price):
        """
        Places a STOP_LOSS_LIMIT order.
        """
        log.info(f"Attempting to place a {side} STOP_LOSS order for {quantity} {symbol} at trigger price {stop_price}...")
        try:
            if side == Client.SIDE_SELL:
                limit_price = stop_price * 0.998
            else: # BUY
                limit_price = stop_price * 1.002

            order = self.client.create_order(
                symbol=symbol,
                side=side,
                type=Client.ORDER_TYPE_STOP_LOSS_LIMIT,
                quantity=quantity,
                timeInForce=Client.TIME_IN_FORCE_GTC,
                stopPrice=f'{stop_price:.2f}',
                price=f'{limit_price:.2f}'
            )
            log.info(f"✅ Stop-loss order placed successfully: {order}")
            return order
        except BinanceAPIException as e:
            log.error(f"❌ Binance API Exception while placing stop-loss: {e}")
            return None


if __name__ == '__main__':
    log.info("--- Running Broker Module Sanity Check ---")
    
    if not config.API_KEY or not config.API_SECRET:
        log.warning("Skipping sanity check: API keys are not configured.")
    else:
        try:
            broker = Broker()
            
            usdt_balance = broker.get_asset_balance('USDT')
            log.info(f"USDT Balance: {usdt_balance}")
            
            btc_position = broker.get_open_positions('BTCUSDT')
            log.info(f"Current BTC Position: {btc_position}")

        except Exception as e:
            log.error(f"An error occurred during the sanity check: {e}")
