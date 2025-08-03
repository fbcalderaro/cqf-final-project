# broker.py
#
# First, ensure you have the necessary libraries installed:
# pip install python-binance python-dotenv

import common
import config
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException

# This will hold our client instance after successful initialization
client = None

try:
    # Initialize the client using the keys from your config.py (which reads from .env)
    # The `testnet=True` flag is crucial for connecting to the Binance Testnet.
    common.log.info("Attempting to connect to Binance Testnet...")
    client = Client(config.API_KEY, config.API_SECRET, testnet=True)

    # Check the connection by pinging the server
    client.ping()
    # Check server time to ensure sync
    server_time = client.get_server_time()
    common.log.info(f"✅ Successfully connected to Binance Testnet. Server time: {server_time}")

except (BinanceAPIException, BinanceRequestException) as e:
    common.log.error(f"❌ Binance API Error: Failed to connect or validate credentials. Please check your .env file. Error: {e}")
except Exception as e:
    common.log.error(f"❌ An unexpected error occurred during client initialization: {e}")


def get_account_balances():
    """
    Retrieves and logs the balances of all assets in the testnet account
    that have a positive balance.
    """
    if not client:
        common.log.error("Cannot get balances: Binance client is not initialized.")
        return None
    try:
        common.log.info("--- Getting Account Balances ---")
        account_info = client.get_account()
        balances = account_info.get('balances', [])
        
        positive_balances = []
        for balance in balances:
            free = float(balance['free'])
            locked = float(balance['locked'])
            if free > 0 or locked > 0:
                positive_balances.append(balance)
                common.log.info(f"- Asset: {balance['asset']}, Free: {balance['free']}, Locked: {balance['locked']}")
        
        if not positive_balances:
            common.log.info("No assets with a positive balance found.")
            
        return positive_balances
    except (BinanceAPIException, BinanceRequestException) as e:
        common.log.error(f"❌ Error fetching account balances: {e}")
        return None


def place_market_order(symbol, side, quantity):
    """
    Places a market order on the Binance Testnet.

    :param symbol: The trading pair (e.g., 'BTCUSDT').
    :param side: The order side ('BUY' or 'SELL').
    :param quantity: The amount of the asset to trade.
    :return: The order response from Binance or None if it fails.
    """
    if not client:
        common.log.error("Cannot place order: Binance client is not initialized.")
        return None
        
    side = side.upper()
    if side not in ['BUY', 'SELL']:
        common.log.error(f"Invalid order side '{side}'. Must be 'BUY' or 'SELL'.")
        return None

    try:
        common.log.info(f"Attempting to place a {side} order for {quantity} {symbol}...")
        
        if side == 'BUY':
            order = client.order_market_buy(symbol=symbol, quantity=quantity)
        else: # SELL
            order = client.order_market_sell(symbol=symbol, quantity=quantity)
            
        common.log.info("✅ Order successfully placed:")
        common.log.info(order)
        return order
    except (BinanceAPIException, BinanceRequestException) as e:
        common.log.error(f"❌ Error placing market order for {symbol}: {e}")
        return None

def get_open_orders(symbol=None):
    """
    Retrieves all open orders for a specific symbol or all symbols.

    :param symbol: (Optional) The trading pair to filter by (e.g., 'BTCUSDT').
    :return: A list of open orders or None if an error occurs.
    """
    if not client:
        common.log.error("Cannot get open orders: Binance client is not initialized.")
        return None
    try:
        log_msg = "--- Getting all open orders ---"
        if symbol:
            log_msg = f"--- Getting open orders for {symbol} ---"
        common.log.info(log_msg)

        open_orders = client.get_open_orders(symbol=symbol)
        if not open_orders:
            common.log.info("No open orders found.")
        else:
            for order in open_orders:
                common.log.info(order)
        return open_orders
    except (BinanceAPIException, BinanceRequestException) as e:
        common.log.error(f"❌ Error fetching open orders: {e}")
        return None

# --- Main execution block to test the functions ---
if __name__ == '__main__':
    # We only proceed if the client was successfully initialized.
    if client:
        common.log.info("\n--- Running Broker Functions ---")

        # 1. Get account balances
        get_account_balances()

        # 2. Get open orders for the symbol from config.ini
        get_open_orders(symbol=config.SYMBOL)

        # 3. Example of placing a market BUY order
        # IMPORTANT: Uncomment the following lines to place a test order.
        #
        # common.log.info("\n--- Placing a test BUY order ---")
        # buy_quantity = 0.001  # Example: buy 0.001 BTC on the testnet
        # place_market_order(config.SYMBOL, 'BUY', buy_quantity)
        
        # 4. Example of placing a market SELL order
        # IMPORTANT: Uncomment the following lines to place a test order.
        #
        # common.log.info("\n--- Placing a test SELL order ---")
        # sell_quantity = 0.001 # Example: sell 0.001 BTC on the testnet
        # place_market_order(config.SYMBOL, 'SELL', sell_quantity)

        # 5. Check balances again after placing orders
        # common.log.info("\n--- Checking balances again ---")
        # get_account_balances()

        common.log.info("\n--- Broker script finished ---")
    else:
        common.log.error("Broker script could not run because the Binance client failed to initialize.")
