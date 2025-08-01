# First, you need to install the python-binance library:
# pip install python-binance

from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException
from decouple import config

#API_KEY = config("BINANCE_KEY")
#API_SECRET = config("BINANCE_SECRET")

API_KEY_TEST = config("BINANCE_KEY_TEST")
API_SECRET_TEST = config("BINANCE_SECRET_TEST") 

# Initialize the client
try:
    #client = Client(API_KEY, API_SECRET)
    client = Client(API_KEY_TEST, API_SECRET_TEST, testnet=True)  # Use test credentials for testing
    # Test connectivity
    client.ping()
    print("Successfully connected to Binance API.")
except BinanceAPIException as e:
    print(f"Binance API Exception: {e}")
except BinanceRequestException as e:
    print(f"Binance Request Exception: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")


def get_account_balances():
    """
    Retrieves and displays the balances of all assets in the account.
    """
    if not isinstance(client, Client):
        print("Client is not initialized. Please check your API credentials.")
        return

    try:
        print("\n--- Getting Account Balances ---")
        account = client.get_account()
        balances = account.get('balances', [])

        if not balances:
            print("No balances found.")
            return

        print("Balances:")
        for balance in balances:
            asset = balance['asset']
            free = float(balance['free'])
            locked = float(balance['locked'])
            total = free + locked
            if total > 0:
                print(f"- {asset}:")
                print(f"  - Free:   {free}")
                print(f"  - Locked: {locked}")
                print(f"  - Total:  {total}")

    except BinanceAPIException as e:
        print(f"Error getting account balances: {e}")
    except BinanceRequestException as e:
        print(f"Request error getting account balances: {e}")


def place_market_order(symbol, side, quantity):
    """
    Places a market order on Binance.

    :param symbol: The trading pair (e.g., 'BTCUSDT').
    :param side: The order side ('BUY' or 'SELL').
    :param quantity: The amount of the asset to trade.
    """
    if not isinstance(client, Client):
        print("Client is not initialized. Please check your API credentials.")
        return

    try:
        print(f"\n--- Placing a {side} order for {quantity} {symbol} ---")

        if side.upper() == 'BUY':
            order = client.order_market_buy(
                symbol=symbol,
                quantity=quantity
            )
        elif side.upper() == 'SELL':
            order = client.order_market_sell(
                symbol=symbol,
                quantity=quantity
            )
        else:
            print("Invalid order side. Must be 'BUY' or 'SELL'.")
            return

        print("Order successfully placed:")
        print(order)
        return order

    except BinanceAPIException as e:
        print(f"Error placing market order: {e}")
    except BinanceRequestException as e:
        print(f"Request error placing market order: {e}")


if __name__ == '__main__':
    # Make sure the client was initialized before proceeding
    if isinstance(client, Client):
        # 1. Check account balances
        #get_account_balances()

        # 2. Example of placing a market BUY order
        # IMPORTANT: Uncomment the following lines to place a real order.
        # Be very careful with real money.
        # It's recommended to start with a very small amount or use the testnet.
        #
        buy_symbol = 'BTCUSDT'
        buy_quantity = 0.0001  # Example: buy 0.0001 BTC
        place_market_order(buy_symbol, 'BUY', buy_quantity)

        # 3. Example of placing a market SELL order
        # IMPORTANT: Uncomment the following lines to place a real order.
        #
        #sell_symbol = 'BTCUSDT'
        #sell_quantity = 0.0001 # Example: sell 0.0001 BTC
        #place_market_order(sell_symbol, 'SELL', sell_quantity)

        # After placing an order, you might want to check balances again
        #get_account_balances()
        pass

