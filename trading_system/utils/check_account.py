"""
Binance Account Check Utility (check_account.py)

This is a standalone diagnostic script that connects to the Binance Testnet
using API keys from the environment. It prints a summary of the account,
including current balances, open orders, and recent trade history for a
specified symbol.
"""

import os
import sys
import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException

# Add project root to Python's path to allow for imports
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, PROJECT_ROOT)

from trading_system.utils.common import log

def check_binance_account():
    """
    Connects to the Binance Testnet and prints a summary of the account.

    The process is as follows:
    1. Load API keys from environment variables.
    2. Connect to the Binance Testnet and verify the connection.
    3. Fetch and display all non-zero asset balances.
    4. Fetch and display any open orders.
    5. Fetch and display the 5 most recent trades for BTCUSDT.
    """
    log.info("--- Binance Account Status Check ---")

    # 1. Load API Keys from Environment Variables
    api_key = os.environ.get('BINANCE_KEY_TEST')
    api_secret = os.environ.get('BINANCE_SECRET_TEST')

    if not api_key or not api_secret:
        log.error("❌ Environment variables BINANCE_KEY_TEST and BINANCE_SECRET_TEST must be set.")
        log.error("   Please ensure you have a .env file and it's being loaded correctly.")
        return

    try:
        # 2. Connect to Binance Testnet
        log.info("Connecting to Binance Testnet...")
        client = Client(api_key, api_secret, testnet=True)
        client.ping()
        log.info("✅ Successfully connected to Binance Testnet.")

        # 3. Fetch and display all asset balances that are not zero.
        log.info("\n--- Account Balances ---")
        account_info = client.get_account()
        
        balances = pd.DataFrame(account_info['balances'])
        balances['free'] = pd.to_numeric(balances['free'])
        balances['locked'] = pd.to_numeric(balances['locked'])
        
        # Filter for assets with a non-zero balance
        positions = balances[(balances['free'] > 0.00001) | (balances['locked'] > 0.00001)]
        
        if positions.empty:
            log.info("No assets found with a balance.")
        else:
            print(positions.to_string(index=False))

        # 4. Fetch and display all currently open orders.
        log.info("\n--- Open Orders ---")
        open_orders = client.get_open_orders()
        if not open_orders:
            log.info("No open orders found.")
        else:
            orders_df = pd.DataFrame(open_orders)
            print(orders_df[['symbol', 'side', 'type', 'origQty', 'price', 'status']].to_string(index=False))

        # 5. Fetch and display recent trades for a major pair (e.g., BTCUSDT) for a quick check.
        log.info("\n--- Recent Trades (BTCUSDT) ---")
        trades = client.get_my_trades(symbol='BTCUSDT', limit=5)
        if not trades:
            log.info("No recent trades found for BTCUSDT.")
        else:
            trades_df = pd.DataFrame(trades)
            trades_df['time'] = pd.to_datetime(trades_df['time'], unit='ms')
            
            # --- FIX: Create 'side' column from 'isBuyer' boolean ---
            trades_df['side'] = trades_df['isBuyer'].apply(lambda x: 'BUY' if x else 'SELL')
            
            print(trades_df[['time', 'symbol', 'side', 'price', 'qty', 'commission']].to_string(index=False))

    except BinanceAPIException as e:
        log.error(f"❌ A Binance API error occurred: {e}")
    except Exception as e:
        log.error(f"❌ An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    # Ensure your .env file is loaded if you are running this directly
    # In a Docker environment with `env_file`, this is handled automatically.
    try:
        from dotenv import load_dotenv
        load_dotenv()
        log.info(".env file loaded.")
    except ImportError:
        log.warning("dotenv library not found. Assuming environment variables are set.")
        
    check_binance_account()
