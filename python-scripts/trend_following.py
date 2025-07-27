import os
import psycopg2
import pandas as pd
import numpy as np

# --- Configuration ---
# Ensure these environment variables are set in your system
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']

TABLE_NAME = "btcusdt_1m_candles"

# --- Strategy Parameters ---
FAST_MA_PERIOD = 20
SLOW_MA_PERIOD = 50
INITIAL_CAPITAL = 10000.0
# For simplicity, we'll assume a fixed trading fee. A more complex model could use a percentage.
TRADING_FEE = 0.001 # 0.1% fee per trade

def get_data_from_db():
    """
    Connects to the PostgreSQL database and fetches all candle data,
    loading it into a pandas DataFrame.
    """
    print("Connecting to the database...")
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("✅ Connection successful.")

        # The SQL query to fetch all data, ordered by time
        sql_query = f"SELECT * FROM {TABLE_NAME} ORDER BY open_time ASC;"

        print(f"Fetching data from '{TABLE_NAME}'...")
        # Use pandas to directly read the SQL query into a DataFrame
        df = pd.read_sql_query(sql_query, conn, index_col='open_time')
        print(f"✅ Successfully loaded {len(df)} rows of data.")

        # Ensure numeric columns are of the correct type
        numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col])

        return df

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return None
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Database connection closed.")

def calculate_moving_averages(df, fast_period, slow_period):
    """
    Calculates the fast and slow simple moving averages (SMA).
    """
    print(f"Calculating {fast_period}-period and {slow_period}-period moving averages...")
    df['sma_fast'] = df['close_price'].rolling(window=fast_period).mean()
    df['sma_slow'] = df['close_price'].rolling(window=slow_period).mean()
    print("✅ Moving averages calculated.")
    return df

def generate_signals(df):
    """
    Generates trading signals based on the moving average crossover.
    """
    print("Generating trading signals...")
    df['position'] = np.where(df['sma_fast'] > df['sma_slow'], 1, -1)
    df['signal'] = df['position'].diff()
    print("✅ Signals generated.")
    return df

def run_backtest(df, initial_capital, trading_fee):
    """
    Runs a simple backtest on the generated signals.
    """
    print("--- Running Backtest ---")
    
    # Filter for actual trade signals
    signals = df[df['signal'] != 0].copy()
    
    cash = initial_capital
    position_size = 0
    portfolio_value = initial_capital
    trade_count = 0
    wins = 0
    losses = 0
    
    print(f"Initial Capital: ${initial_capital:,.2f}\n")

    for index, row in signals.iterrows():
        trade_price = row['close_price']
        
        # --- SELL SIGNAL ---
        # If signal is -1 and we have a position
        if row['signal'] < 0 and position_size > 0:
            sell_value = position_size * trade_price
            fee = sell_value * trading_fee
            cash += sell_value - fee
            
            if sell_value > buy_price * position_size:
                wins += 1
            else:
                losses += 1

            print(f"{index} | SELL at ${trade_price:,.2f} | PnL: ${sell_value - (buy_price * position_size):,.2f} | Portfolio: ${cash:,.2f}")
            position_size = 0
            trade_count += 1
        
        # --- BUY SIGNAL ---
        # If signal is 1 and we are out of the market
        elif row['signal'] > 0 and position_size == 0:
            fee = cash * trading_fee
            cash_to_invest = cash - fee
            position_size = cash_to_invest / trade_price
            buy_price = trade_price # Store the buy price for PnL calculation
            cash = 0
            print(f"{index} | BUY  at ${trade_price:,.2f} | Holding {position_size:,.4f} units")

    # Final portfolio value if still holding a position
    if position_size > 0:
        portfolio_value = position_size * df['close_price'].iloc[-1]
    else:
        portfolio_value = cash
        
    # --- Performance Metrics ---
    total_return = ((portfolio_value - initial_capital) / initial_capital) * 100
    win_rate = (wins / trade_count * 100) if trade_count > 0 else 0
    
    print("\n--- Backtest Results ---")
    print(f"Final Portfolio Value: ${portfolio_value:,.2f}")
    print(f"Total Return: {total_return:.2f}%")
    print(f"Total Trades: {trade_count}")
    print(f"Win Rate: {win_rate:.2f}%")
    
    return portfolio_value

def main():
    """Main function to run the full strategy backtest."""
    print("--- Starting Trend-Following Strategy Backtest ---")
    
    # Step 1: Fetch data
    data_df = get_data_from_db()

    if data_df is None or data_df.empty:
        print("Could not fetch data. Exiting.")
        return

    # Step 2: Calculate indicators
    data_df = calculate_moving_averages(data_df, FAST_MA_PERIOD, SLOW_MA_PERIOD)

    # Step 3: Generate signals
    data_df = generate_signals(data_df)
    
    # Step 4: Run the backtest
    run_backtest(data_df, INITIAL_CAPITAL, TRADING_FEE)


if __name__ == "__main__":
    main()
