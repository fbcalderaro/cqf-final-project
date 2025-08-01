import os
import psycopg2
import pandas as pd
import numpy as np
import quantstats as qs
import matplotlib

# --- Database Configuration ---
# This script assumes you have the same environment variables set up as your other files.
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

TABLE_NAME = "btcusdt_1m_candles"

def get_data_from_db():
    """
    Connects to the PostgreSQL database and fetches all candle data,
    loading it into a pandas DataFrame. This function is adapted from your
    trend_following.py script.
    """
    print("Connecting to the database...")
    if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
        print("❌ Error: Database environment variables are not set.")
        return None
        
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("✅ Connection successful.")

        sql_query = f"SELECT open_time, open_price, high_price, low_price, close_price, volume FROM {TABLE_NAME} ORDER BY open_time ASC LIMIT 10000;"
        print(f"Fetching all data from '{TABLE_NAME}'...")
        
        df = pd.read_sql_query(sql_query, conn, index_col='open_time')
        print(f"✅ Successfully loaded {len(df)} rows of data.")

        # Ensure numeric columns are of the correct type
        numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df.dropna(inplace=True)

        return df

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return None
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Database connection closed.")

class MovingAverageCrossover:
    """
    Encapsulates the logic for the moving average crossover strategy.
    """
    def __init__(self, df, fast_ma, slow_ma):
        self.df = df
        self.fast_ma = fast_ma
        self.slow_ma = slow_ma

    def generate_signals(self):
        """
        Calculates moving averages and generates trading signals based on crossovers.
        
        Returns:
            pd.DataFrame: A DataFrame with original data, MAs, and a 'position' column
                          indicating trade triggers (2 for buy, -2 for sell).
        """
        print(f"Calculating {self.fast_ma}-period and {self.slow_ma}-period moving averages...")
        df = self.df.copy()
        df['sma_fast'] = df['close_price'].rolling(window=self.fast_ma, min_periods=1).mean()
        df['sma_slow'] = df['close_price'].rolling(window=self.slow_ma, min_periods=1).mean()
        
        # Generate a signal: 1 for long, -1 for short/flat
        df['signal'] = np.where(df['sma_fast'] > df['sma_slow'], 1, -1)
        
        # Generate a position trigger on the crossover event
        # 2.0 indicates a crossover to long (buy)
        # -2.0 indicates a crossover to flat (sell)
        df['position'] = df['signal'].diff()
        
        print("✅ Signals generated.")
        return df

class Backtester:
    """
    A class to run a vectorized backtest on a given strategy.
    """
    def __init__(self, data, strategy_class, params, initial_capital=10000.0, commission=0.001):
        self.data = data
        self.strategy = strategy_class(self.data, **params)
        self.initial_capital = initial_capital
        self.commission = commission
        self.trades = []
        self.equity_curve = None

    def run(self):
        """
        Executes the backtest.
        """
        print("\n--- Running Backtest ---")
        signals_df = self.strategy.generate_signals()

        cash = self.initial_capital
        position_size = 0  # Number of units of the asset held
        portfolio_values = []

        for i, row in signals_df.iterrows():
            trade_trigger = row['position']

            # --- SELL LOGIC ---
            # If trigger is -2.0 (sell) and we currently have a position
            if trade_trigger == -2.0 and position_size > 0:
                trade_value = position_size * row['close_price']
                fee = trade_value * self.commission
                cash = trade_value - fee
                
                self.trades.append({
                    'date': i, 'type': 'sell', 'price': row['close_price'],
                    'size': position_size, 'value': trade_value
                })
                print(f"{i} | SELL at ${row['close_price']:,.2f} | Portfolio: ${cash:,.2f}")
                position_size = 0

            # --- BUY LOGIC ---
            # If trigger is 2.0 (buy) and we are currently in cash
            elif trade_trigger == 2.0 and cash > 0:
                trade_value = cash
                fee = trade_value * self.commission
                cash_to_invest = trade_value - fee
                position_size = cash_to_invest / row['close_price']
                
                self.trades.append({
                    'date': i, 'type': 'buy', 'price': row['close_price'],
                    'size': position_size, 'value': trade_value
                })
                print(f"{i} | BUY  at ${row['close_price']:,.2f} | Holding {position_size:,.4f} units")
                cash = 0
            
            # Update portfolio value at every step
            current_portfolio_value = cash + (position_size * row['close_price'])
            portfolio_values.append(current_portfolio_value)

        self.equity_curve = pd.Series(portfolio_values, index=signals_df.index)
        self.equity_curve.name = 'equity'
        print("--- Backtest Finished ---")

    def report(self):
        """
        Generates a detailed performance report using quantstats.
        """
        if self.equity_curve is None:
            print("❌ Please run the backtest first using .run()")
            return

        print("\n--- Generating Performance Report ---")
        
        # --- FIX: Set a default font family to avoid errors in environments without Arial ---
        try:
            matplotlib.rcParams['font.family'] = 'sans-serif'
            print("Font family set to 'sans-serif' to avoid font errors.")
        except Exception as e:
            print(f"⚠️ Could not set font family: {e}")

        qs.extend_pandas()
        
        # quantstats requires percentage returns, not absolute equity values
        returns = self.equity_curve.pct_change().fillna(0)
        
        # Use the asset's own returns as the benchmark (Buy and Hold strategy)
        benchmark = self.data['close_price'].pct_change().fillna(0)

        # --- FIX: Convert timezone-aware index to timezone-naive for quantstats ---
        returns.index = returns.index.tz_localize(None)
        benchmark.index = benchmark.index.tz_localize(None)

        # Generate and save the report as an HTML file
        report_filename = 'backtest_report.html'
        qs.reports.html(returns, benchmark=benchmark, output=report_filename, title='Trend Following Strategy (MA Crossover)')
        
        print(f"✅ Full performance report saved as '{report_filename}'.")
        print("You can download this file and open it in your browser for detailed stats and charts.")


if __name__ == '__main__':
    print("--- Starting Trend-Following Strategy Backtest ---")
    print("NOTE: This script uses the 'quantstats' library. If you don't have it, please install it: pip install quantstats")
    
    # 1. Fetch historical data from the database
    data_df = get_data_from_db()

    if data_df is not None and not data_df.empty:
        # 2. Define strategy parameters
        strategy_params = {'fast_ma': 20, 'slow_ma': 50}
        
        # 3. Initialize and run the backtester
        backtester = Backtester(
            data=data_df,
            strategy_class=MovingAverageCrossover,
            params=strategy_params,
            initial_capital=10000.0,
            commission=0.001 # 0.1%
        )
        backtester.run()
        
        # 4. Generate and save the performance report
        backtester.report()
    else:
        print("\nCould not fetch data. Exiting backtest.")
