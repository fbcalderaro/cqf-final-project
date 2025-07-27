import os
import psycopg2
import pandas as pd
import plotly.graph_objects as go

# --- Database Configuration ---
# Ensure these environment variables are set in your system
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']

CANDLES_TABLE = "btcusdt_1m_candles"
INDICATORS_TABLE = "btcusdt_1m_indicators"
LIMIT = 1000  # Number of records to fetch for the chart

def get_data_from_db():
    """
    Connects to the PostgreSQL database and fetches the latest 1000 candles,
    joining them with their corresponding Bollinger Band indicators.
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

        # SQL Query to join the latest 1000 candles with their indicators.
        sql_query = f"""
        WITH latest_candles AS (
            SELECT * FROM {CANDLES_TABLE}
            ORDER BY open_time DESC
            LIMIT {LIMIT}
        )
        SELECT 
            c.open_time as time, 
            c.open_price as open,
            c.high_price as high,
            c.low_price as low,
            c.close_price as close,
            c.volume,
            i.bb_upper,
            i.bb_middle,
            i.bb_lower
        FROM latest_candles c
        LEFT JOIN {INDICATORS_TABLE} i ON c.open_time = i.open_time
        ORDER BY c.open_time ASC;
        """

        print(f"Fetching latest {LIMIT} records with indicators...")
        # Use pandas to directly read the SQL query into a DataFrame
        df = pd.read_sql_query(sql_query, conn)
        print(f"✅ Successfully loaded {len(df)} rows of data.")

        # Ensure the numeric columns have the correct type
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'bb_upper', 'bb_middle', 'bb_lower']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col])

        return df

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return None
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    # Step 1: Load combined candle and indicator data from the database
    df = get_data_from_db()

    if df is None or df.empty:
        print("Could not load data. Exiting.")
    else:
        # Step 2: Initialize the chart figure with Plotly
        fig = go.Figure()

        # Step 3: Add the Candlestick series
        fig.add_trace(go.Candlestick(x=df['time'],
                                     open=df['open'],
                                     high=df['high'],
                                     low=df['low'],
                                     close=df['close'],
                                     name='Candles'))

        # Step 4: Add the Bollinger Band lines
        # Upper Band
        fig.add_trace(go.Scatter(x=df['time'], y=df['bb_upper'],
                                 mode='lines',
                                 name='Upper Band',
                                 line=dict(color='rgba(173, 216, 230, 0.5)', width=1))) # Light blue
        
        # Middle Band
        fig.add_trace(go.Scatter(x=df['time'], y=df['bb_middle'],
                                 mode='lines',
                                 name='BB Middle (SMA 20)',
                                 line=dict(color='rgba(255, 165, 0, 0.5)', width=1, dash='dash'))) # Orange, dashed
        
        # Lower Band - with fill to the upper band
        fig.add_trace(go.Scatter(x=df['time'], y=df['bb_lower'],
                                 mode='lines',
                                 name='Lower Band',
                                 line=dict(color='rgba(173, 216, 230, 0.5)', width=1),
                                 fill='tonexty', # Fills the area to the previous trace (the upper band)
                                 fillcolor='rgba(173, 216, 230, 0.1)'))


        # Step 5: Customize the chart layout
        fig.update_layout(
            title=f'BTCUSDT (Latest {LIMIT} Records)',
            yaxis_title='Price (USD)',
            xaxis_rangeslider_visible=False, 
            legend_title="Legend",
            template="plotly_dark"
        )

        # Step 6: Save the chart to an HTML file
        output_filename = 'btc_chart.html'
        fig.write_html(output_filename)
        
        print(f"\n✅ Chart successfully saved as '{output_filename}'.")
        print("You can download this file and open it in your browser.")
