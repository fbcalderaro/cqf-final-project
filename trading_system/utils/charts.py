import os
import argparse
from datetime import datetime, timezone, timedelta
import pandas as pd
import plotly.graph_objects as go
import yaml

# Add project root to Python's path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
import sys
sys.path.insert(0, PROJECT_ROOT)

from trading_system.utils import db_utils
from trading_system.utils.common import log

CONFIG_PATH = os.path.join(PROJECT_ROOT, 'trading_system', 'config', 'config.yaml')

def generate_gap_chart(asset, start_dt, end_dt, config):
    """
    Fetches 1-minute candle data and generates an HTML candlestick chart.

    The key technique used here is resampling the data to a fixed 1-minute
    frequency. Any missing timestamps in the original data will become `NaN`
    rows, which Plotly renders as a visible break or "gap" in the chart.

    Args:
        asset (str): The asset to chart (e.g., 'BTC-USDT').
        start_dt (datetime): The start of the date range.
        end_dt (datetime): The end of the date range.
        config (dict): The application's configuration dictionary.
    """
    log.info(f"Generating gap chart for {asset} from {start_dt.date()} to {end_dt.date()}")

    df = db_utils.fetch_candles_for_range(config['system']['database'], asset, start_dt, end_dt)

    if df is None or df.empty:
        log.warning("No data found for the specified period. Cannot generate chart.")
        return

    # --- This is the key part to visualize gaps ---
    # Resample the data to a consistent 1-minute frequency.
    # Missing timestamps will be filled with NaN (Not a Number).
    df_resampled = df.resample('1min').asfreq()
    log.info(f"Resampled data to 1-minute frequency to identify gaps.")
    # Plotly automatically creates breaks in the chart where it finds NaN values.

    fig = go.Figure()

    # Add the candlestick trace using the resampled data.
    fig.add_trace(go.Candlestick(
        x=df_resampled.index, 
        open=df_resampled['open_price'], 
        high=df_resampled['high_price'],
        low=df_resampled['low_price'], 
        close=df_resampled['close_price'],
        name=asset
    ))

    # Customize the chart layout for better readability.
    fig.update_layout(
        title_text=f"{asset} 1-Minute Candlestick Chart (Gaps are visible as breaks)",
        xaxis_title="Date",
        yaxis_title="Price (USDT)",
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        height=800
    )

    # Save the generated chart as an HTML file in the 'output' directory.
    output_dir = os.path.join(PROJECT_ROOT, 'output')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        log.info(f"Created directory: {output_dir}")

    filename = f"{asset.replace('-', '')}_gap_chart_{start_dt.date()}_to_{end_dt.date()}.html"
    filepath = os.path.join(output_dir, filename)
    fig.write_html(filepath)
    log.info(f"✅ Chart successfully saved to '{filepath}'")


if __name__ == "__main__":
    """
    Main execution block to run the script from the command line.
    """
    parser = argparse.ArgumentParser(description="Generate candlestick charts to find data gaps.")
    parser.add_argument('--asset', required=True, help="The asset to chart (e.g., BTC-USDT).")
    parser.add_argument('--start', required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument('--end', required=True, help="End date in YYYY-MM-DD format.")
    args = parser.parse_args()

    try:
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
            
        start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        # Add 1 day to the end date to include the entire day
        end_date = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
        
        generate_gap_chart(args.asset, start_date, end_date, config)

    except FileNotFoundError:
        log.error(f"❌ Configuration file not found at: {CONFIG_PATH}")
    except ValueError:
        log.error("❌ Invalid date format. Please use YYYY-MM-DD.")