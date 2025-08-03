import os
import argparse
from datetime import datetime, timezone, timedelta
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import db_utils
import config
from common import log

def generate_chart(start_dt, end_dt):
    """
    Generates and saves a detailed strategy chart for a given date range.
    """
    log.info(f"Generating chart for period: {start_dt.date()} to {end_dt.date()}")

    # Step 1: Fetch data using the function in db_utils
    df = db_utils.fetch_candles_and_indicators_for_range(start_dt, end_dt)

    if df is None or df.empty:
        return

    # Step 2: Create a figure with subplots
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.6, 0.2, 0.2]
    )

    # --- Subplot 1: Price Candlesticks and Supertrend ---
    fig.add_trace(go.Candlestick(
        x=df.index, open=df['open_price'], high=df['high_price'],
        low=df['low_price'], close=df['close_price'],
        name='Price'
    ), row=1, col=1)

    # --- BUG FIX: Optimized Supertrend Background Logic ---
    # This new logic draws one large rectangle per trend period instead of thousands
    # of small ones, which prevents the script from hanging.
    current_trend = None
    trend_start_time = None

    for i in range(len(df)):
        timestamp = df.index[i]
        trend = df['supertrend_direction'].iloc[i]

        if trend != current_trend:
            if current_trend is not None:
                color = 'rgba(0, 255, 0, 0.1)' if current_trend == 1 else 'rgba(255, 0, 0, 0.1)'
                fig.add_vrect(
                    x0=trend_start_time, x1=timestamp,
                    fillcolor=color, layer="below", line_width=0,
                    row=1, col=1
                )
            current_trend = trend
            trend_start_time = timestamp

    # Add the final rectangle for the last trend period
    if current_trend is not None and trend_start_time is not None:
        color = 'rgba(0, 255, 0, 0.1)' if current_trend == 1 else 'rgba(255, 0, 0, 0.1)'
        fig.add_vrect(
            x0=trend_start_time, x1=df.index[-1],
            fillcolor=color, layer="below", line_width=0,
            row=1, col=1
        )

    # --- Subplot 2: RSI and Buy/Sell Zones ---
    fig.add_trace(go.Scatter(
        x=df.index, y=df['rsi'], mode='lines', name='RSI',
        line=dict(color='yellow', width=1)
    ), row=2, col=1)
    fig.add_hrect(y0=config.RSI_BUY_ZONE[0], y1=config.RSI_BUY_ZONE[1], line_width=0, fillcolor="green", opacity=0.2, row=2, col=1)
    fig.add_hrect(y0=config.RSI_SELL_ZONE[0], y1=config.RSI_SELL_ZONE[1], line_width=0, fillcolor="red", opacity=0.2, row=2, col=1)

    # --- Subplot 3: ADX and DMI ---
    fig.add_trace(go.Scatter(x=df.index, y=df['adx'], mode='lines', name='ADX', line=dict(color='cyan', width=2)), row=3, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['plus_di'], mode='lines', name='+DI', line=dict(color='lime', width=1)), row=3, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['minus_di'], mode='lines', name='-DI', line=dict(color='tomato', width=1)), row=3, col=1)
    fig.add_hline(y=config.ADX_THRESHOLD, line_width=1, line_dash="dash", line_color="white", row=3, col=1)

    # --- Layout Customization ---
    fig.update_layout(
        title_text=f"{config.SYMBOL} Strategy Chart ({config.STREAM_INTERVAL})",
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        height=800,
        showlegend=False
    )
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="RSI", row=2, col=1)
    fig.update_yaxes(title_text="ADX/DMI", row=3, col=1)

    # --- Save to File ---
    if not os.path.exists(config.OUTPUT_DIR):
        os.makedirs(config.OUTPUT_DIR)
        log.info(f"Created directory: {config.OUTPUT_DIR}")

    filename = f"{config.SYMBOL}_{start_dt.date()}_to_{end_dt.date()}.html"
    filepath = os.path.join(config.OUTPUT_DIR, filename)
    fig.write_html(filepath)
    log.info(f"✅ Chart successfully saved to '{filepath}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate strategy charts for a given date range.")
    parser.add_argument('--start', required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument('--end', required=True, help="End date in YYYY-MM-DD format.")
    args = parser.parse_args()

    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_date = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
        generate_chart(start_date, end_date)
    except ValueError:
        log.error("❌ Invalid date format. Please use YYYY-MM-DD.")

