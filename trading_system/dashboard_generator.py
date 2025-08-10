# trading_system/dashboard_generator.py

import os
import sys
import json
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

# --- Path Setup ---
# This section is crucial for making the script runnable from anywhere and ensuring
# that Python can find the 'trading_system' package for imports.

# 1. Get the absolute path of the project's root directory.
#    Since this script is inside 'trading_system', we navigate one level up ('..').
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# 2. Add the project root to Python's path.
#    This allows us to use absolute imports like 'from trading_system.utils...'.
sys.path.insert(0, PROJECT_ROOT)

# --- Imports from within the project ---
from trading_system.utils.common import log

# --- Global Path Constants ---
MONITOR_DIR = os.path.join(PROJECT_ROOT, 'output', 'live_monitoring')
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'output')
DASHBOARD_FILE = os.path.join(OUTPUT_DIR, 'dashboard.html')

def find_and_read_summaries():
    """Scans the monitoring directory for JSON files and loads them."""
    summaries = []
    if not os.path.exists(MONITOR_DIR):
        log.warning(f"Monitoring directory not found, cannot generate dashboard: {MONITOR_DIR}")
        return summaries

    json_files = [f for f in os.listdir(MONITOR_DIR) if f.endswith('.json')]
    log.info(f"Found {len(json_files)} JSON summary files in {MONITOR_DIR}.")

    for filename in json_files:
        filepath = os.path.join(MONITOR_DIR, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                summaries.append(json.load(f))
        except Exception as e:
            log.error(f"Error reading summary file {filename}: {e}")

    return summaries

def generate_dashboard_html(summaries: list) -> str:
    """Builds the complete HTML content for the dashboard."""
    if not summaries:
        return "<html><body><h1>Live Dashboard</h1><p>No running strategies found or no data available yet.</p></body></html>"

    # --- 1. Build Summary Table ---
    summary_df = pd.DataFrame(summaries)
    summary_df['pnl_pct'] = summary_df['pnl_pct'].apply(lambda x: f"{x:+.2f}%")
    summary_df['total_equity'] = summary_df['total_equity'].apply(lambda x: f"${x:,.2f}")
    summary_df['last_update'] = pd.to_datetime(summary_df['last_update']).dt.strftime('%Y-%m-%d %H:%M:%S')
    summary_df['report_html_file'] = summary_df['report_html_file'].apply(
        lambda x: f'<a href="live_monitoring/{x}" target="_blank">View Report</a>'
    )
    
    summary_df = summary_df.rename(columns={
        'strategy_name': 'Strategy', 'asset': 'Asset', 'timeframe': 'TF',
        'strategy_state': 'State', 'total_equity': 'Equity', 'pnl_pct': 'Return %',
        'total_trades': 'Trades', 'last_update': 'Last Update (UTC)', 'report_html_file': 'Link'
    })
    
    table_html = summary_df[['Strategy', 'Asset', 'TF', 'State', 'Equity', 'Return %', 'Trades', 'Last Update (UTC)', 'Link']].to_html(escape=False, index=False, classes='styled-table')

    # --- 2. Build Combined Equity Chart ---
    fig_equity = go.Figure()
    for summary in summaries:
        if summary.get('equity_curve'):
            equity_df = pd.DataFrame(summary['equity_curve'])
            if not equity_df.empty:
                fig_equity.add_trace(go.Scatter(
                    x=pd.to_datetime(equity_df['Timestamp']), y=equity_df['Equity'],
                    name=summary['strategy_name'], mode='lines'
                ))
    
    fig_equity.update_layout(title_text='Combined Strategy Equity', template='plotly_dark', height=500)
    chart_html = fig_equity.to_html(full_html=False, include_plotlyjs='cdn')

    # --- 3. Assemble Final HTML ---
    html_content = f"""
    <html><head><title>Live Strategies Dashboard</title><meta http-equiv="refresh" content="60">
    <style>
        body {{ font-family: 'Verdana', sans-serif; background-color: #111; color: #eee; margin: 0; padding: 20px; }}
        h1, h2 {{ color: #00aaff; border-bottom: 2px solid #00aaff; padding-bottom: 5px; }}
        .styled-table {{ border-collapse: collapse; width: 100%; margin: 20px 0; font-size: 0.9em; }}
        .styled-table thead tr {{ background-color: #00aaff; color: #ffffff; text-align: left; }}
        .styled-table th, .styled-table td {{ padding: 12px 15px; }}
        .styled-table tbody tr {{ border-bottom: 1px solid #333; }}
        .styled-table tbody tr:nth-of-type(even) {{ background-color: #222; }}
        .styled-table a {{ color: #00aaff; font-weight: bold; text-decoration: none; }}
        .styled-table a:hover {{ text-decoration: underline; }}
    </style></head>
    <body>
        <h1>Live Strategies Dashboard</h1><p>Last generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <h2>Performance Summary</h2>{table_html}
        <h2>Equity Curves</h2>{chart_html}
    </body></html>
    """
    return html_content

def main():
    """Main function to generate the dashboard."""
    log.info("--- Generating Live Dashboard ---")
    # --- FIX: Ensure the output directory exists before trying to write to it ---
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    summaries = find_and_read_summaries()
    html_content = generate_dashboard_html(summaries)
    with open(DASHBOARD_FILE, 'w', encoding='utf-8') as f:
        f.write(html_content)
    log.info(f"✅ Dashboard successfully generated at: {DASHBOARD_FILE}")

if __name__ == "__main__":
    main()