# trading_system/engine/strategy_monitor.py

import os
from datetime import datetime, timezone
import pandas as pd
import json
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from trading_system.engine.portfolio_manager import PortfolioManager
from trading_system.strategies.base_strategy import Strategy
from trading_system.utils.common import log

# Define output directory
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'output', 'live_monitoring')

class StrategyMonitor:
    """
    Generates a real-time HTML report for a running strategy.
    """

    def __init__(self, strategy: Strategy, portfolio_manager: PortfolioManager, asset: str, timeframe: str):
        """
        Initializes the StrategyMonitor.
        """
        self.strategy = strategy
        self.pm = portfolio_manager
        self.asset = asset
        self.timeframe = timeframe
        self.start_time = datetime.now(timezone.utc)

        # Setup output file path with a consistent name for overwriting
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        self.report_filename = f"live_{self.strategy.name}_{self.asset.replace('-', '')}_{self.timeframe}.html"
        self.report_filepath = os.path.join(OUTPUT_DIR, self.report_filename)
        log.info(f"Live monitoring report will be generated at: {self.report_filepath}")

    def generate_report(self, strategy_state: str, latest_signal: int, current_price: float, price_data: pd.DataFrame):
        """
        Generates and overwrites an HTML file with the latest strategy status and charts.
        """
        # --- Calculate current portfolio values ---
        base_asset = self.asset.split('-')[0]
        position_qty = self.pm.positions.get(self.asset, 0.0)
        
        self.pm.update_market_value(self.asset, current_price)
        total_equity = self.pm.get_total_equity()
        position_value = position_qty * current_price

        # --- Calculate performance metrics ---
        initial_equity = self.pm.initial_cash
        pnl = total_equity - initial_equity
        pnl_pct = (pnl / initial_equity) * 100 if initial_equity > 0 else 0.0

        # --- Create Plotly Figure ---
        fig = self._create_chart(price_data)

        # --- Build HTML Content ---
        html_content = self._build_html(
            strategy_state, latest_signal, current_price, base_asset,
            position_qty, position_value, total_equity, pnl, pnl_pct, fig
        )

        # --- NEW: Save a JSON summary for the dashboard ---
        self._save_json_summary(
            strategy_state, latest_signal, current_price,
            position_qty, total_equity, pnl, pnl_pct
        )

        # --- Write to file ---
        try:
            with open(self.report_filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)
        except Exception as e:
            log.error(f"Error writing monitor report: {e}", exc_info=True)

    def _create_chart(self, price_data: pd.DataFrame) -> go.Figure:
        """Creates the Plotly chart with price, trades, and equity curve."""
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            subplot_titles=('Portfolio Equity Over Time', 'Price and Executed Trades'),
            row_heights=[0.3, 0.7]
        )

        # --- Plot 1: Equity Curve ---
        equity_df = self.pm.equity_curve_df
        if not equity_df.empty:
            fig.add_trace(go.Scatter(
                x=equity_df.index, y=equity_df['Equity'],
                name='Equity', line=dict(color='cyan')
            ), row=1, col=1)

        # --- Plot 2: Price and Trades ---
        # Use Candlestick for a more detailed price view
        if not price_data.empty:
            fig.add_trace(go.Candlestick(
                x=price_data.index,
                open=price_data['Open'],
                high=price_data['High'],
                low=price_data['Low'],
                close=price_data['Close'],
                name='Price'
            ), row=2, col=1)
        
        trade_log_df = pd.DataFrame(self.pm.trade_log)
        if not trade_log_df.empty:
            buy_trades = trade_log_df[trade_log_df['direction'] == 'BUY']
            sell_trades = trade_log_df[trade_log_df['direction'] == 'SELL']
            
            fig.add_trace(go.Scatter(
                x=buy_trades['timestamp'], y=buy_trades['price'],
                name='Buy', mode='markers', marker=dict(color='lime', size=10, symbol='triangle-up')
            ), row=2, col=1)
            
            fig.add_trace(go.Scatter(
                x=sell_trades['timestamp'], y=sell_trades['price'],
                name='Sell', mode='markers', marker=dict(color='magenta', size=10, symbol='triangle-down')
            ), row=2, col=1)

        fig.update_layout(
            title_text=f"Live Performance Analysis: {self.strategy.name}",
            template='plotly_dark',
            height=800,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        return fig

    def _save_json_summary(self, strategy_state: str, latest_signal: int, current_price: float,
                           position_qty: float, total_equity: float, pnl: float, pnl_pct: float):
        """Saves a machine-readable JSON summary of the strategy's state."""
        summary_filepath = self.report_filepath.replace('.html', '.json')

        # Convert equity curve to a JSON-friendly format (list of dicts)
        equity_curve_data = []
        if not self.pm.equity_curve_df.empty:
            equity_df = self.pm.equity_curve_df.reset_index()
            equity_df['Timestamp'] = equity_df['Timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            equity_curve_data = equity_df.to_dict(orient='records')

        summary_data = {
            'strategy_name': self.strategy.name,
            'asset': self.asset,
            'timeframe': self.timeframe,
            'last_update': datetime.now(timezone.utc).isoformat(),
            'strategy_state': strategy_state,
            'total_equity': total_equity,
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'total_trades': len(self.pm.trade_log),
            'report_html_file': os.path.basename(self.report_filepath),
            'equity_curve': equity_curve_data
        }

        try:
            with open(summary_filepath, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f)
        except Exception as e:
            log.error(f"Error writing JSON summary for dashboard: {e}", exc_info=True)

    def _build_html(self, strategy_state: str, latest_signal: int, current_price: float, base_asset: str,
                    position_qty: float, position_value: float, total_equity: float,
                    pnl: float, pnl_pct: float, fig: go.Figure) -> str:
        """Builds the full HTML content for the report, including a trade log."""

        metrics_data = {
            "Live Status": {
                "Last Update": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "Strategy State": strategy_state,
                "Last Signal": latest_signal,
                "Current Price": f"${current_price:,.2f}"
            },
            "Portfolio (Allocated)": {
                "Cash (USDT)": f"${self.pm.cash:,.2f}",
                f"Position ({base_asset})": f"{position_qty:.8f}",
                "Position Value": f"${position_value:,.2f}",
                "Total Equity": f"${total_equity:,.2f}"
            },
            "Performance Since Start": {
                "Total P&L": f"${pnl:,.2f}",
                "Total Return": f"{pnl_pct:+.2f}%",
                "Total Trades": len(self.pm.trade_log)
            }
        }

        # --- Build Trade Log Table ---
        trade_log_html_rows = ""
        if not self.pm.trade_log:
            trade_log_html_rows = '<tr><td colspan="6" style="text-align:center;">No trades executed yet.</td></tr>'
        else:
            # Show most recent trades first, limit to last 50 for performance
            for trade in reversed(self.pm.trade_log[-50:]):
                ts = trade['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                direction_color = 'lime' if trade['direction'] == 'BUY' else 'magenta'
                trade_log_html_rows += f"""
                    <tr>
                        <td>{ts}</td>
                        <td>{trade['asset']}</td>
                        <td style="color: {direction_color}; font-weight: bold;">{trade['direction']}</td>
                        <td>{trade['quantity']:.8f}</td>
                        <td>${trade['price']:,.2f}</td>
                        <td>${trade['commission']:,.4f}</td>
                    </tr>
                """

        html = f"""
        <html><head><title>Live Monitor: {self.strategy.name}</title><meta http-equiv="refresh" content="60">
        <style> 
            body {{ font-family: 'Verdana', sans-serif; background-color: #111; color: #eee; margin: 0; padding: 20px; }} 
            h1, h2 {{ color: #00aaff; border-bottom: 2px solid #00aaff; padding-bottom: 5px; }} 
            .container {{ display: flex; flex-wrap: wrap; gap: 20px; }} 
            .metric-card {{ background-color: #222; border: 1px solid #333; border-radius: 5px; padding: 15px; flex-grow: 1; min-width: 300px; }} 
            .metric-card h2 {{ font-size: 1.2em; margin-top: 0; }} 
            table {{ border-collapse: collapse; width: 100%; }} 
            th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #333; }} 
            th {{ font-weight: bold; color: #00aaff; }}
            .trade-log-container {{ margin-top: 20px; background-color: #222; border: 1px solid #333; border-radius: 5px; padding: 15px; }}
            .trade-log-container h2 {{ font-size: 1.2em; margin-top: 0; }}
        </style></head>
        <body><h1>Live Strategy Monitor: {self.strategy.name}</h1><p>Asset: <strong>{self.asset}</strong> | Timeframe: <strong>{self.timeframe}</strong></p><div class="container">
        """

        for category, data in metrics_data.items():
            html += f'<div class="metric-card"><h2>{category}</h2><table>'
            for key, value in data.items(): html += f"<tr><th>{key}</th><td>{value}</td></tr>"
            html += "</table></div>"
        
        html += '</div>'  # Close container

        # Add Chart Section first
        html += f'<div>{fig.to_html(full_html=False, include_plotlyjs="cdn")}</div>'

        # Add Trade Log Section last
        html += f'''
        <div class="trade-log-container">
            <h2>Recent Trades</h2>
            <table>
                <thead>
                    <tr>
                        <th>Timestamp (UTC)</th>
                        <th>Asset</th>
                        <th>Direction</th>
                        <th>Quantity</th>
                        <th>Price</th>
                        <th>Commission</th>
                    </tr>
                </thead>
                <tbody>
                    {trade_log_html_rows}
                </tbody>
            </table>
        </div>
        '''

        html += '</body></html>'
        return html
