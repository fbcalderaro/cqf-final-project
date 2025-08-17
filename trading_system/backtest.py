# trading_system/engine/backtest.py

import sys
import os
import yaml
import pandas as pd
import numpy as np
from datetime import datetime
import importlib
import plotly.graph_objects as go
import argparse
from plotly.subplots import make_subplots

# --- Path Correction ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)
# --- End of Path Correction ---

from trading_system.utils.common import log
from trading_system.utils import db_utils
from trading_system.engine.portfolio_manager import PortfolioManager

CONFIG_PATH = os.path.join(PROJECT_ROOT, 'trading_system', 'config', 'config.yaml')
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'output', 'backtest')

class Backtest:
    """
    Handles running a backtest simulation for a single strategy and can generate
    a detailed individual report.
    """

    def __init__(self, strategy_instance, strategy_config: dict, system_config: dict, backtest_config: dict, period: str):
        self.strategy = strategy_instance
        self.strategy_config = strategy_config
        self.system_config = system_config
        self.backtest_config = backtest_config
        self.asset = self.strategy_config['asset']
        self.timeframe = self.strategy_config.get('timeframe', '1h')

        initial_cash = self.system_config.get('initial_cash', 100000.0)

        # Initialize the master portfolio manager for the backtest
        self.portfolio_manager = PortfolioManager(
            system_config=self.system_config,
            initial_cash=initial_cash,
            initial_positions={}, # Backtests start with no positions
            relevant_assets={self.asset}
        )

        # For a backtest, the single strategy gets 100% of the initial cash.
        self.portfolio_manager.register_strategy(
            strategy_name=self.strategy.name,
            config=self.strategy_config,
            initial_equity=initial_cash
        )

        # Get the specific sub-portfolio for this strategy to use in the simulation
        self.strategy_portfolio = self.portfolio_manager.get_strategy_portfolio(self.strategy.name)

        # --- NEW: Select dates based on period ---
        self.period = period
        try:
            period_dates = self.backtest_config['periods'][self.period]
            self.start_date = datetime.fromisoformat(period_dates['start_date'])
            self.end_date = datetime.fromisoformat(period_dates['end_date'])
        except KeyError:
            log.error(f"Backtest period '{self.period}' not found in config.yaml under backtest.periods.")
            raise ValueError(f"Invalid backtest period: {self.period}")
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        log.info(f"--- Initializing Backtest for '{self.strategy.name}' on {self.asset} ({self.period.replace('_', ' ').title()}) ---")

    def _load_and_prepare_data(self) -> pd.DataFrame:
        log.info(f"Loading 1-minute data for {self.asset}...")
        df_1m = db_utils.fetch_candles_for_range(
            self.system_config['database'], self.asset, self.start_date, self.end_date
        )
        if df_1m is None or df_1m.empty: return pd.DataFrame()
        
        df_1m.rename(columns={'open_price': 'Open', 'high_price': 'High', 'low_price': 'Low', 'close_price': 'Close', 'volume': 'Volume'}, inplace=True)
        resample_freq = self.timeframe.replace('m', 'T').replace('h', 'H')
        agg_rules = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}
        df_resampled = df_1m.resample(resample_freq).agg(agg_rules).dropna()
        log.info(f"Resampling complete. Resulted in {len(df_resampled)} bars.")
        return df_resampled

    def run(self) -> dict | None:
        """
        Runs the simulation, generates an individual report if flagged,
        and returns the performance results.
        """
        historical_data = self._load_and_prepare_data()
        if historical_data.empty:
            log.warning(f"No data to process for '{self.strategy.name}'. Skipping.")
            return None

        log.info(f"Generating signals for '{self.strategy.name}'...")
        signals_df = self.strategy.generate_signals(historical_data.copy())
        
        log.info(f"Starting simulation for '{self.strategy.name}'...")
        self._run_simulation(signals_df)

        log.info(f"Calculating performance for '{self.strategy.name}'...")
        results = self._calculate_performance_metrics()

        # --- NEW: Conditionally generate individual report ---
        if results and self.backtest_config.get('generate_individual_reports', False):
            report_path = self._generate_individual_report(signals_df, results)
            results['Report Path'] = report_path
        elif results:
            # Ensure the key exists even if no report is generated
            results['Report Path'] = None
            
        return results

    def _run_simulation(self, signals_df: pd.DataFrame):
        # Reset the equity curve to ensure it only contains data from this specific backtest run,
        # preventing any initial values (e.g., from live trading mode) from polluting the chart.
        self.portfolio_manager.equity_curve = []
        position = 0 # Simple state tracker: 0 for flat, 1 for in-position
        for i in range(len(signals_df)):
            timestamp = signals_df.index[i]
            current_signal = signals_df['signal'].iloc[i]
            # Assume trade executes at the open of the next bar
            trade_price = signals_df['Open'].iloc[i+1] if i + 1 < len(signals_df) else signals_df['Close'].iloc[i]
            
            # Update market values for the master portfolio, which also updates sub-portfolios
            self.portfolio_manager.update_market_values({self.asset: trade_price})

            if current_signal == 1 and position == 0:
                # Use the strategy's sub-portfolio to calculate position size
                risk_amount = self.strategy_portfolio.calculate_position_size()
                if risk_amount > 0:
                    quantity = risk_amount / trade_price
                    
                    # Simulate fill details (slippage, commission)
                    commission_pct = self.system_config.get('commission_pct', 0.001)
                    slippage_pct_config = self.system_config.get('paper_slippage_pct', 0.0005)
                    fill_price = trade_price * (1 + slippage_pct_config)
                    trade_value = quantity * fill_price
                    commission = trade_value * commission_pct
                    trade_value_quote = trade_value + commission

                    # Call the master portfolio manager's on_fill method with all required arguments
                    self.portfolio_manager.on_fill(
                        strategy_name=self.strategy.name, timestamp=timestamp, asset=self.asset, 
                        quantity=quantity, fill_price=fill_price, direction='BUY', 
                        trade_value_quote=trade_value_quote, slippage_pct=slippage_pct_config * 100
                    )
                    position = 1
            elif current_signal == -1 and position == 1:
                # Get quantity to sell from the strategy's sub-portfolio
                quantity_to_sell = self.strategy_portfolio.positions.get(self.asset, 0)
                if quantity_to_sell > 0:
                    # Simulate fill details
                    commission_pct = self.system_config.get('commission_pct', 0.001)
                    slippage_pct_config = self.system_config.get('paper_slippage_pct', 0.0005)
                    fill_price = trade_price * (1 - slippage_pct_config)
                    trade_value = quantity_to_sell * fill_price
                    commission = trade_value * commission_pct
                    trade_value_quote = trade_value - commission

                    self.portfolio_manager.on_fill(
                        strategy_name=self.strategy.name, timestamp=timestamp, asset=self.asset, 
                        quantity=quantity_to_sell, fill_price=fill_price, direction='SELL', 
                        trade_value_quote=trade_value_quote, slippage_pct=slippage_pct_config * 100
                    )
                    position = 0

            # The master PM's equity curve is updated on_fill. For non-trade bars, we need to record the equity too.
            # This ensures the equity curve has a point for every bar in the backtest.
            self.portfolio_manager.equity_curve.append((timestamp, self.portfolio_manager.get_total_equity()))

    def _calculate_performance_metrics(self) -> dict | None:
        if not self.portfolio_manager.equity_curve:
            log.warning(f"Equity curve for '{self.strategy.name}' is empty. Cannot calculate performance.")
            return None

        # Use the new equity_curve_df property to get the DataFrame directly.
        # This avoids the AttributeError from trying to set a read-only property.
        equity_df = self.portfolio_manager.equity_curve_df

        equity_df['Return'] = equity_df['Equity'].pct_change()
        
        initial_equity = self.portfolio_manager.initial_cash
        final_equity = equity_df['Equity'].iloc[-1]
        total_return_pct = ((final_equity / initial_equity) - 1) * 100
        
        peak = equity_df['Equity'].expanding(min_periods=1).max()
        drawdown = (equity_df['Equity'] - peak) / peak
        max_drawdown_pct = drawdown.min() * 100 if not drawdown.empty else 0
        
        trading_days_per_year = 252
        timeframe_lower = self.timeframe.lower()
        minutes_per_bar = 0
        if 'h' in timeframe_lower: minutes_per_bar = int(timeframe_lower.replace('h', '')) * 60
        elif 'm' in timeframe_lower: minutes_per_bar = int(timeframe_lower.replace('m', ''))
        
        sharpe_ratio = 0
        if minutes_per_bar > 0 and equity_df['Return'].std() != 0:
            bars_per_day = (24 * 60) / minutes_per_bar
            annualization_factor = np.sqrt(trading_days_per_year * bars_per_day)
            sharpe_ratio = equity_df['Return'].mean() / equity_df['Return'].std() * annualization_factor
        
        return {
            'Strategy Name': self.strategy.name,
            'Asset': self.asset,
            'Timeframe': self.timeframe,
            'Total Return %': total_return_pct,
            'Max Drawdown %': max_drawdown_pct,
            'Sharpe Ratio': sharpe_ratio,
            'Total Trades': len(self.strategy_portfolio.trade_log),
            'Total P&L $': final_equity - initial_equity,
            'Equity Curve': equity_df,
            'Drawdown Curve': drawdown
        }

    def _generate_individual_report(self, signals_df: pd.DataFrame, results: dict):
        """
        Generates a detailed, interactive HTML report for a single strategy run.
        """
        log.info(f"Generating individual report for '{self.strategy.name}'...")
        
        metrics_data = {
            "P&L Performance": {
                "Initial Portfolio Value": f"${self.portfolio_manager.initial_cash:,.2f}",
                "Final Portfolio Value": f"${results['Equity Curve']['Equity'].iloc[-1]:,.2f}",
                "Total Net P&L": f"${results['Total P&L $']:,.2f}",
                "Total Return": f"{results['Total Return %']:.2f}%"
            },
            "Risk & Trading Metrics": {
                "Sharpe Ratio (Annualized)": f"{results['Sharpe Ratio']:.2f}",
                "Max Drawdown": f"{results['Max Drawdown %']:.2f}%",
                "Total Trades": results['Total Trades']
            }
        }
        
        fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.05,
                            subplot_titles=('Portfolio Equity Over Time', 'Portfolio Drawdown', 'Price and Executed Trades'))

        # --- Plot 1: Equity Curve ---
        equity_curve_df = results['Equity Curve']
        initial_equity = self.portfolio_manager.initial_cash

        # Plot the actual portfolio equity over time
        fig.add_trace(go.Scatter(
            x=equity_curve_df.index, y=equity_curve_df['Equity'],
            name='Portfolio Equity', line=dict(color='deepskyblue')
        ), row=1, col=1)

        fig.add_trace(go.Scatter(x=results['Drawdown Curve'].index, y=results['Drawdown Curve'] * 100, name='Drawdown', fill='tozeroy', line=dict(color='red')), row=2, col=1)
        fig.add_trace(go.Scatter(x=signals_df.index, y=signals_df['Close'], name='Close Price', line=dict(color='gray', width=1)), row=3, col=1)
        
        trade_log_df = pd.DataFrame(self.strategy_portfolio.trade_log)
        if not trade_log_df.empty:
            buy_trades = trade_log_df[trade_log_df['direction'] == 'BUY']
            sell_trades = trade_log_df[trade_log_df['direction'] == 'SELL']
            fig.add_trace(go.Scatter(x=buy_trades['timestamp'], y=buy_trades['price'], name='Buy', mode='markers', marker=dict(color='lime', size=10, symbol='triangle-up')), row=3, col=1)
            fig.add_trace(go.Scatter(x=sell_trades['timestamp'], y=sell_trades['price'], name='Sell', mode='markers', marker=dict(color='magenta', size=10, symbol='triangle-down')), row=3, col=1)

        fig.update_layout(title_text=f"Performance Analysis: {self.strategy.name} ({self.period.replace('_', ' ').title()})", template='plotly_dark', height=900,
                          legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        
        report_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_basename = f"{self.strategy.name}_{self.asset}_{self.timeframe}_{self.period}_{report_timestamp}"
        html_path = os.path.join(OUTPUT_DIR, f"{report_basename}_individual_report.html")
        
        html_content = f"""
        <html><head><title>Backtest Report: {self.strategy.name}</title>
        <style> body {{ font-family: 'Arial', sans-serif; background-color: #111; color: #eee; }} h1, h2 {{ color: #44aaff; border-bottom: 2px solid #44aaff; }} table {{ border-collapse: collapse; width: 50%; margin: 20px 0; }} th, td {{ border: 1px solid #444; padding: 8px; text-align: left; }} th {{ background-color: #222; }} </style></head>
        <body><h1>Backtest Report: {self.strategy.name}</h1>
        <h2>Asset: {self.asset} | Timeframe: {self.timeframe} | Period: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')} ({self.period.replace('_', ' ').title()})</h2>"""
        for category, data in metrics_data.items():
            html_content += f"<h3>{category}</h3><table>"
            for key, value in data.items(): html_content += f"<tr><th>{key}</th><td>{value}</td></tr>"
            html_content += "</table>"
        html_content += fig.to_html(full_html=False, include_plotlyjs='cdn')
        html_content += "</body></html>"
        
        with open(html_path, 'w') as f: f.write(html_content)
        log.info(f"Individual HTML report saved to {html_path}")
        return html_path

def generate_comparison_report(all_results: list, backtest_config: dict, period: str):
    if not all_results:
        log.error("No backtest results to generate a comparison report.")
        return

    log.info("Generating strategy comparison report...")
    summary_data = []
    for r in all_results:
        report_path = r.get('Report Path')
        report_link = 'N/A'
        if report_path:
            report_filename = os.path.basename(report_path)
            report_link = f'<a href="{report_filename}" target="_blank">View</a>'

        summary_data.append({
            'Strategy': r['Strategy Name'],
            'Asset': r['Asset'],
            'Timeframe': r['Timeframe'],
            'Total Return %': f"{r['Total Return %']:.2f}",
            'Max Drawdown %': f"{r['Max Drawdown %']:.2f}",
            'Sharpe Ratio': f"{r['Sharpe Ratio']:.2f}",
            'Total Trades': r['Total Trades'],
            'Total P&L $': f"${r['Total P&L $']:,.2f}",
            'Report': report_link
        })
    summary_df = pd.DataFrame(summary_data)
    
    # --- Equity Curve Comparison Chart ---
    fig_equity = go.Figure()
    for r in all_results: fig_equity.add_trace(go.Scatter(x=r['Equity Curve'].index, y=r['Equity Curve']['Equity'], name=r['Strategy Name']))
    fig_equity.update_layout(title_text='Equity Curve Comparison', template='plotly_dark', margin=dict(l=10, r=10, t=40, b=10))

    # --- Key Metrics Bar Chart Comparison ---
    fig_bars = make_subplots(rows=1, cols=3, subplot_titles=('Total Return %', 'Max Drawdown %', 'Sharpe Ratio'))
    names = [r['Strategy Name'] for r in all_results]
    fig_bars.add_trace(go.Bar(x=names, y=[r['Total Return %'] for r in all_results], name='Return'), row=1, col=1)
    fig_bars.add_trace(go.Bar(x=names, y=[r['Max Drawdown %'] for r in all_results], name='Drawdown'), row=1, col=2)
    fig_bars.add_trace(go.Bar(x=names, y=[r['Sharpe Ratio'] for r in all_results], name='Sharpe'), row=1, col=3)
    fig_bars.update_layout(title_text='Key Metric Comparison', template='plotly_dark', showlegend=False, margin=dict(l=10, r=10, t=40, b=10))

    execution_time = datetime.now()
    start_date_str = backtest_config['periods'][period]['start_date'].split('T')[0]
    end_date_str = backtest_config['periods'][period]['end_date'].split('T')[0]
    report_timestamp = execution_time.strftime('%Y%m%d_%H%M%S')
    html_path = os.path.join(OUTPUT_DIR, f"comparison_report_{period}_{report_timestamp}.html")

    # --- Performance Summary Table ---
    # Convert the DataFrame to an HTML table string with a specific class for styling.
    summary_table_html = summary_df.to_html(escape=False, index=False, classes='styled-table')

    # --- Assemble the HTML Report ---
    html_content = f"""
    <html><head><title>Strategy Comparison Report ({period.replace('_', ' ').title()})</title>
    <style>
        body {{ font-family: 'Arial', sans-serif; background-color: #111; color: #eee; margin: 20px; }}
        h1, h2 {{ color: #44aaff; border-bottom: 1px solid #44aaff; padding-bottom: 8px; font-weight: 300; }}
        h2 {{ font-size: 1.2em; color: #bbb; border-bottom: none; }}
        .report-container {{ display: flex; flex-direction: column; gap: 20px; }}
        .chart-container {{ display: flex; flex-direction: row; flex-wrap: wrap; gap: 20px; }}
        .chart-item {{ flex: 1; min-width: 500px; }}
        /* Table Styles */
        .styled-table {{ border-collapse: collapse; width: 100%; margin-top: 10px; font-size: 0.9em; }}
        .styled-table thead tr {{ background-color: #2a3f5f; color: #ffffff; text-align: left; }}
        .styled-table th, .styled-table td {{ padding: 12px 15px; border: 1px solid #333; }}
        .styled-table tbody tr {{ border-bottom: 1px solid #333; }}
        .styled-table tbody tr:nth-of-type(even) {{ background-color: #1e2a3a; }}
        .styled-table tbody tr:hover {{ background-color: #2c3e50; }}
        .styled-table a {{ color: #44aaff; font-weight: bold; text-decoration: none; }}
        .styled-table a:hover {{ text-decoration: underline; }}
    </style></head>
    <body>
    <div class="report-container">
        <div>
            <h1>Strategy Comparison Report ({period.replace('_', ' ').title()})</h1>
            <h2>Executed on: {execution_time.strftime('%Y-%m-%d %H:%M:%S')} | Period: {start_date_str} to {end_date_str}</h2>
        </div>
        <div>
            <h3>Performance Summary</h3>
            {summary_table_html}
        </div>
        <div class="chart-container">
            <div class="chart-item">{fig_equity.to_html(full_html=False, include_plotlyjs='cdn')}</div>
            <div class="chart-item">{fig_bars.to_html(full_html=False, include_plotlyjs=False)}</div>
        </div>
    </div>
    </body></html>
    """
    
    with open(html_path, 'w') as f: f.write(html_content)
    log.info(f"Strategy comparison report saved to {html_path}")

def load_strategies_from_config(config_path: str) -> tuple:
    log.info(f"Loading strategies from config: {config_path}")
    with open(config_path, 'r') as f: config = yaml.safe_load(f)
    initialized_strategies = []
    for sc in config.get('strategies', []):
        try:
            module_path = f"trading_system.{sc['module']}"
            class_name = sc['class']
            log.info(f"  -> Loading strategy '{class_name}' from '{module_path}'")
            module = importlib.import_module(module_path)
            StrategyClass = getattr(module, class_name)
            instance = StrategyClass()
            instance.initialize(sc)
            initialized_strategies.append((instance, sc))
        except Exception as e:
            log.error(f"Failed to load strategy '{sc.get('name', 'N/A')}': {e}", exc_info=True)
    return initialized_strategies, config.get('system', {}), config.get('backtest', {})

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backtesting Engine for the Trading System.")
    parser.add_argument(
        '--period',
        type=str,
        default='in_sample',
        choices=['in_sample', 'out_of_sample'],
        help="The backtesting period to use, as defined in config.yaml. Defaults to 'in_sample'."
    )
    args = parser.parse_args()

    log.info(f"--- Starting Backtesting Engine for '{args.period.replace('_', ' ').title()}' period ---")
    strategies, system_config, backtest_config = load_strategies_from_config(CONFIG_PATH)
    
    if not strategies or not system_config or not backtest_config:
        log.error("A required configuration section (strategies, system, or backtest) is missing. Exiting.")
        sys.exit(1)

    all_results = []
    for strategy_instance, strategy_config in strategies:
        try:
            backtest_runner = Backtest(strategy_instance, strategy_config, system_config, backtest_config, args.period)
            result = backtest_runner.run()
            if result:
                all_results.append(result)
        except Exception as e:
            log.error(f"An error occurred during the backtest for '{strategy_instance.name}': {e}", exc_info=True)

    if all_results:
        generate_comparison_report(all_results, backtest_config, args.period)
    else:
        log.warning("No successful backtests were completed. Comparison report will not be generated.")

    log.info("--- All backtests completed. ---")
