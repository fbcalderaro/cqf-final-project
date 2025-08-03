import numpy as np
import pandas as pd
import quantstats as qs
import matplotlib
import db_utils 
from common import log 
import os 
import config

class Backtester:
    """
    A class to run an event-driven backtest on a given strategy.
    """
    def __init__(self, data, initial_capital=10000.0, commission=0.001, risk_per_trade=0.01, atr_stop_multiplier=2.5, rsi_buy_zone=(40, 55), rsi_sell_zone=(45, 60), adx_threshold=25, verbose=False, max_position_pct=0.25):
        self.data = data.rename(columns={
            'high_price': 'High', 'low_price': 'Low', 'close_price': 'Close', 'open_price': 'Open',
            'rsi_9': 'rsi', 'atr_14': 'atr', 'adx_14': 'adx', 'plus_di_14': 'plus_di', 'minus_di_14': 'minus_di'
        })
        self.initial_capital = initial_capital
        self.commission = commission
        self.risk_per_trade = risk_per_trade
        self.atr_stop_multiplier = atr_stop_multiplier
        self.rsi_buy_zone = rsi_buy_zone
        self.rsi_sell_zone = rsi_sell_zone
        self.adx_threshold = adx_threshold
        self.trades = []
        self.equity_curve = None
        self.verbose = verbose
        self.max_position_pct = max_position_pct

    def run(self):
        """
        Executes the event-driven backtest.
        """
        log.info("\n--- Running Backtest for Adaptive Trend Rider (15-Min Resampled) ---")
        
        indicators_df = self.data 

        cash = self.initial_capital
        position_size_units, position_type, stop_loss_price, entry_price, highest_high_since_entry = 0, None, 0, 0, 0
        in_position = False
        lowest_low_since_entry = float('inf')
        cash_before_trade = cash
        portfolio_values = []

        for i in range(1, len(indicators_df)):
            current_row = indicators_df.iloc[i]

            if in_position:
                if position_type == 'long':
                    highest_high_since_entry = max(highest_high_since_entry, current_row['High'])
                    new_stop_loss = highest_high_since_entry - (current_row['atr'] * self.atr_stop_multiplier)
                    stop_loss_price = max(stop_loss_price, new_stop_loss) 

                    if current_row['Low'] <= stop_loss_price:
                        trade_value = position_size_units * stop_loss_price; fee = trade_value * self.commission
                        cash += trade_value - fee
                        self.trades.append({'date': current_row.name, 'type': 'stop_loss_long', 'price': stop_loss_price, 'size': position_size_units, 'value': trade_value})
                        log.info(f"{current_row.name} | STOP-LOSS (LONG) at ${stop_loss_price:,.2f} | Portfolio: ${cash:,.2f}")
                        in_position = False
                
                elif position_type == 'short':
                    lowest_low_since_entry = min(lowest_low_since_entry, current_row['Low'])
                    new_stop_loss = lowest_low_since_entry + (current_row['atr'] * self.atr_stop_multiplier)
                    stop_loss_price = min(stop_loss_price, new_stop_loss)

                    if current_row['High'] >= stop_loss_price:
                        cost_to_cover = position_size_units * stop_loss_price; profit = self.trades[-1]['value'] - cost_to_cover
                        fee = cost_to_cover * self.commission; cash = cash_before_trade + profit - fee
                        self.trades.append({'date': current_row.name, 'type': 'stop_loss_short', 'price': stop_loss_price, 'size': position_size_units, 'value': cost_to_cover})
                        log.info(f"{current_row.name} | STOP-LOSS (SHORT) at ${stop_loss_price:,.2f} | Portfolio: ${cash:,.2f}")
                        in_position = False

            if not in_position:
                is_trending = current_row['adx'] > self.adx_threshold
                is_bullish = current_row['plus_di'] > current_row['minus_di']
                is_bearish = current_row['minus_di'] > current_row['plus_di']
                in_buy_zone = self.rsi_buy_zone[0] <= current_row['rsi'] <= self.rsi_buy_zone[1]
                in_sell_zone = self.rsi_sell_zone[0] <= current_row['rsi'] <= self.rsi_sell_zone[1]

                if self.verbose:
                    log.info(f"{current_row.name} | Trend: {current_row['supertrend_direction']}, Trending: {is_trending}, Bullish: {is_bullish}, InBuyZone: {in_buy_zone}, InSellZone: {in_sell_zone}, RSI: {current_row['rsi']:.2f}")

                if (current_row['supertrend_direction'] == 1 and is_trending and is_bullish and in_buy_zone):
                    entry_price = current_row['Close']
                    initial_stop_loss = entry_price - (current_row['atr'] * self.atr_stop_multiplier)
                    risk_per_unit = entry_price - initial_stop_loss
                    if risk_per_unit > 0:
                        capital_at_risk = cash * self.risk_per_trade
                        risk_based_size = capital_at_risk / risk_per_unit
                        
                        max_position_value = cash * self.max_position_pct
                        max_position_size = max_position_value / entry_price
                        
                        position_size_units = min(risk_based_size, max_position_size)
                        
                        trade_value = position_size_units * entry_price
                        if trade_value > 0 and cash >= trade_value:
                            fee = trade_value * self.commission; cash -= (trade_value + fee)
                            in_position = True; position_type = 'long'; stop_loss_price = initial_stop_loss
                            highest_high_since_entry = current_row['High']
                            self.trades.append({'date': current_row.name, 'type': 'buy', 'price': entry_price, 'size': position_size_units, 'value': trade_value})
                            log.info(f"{current_row.name} | BUY at ${entry_price:,.2f} | Holding {position_size_units:,.4f} units")

                elif (current_row['supertrend_direction'] == -1 and is_trending and is_bearish and in_sell_zone):
                    entry_price = current_row['Close']
                    initial_stop_loss = entry_price + (current_row['atr'] * self.atr_stop_multiplier)
                    risk_per_unit = initial_stop_loss - entry_price
                    if risk_per_unit > 0:
                        capital_at_risk = cash * self.risk_per_trade
                        risk_based_size = capital_at_risk / risk_per_unit

                        max_position_value = cash * self.max_position_pct
                        max_position_size = max_position_value / entry_price

                        position_size_units = min(risk_based_size, max_position_size)
                        
                        trade_value = position_size_units * entry_price
                        if trade_value > 0:
                            cash_before_trade = cash; in_position = True; position_type = 'short'
                            stop_loss_price = initial_stop_loss; lowest_low_since_entry = current_row['Low']
                            self.trades.append({'date': current_row.name, 'type': 'sell_short', 'price': entry_price, 'size': position_size_units, 'value': trade_value})
                            log.info(f"{current_row.name} | SELL SHORT at ${entry_price:,.2f} | Holding {position_size_units:,.4f} units")

            current_portfolio_value = cash
            if in_position:
                if position_type == 'long':
                    current_portfolio_value += position_size_units * current_row['Close']
                elif position_type == 'short':
                    unrealized_pnl = self.trades[-1]['value'] - (position_size_units * current_row['Close'])
                    current_portfolio_value = cash_before_trade + unrealized_pnl
            portfolio_values.append(current_portfolio_value)

        self.equity_curve = pd.Series(portfolio_values, index=indicators_df.index[1:])
        self.equity_curve.name = 'equity'
        log.info("--- Backtest Finished ---")

    def report(self):
        """
        Generates a detailed performance report using quantstats.
        """
        if self.equity_curve is None or self.equity_curve.empty:
            log.error("❌ Equity curve is empty. Cannot generate report.")
            return

        log.info("\n--- Generating Performance Report ---")
        qs.extend_pandas()
        returns = self.equity_curve.pct_change().fillna(0)

        if returns.std() == 0:
            log.warning("⚠️ No trades were executed. Strategy returns are flat.")
            log.warning("   A performance report cannot be generated.")
            print("\n--- Basic Stats ---")
            print(f"Initial Capital: ${self.initial_capital:,.2f}")
            print(f"Final Equity:    ${self.equity_curve.iloc[-1]:,.2f}")
            print(f"Trades Made:     {len([t for t in self.trades if t['type'] in ['buy', 'sell_short']])}")
            print("---------------------\n")
            return

        try:
            matplotlib.use('Agg')
            matplotlib.rcParams['font.family'] = 'sans-serif'
        except Exception as e:
            log.warning(f"⚠️ Could not set font family: {e}")
            
        benchmark_raw = self.data['Close'].pct_change().fillna(0)
        benchmark = benchmark_raw.reindex(returns.index).fillna(0)
        returns.index = returns.index.tz_localize(None)
        benchmark.index = benchmark.index.tz_localize(None)

        # --- CHANGE: Define output directory and construct full file path ---
        output_dir = 'output'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            log.info(f"Created directory: {output_dir}")

        report_filename = 'adaptive_trend_rider_report.html'
        report_filepath = os.path.join(output_dir, report_filename)
        
        qs.reports.html(returns, benchmark=benchmark, output=report_filepath, title='Adaptive Trend Rider Strategy (15-Min Resampled)')
        
        log.info(f"✅ Full performance report saved as '{report_filepath}'.")


if __name__ == '__main__':
    log.info("--- Starting Adaptive Trend Rider Strategy Backtest (15-Min Resampled) ---")
    
    data_df = db_utils.fetch_resampled_candles_and_indicators(
        source_candles_table=config.CANDLES_TABLE_NAME,
        source_indicators_table=config.INDICATORS_TABLE_NAME,
        interval=config.RESAMPLE_INTERVAL,
        limit=config.DATA_LIMIT 
    )

    if data_df is not None and not data_df.empty:
        backtester = Backtester(
            data=data_df,
            initial_capital=config.INITIAL_CAPITAL,
            commission=config.COMMISSION_PCT,
            risk_per_trade=config.RISK_PER_TRADE_PCT,
            atr_stop_multiplier=config.ATR_STOP_MULTIPLIER,
            rsi_buy_zone=config.RSI_BUY_ZONE,
            rsi_sell_zone=config.RSI_SELL_ZONE,
            adx_threshold=config.ADX_THRESHOLD,
            verbose=False, # This can remain a direct parameter for easy debugging
            max_position_pct=config.MAX_POSITION_PCT
        )

    if data_df is not None and not data_df.empty:
        backtester = Backtester(
            data=data_df,
            initial_capital=1000000.0,
            commission=0.001,
            risk_per_trade=0.02,
            atr_stop_multiplier=2.5,
            rsi_buy_zone=(55, 70),
            rsi_sell_zone=(30, 45),
            adx_threshold=22,
            verbose=False, # Set to False now that it should be working
            max_position_pct=0.25
        )
        backtester.run()
        backtester.report()
    else:
        log.error(f"Could not fetch and resample data from {SOURCE_CANDLES_TABLE}. Exiting backtest.")
        log.error("Please ensure you have 1-minute data in your database.")