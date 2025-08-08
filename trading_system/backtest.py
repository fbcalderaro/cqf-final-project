# trading_system/backtest.py

import sys
import os
import yaml
import importlib
import pandas as pd
from backtesting import Backtest, Strategy as BacktestingStrategy

# Add project root to Python's path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)

from trading_system.utils import db_utils
from trading_system.utils.common import log

CONFIG_PATH = os.path.join(PROJECT_ROOT, 'trading_system', 'config', 'config.yaml')


# --- Data Loader with Resampling ---
def load_and_resample_data(asset: str, timeframe: str, db_config: dict) -> pd.DataFrame | None:
    """
    Loads 1-minute base data from the database and resamples it to the
    target timeframe for the strategy.
    """
    log.info(f"Loading 1m base data for {asset} to resample to {timeframe}...")
    try:
        from datetime import datetime, timedelta
        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(days=730) # Load 2 years of 1m data
        
        df_1m = db_utils.fetch_candles_for_range(db_config, asset, start_dt, end_dt)
        
        if df_1m is None or df_1m.empty:
            return None

        df_1m.rename(columns={
            'open_price': 'Open', 'high_price': 'High',
            'low_price': 'Low', 'close_price': 'Close', 'volume': 'Volume'
        }, inplace=True)

        agg_rules = {
            'Open': 'first', 'High': 'max', 'Low': 'min',
            'Close': 'last', 'Volume': 'sum'
        }
        df_resampled = df_1m.resample(timeframe).agg(agg_rules).dropna()
        
        log.info(f"Successfully resampled {len(df_1m)} 1m records to {len(df_resampled)} '{timeframe}' records.")
        return df_resampled

    except Exception as e:
        log.error(f"Failed to load and resample data for {asset}: {e}", exc_info=True)
        return None


def load_strategies_from_config(config_path: str) -> list:
    log.info(f"Loading strategies from config: {config_path}")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # --- FIXED: Corrected the syntax error here ---
    initialized_strategies = []
    for strategy_config in config.get('strategies', []):
        try:
            module_path = "trading_system." + strategy_config['module']
            class_name = strategy_config['class']
            log.info(f"  -> Loading strategy '{class_name}' from '{module_path}'")
            
            module = importlib.import_module(module_path)
            StrategyClass = getattr(module, class_name)
            strategy_instance = StrategyClass()
            strategy_instance.initialize(strategy_config)
            
            initialized_strategies.append((strategy_instance, strategy_config))
        except Exception as e:
            log.error(f"Failed to load strategy '{strategy_config.get('name', 'N/A')}': {e}", exc_info=True)

    return initialized_strategies, config.get('system', {})


def create_backtesting_strategy(strategy_instance):
    class BridgeStrategy(BacktestingStrategy):
        def init(self):
            self.signals = self.I(strategy_instance.generate_signals, self.data.df)

        def next(self):
            current_signal = self.signals[-1]
            if current_signal == 1 and not self.position:
                self.buy()
            elif current_signal == -1 and self.position:
                self.position.close()
    return BridgeStrategy


def main():
    log.info("--- Starting Multi-Strategy Backtesting Engine ---")
    
    strategies, system_config = load_strategies_from_config(CONFIG_PATH)
    
    db_config = system_config.get('database')
    if not db_config:
        log.error("Database configuration missing from config.yaml.")
        return
        
    if not strategies:
        log.warning("No strategies were loaded. Exiting.")
        return

    # --- NEW: Create an output directory for our results ---
    output_dir = os.path.join(PROJECT_ROOT, 'trading_system', 'output')
    os.makedirs(output_dir, exist_ok=True)
    log.info(f"Results will be saved in: {output_dir}")

    for strategy_instance, config in strategies:
        log.info(f"\n{'='*60}\nRunning backtest for: {strategy_instance.name} on {config['asset']}\n{'='*60}")

        timeframe = config.get('timeframe', '1h')
        data_df = load_and_resample_data(config['asset'], timeframe, db_config)
        
        if data_df is None:
            log.error(f"Skipping strategy {strategy_instance.name} due to data loading failure.")
            continue
            
        BacktestingBridge = create_backtesting_strategy(strategy_instance)
        
        bt = Backtest(
            data_df,
            BacktestingBridge,
            cash=system_config.get('initial_cash', 100_000),
            commission=system_config.get('commission_pct', 0.001)
        )
        
        stats = bt.run()
        
        log.info(f"--- Results for {strategy_instance.name} ---")
        print(stats)

        # --- NEW: Save stats and plot to the output directory ---
        # Sanitize the name for use in a filename
        safe_name = strategy_instance.name.replace(' ', '_').replace('/', '_')
        
        stats_filename = os.path.join(output_dir, f"{safe_name}_stats.csv")
        # The stats object from backtesting.py is a pandas Series, so we save it as such
        stats.to_csv(stats_filename)
        log.info(f"Saved stats to {stats_filename}")

        plot_filename = os.path.join(output_dir, f"{safe_name}_plot.html")
        bt.plot(filename=plot_filename, open_browser=False)
        log.info(f"Saved interactive plot to {plot_filename}")

    log.info("--- All backtests completed. ---")

if __name__ == "__main__":
    main()
