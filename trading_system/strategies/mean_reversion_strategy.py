# trading_system/strategies/mean_reversion_ou.py

import pandas as pd
import numpy as np
import pandas_ta as ta
from trading_system.strategies.base_strategy import Strategy
from trading_system.utils.common import log

class MeanReversionOU(Strategy):
    """
    Implements a single-asset, LONG-ONLY mean-reversion strategy based on the
    Ornstein-Uhlenbeck (OU) process. It assumes that the price of an asset will
    revert to a "true" but unobserved mean value over time.

    A Kalman Filter is used to dynamically estimate this drifting mean in real-time.
    """

    @property
    def name(self) -> str:
        return self._name

    def initialize(self, config: dict):
        """Initializes the strategy with parameters from the config file."""
        self._name = config.get('name', 'MeanReversionOU')
        self._params = config.get('params', {})
        self.lookback_window = int(self._params.get('lookback_window', 60))
        self.entry_z_score = float(self._params.get('entry_z_score', 2.0))
        self.exit_z_score = float(self._params.get('exit_z_score', 0.5))
        self.stop_loss_z_score = float(self._params.get('stop_loss_z_score', 3.0))
        
        # --- Kalman Filter parameters ---
        self.kalman_process_noise = float(self._params.get('kalman_process_noise', 1e-5))
        self.kalman_measurement_noise = float(self._params.get('kalman_measurement_noise', 1e-4))

        # --- Volatility Filter Parameters ---
        self.use_volatility_filter = self._params.get('use_volatility_filter', True)
        self.atr_period = int(self._params.get('atr_period', 14))
        self.atr_multiplier = float(self._params.get('atr_multiplier', 1.5))

        # --- Trend Filter Parameters ---
        self.use_trend_filter = self._params.get('use_trend_filter', True)
        self.trend_ma_period = int(self._params.get('trend_ma_period', 200))
        
        log.info(f"Strategy '{self.name}' initialized (Long-Only) with lookback={self.lookback_window}, entry_z={self.entry_z_score}, exit_z={self.exit_z_score}")

    def _calculate_dynamic_mean(self, prices: pd.Series) -> pd.Series:
        """
        Calculates the time-varying mean of a price series using a 1D Kalman Filter.
        """
        Q = self.kalman_process_noise
        R = self.kalman_measurement_noise

        x_hat = np.zeros(len(prices))
        P = np.zeros(len(prices))
        x_hat_minus = np.zeros(len(prices))
        P_minus = np.zeros(len(prices))
        K = np.zeros(len(prices))

        x_hat[0] = prices.iloc[0]
        P[0] = 1.0

        for t in range(1, len(prices)):
            x_hat_minus[t] = x_hat[t-1]
            P_minus[t] = P[t-1] + Q
            K[t] = P_minus[t] / (P_minus[t] + R)
            x_hat[t] = x_hat_minus[t] + K[t] * (prices.iloc[t] - x_hat_minus[t])
            P[t] = (1 - K[t]) * P_minus[t]

        return pd.Series(x_hat, index=prices.index)

    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Generates event-based (1 for buy, -1 for sell) trading signals.
        """
        if not all(col in data.columns for col in ['Open', 'High', 'Low', 'Close']):
            raise ValueError("Input DataFrame must contain OHLC columns.")

        # --- 1. Calculate Z-Score ---
        # Work with log prices to stabilize variance.
        log_price = np.log(data['Close'])
        dynamic_mean = self._calculate_dynamic_mean(log_price)
        spread = log_price - dynamic_mean
        
        # The z-score measures how many standard deviations the current spread is from its rolling mean.
        spread_mean = spread.rolling(window=self.lookback_window, min_periods=20).mean() # Use a shorter min_periods to get signals earlier
        spread_std = spread.rolling(window=self.lookback_window, min_periods=20).std()
        z_score = (spread - spread_mean) / spread_std

        # --- Define Filters ---
        # By default, filters are "True" (i.e., allow trades)
        trend_filter = pd.Series(True, index=data.index)
        volatility_filter = pd.Series(True, index=data.index)
        if self.use_volatility_filter:
            # Volatility filter: Avoid trading when the market is excessively volatile.
            # We only trade if the current ATR is less than a multiple of its long-term average.
            atr = ta.atr(data['High'], data['Low'], data['Close'], length=self.atr_period)
            atr_ma = atr.rolling(window=self.lookback_window, min_periods=20).mean()
            volatility_filter = atr < (atr_ma * self.atr_multiplier)
            data['atr_ma_threshold'] = atr_ma * self.atr_multiplier # Add to df for analysis

        if self.use_trend_filter:
            # Trend filter: Only take long mean-reversion trades if the asset is in a long-term uptrend.
            # This helps avoid "catching a falling knife".
            long_ma = ta.sma(data['Close'], length=self.trend_ma_period)
            trend_filter = data['Close'] > long_ma
            data['long_ma'] = long_ma # Add to df for analysis

        # --- 3. Generate Signals from State Changes ---
        # This is a two-step process to convert state (are we in a position?) to events (buy/sell signals).

        # Step A: Determine the desired position state for each bar.
        # State is 1 if we should be in a long position, 0 if we should be flat.
        position_state = pd.Series(np.nan, index=z_score.index)
        
        # Entry condition: Price has dropped significantly below the mean (negative z-score) AND filters are passed.
        entry_condition = (z_score < -self.entry_z_score) & volatility_filter & trend_filter
        position_state[entry_condition] = 1

        # Exit conditions: Price has reverted back to the mean OR has moved even further away (stop-loss).
        exit_on_revert = abs(z_score) < self.exit_z_score
        exit_on_stop_loss = z_score < -self.stop_loss_z_score
        position_state[exit_on_revert | exit_on_stop_loss] = 0
        position_state = position_state.ffill().fillna(0) # Carry forward the last known state, fill initial NaNs with 0.

        # Step B: Generate event signals by finding where the state changes.
        # A change from 0 to 1 is a buy signal (1). A change from 1 to 0 is a sell signal (-1).
        signals = position_state.diff().fillna(0)
        
        data['dynamic_mean'] = np.exp(dynamic_mean) # Convert log-mean back to price for plotting
        data['z_score'] = z_score
        data['signal'] = signals.astype(int)
        
        return data