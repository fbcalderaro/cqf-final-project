# trading_system/strategies/mean_reversion_ou.py

import pandas as pd
import numpy as np
from trading_system.strategies.base_strategy import Strategy
from trading_system.utils.common import log

class MeanReversionOU(Strategy):
    """
    Implements a single-asset, LONG-ONLY mean-reversion strategy based on the
    Ornstein-Uhlenbeck process, using a Kalman Filter to dynamically
    estimate the asset's drifting mean.
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
        
        self.kalman_process_noise = float(self._params.get('kalman_process_noise', 1e-5))
        self.kalman_measurement_noise = float(self._params.get('kalman_measurement_noise', 1e-4))
        
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

        log_price = np.log(data['Close'])
        dynamic_mean = self._calculate_dynamic_mean(log_price)
        spread = log_price - dynamic_mean
        
        spread_mean = spread.rolling(window=self.lookback_window, min_periods=20).mean()
        spread_std = spread.rolling(window=self.lookback_window, min_periods=20).std()
        z_score = (spread - spread_mean) / spread_std

        # --- NEW: Convert Position State to Event-Based Signals ---
        
        # 1. Determine the desired position state (1 for in-position, 0 for flat)
        position_state = pd.Series(np.nan, index=z_score.index)
        position_state[z_score < -self.entry_z_score] = 1  # Condition to ENTER a long position
        position_state[abs(z_score) < self.exit_z_score] = 0  # Condition to EXIT (be flat)
        position_state = position_state.ffill().fillna(0) # Carry forward the state

        # 2. Generate signals by finding the change in position state
        # .diff() will be 1 on entry (0 -> 1), -1 on exit (1 -> 0), and 0 otherwise.
        signals = position_state.diff().fillna(0)
        
        # --- END OF NEW LOGIC ---
        
        data['dynamic_mean'] = np.exp(dynamic_mean)
        data['spread'] = spread
        data['z_score'] = z_score
        data['signal'] = signals.astype(int)
        
        return data