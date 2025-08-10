# trading_system/strategies/test_strategy.py

import pandas as pd
import numpy as np
from trading_system.strategies.base_strategy import Strategy
from trading_system.utils.common import log

class TestStrategy(Strategy):
    """
    A simple test strategy designed to generate signals at a fixed interval
    to verify that the trading engine is working correctly.
    It alternates between BUY (1) and SELL (-1) signals.
    """

    @property
    def name(self) -> str:
        return self._name

    def initialize(self, config: dict):
        """Initializes the strategy with parameters from the config file."""
        self._name = config.get('name', 'TestStrategy')
        self._params = config.get('params', {})
        self.signal_interval = int(self._params.get('signal_interval', 5))
        log.info(f"Strategy '{self.name}' initialized to generate a signal every {self.signal_interval} bars.")

    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Generates alternating BUY (1) and SELL (-1) signals.
        """
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Input data must be a pandas DataFrame.")
        
        signals = pd.Series(0, index=data.index, dtype=int)
        
        # Create an array of indices where signals should be generated
        signal_indices = np.arange(self.signal_interval, len(data), self.signal_interval)
        
        # Determine the signal value (1 or -1) based on whether the signal number is odd or even
        # (signal_indices // self.signal_interval) -> 1, 2, 3, 4...
        # % 2 -> 1, 0, 1, 0...
        # * 2 - 1 -> 1, -1, 1, -1...
        signal_values = np.where((signal_indices // self.signal_interval) % 2 == 1, 1, -1)
        
        signals.iloc[signal_indices] = signal_values
        data['signal'] = signals
        return data
