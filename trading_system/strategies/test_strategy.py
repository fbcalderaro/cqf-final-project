# trading_system/strategies/test_strategy.py

import pandas as pd
import numpy as np
from trading_system.strategies.base_strategy import Strategy
from trading_system.utils.common import log

class TestStrategy(Strategy):
    """
    A simple test strategy designed to generate signals at a fixed interval
    to verify that the trading engine is working correctly. It alternates
    between BUY (1) and SELL (-1) signals, with a configurable probability
    of HOLDING (0) the position instead of trading.
    """

    @property
    def name(self) -> str:
        return self._name

    def initialize(self, config: dict):
        """Initializes the strategy with parameters from the config file."""
        self._name = config.get('name', 'TestStrategy')
        self._params = config.get('params', {})
        self.signal_interval = int(self._params.get('signal_interval', 5))
        self.hold_probability = float(self._params.get('hold_probability', 0.3))
        log.info(f"Strategy '{self.name}' initialized to generate a potential signal every {self.signal_interval} bars.")
        if self.hold_probability > 0:
            log.info(f" -> Hold probability set to {self.hold_probability:.2f}")

    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Generates alternating BUY (1), SELL (-1), or HOLD (0) signals.

        At each `signal_interval`, the strategy decides whether to generate a
        trade signal or to hold, based on the `hold_probability` parameter.
        When it does generate a signal, it alternates between BUY and SELL.
        """
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Input data must be a pandas DataFrame.")

        if data.empty:
            data['signal'] = 0
            return data

        # Start with a series of all zeros (no signal).
        signals = pd.Series(0, index=data.index, dtype=int)

        # Get the integer locations (iloc) where a signal *could* be generated.
        signal_indices = np.arange(self.signal_interval, len(data), self.signal_interval)

        if len(signal_indices) == 0:
            data['signal'] = signals
            return data

        # For each potential signal point, decide randomly whether to trade or hold.
        rands = np.random.rand(len(signal_indices))
        trade_mask = rands >= self.hold_probability

        # Get the actual integer locations (iloc) where we will place a trade signal.
        trade_indices = signal_indices[trade_mask]

        # If no trades are to be made, we are done.
        if len(trade_indices) == 0:
            data['signal'] = signals
            return data

        # Generate the alternating BUY (1) and SELL (-1) signals only for the
        # indices that were not filtered out by the hold probability.
        num_trades = len(trade_indices)

        # This clever line of numpy code does the following:
        # 1. `np.arange(num_trades)`: Creates a sequence [0, 1, 2, ...].
        # 2. `% 2`: Takes the modulus, resulting in [0, 1, 0, 1, ...].
        # 3. `np.where(..., 1, -1)`: Maps 0 to a BUY signal (1) and 1 to a SELL signal (-1).
        # This ensures the sequence of actual trades is always alternating, starting with a BUY.
        signal_values = np.where(np.arange(num_trades) % 2 == 0, 1, -1)

        # Place the generated signal values (1 or -1) at the correct integer locations.
        signals.iloc[trade_indices] = signal_values
        data['signal'] = signals
        return data
