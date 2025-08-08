# trading_system/strategies/momentum_strategy.py

import pandas as pd
import pandas_ta as ta
from trading_system.strategies.base_strategy import Strategy

class MomentumStrategy(Strategy):
    """
    A simple momentum crossover strategy.
    """

    @property
    def name(self) -> str:
        """Returns the configured name of the strategy."""
        return self._name

    def initialize(self, config: dict):
        """Initializes the strategy with parameters from the config file."""
        self._name = config.get('name', 'MomentumStrategy')
        self._params = config.get('params', {})
        self.short_window = self._params.get('short_window', 20)
        self.long_window = self._params.get('long_window', 50)

        if self.short_window >= self.long_window:
            raise ValueError("Short window must be smaller than long window.")

    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Generates trading signals based on the moving average crossover logic.
        """
        if not isinstance(data, pd.DataFrame) or 'Close' not in data.columns:
            raise ValueError("Input data must be a pandas DataFrame with a 'Close' column.")
        
        signals = pd.DataFrame(index=data.index)
        signals['signal'] = 0

        signals[f'sma_short'] = ta.sma(data['Close'], length=self.short_window)
        signals[f'sma_long'] = ta.sma(data['Close'], length=self.long_window)

        signals.loc[
            (signals[f'sma_short'] > signals[f'sma_long']) & 
            (signals[f'sma_short'].shift(1) <= signals[f'sma_long'].shift(1)),
            'signal'
        ] = 1

        signals.loc[
            (signals[f'sma_short'] < signals[f'sma_long']) & 
            (signals[f'sma_short'].shift(1) >= signals[f'sma_long'].shift(1)),
            'signal'
        ] = -1
        
        data['signal'] = signals['signal']
        
        return data
