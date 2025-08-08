# trading_system/strategies/mean_reversion_bb.py

import pandas as pd
import pandas_ta as ta
from trading_system.strategies.base_strategy import Strategy

class MeanReversionBB(Strategy):
    """
    Implements a mean-reversion trading strategy using Bollinger Bands.
    """

    @property
    def name(self) -> str:
        """Returns the configured name of the strategy."""
        return self._name

    def initialize(self, config: dict):
        """Initializes the strategy with parameters from the config file."""
        self._name = config.get('name', 'MeanReversionBB')
        self._params = config.get('params', {})
        self.bb_period = self._params.get('bb_period', 20)
        self.bb_std_dev = self._params.get('bb_std_dev', 2.0)
        self.use_rsi_filter = self._params.get('use_rsi_filter', True)
        self.rsi_period = self._params.get('rsi_period', 14)
        self.rsi_oversold = self._params.get('rsi_oversold', 30)

    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Generates trading signals based on the Bollinger Bands mean-reversion logic.
        """
        if not all(col in data.columns for col in ['Open', 'High', 'Low', 'Close']):
            raise ValueError("Input DataFrame must contain OHLC columns.")

        bbands = ta.bbands(data['Close'], length=self.bb_period, std=self.bb_std_dev)
        if bbands is None or bbands.empty:
            data['signal'] = 0
            return data

        # Correctly access columns from the pandas-ta result
        data['bb_lower'] = bbands[f'BBL_{self.bb_period}_{self.bb_std_dev:.1f}']
        data['bb_middle'] = bbands[f'BBM_{self.bb_period}_{self.bb_std_dev:.1f}']
        data['bb_upper'] = bbands[f'BBU_{self.bb_period}_{self.bb_std_dev:.1f}']
        
        if self.use_rsi_filter:
            data['rsi'] = ta.rsi(data['Close'], length=self.rsi_period)

        signals = pd.Series(0, index=data.index)
        
        price_crossed_lower_band_up = (data['Close'].shift(1) < data['bb_lower'].shift(1)) & \
                                      (data['Close'] > data['bb_lower'])

        rsi_condition = True
        if self.use_rsi_filter and 'rsi' in data.columns:
            rsi_condition = data['rsi'] < self.rsi_oversold

        entry_signals = price_crossed_lower_band_up & rsi_condition
        
        signals[entry_signals] = 1
        
        data['signal'] = signals
        return data
