# trading_system/strategies/momentum_strategy.py

import pandas as pd
import pandas_ta as ta
from trading_system.strategies.base_strategy import Strategy
from trading_system.utils.common import log

class MomentumStrategy(Strategy):
    """
    A flexible momentum strategy that can use SMA or EMA crossovers,
    with optional ADX and MACD filters for trend strength and confirmation.
    """

    @property
    def name(self) -> str:
        """Returns the configured name of the strategy."""
        return self._name

    def initialize(self, config: dict):
        """Initializes the strategy with parameters from the config file."""
        self._name = config.get('name', 'MomentumStrategy')
        self._params = config.get('params', {})
        
        # Core crossover parameters
        self.ma_type = self._params.get('ma_type', 'sma').lower()
        self.short_window = int(self._params.get('short_window', 20))
        self.long_window = int(self._params.get('long_window', 50))

        # Optional ADX trend filter
        self.use_adx_filter = self._params.get('use_adx_filter', False)
        self.adx_length = int(self._params.get('adx_length', 14))
        self.adx_threshold = float(self._params.get('adx_threshold', 25.0))
        
        # Optional MACD confirmation filter
        self.use_macd_filter = self._params.get('use_macd_filter', False)
        self.macd_fast = int(self._params.get('macd_fast', 12))
        self.macd_slow = int(self._params.get('macd_slow', 26))
        self.macd_signal = int(self._params.get('macd_signal', 9))

        log.info(f"Strategy '{self.name}' initialized with MA Type: {self.ma_type.upper()}, ADX Filter: {self.use_adx_filter}, MACD Filter: {self.use_macd_filter}")

        if self.short_window >= self.long_window:
            raise ValueError("Short window must be smaller than long window.")

    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Generates trading signals based on the configured logic.
        """
        if not isinstance(data, pd.DataFrame) or not all(col in data.columns for col in ['High', 'Low', 'Close']):
            raise ValueError("Input data must be a pandas DataFrame with High, Low, and Close columns.")
        
        # --- 1. Calculate Core Crossover Indicator ---
        if self.ma_type == 'ema':
            short_ma = ta.ema(data['Close'], length=self.short_window)
            long_ma = ta.ema(data['Close'], length=self.long_window)
        else: # Default to SMA
            short_ma = ta.sma(data['Close'], length=self.short_window)
            long_ma = ta.sma(data['Close'], length=self.long_window)
        
        # --- 2. Calculate Optional Filter Indicators ---
        adx = None
        if self.use_adx_filter:
            adx_series = ta.adx(data['High'], data['Low'], data['Close'], length=self.adx_length)
            if adx_series is not None and not adx_series.empty:
                 adx = adx_series[f'ADX_{self.adx_length}']

        macd = None
        if self.use_macd_filter:
            macd_series = ta.macd(data['Close'], fast=self.macd_fast, slow=self.macd_slow, signal=self.macd_signal)
            if macd_series is not None and not macd_series.empty:
                macd = macd_series[f'MACD_{self.macd_fast}_{self.macd_slow}_{self.macd_signal}']
                macd_signal_line = macd_series[f'MACDs_{self.macd_fast}_{self.macd_slow}_{self.macd_signal}']

        # --- 3. Determine Entry and Exit Conditions ---
        
        # Base crossover conditions
        enter_long = (short_ma > long_ma) & (short_ma.shift(1) <= long_ma.shift(1))
        exit_long = (short_ma < long_ma) & (short_ma.shift(1) >= long_ma.shift(1))
        
        # Apply filters if enabled
        if self.use_adx_filter and adx is not None:
            log.info(f"Applying ADX filter with threshold {self.adx_threshold}...")
            is_trending = (adx > self.adx_threshold)
            enter_long &= is_trending
            
        if self.use_macd_filter and macd is not None:
            log.info("Applying MACD confirmation filter...")
            macd_confirmation = (macd > macd_signal_line)
            enter_long &= macd_confirmation

        # --- 4. Generate Final Signals ---
        signals = pd.DataFrame(index=data.index)
        signals['signal'] = 0
        signals.loc[enter_long, 'signal'] = 1
        signals.loc[exit_long, 'signal'] = -1
        
        data['signal'] = signals['signal']
        
        return data