# trading_system/strategies/base_strategy.py

from abc import ABC, abstractmethod
import pandas as pd

class Strategy(ABC):
    """
    Abstract Base Class for all trading strategies.

    This class defines the "contract" that all concrete strategy classes must adhere to.
    It ensures that the trading engine can interact with any strategy in a consistent way.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """A unique, human-readable name for the strategy."""
        pass

    @abstractmethod
    def initialize(self, config: dict):
        """
        Initializes the strategy with its specific parameters from a config dict.
        """
        pass

    @abstractmethod
    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        The core logic of the strategy. Receives historical market data and returns trading signals.

        Args:
            data (pd.DataFrame): A DataFrame containing at least OHLCV data, indexed by timestamp.

        Returns:
            pd.DataFrame: The input DataFrame with an added 'signal' column.
                          The 'signal' column should contain:
                          -  1: for a buy signal
                          - -1: for a sell signal
                          -  0: for no signal (hold)
        """
        pass
