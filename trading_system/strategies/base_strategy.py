# trading_system/strategies/base_strategy.py

from abc import ABC, abstractmethod
import pandas as pd

class Strategy(ABC):
    """
    Abstract Base Class for all trading strategies.
    This defines the "contract" that all strategies must follow.
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
        The core logic of the strategy. Receives market data and returns signals.
        """
        pass
