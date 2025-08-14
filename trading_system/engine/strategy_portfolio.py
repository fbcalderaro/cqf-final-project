# trading_system/engine/strategy_portfolio.py

from datetime import datetime, timezone
from trading_system.utils.common import log

class StrategyPortfolio:
    """
    Manages the virtual account state for a single, isolated strategy.

    This class tracks the performance and state of one strategy as if it were
    trading in its own dedicated account. It manages its own allocated equity,
    cash, positions, and trade log, separate from the master portfolio.
    """
    def __init__(self, strategy_name: str, initial_equity: float, risk_per_trade_pct: float, traded_asset: str):
        """
        Initializes the sub-portfolio for a specific strategy.
        """
        self.strategy_name = strategy_name
        self.initial_equity = initial_equity
        self.equity = initial_equity
        self.cash = initial_equity # Initially, all equity is cash
        self.positions = {} # {asset: quantity}
        self.market_values = {} # {asset: price}
        self.risk_per_trade_pct = risk_per_trade_pct
        self.traded_asset = traded_asset
        self.trade_log = []
        self.equity_curve = [(datetime.now(timezone.utc), initial_equity)] # Start with initial equity
        log.info(f"  [Sub-Portfolio] Created for '{strategy_name}' with initial equity ${initial_equity:,.2f}")

    def get_current_equity(self) -> float:
        """Calculates the current total equity of the sub-portfolio."""
        position_value = self.positions.get(self.traded_asset, 0) * self.market_values.get(self.traded_asset, 0)
        return self.cash + position_value

    def update_market_value(self, price: float):
        """Updates the market price for the asset and recalculates equity."""
        self.market_values[self.traded_asset] = price
        self.equity = self.get_current_equity()

    def calculate_position_size(self) -> float:
        """
        Calculates the position size in quote currency (e.g., USDT) for a new trade.
        This is based on the strategy's *own* current equity and its configured risk percentage.
        """
        risk_amount = self.equity * self.risk_per_trade_pct
        # As a safeguard, ensure the calculated risk amount does not exceed available cash.
        if risk_amount > self.cash:
            log.warning(f"[{self.strategy_name}] Risk amount ${risk_amount:,.2f} exceeds its available cash ${self.cash:,.2f}. Sizing down.")
            return self.cash
        return risk_amount

    def on_fill(self, timestamp: datetime, quantity: float, fill_price: float, direction: str, trade_value_quote: float):
        """
        Updates the sub-portfolio's state after a trade is filled.
        This method is called by the master PortfolioManager.
        """
        # 1. Log the trade details.
        trade_value = quantity * fill_price
        commission = abs(trade_value_quote - trade_value)
        self.trade_log.append({
            'timestamp': timestamp, 'asset': self.traded_asset, 'direction': direction,
            'quantity': quantity, 'price': fill_price, 'commission': commission
        })

        # 2. Update market value based on the fill price.
        self.update_market_value(fill_price)

        # 3. Adjust cash and positions.
        if direction.upper() == 'BUY': self.cash -= trade_value_quote
        elif direction.upper() == 'SELL': self.cash += trade_value_quote
        self.positions[self.traded_asset] = self.positions.get(self.traded_asset, 0) + quantity if direction.upper() == 'BUY' else self.positions.get(self.traded_asset, 0) - quantity
        if self.positions.get(self.traded_asset, 0) <= 1e-9: del self.positions[self.traded_asset]

        # 4. Recalculate equity and record it.
        self.equity = self.get_current_equity()
        self.equity_curve.append((timestamp, self.equity))
        log.info(f"  [{self.strategy_name}] Sub-Portfolio Updated. New Equity: ${self.equity:,.2f}, New Cash: ${self.cash:,.2f}")