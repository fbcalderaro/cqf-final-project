# trading_system/engine/portfolio_manager.py

import time
from trading_system.utils.common import log

class PortfolioManager:
    """
    Manages the overall portfolio state, including cash, positions, and equity.
    This is a crucial component for risk management and performance tracking.
    """
    def __init__(self, system_config: dict):
        """
        Initializes the PortfolioManager.

        Args:
            system_config (dict): Global system configuration.
        """
        self.initial_cash = system_config.get('initial_cash', 100000.0)
        self.risk_per_trade_pct = system_config.get('risk_per_trade_pct', 0.01) # Risk 1% of equity per trade
        
        self.cash = self.initial_cash
        self.positions = {}  # { 'BTC-USDT': quantity, ... }
        self.market_values = {} # { 'BTC-USDT': price, ... }
        
        log.info("Portfolio Manager initialized.")
        log.info(f"  Initial Cash: ${self.initial_cash:,.2f}")
        log.info(f"  Risk per Trade: {self.risk_per_trade_pct * 100:.2f}%")

    def get_total_equity(self) -> float:
        """Calculates the real-time total equity of the portfolio."""
        holdings_value = 0.0
        for asset, quantity in self.positions.items():
            price = self.market_values.get(asset, 0)
            holdings_value += quantity * price
        return self.cash + holdings_value

    def update_market_value(self, asset: str, price: float):
        """Updates the last known market price for an asset."""
        self.market_values[asset] = price

    def calculate_position_size(self, asset: str) -> float:
        """
        Calculates the amount of cash to allocate to a new position based on risk settings.
        
        Returns:
            float: The dollar amount to risk on the trade.
        """
        total_equity = self.get_total_equity()
        risk_amount = total_equity * self.risk_per_trade_pct
        
        log.info(f"  Portfolio Equity: ${total_equity:,.2f}")
        log.info(f"  Calculated Risk Amount for new trade: ${risk_amount:,.2f}")
        
        # Simple check to ensure we don't risk more than our available cash
        if risk_amount > self.cash:
            log.warning("Risk amount exceeds available cash. Reducing trade size.")
            return self.cash
            
        return risk_amount

    def on_fill(self, asset: str, quantity: float, fill_price: float, direction: str):
        """
        Updates the portfolio state after an order has been filled.

        Args:
            asset (str): The asset that was traded.
            quantity (float): The quantity that was filled.
            fill_price (float): The price at which the order was filled.
            direction (str): 'BUY' or 'SELL'.
        """
        trade_cost = quantity * fill_price
        
        if direction.upper() == 'BUY':
            self.cash -= trade_cost
            self.positions[asset] = self.positions.get(asset, 0) + quantity
            log.info(f"  FILLED BUY: {quantity:.6f} {asset} @ ${fill_price:,.2f}. Cost: ${trade_cost:,.2f}")
        elif direction.upper() == 'SELL':
            self.cash += trade_cost
            self.positions[asset] = self.positions.get(asset, 0) - quantity
            if self.positions[asset] <= 1e-9: # Handle float precision issues
                del self.positions[asset]
            log.info(f"  FILLED SELL: {quantity:.6f} {asset} @ ${fill_price:,.2f}. Proceeds: ${trade_cost:,.2f}")
        
        log.info(f"  New Cash: ${self.cash:,.2f}")
        log.info(f"  New Positions: {self.positions}")
        log.info(f"  New Equity: ${self.get_total_equity():,.2f}")
