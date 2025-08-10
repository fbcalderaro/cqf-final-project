# trading_system/engine/portfolio_manager.py

import time
import pandas as pd
from trading_system.utils.common import log

class PortfolioManager:
    """
    Manages the overall portfolio state, including cash, positions, and equity.
    """
    def __init__(self, system_config: dict, initial_cash: float = None, initial_positions: dict = None, traded_asset: str = None):
        """
        Initializes the PortfolioManager.

        Args:
            system_config (dict): Global system configuration.
            initial_cash (float, optional): The starting cash. If None, uses value from config.
            initial_positions (dict, optional): The starting positions. If None, starts with none.
            traded_asset (str, optional): The specific asset this instance is trading, for focused logging.
        """
        self.traded_asset = traded_asset # Store the asset this PM is responsible for
        self.risk_per_trade_pct = system_config.get('risk_per_trade_pct', 0.01)
        self.commission_pct = system_config.get('commission_pct', 0.0)
        
        if initial_cash is not None:
            self.initial_cash = initial_cash
            self.cash = initial_cash
        else:
            self.initial_cash = system_config.get('initial_cash', 100000.0)
            self.cash = self.initial_cash

        # Filter initial positions to only include the traded asset for this instance
        self.positions = {}
        if initial_positions and self.traded_asset and self.traded_asset in initial_positions:
            self.positions[self.traded_asset] = initial_positions[self.traded_asset]
        
        self.market_values = {}
        self.equity_curve = []
        self.total_turnover = 0.0
        self.total_commissions = 0.0
        self.trade_log = []
        
        log.info("Portfolio Manager initialized.")
        log.info(f"  Risk per Trade: {self.risk_per_trade_pct * 100:.2f}%")
        log.info(f"  Commission per Trade: {self.commission_pct * 100:.3f}%")

        log.info(f"  Available Cash (USDT): ${self.cash:,.2f}")
        if self.traded_asset:
            base_asset = self.traded_asset.split('-')[0]
            position_qty = self.positions.get(self.traded_asset, 0.0)
            log.info(f"  Initial Position ({base_asset}): {position_qty}")
        else:
            # For portfolio-level backtesting, log all initial positions
            log.info(f"  Initial Positions: {initial_positions if initial_positions is not None else {}}")

    @property
    def equity_curve_df(self) -> pd.DataFrame:
        """Returns the equity curve as a pandas DataFrame."""
        if not self.equity_curve:
            # Return an empty DataFrame with the correct structure if the curve is empty
            return pd.DataFrame(columns=['Equity']).set_index(pd.to_datetime([]))
        
        df = pd.DataFrame(self.equity_curve, columns=['Timestamp', 'Equity'])
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        return df.set_index('Timestamp')


    def get_total_equity(self) -> float:
        holdings_value = 0.0
        for asset, quantity in self.positions.items():
            price = self.market_values.get(asset, 0)
            holdings_value += quantity * price
        return self.cash + holdings_value

    def update_market_value(self, asset: str, price: float):
        self.market_values[asset] = price

    def calculate_position_size(self, asset: str) -> float:
        total_equity = self.get_total_equity()
        risk_amount = total_equity * self.risk_per_trade_pct
        if risk_amount > self.cash:
            log.warning("Risk amount exceeds available cash. Reducing trade size to available cash.")
            return self.cash
        return risk_amount

    def on_fill(self, timestamp, asset: str, quantity: float, fill_price: float, direction: str):
        trade_value = quantity * fill_price
        commission = trade_value * self.commission_pct
        
        self.total_turnover += trade_value
        self.total_commissions += commission
        self.trade_log.append({
            'timestamp': timestamp, 'asset': asset, 'direction': direction,
            'quantity': quantity, 'price': fill_price, 'commission': commission
        })
        
        if direction.upper() == 'BUY':
            self.cash -= (trade_value + commission)
            self.positions[asset] = self.positions.get(asset, 0) + quantity
            log.info(f"  FILLED BUY: {quantity:.6f} {asset} @ ${fill_price:,.2f}. Cost: ${trade_value:,.2f}, Comm: ${commission:,.2f}")
        elif direction.upper() == 'SELL':
            self.cash += (trade_value - commission)
            self.positions[asset] = self.positions.get(asset, 0) - quantity
            if self.positions[asset] <= 1e-9:
                del self.positions[asset]
            log.info(f"  FILLED SELL: {quantity:.6f} {asset} @ ${fill_price:,.2f}. Proceeds: ${trade_value:,.2f}, Comm: ${commission:,.2f}")
        
        # --- NEW: Focused logging for the specific traded asset ---
        base_asset = asset.split('-')[0]
        position_qty = self.positions.get(asset, 0.0)
        
        log.info(f"  New Cash: ${self.cash:,.2f}")
        log.info(f"  New Position ({base_asset}): {position_qty}")
        
        # Update equity curve after every fill and log the new total equity
        self.update_market_value(asset, fill_price)
        self.equity_curve.append((timestamp, self.get_total_equity()))
        log.info(f"  New Equity: ${self.get_total_equity():,.2f}")

    def reconcile(self, actual_positions: dict, allocated_actual_cash: float):
        log.info("Reconciling portfolio state against allocated capital...")

        # --- Position Reconciliation (for the specific asset this manager handles) ---
        if self.traded_asset:
            internal_qty = self.positions.get(self.traded_asset, 0.0)
            actual_qty = actual_positions.get(self.traded_asset, 0.0)
            
            # Compare with a small tolerance for float precision
            if abs(internal_qty - actual_qty) > 1e-9:
                log.warning(f"Position discrepancy for {self.traded_asset}! Internal: {internal_qty:.8f}, Actual: {actual_qty:.8f}. Forcing update.")
                if actual_qty > 0:
                    self.positions[self.traded_asset] = actual_qty
                elif self.traded_asset in self.positions:
                    # If actual is zero, remove from internal positions
                    del self.positions[self.traded_asset]
        else:
            # Fallback for portfolio-level backtesting where one PM manages all assets
            log.warning(f"Position discrepancy found! Internal: {self.positions}, Actual: {actual_positions}. Forcing update.")
            self.positions = actual_positions

        # --- Cash Reconciliation (against the allocated portion of cash) ---
        if abs(self.cash - allocated_actual_cash) > 0.01:
            log.warning(f"Cash discrepancy found! Internal: ${self.cash:,.2f}, Actual Allocated: ${allocated_actual_cash:,.2f}. Forcing update.")
            self.cash = allocated_actual_cash
        log.info("Reconciliation complete.")
