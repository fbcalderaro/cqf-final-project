# trading_system/engine/portfolio_manager.py

import pandas as pd
from datetime import datetime, timezone
from trading_system.utils.common import log
from trading_system.engine.strategy_portfolio import StrategyPortfolio

class PortfolioManager:
    """
    Manages the master portfolio, representing the entire broker account.

    This class acts as the central source of truth for the account's overall
    state (cash, positions, equity). It also creates, registers, and delegates
    trade fills to the individual `StrategyPortfolio` instances it manages.
    """
    def __init__(self, system_config: dict, initial_cash: float, initial_positions: dict = None, relevant_assets: set = None):
        """
        Initializes the master PortfolioManager.

        Args:
            system_config (dict): Global system configuration.
            initial_cash (float): The total starting cash from the broker.
            initial_positions (dict, optional): Starting positions from the broker.
                                                Format: {'BTC-USDT': 0.1, ...}.
            relevant_assets (set, optional): A set of assets that the engine will manage.
        """
        # --- State for the entire account ---
        self.system_config = system_config
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.positions = initial_positions if initial_positions is not None else {}
        self.relevant_assets = relevant_assets
        self.market_values = {}
        self.equity_curve = [(datetime.now(timezone.utc), initial_cash)]
        self.trade_log = []
        self.total_commissions = 0.0

        # --- Management of sub-portfolios ---
        self.strategy_portfolios = {}

        # --- Config values ---
        self.trading_mode = system_config.get('trading_mode', 'paper')
        
        log.info("Master Portfolio Manager initialized.")
        log.info(f"  Total Initial Cash (USDT): ${self.cash:,.2f}")

        # --- Log initial positions in a cleaner, more relevant way ---
        if relevant_assets:
            # Show only positions for assets managed by the running strategies
            managed_positions = {asset: qty for asset, qty in self.positions.items() if asset in relevant_assets}
            log.info(f"  Initial Positions (Managed Assets): {managed_positions if managed_positions else 'None'}")
            
            # Check if there are other, unmanaged positions in the account without listing them all
            unmanaged_position_exists = any(asset not in relevant_assets for asset in self.positions)
            if unmanaged_position_exists:
                log.info("  Note: Other unmanaged positions exist in the account.")
        else:
            # Fallback to old behavior if no relevant assets are provided (e.g., for backtesting)
            log.info(f"  Total Initial Positions: {self.positions}")

    def register_strategy(self, strategy_name: str, config: dict, initial_equity: float):
        """
        Creates and registers a virtual sub-portfolio for a new strategy.

        Args:
            strategy_name (str): The unique name of the strategy.
            config (dict): The specific configuration dictionary for this strategy.
            initial_equity (float): The amount of cash allocated to this strategy.
        """
        if strategy_name in self.strategy_portfolios:
            log.warning(f"Strategy '{strategy_name}' is already registered.")
            return
        
        # Use strategy-specific risk if defined, otherwise fall back to system default
        risk_pct = config.get('params', {}).get('risk_per_trade_pct', self.system_config.get('risk_per_trade_pct', 0.01))

        self.strategy_portfolios[strategy_name] = StrategyPortfolio(
            strategy_name=strategy_name,
            initial_equity=initial_equity,
            risk_per_trade_pct=risk_pct,
            traded_asset=config['asset']
        )

    def get_strategy_portfolio(self, strategy_name: str) -> StrategyPortfolio:
        """Retrieves a registered strategy's sub-portfolio."""
        return self.strategy_portfolios.get(strategy_name)

    @property
    def equity_curve_df(self) -> pd.DataFrame:
        """Returns the equity curve as a pandas DataFrame."""
        if not self.equity_curve:
            return pd.DataFrame(columns=['Equity']).set_index(pd.to_datetime([]))
        
        df = pd.DataFrame(self.equity_curve, columns=['Timestamp', 'Equity'])
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        return df.set_index('Timestamp')

    def get_total_equity(self) -> float:
        """Calculates the current total equity of the entire account."""
        holdings_value = 0.0
        for asset, quantity in self.positions.items():
            price = self.market_values.get(asset, 0)
            holdings_value += quantity * price
        return self.cash + holdings_value

    def update_market_values(self, asset_prices: dict):
        """Updates market values for all assets and recalculates equity for all sub-portfolios."""
        for asset, price in asset_prices.items():
            self.market_values[asset] = price
            # Also update any strategy portfolio that trades this asset
            for sp in self.strategy_portfolios.values():
                if sp.traded_asset == asset:
                    sp.update_market_value(price)

    def on_fill(self, strategy_name: str, timestamp: datetime, asset: str, quantity: float, fill_price: float, direction: str, trade_value_quote: float):
        """
        Processes a trade fill event from the execution handler.

        This is a critical method that updates state in a specific order:
        1. Update the master portfolio (the source of truth for the real account).
        2. Delegate the fill to the responsible strategy's sub-portfolio.
        3. Update market values and the master equity curve.
        """
        # --- 1. Update the Master Portfolio ---
        trade_value = quantity * fill_price
        commission = abs(trade_value_quote - trade_value)
        self.total_commissions += commission

        if direction.upper() == 'BUY':
            self.cash -= trade_value_quote
            self.positions[asset] = self.positions.get(asset, 0) + quantity
            log_cost_label = "Total Cost"
        elif direction.upper() == 'SELL':
            self.cash += trade_value_quote
            self.positions[asset] = self.positions.get(asset, 0) - quantity
            if self.positions[asset] <= 1e-9:
                del self.positions[asset]
            log_cost_label = "Total Proceeds"
        
        log.info(f"  [Master Portfolio] FILLED {direction.upper()}: {quantity:.6f} {asset} @ ${fill_price:,.2f} by '{strategy_name}'.")
        log.info(f"    -> {log_cost_label}: ${trade_value_quote:,.2f}, Implied Comm: ${commission:,.2f}")
        log.info(f"    -> New Master Cash: ${self.cash:,.2f}")
        
        # --- Log only the positions of managed assets for clarity ---
        if self.relevant_assets:
            managed_positions = {a: q for a, q in self.positions.items() if a in self.relevant_assets}
            log.info(f"    -> New Master Positions (Managed): {managed_positions if managed_positions else 'None'}")
        else:
            log.info(f"    -> New Master Positions: {self.positions}")

        # --- 2. Delegate the fill to the correct Strategy Sub-Portfolio ---
        strategy_portfolio = self.get_strategy_portfolio(strategy_name)
        if strategy_portfolio:
            strategy_portfolio.on_fill(timestamp, quantity, fill_price, direction, trade_value_quote)
        else:
            log.warning(f"Could not find sub-portfolio for strategy '{strategy_name}' to log fill.")

        # --- 3. Update master equity curve post-trade ---
        self.update_market_values({asset: fill_price})
        total_equity = self.get_total_equity()
        self.equity_curve.append((timestamp, total_equity))
        log.info(f"    -> New Master Equity: ${total_equity:,.2f}")

    def reconcile(self, actual_cash: float, actual_positions: dict):
        """
        Reconciles the master portfolio state against the actual broker state.

        This acts as a safety mechanism. If the internal state has drifted from
        the broker's reality (e.g., due to manual trades or API issues), this
        method forces the internal state to match the broker's.
        """
        log.info("--- Reconciling Master Portfolio State ---")
        discrepancy_found = False

        # --- Position Reconciliation ---
        if self.positions != actual_positions:
            discrepancy_found = True
            log.warning("Position discrepancy found! Forcing update.")
            log.warning(f"  Internal: {self.positions}")
            log.warning(f"  Actual:   {actual_positions}")
            self.positions = actual_positions

        # --- Cash Reconciliation ---
        if abs(self.cash - actual_cash) > 0.01: # Use a small tolerance for float precision
            discrepancy_found = True
            log.warning("Cash discrepancy found! Forcing update.")
            log.warning(f"  Internal: ${self.cash:,.2f}")
            log.warning(f"  Actual:   ${actual_cash:,.2f}")
            self.cash = actual_cash

        if not discrepancy_found:
            log.info("✅ Master portfolio is in sync with the broker.")
        else:
            log.info("Reconciliation forced an update to the master portfolio state.")
        
        log.info("--- Reconciliation Complete ---")
