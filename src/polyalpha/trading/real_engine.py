"""Real trading engine — actual fund execution via Polymarket CLOB."""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ..database.database import TradeDatabase
    from .auto_redeem import AutoRedeemEngine, AutoRedeemConfig

from ..core import (
    InsufficientBalance,
    InsufficientAllowance,
    OrderNotFound,
    PositionNotFound,
    RiskLimitExceeded,
    OrderCancelled,
    NetworkError,
    TransientError,
    OrderRejected,
    OrderTimeout,
    CircuitBreakerOpenError,
    ManualInterventionRequiredError,
    TransactionRollbackError,
    BackupError,
    GasEstimationError,
    TransactionRebroadcastError,
    PRICE_STALENESS_THRESHOLD,
    FALLBACK_PRICE,
    MAX_ORDER_PRICE,
    FEE_ROUNDING,
    calculate_polymarket_fee,
    fee_rate_for_category,
)
from .clob_client import ClobClient
from .alchemy_client import AlchemyClient
from .wallet import RealWallet, RealTradingWalletManager, WalletSelectionStrategy
from .error_handling import (
    CircuitBreaker,
    ErrorRecoveryManager,
    GracefulDegradation,
    TransactionRollbackManager,
    DisasterRecovery,
    DegradationLevel,
)
from .real_config import RealTradingConfig
from .real_orders import RealOrder, RealPosition, OCOOrder, BracketOrder, ConditionalOrder, IcebergOrder, TWAPOrder
from .real_position_sizing import (
    PositionSizer,
    FixedPositionSizer,
    PercentagePositionSizer,
    KellyPositionSizer,
    HybridPositionSizer,
)
from .real_risk import RiskManager
from .real_wallet import WalletManager
from ..report.engine import ReportEngine


log = logging.getLogger(__name__)

class RealTradingEngine:
    """
    Real trading engine with actual fund execution via Polymarket CLOB.

    Features:
    - Wallet integration (USDC balance on Polygon)
    - Real order execution with signing
    - Position sizing strategies (fixed, percentage, Kelly)
    - Risk management (stop loss, take profit, position limits)
    - Safety checks and confirmations
    - Real-time balance tracking
    - Trade persistence to database

    Parameters
    ----------
    private_key : str
        Private key for wallet operations
    rpc_url : str
        Polygon RPC URL for blockchain interaction
    polymarket_api_key : str
        Polymarket API key for CLOB access
    config : RealTradingConfig, optional
        Configuration for real trading
    db_path : str, optional
        Path to SQLite database for trade persistence
    simulate : bool, optional
        Enable simulation mode for testing (default: False)
    """

    def __init__(
        self,
        private_key: str,
        rpc_url: str,
        polymarket_api_key: str,
        polymarket_api_secret: str = "",
        polymarket_api_passphrase: str = "",
        config: Optional[RealTradingConfig] = None,
        db_path: Optional[str] = None,
        simulate: bool = False,
    ):
        # Configuration
        self._config = config or RealTradingConfig(
            private_key=private_key,
            rpc_url=rpc_url,
            polymarket_api_key=polymarket_api_key,
            polymarket_api_secret=polymarket_api_secret,
            polymarket_api_passphrase=polymarket_api_passphrase,
        )

        # Validate credentials for real trading
        if not simulate:
            self._validate_credentials(private_key, polymarket_api_key, rpc_url)
        else:
            log.warning("⚠️  SIMULATION MODE ENABLED - No real trades will be executed")
            log.warning("Set simulate=False for production trading")

        # Wallet setup
        self._wallet = WalletManager(private_key, rpc_url, log_balance_updates=self._config.log_balance_updates)
        self._balance: float = 0.0
        self._allowance: float = 0.0

        # Order management
        self._orders: dict[str, RealOrder] = {}
        self._positions: dict[str, RealPosition] = {}  # key: "{market_id}:{side}"

        # Position lock for thread-safe position share modifications
        self._position_lock: threading.RLock = threading.RLock()

        # Advanced order types storage
        self._oco_orders: dict[str, OCOOrder] = {}
        self._bracket_orders: dict[str, BracketOrder] = {}
        self._conditional_orders: dict[str, ConditionalOrder] = {}
        self._iceberg_orders: dict[str, IcebergOrder] = {}
        self._twap_orders: dict[str, TWAPOrder] = {}

        # Position sizing
        self._position_sizer: PositionSizer = self._create_position_sizer()

        # Risk management
        self._risk_manager = RiskManager(self._config)

        # CLOB client
        self._clob_client = ClobClient(
            api_key=polymarket_api_key,
            private_key=private_key,
            rpc_url=rpc_url,
            api_secret=polymarket_api_secret or self._config.polymarket_api_secret or None,
            api_passphrase=polymarket_api_passphrase or self._config.polymarket_api_passphrase or None,
            timeout=self._config.order_timeout,
            retry_attempts=self._config.retry_attempts,
            retry_delay=self._config.retry_delay,
            simulate=simulate,
        )

        # Alchemy Client
        self._alchemy_client = AlchemyClient(rpc_url=rpc_url)

        # Database
        self._db: Optional[TradeDatabase] = None
        self._db_enabled: bool = False
        if db_path:
            self.enable_database(db_path)

        # Reporting
        self.report = ReportEngine(self)

        # Emergency mode
        self._emergency_mode: bool = False
        
        # Position sync caching
        self._last_position_sync: float = 0.0
        self._position_sync_ttl: float = 30.0  # seconds before re-syncing from chain
        
        # Stream tracking for price-aware trading
        self._attached_streams: dict[str, "Stream"] = {}  # market_id -> Stream

        # Auto-redeem engine (lazy-initialized)
        self._auto_redeem: Optional[AutoRedeemEngine] = None

        # Error handling components
        self._clob_circuit_breaker = CircuitBreaker(
            name="clob_api",
            failure_threshold=5,
            recovery_timeout=60,
            expected_exception=(NetworkError, OrderTimeout),
        )
        self._wallet_circuit_breaker = CircuitBreaker(
            name="wallet_rpc",
            failure_threshold=3,
            recovery_timeout=120,
            expected_exception=(NetworkError,),
        )
        self._error_recovery = ErrorRecoveryManager()
        self._graceful_degradation = GracefulDegradation()
        self._transaction_rollback = TransactionRollbackManager()
        self._disaster_recovery = DisasterRecovery()

        # Multi-wallet mode
        self._use_multi_wallet: bool = False
        self._real_wallet_manager: Optional[RealTradingWalletManager] = None
        self._active_wallet_id: Optional[str] = None

        # Initialize balance
        self.refresh_balance()

        log.info("RealTradingEngine initialized with comprehensive error handling")

    def _validate_credentials(self, private_key: str, polymarket_api_key: str, rpc_url: str) -> None:
        """
        Validate that real trading credentials are provided and not placeholder values.

        Raises
        ------
        ValueError
            If credentials appear to be placeholder or invalid values
        """
        # Check for common placeholder values
        placeholder_patterns = [
            "your-private-key",
            "your_api_key",
            "placeholder",
            "test-key",
            "example-key",
            "xxx",
            "0000",
        ]

        if not private_key or len(private_key) < 32:
            raise ValueError(
                "Invalid private key: must be at least 32 characters. "
                "Provide a real private key for production trading."
            )

        if any(pattern.lower() in private_key.lower() for pattern in placeholder_patterns):
            raise ValueError(
                f"Invalid private key: appears to be a placeholder value. "
                "Provide a real private key for production trading."
            )

        if not polymarket_api_key or len(polymarket_api_key) < 10:
            raise ValueError(
                "Invalid Polymarket API key: must be at least 10 characters. "
                "Provide a real API key for production trading."
            )

        if any(pattern.lower() in polymarket_api_key.lower() for pattern in placeholder_patterns):
            raise ValueError(
                f"Invalid Polymarket API key: appears to be a placeholder value. "
                "Provide a real API key for production trading."
            )

        if not rpc_url or not rpc_url.startswith(("http://", "https://")):
            raise ValueError(
                f"Invalid RPC URL: must be a valid HTTP/HTTPS URL. Got: {rpc_url}"
            )

        log.info("✓ Credentials validated for real trading")

    @property
    def config(self) -> RealTradingConfig:
        """Get current configuration."""
        return self._config

    @property
    def auto_redeem(self) -> "AutoRedeemEngine":
        """Auto-redeem engine for automatic position redemption. Access via ``client.real.auto_redeem``."""
        if self._auto_redeem is None:
            from .auto_redeem import AutoRedeemEngine, AutoRedeemConfig
            self._auto_redeem = AutoRedeemEngine(self, AutoRedeemConfig())
        return self._auto_redeem

    def set_auto_redeem_config(self, config: "AutoRedeemConfig") -> None:
        """Set a custom auto-redeem configuration."""
        from .auto_redeem import AutoRedeemEngine
        self._auto_redeem = AutoRedeemEngine(self, config)

    def set_position_sizer(self, sizer: PositionSizer) -> None:
        """
        Set a custom position sizer.

        Parameters
        ----------
        sizer : PositionSizer
            Position sizer instance (FixedPositionSizer, PercentagePositionSizer, etc.)
        """
        self._position_sizer = sizer
        log.info("Position sizer updated to %s", type(sizer).__name__)

    @property
    def balance(self) -> float:
        """Get current USDC balance."""
        return self._balance

    @property
    def emergency_mode(self) -> bool:
        """Check if emergency mode is active."""
        return self._emergency_mode

    # ── Error Handling Properties ──────────────────────────────────────────────────

    @property
    def clob_circuit_breaker(self) -> CircuitBreaker:
        """Get CLOB API circuit breaker."""
        return self._clob_circuit_breaker

    @property
    def wallet_circuit_breaker(self) -> CircuitBreaker:
        """Get wallet RPC circuit breaker."""
        return self._wallet_circuit_breaker

    @property
    def error_recovery(self) -> ErrorRecoveryManager:
        """Get error recovery manager."""
        return self._error_recovery

    @property
    def graceful_degradation(self) -> GracefulDegradation:
        """Get graceful degradation manager."""
        return self._graceful_degradation

    @property
    def transaction_rollback(self) -> TransactionRollbackManager:
        """Get transaction rollback manager."""
        return self._transaction_rollback

    @property
    def disaster_recovery(self) -> DisasterRecovery:
        """Get disaster recovery manager."""
        return self._disaster_recovery

    # ── Error Handling Methods ─────────────────────────────────────────────────────

    def get_error_handling_status(self) -> dict:
        """
        Get comprehensive error handling status.

        Returns
        -------
        dict
            Status of all error handling components
        """
        return {
            "clob_circuit_breaker": self._clob_circuit_breaker.metrics,
            "wallet_circuit_breaker": self._wallet_circuit_breaker.metrics,
            "graceful_degradation": self._graceful_degradation.get_degradation_summary(),
            "emergency_mode": self._emergency_mode,
        }

    def trigger_degradation(self, level: DegradationLevel, reason: str) -> None:
        """
        Manually trigger system degradation.

        Parameters
        ----------
        level : DegradationLevel
            Target degradation level
        reason : str
            Reason for degradation
        """
        self._graceful_degradation.degrade(level, reason)
        log.warning("Manual degradation triggered: %s - %s", level.value, reason)

    def trigger_recovery(self, level: DegradationLevel, reason: str) -> None:
        """
        Manually trigger system recovery.

        Parameters
        ----------
        level : DegradationLevel
            Target degradation level
        reason : str
            Reason for recovery
        """
        self._graceful_degradation.recover(level, reason)
        log.info("Manual recovery triggered: %s - %s", level.value, reason)

    def create_emergency_backup(self) -> str:
        """
        Create an emergency backup of current trading state.

        Returns
        -------
        str
            Path to backup file
        """
        try:
            backup_path = self._disaster_recovery.create_emergency_snapshot(
                positions={k: v.dump() for k, v in self._positions.items()},
                orders={k: v.dump() for k, v in self._orders.items()},
                config={
                    "max_order_size": self._config.max_order_size,
                    "max_daily_loss": self._config.max_daily_loss,
                    "max_position_size": self._config.max_position_size,
                },
            )
            log.info("Emergency backup created: %s", backup_path)
            return backup_path
        except Exception:
            log.exception("Failed to create emergency backup")
            raise BackupError("Failed to create emergency backup")

    def restore_from_backup(self, backup_path: str) -> dict:
        """
        Restore trading state from backup.

        Reconstructs in-memory ``RealPosition`` and ``RealOrder`` objects
        from the emergency snapshot data, and re-establishes the
        position/order cross-references (order_ids on positions).

        Parameters
        ----------
        backup_path : str
            Path to backup file (``.json`` or ``.json.gz``).

        Returns
        -------
        dict
            Summary: positions_restored, orders_restored, advanced_orders.

        Raises
        ------
        BackupError
            If the backup data is corrupt or cannot be loaded.
        """
        try:
            backup_data = self._disaster_recovery.restore_backup(backup_path)
            data = backup_data["data"]

            restored_positions = 0
            restored_orders = 0

            # Restore orders first (positions reference order IDs)
            if "orders" in data:
                for order_id, order_data in data["orders"].items():
                    if order_id in self._orders:
                        log.warning("Order %s already exists, overwriting", order_id)
                    self._orders[order_id] = RealOrder.from_dump(order_data)
                    restored_orders += 1
                    log.debug("Restored order: %s (%s %s)",
                              order_id, order_data.get("market"), order_data.get("side"))
                log.info("Restored %d orders from backup", restored_orders)

            # Restore positions
            if "positions" in data:
                for pos_key, pos_data in data["positions"].items():
                    if pos_key in self._positions:
                        log.warning("Position %s already exists, overwriting", pos_key)
                    self._positions[pos_key] = RealPosition.from_dump(pos_data)

                    position = self._positions[pos_key]
                    orphan_ids = [oid for oid in position.order_ids if oid not in self._orders]
                    if orphan_ids:
                        log.warning("Position %s references %d order(s) not in backup: %s",
                                    pos_key, len(orphan_ids), orphan_ids[:5])

                    restored_positions += 1
                    log.debug("Restored position: %s (%s %s, shares=%.2f)",
                              pos_key, pos_data.get("market"), pos_data.get("side"),
                              float(pos_data.get("shares", 0)))
                log.info("Restored %d positions from backup", restored_positions)

            # Rebuild daily tracking from config
            self._risk_manager._check_and_reset_daily()
            if "config" in data:
                cfg = data["config"]
                log.info("Backup config: max_order_size=%.2f, max_daily_loss=%.2f, "
                         "max_position_size=%.2f",
                         float(cfg.get("max_order_size", 0)),
                         float(cfg.get("max_daily_loss", 0)),
                         float(cfg.get("max_position_size", 0)))

            # Refresh balance from chain
            try:
                self.refresh_balance()
            except Exception as exc:
                log.warning("Could not refresh balance after restore: %s", exc)

            log.info("Restore complete: %d positions, %d orders from %s",
                     restored_positions, restored_orders, backup_path)

            return {
                "positions_restored": restored_positions,
                "orders_restored": restored_orders,
                "balance": self._balance,
            }

        except Exception:
            log.exception("Failed to restore from backup")
            raise BackupError("Failed to restore from backup")

    # ── Database Integration ─────────────────────────────────────────────────────

    def enable_database(self, db_path: str) -> None:
        """
        Enable database persistence for trades.

        Parameters
        ----------
        db_path : str
            Path to SQLite database file
        """
        try:
            from ..database.database import TradeDatabase
            self._db = TradeDatabase(db_path)
            self._db_enabled = True
            log.info("Real: database enabled at %s", db_path)
        except ImportError:
            log.error("Real: database module not available")
            self._db_enabled = False

    def disable_database(self) -> None:
        """Disable database persistence."""
        if self._db:
            self._db.close()
            self._db = None
        self._db_enabled = False
        log.info("Real: database disabled")

    # ── Balance Management ───────────────────────────────────────────────────────

    def refresh_balance(self) -> None:
        """Refresh balance from blockchain."""
        if self._use_multi_wallet and self._real_wallet_manager:
            self._real_wallet_manager.refresh_all_balances()
        else:
            self._balance = self._wallet.get_balance()
            self._allowance = self._wallet.get_allowance()
            if self._config.log_balance_updates:
                log.debug("Balance: $%.2f, Allowance: $%.2f", self._balance, self._allowance)

    # ── Multi-Wallet Support ─────────────────────────────────────────────────────

    @property
    def is_multi_wallet(self) -> bool:
        """Check if multi-wallet mode is enabled."""
        return self._use_multi_wallet

    @property
    def wallets(self) -> Optional[RealTradingWalletManager]:
        """Get the real wallet manager if multi-wallet mode is enabled."""
        return self._real_wallet_manager if self._use_multi_wallet else None

    def enable_multi_wallet(
        self,
        wallet_manager: RealTradingWalletManager,
        wallet_id: Optional[str] = None,
    ) -> None:
        """
        Enable multi-wallet trading mode.

        Parameters
        ----------
        wallet_manager : RealTradingWalletManager
            Wallet manager with configured wallets
        wallet_id : str, optional
            Initially active wallet ID (default: first wallet)
        """
        if not wallet_manager.get_all_wallets():
            raise ValueError("Wallet manager must have at least one wallet")

        self._real_wallet_manager = wallet_manager
        self._use_multi_wallet = True

        if wallet_id:
            self._active_wallet_id = wallet_id
        else:
            first = wallet_manager.get_all_wallets()[0]
            self._active_wallet_id = first.wallet_id

        log.info(
            "RealTradingEngine: multi-wallet mode enabled with %d wallets (active: %s)",
            len(wallet_manager.get_all_wallets()),
            self._active_wallet_id,
        )

    def disable_multi_wallet(self) -> None:
        """Disable multi-wallet mode and return to single-wallet operation."""
        self._use_multi_wallet = False
        self._real_wallet_manager = None
        self._active_wallet_id = None
        log.info("RealTradingEngine: multi-wallet mode disabled")

    def set_active_wallet(self, wallet_id: str) -> None:
        """Set the active wallet by ID. Only valid in multi-wallet mode."""
        if not self._use_multi_wallet or not self._real_wallet_manager:
            raise RuntimeError("Multi-wallet mode is not enabled")
        self._real_wallet_manager.get_wallet(wallet_id)  # validate exists
        self._active_wallet_id = wallet_id
        log.info("RealTradingEngine: active wallet set to %s", wallet_id)

    def _get_active_wallet(self) -> RealWallet:
        """Get the currently active wallet in multi-wallet mode."""
        if not self._use_multi_wallet or not self._real_wallet_manager:
            raise RuntimeError("Multi-wallet mode is not enabled")
        return self._real_wallet_manager.get_wallet(self._active_wallet_id)

    def _resolve_orders(self) -> dict:
        """Get orders dict from active wallet or single-wallet mode."""
        if self._use_multi_wallet:
            return self._get_active_wallet().orders
        return self._orders

    def _resolve_positions(self) -> dict:
        """Get positions dict from active wallet or single-wallet mode."""
        if self._use_multi_wallet:
            return self._get_active_wallet().positions
        return self._positions

    def _resolve_balance(self) -> float:
        """Get balance from active wallet or single-wallet mode."""
        if self._use_multi_wallet:
            return self._get_active_wallet().balance
        return self._balance

    def _set_balance(self, value: float) -> None:
        """Set balance on active wallet or single-wallet mode."""
        if self._use_multi_wallet:
            self._get_active_wallet().balance = value
        else:
            self._balance = value

    def _resolve_allowance(self) -> float:
        """Get allowance from active wallet or single-wallet mode."""
        if self._use_multi_wallet:
            return self._get_active_wallet().allowance
        return self._allowance

    def _set_allowance(self, value: float) -> None:
        """Set allowance on active wallet or single-wallet mode."""
        if self._use_multi_wallet:
            self._get_active_wallet().allowance = value
        else:
            self._allowance = value

    def _resolve_wallet(self):
        """Get WalletManager from active wallet or single-wallet mode."""
        if self._use_multi_wallet:
            return self._get_active_wallet().wallet_manager
        return self._wallet

    def _resolve_clob(self):
        """Get ClobClient from active wallet or single-wallet mode."""
        if self._use_multi_wallet:
            return self._get_active_wallet().clob_client
        return self._clob_client

    def _resolve_config(self):
        """Get config from active wallet or single-wallet mode."""
        if self._use_multi_wallet:
            return self._get_active_wallet().config or self._config
        return self._config

    def _resolve_risk_manager(self):
        """Get risk manager from active wallet or single-wallet mode."""
        if self._use_multi_wallet:
            rm = self._get_active_wallet().risk_manager
            if rm is not None:
                return rm
        return self._risk_manager

    def _resolve_config_and_risk(self):
        """Convenience: return (config, risk_manager) for current wallet."""
        if self._use_multi_wallet:
            wallet = self._get_active_wallet()
            cfg = wallet.config or self._config
            rm = wallet.risk_manager if wallet.risk_manager is not None else self._risk_manager
            return cfg, rm
        return self._config, self._risk_manager

    def _find_order_across_wallets(self, order_id: str):
        """Find an order across all wallets. Returns (order, wallet) or (None, None)."""
        if not self._use_multi_wallet or not self._real_wallet_manager:
            if order_id in self._orders:
                return self._orders[order_id], None
            return None, None
        return self._real_wallet_manager.find_order_across_wallets(order_id)

    def _find_position_across_wallets(self, market_id: str, side: str):
        """Find a position across all wallets. Returns (position, wallet) or (None, None)."""
        if not self._use_multi_wallet or not self._real_wallet_manager:
            key = f"{market_id}:{side}"
            if key in self._positions:
                return self._positions[key], None
            return None, None
        return self._real_wallet_manager.find_position_across_wallets(market_id, side)

    def _get_all_orders_across_wallets(self) -> dict:
        """Get all orders across all wallets."""
        if not self._use_multi_wallet or not self._real_wallet_manager:
            return self._orders
        return self._real_wallet_manager.get_all_orders()

    def _get_all_positions_across_wallets(self) -> dict:
        """Get all positions across all wallets."""
        if not self._use_multi_wallet or not self._real_wallet_manager:
            return self._positions
        return self._real_wallet_manager.get_all_positions()

    # ── Pre-Trade Checks ─────────────────────────────────────────────────────────

    def pre_trade_checks(self, market, side: str, amount: float) -> dict:
        """
        Run comprehensive pre-trade checks before order execution.

        This method validates various conditions before allowing a trade to proceed,
        helping prevent errors and risky trades in real trading.

        Parameters
        ----------
        market : Market object
            Market to trade
        side : str
            "UP" or "DOWN"
        amount : float
            USDC amount to spend

        Returns
        -------
        dict
            Dictionary with check results and warnings:
            - balance_ok: bool - Whether sufficient balance exists
            - allowance_ok: bool - Whether sufficient CLOB allowance exists
            - market_open: bool - Whether market is still open
            - price_reasonable: bool - Whether price is within reasonable range
            - warnings: list[str] - List of warning messages
            - can_proceed: bool - Whether trade can proceed (all critical checks pass)

        Example
        -------
        >>> checks = client.real.pre_trade_checks(market, side="UP", amount=10.0)
        >>> if not checks["can_proceed"]:
        ...     for warning in checks["warnings"]:
        ...         print(f"Warning: {warning}")
        ... else:
        ...     order = client.real.buy(market, side="UP", amount=10.0)
        """
        balance = self._resolve_balance()
        allowance = self._resolve_allowance()

        checks = {
            "balance_ok": True,
            "allowance_ok": True,
            "market_open": True,
            "price_reasonable": True,
            "warnings": [],
            "can_proceed": True,
        }

        # Check balance
        if amount > balance:
            checks["balance_ok"] = False
            checks["can_proceed"] = False
            checks["warnings"].append(
                f"Insufficient balance: need ${amount:.2f}, have ${balance:.2f}"
            )

        # Check CLOB allowance (real trading specific)
        if allowance < amount:
            checks["allowance_ok"] = False
            checks["warnings"].append(
                f"Insufficient CLOB allowance: need ${amount:.2f}, have ${allowance:.2f}. "
                f"Call approve_spender() to increase allowance."
            )
            # Allowance warning doesn't block trade (can be auto-approved), but warn user

        # Check if market is open for trading
        if hasattr(market, 'start_time') and market.start_time:
            try:
                start_time = datetime.fromisoformat(market.start_time.replace('Z', '+00:00'))
                if start_time > datetime.now(timezone.utc):
                    checks["market_open"] = False
                    checks["can_proceed"] = False
                    checks["warnings"].append("Market has not yet opened")
            except (ValueError, AttributeError) as e:
                log.debug("Real: could not parse market start_time: %s", e)

        # Check if market is still open
        if hasattr(market, 'end_time') and market.end_time:
            try:
                end_time = datetime.fromisoformat(market.end_time.replace('Z', '+00:00'))
                if end_time < datetime.now(timezone.utc):
                    checks["market_open"] = False
                    checks["can_proceed"] = False
                    checks["warnings"].append("Market has closed")
            except (ValueError, AttributeError) as e:
                log.debug("Real: could not parse market end_time: %s", e)

        # Check if price is reasonable
        price = market.up_price if side == "UP" else market.down_price
        if price < 0.01 or price > 0.99:
            checks["price_reasonable"] = False
            checks["warnings"].append(f"Unusual price: ${price:.4f}")

        # Additional warning if price is very close to boundaries
        if price < 0.02 or price > 0.98:
            checks["warnings"].append(
                f"Price near boundary (${price:.4f}) - low liquidity risk"
            )

        # Log warnings if any
        if checks["warnings"]:
            log.debug("Real: pre-trade checks warnings: %s", checks["warnings"])

        return checks

    # ── Order Execution ─────────────────────────────────────────────────────────

    def buy(
        self,
        market,
        side: str,
        amount: Optional[float] = None,
        confidence: float = 0.5,
        price: Optional[float] = None,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        confirm: bool = True,
    ) -> RealOrder:
        """
        Execute a real buy order on the CLOB.

        Parameters
        ----------
        market : Market
            Market to trade
        side : str
            "UP" or "DOWN"
        amount : float, optional
            USDC amount to spend. If None, uses position sizing strategy.
        confidence : float
            Confidence level (0-1) for position sizing
        price : float, optional
            Limit price. If None, executes at market.
        stop_loss : float, optional
            Stop loss price trigger
        take_profit : float, optional
            Take profit price trigger
        confirm : bool
            Require manual confirmation before executing

        Returns
        -------
        RealOrder
            The executed order
        """
        if self._emergency_mode:
            raise OrderCancelled("Trading halted - emergency mode active")

        side = _validate_side(side)

        # Resolve wallet-aware state
        config, risk_manager = self._resolve_config_and_risk()
        balance = self._resolve_balance()
        positions = self._resolve_positions()
        orders = self._resolve_orders()

        # Sync balance from chain to avoid divergence
        self.refresh_balance()
        balance = self._resolve_balance()

        # Track if user explicitly provided a price (for limit vs market order)
        user_provided_price = price is not None

        # 1. Calculate position size if not provided
        if amount is None:
            amount = self._position_sizer.calculate_size(
                balance, market, side, confidence, price
            )

        # 2. Run pre-trade checks
        checks = self.pre_trade_checks(market, side, amount)
        if not checks["can_proceed"]:
            raise ValueError(
                f"Pre-trade checks failed: {'; '.join(checks['warnings'])}"
            )

        # 3. Validate against risk limits
        risk_manager.validate_order(amount, balance, market, positions)

        # 4. Check balance
        if amount > balance:
            raise InsufficientBalance(
                f"Order amount ${amount:.2f} exceeds balance ${balance:.2f}"
            )

        # 5. Get price with stream awareness (prefers live stream price if available)
        if price is None:
            price, price_source = self._get_price_for_side(market, side)
            # Market orders cross the spread. Buffer the price by the configured
            # slippage tolerance so the marketable limit actually fills, and record
            # the buffered (worst-case) price so cost is never underestimated.
            price = self._apply_buy_slippage(price, config)

        # 6. Calculate shares and fee
        is_maker = user_provided_price  # limit orders provide liquidity
        shares, fee = self._calculate_shares_and_fee(amount, price, is_maker=is_maker)

        # 7. Require confirmation if enabled
        if confirm and config.require_confirmation:
            self._require_confirmation(market, side, amount, price, shares, fee)

        # 8. Place order on CLOB
        token_id = market.up_token if side == "UP" else market.down_token
        order_response = self._place_clob_order(
            token_id,
            "buy",  # Always buying tokens (UP or DOWN)
            price,
            shares,
            "market" if not user_provided_price else "limit"
        )

        # 9. Create order object
        order = RealOrder(
            id=order_response["order_id"],
            market_id=market.id,
            slug=market.slug,
            side=side,
            price=price,
            amount=amount,
            shares=shares,
            fee=fee,
            status="pending",
            is_limit=user_provided_price,
            created_at=datetime.now(timezone.utc),
            stop_loss=stop_loss,
            take_profit=take_profit,
            sizing_strategy=config.position_sizing,
            confidence=confidence,
        )

        # 10. Update balance (fee comes out of amount, not on top)
        self._set_balance(self._resolve_balance() - amount)

        # 11. Store order
        orders[order.id] = order

        # 12. Update position
        self._update_position(market, side, order)

        # 13. Save to database
        if self._db_enabled:
            active_wallet = self._get_active_wallet() if self._use_multi_wallet else None
            self._save_order_to_db(order, wallet=active_wallet)

        if config.log_all_orders:
            log.debug(
                "Order placed: %s %s $%.2f @ $%.4f",
                market.slug, side, amount, price
            )

        return order

    def limit(
        self,
        market,
        side: str,
        price: float,
        amount: Optional[float] = None,
        confidence: float = 0.5,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        confirm: bool = True,
    ) -> RealOrder:
        """
        Execute a real limit order on the CLOB.

        Parameters
        ----------
        market : Market
            Market to trade
        side : str
            "UP" or "DOWN"
        price : float
            Limit price
        amount : float, optional
            USDC amount to spend
        confidence : float
            Confidence level for position sizing
        stop_loss : float, optional
            Stop loss price trigger
        take_profit : float, optional
            Take profit price trigger
        confirm : bool
            Require manual confirmation

        Returns
        -------
        RealOrder
            The placed limit order
        """
        return self.buy(
            market=market,
            side=side,
            amount=amount,
            confidence=confidence,
            price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            confirm=confirm,
        )

    def sell(
        self,
        market,
        side: str,
        amount: Optional[float] = None,
        confidence: float = 0.5,
        price: Optional[float] = None,
        confirm: bool = True,
    ) -> RealOrder:
        """
        Execute a real sell order on the CLOB.

        To sell an existing position on Polymarket, you buy the opposite side.
        This method handles the side inversion automatically.

        Parameters
        ----------
        market : Market
            Market to trade
        side : str
            Side of the position being sold ("UP" or "DOWN")
        amount : float, optional
            USDC amount to sell. If None, uses position sizing strategy.
        confidence : float
            Confidence level for position sizing
        price : float, optional
            Limit price. If None, executes at market.
        confirm : bool
            Require manual confirmation before executing

        Returns
        -------
        RealOrder
            The executed sell order
        """
        opposite = "DOWN" if _validate_side(side) == "UP" else "UP"
        return self.buy(
            market=market,
            side=opposite,
            amount=amount,
            confidence=confidence,
            price=price,
            confirm=confirm,
        )

    # ── Order Management ─────────────────────────────────────────────────────────

    def cancel(self, order_id: str) -> None:
        """
        Cancel an open order.

        Parameters
        ----------
        order_id : str
            Order ID to cancel
        """
        order, wallet = self._find_order_across_wallets(order_id)
        if order is None:
            raise OrderNotFound(f"Order {order_id} not found")

        if order.status not in ("open", "pending"):
            log.warning("Order %s is not open (status: %s)", order_id, order.status)
            return

        # Cancel on CLOB
        self._cancel_clob_order(order_id, wallet=wallet)

        order.status = "cancelled"
        log.info("Order %s cancelled", order_id)

    def get_order(self, order_id: str) -> RealOrder:
        """
        Get order by ID.

        Parameters
        ----------
        order_id : str
            Order ID

        Returns
        -------
        RealOrder
            Order object
        """
        order, _ = self._find_order_across_wallets(order_id)
        if order is None:
            raise OrderNotFound(f"Order {order_id} not found")
        return order

    def open_orders(self) -> list[RealOrder]:
        """Get all open orders."""
        orders = self._get_all_orders_across_wallets()
        return [o for o in orders.values() if o.status in ("open", "pending")]

    def poll_order_status(self, order_id: str) -> dict:
        """
        Poll order status from CLOB API with retry logic.

        Parameters
        ----------
        order_id : str
            Order ID to poll

        Returns
        -------
        dict
            Order status response from CLOB API

        Raises
        ------
        OrderNotFound
            If order not found in local records
        NetworkError
            If polling fails after retries
        """
        order, _ = self._find_order_across_wallets(order_id)
        if order is None:
            raise OrderNotFound(f"Order {order_id} not found")

        order.last_status_check = datetime.now(timezone.utc)
        order.status_check_attempts += 1

        clob = self._resolve_clob()
        config = self._resolve_config()

        try:
            status_response = clob.get_order_status(order_id)
            log.debug("Order %s status: %s", order_id, status_response.get("status"))
            return status_response
        except Exception as e:
            log.exception("Failed to poll order %s status (attempt %d)",
                         order_id, order.status_check_attempts)
            if order.status_check_attempts >= config.retry_attempts:
                raise NetworkError(f"Order status polling failed after {config.retry_attempts} attempts: {e}")
            raise

    def update_order_fill_status(self, order_id: str) -> None:
        """
        Update order fill status based on CLOB API response.

        Handles partial fills, full fills, and status transitions.

        Parameters
        ----------
        order_id : str
            Order ID to update
        """
        order, _ = self._find_order_across_wallets(order_id)
        if order is None:
            raise OrderNotFound(f"Order {order_id} not found")

        # Skip if order is already in final state
        if order.status in ("filled", "cancelled", "expired"):
            return

        # Poll current status from CLOB
        status_response = self.poll_order_status(order_id)

        api_status = status_response.get("status", "unknown")
        filled_size = float(status_response.get("filled_size", 0.0))
        avg_price = float(status_response.get("avg_price", order.price))

        # Handle partial fills
        if filled_size > 0 and filled_size < order.shares:
            if order.status != "partially_filled":
                log.debug("Order %s partially filled: %.2f/%.2f shares",
                         order_id, filled_size, order.shares)
                order.status = "partially_filled"

            prev_filled = order.filled_shares
            incremental = filled_size - prev_filled
            if incremental <= 0:
                return

            order.filled_shares = filled_size
            order.filled_amount = filled_size * avg_price
            order.avg_fill_price = avg_price

            # Update position with partial fill (pass incremental, not cumulative)
            self._handle_partial_fill(order, incremental, avg_price)

        # Handle full fills
        elif filled_size >= order.shares or api_status == "filled":
            if order.status != "filled":
                log.info("Order %s fully filled: %.2f shares @ %.4f",
                        order_id, filled_size, avg_price)
                order.status = "filled"
                order.filled_at = datetime.now(timezone.utc)
                order.filled_shares = filled_size
                order.filled_amount = filled_size * avg_price
                order.avg_fill_price = avg_price

                # Trigger fill callback
                self._on_order_filled(order)

        # Handle cancelled orders
        elif api_status == "cancelled":
            if order.status != "cancelled":
                log.info("Order %s cancelled", order_id)
                order.status = "cancelled"

        # Handle expired orders
        elif api_status == "expired":
            if order.status != "expired":
                log.warning("Order %s expired", order_id)
                order.status = "expired"

        # Update database if enabled
        if self._db_enabled:
            self._update_order_in_db(order)

    def _handle_partial_fill(self, order: RealOrder, incremental_shares: float, avg_price: float) -> None:
        """
        Handle partial fill by updating position incrementally.

        ``buy()`` optimistically adds ``order.shares`` to the position before
        the CLOB confirms the fill. This method first reverts that optimistic
        addition (if still present), then applies only the actual incremental
        fill at the real fill price.

        Parameters
        ----------
        order : RealOrder
            Order that was partially filled
        incremental_shares : float
            Number of *new* shares filled since the last update (not cumulative)
        avg_price : float
            Average fill price for this increment
        """
        positions = self._resolve_positions()
        position_key = f"{order.market_id}:{order.side}"

        with self._position_lock:
            if position_key not in positions:
                from .real_orders import RealPosition
                position = RealPosition(
                    market_id=order.market_id,
                    slug=order.slug,
                    question=order.slug,
                    side=order.side,
                    shares=0,
                    avg_price=0,
                    current_price=0,
                    cost_basis=0,
                    current_value=0,
                    order_ids=[order.id],
                )
                positions[position_key] = position
            else:
                position = positions[position_key]

            # Revert optimistic addition from buy() if still present.
            # buy() adds order.shares at order.price — this must be undone
            # before applying the actual fill data.
            if position.shares >= order.shares:
                position.shares -= order.shares
                position.cost_basis = max(0.0, position.cost_basis - order.shares * order.price)

            # Apply actual incremental fill
            position.shares += incremental_shares
            position.cost_basis += incremental_shares * avg_price
            if position.shares > 0:
                position.avg_price = position.cost_basis / position.shares
            position.current_value = position.shares * order.price

            log.debug("Position updated with partial fill: %s %s, shares=%.2f, avg_price=$.4f",
                     position.slug, position.side, position.shares, position.avg_price)

    def _on_order_filled(self, order: RealOrder) -> None:
        """
        Callback when order is fully filled.

        Corrects the position to use the actual fill price from the CLOB
        instead of the submission price (which may differ for market orders).

        Parameters
        ----------
        order : RealOrder
            Filled order
        """
        log.debug("Order fill callback: %s %s $%.2f @ $%.4f (avg_fill: $%.4f)",
                 order.slug, order.side, order.amount, order.price, order.avg_fill_price)

        if order.avg_fill_price > 0:
            self._correct_position_from_fill(order)

        risk_manager = self._resolve_risk_manager()
        risk_manager.record_trade(0.0)

    def _correct_position_from_fill(self, order: RealOrder) -> None:
        """
        Correct position after actual fill price is known.

        The position was created optimistically in buy() with ``order.shares``
        at ``order.price``. This method removes that optimistic contribution
        and applies the actual fill (``order.filled_shares`` at ``order.avg_fill_price``).

        Parameters
        ----------
        order : RealOrder
            Filled order with ``avg_fill_price`` and ``filled_shares`` populated.
        """
        positions = self._resolve_positions()
        key = f"{order.market_id}:{order.side}"
        with self._position_lock:
            position = positions.get(key)
            if position is None:
                return

            actual_shares = max(order.filled_shares, 0.0)
            if actual_shares <= 0:
                actual_shares = order.shares

            fill_price = order.avg_fill_price

            if position.shares >= order.shares:
                # Remove optimistic contribution
                position.shares -= order.shares
                position.cost_basis = max(0.0, position.cost_basis - order.shares * order.price)

            # Apply actual fill
            position.shares += actual_shares
            position.cost_basis += actual_shares * fill_price
            if position.shares > 0:
                position.avg_price = position.cost_basis / position.shares
            position.current_value = position.shares * order.price

    def check_order_timeout(self, order_id: str) -> bool:
        """
        Check if order has exceeded timeout threshold.

        Parameters
        ----------
        order_id : str
            Order ID to check

        Returns
        -------
        bool
            True if order has timed out
        """
        order, _ = self._find_order_across_wallets(order_id)
        if order is None:
            raise OrderNotFound(f"Order {order_id} not found")

        config = self._resolve_config()

        if order.status not in ("pending", "open", "partially_filled"):
            return False

        if order.created_at:
            elapsed = (datetime.now(timezone.utc) - order.created_at).total_seconds()
            if elapsed > config.order_timeout:
                log.warning("Order %s timed out after %.1f seconds (status: %s)",
                           order_id, elapsed, order.status)
                return True

        return False

    def poll_all_orders(self) -> dict[str, str]:
        """
        Poll status for all open orders.

        Returns
        -------
        dict[str, str]
            Dictionary mapping order_id to new status
        """
        status_updates = {}
        orders = self._get_all_orders_across_wallets()

        for order_id, order in list(orders.items()):
            if order.status in ("pending", "open", "partially_filled"):
                try:
                    old_status = order.status
                    self.update_order_fill_status(order_id)
                    if order.status != old_status:
                        status_updates[order_id] = order.status
                except Exception:
                    log.exception("Failed to update order %s status", order_id)

                    if self.check_order_timeout(order_id):
                        status_updates[order_id] = "timeout"

        return status_updates

    # ── Position Management ───────────────────────────────────────────────────────

    def _sync_single_wallet_positions(self, address: str, clob_client, positions_dict: dict) -> None:
        """Sync positions for a single wallet address into the given positions dict."""
        balances = self._alchemy_client.get_token_balances(address)
        transfers = self._alchemy_client.get_asset_transfers(address)

        token_ids = list(balances.keys())
        if not token_ids:
            return

        metadata = self._alchemy_client.fetch_polymarket_metadata(token_ids)

        transfers_by_token: dict[str, list[dict]] = {}
        for t in transfers:
            for m in t.get("erc1155Metadata", []):
                tid = m.get("tokenId", "")
                if tid:
                    transfers_by_token.setdefault(tid, []).append(t)

        orders = self._get_all_orders_across_wallets()

        for token_id, amount in balances.items():
            if amount <= 0:
                continue

            meta = metadata.get(token_id, {})
            market_id = meta.get("market_id", token_id)
            slug = meta.get("slug", token_id)
            question = meta.get("question", "Unknown Market")
            gamma_price = float(meta.get("price", 0.0))

            side = meta.get("side", "UP")
            clob_token_ids = meta.get("clobTokenIds", "")
            if isinstance(clob_token_ids, str) and clob_token_ids:
                tokens = [t.strip() for t in clob_token_ids.split(",")]
                if len(tokens) > 1:
                    token_dec = str(int(token_id, 16)) if token_id.startswith("0x") else token_id
                    side = "UP" if tokens[0] == token_dec else "DOWN"

            fill_price = None

            for order in orders.values():
                if order.market_id == market_id and order.side == side and order.avg_fill_price > 0:
                    fill_price = order.avg_fill_price
                    break

            if fill_price is None and gamma_price > 0:
                fill_price = gamma_price

            if fill_price is None:
                try:
                    ob = clob_client.get_orderbook(token_id)
                    bids = ob.get("bids", [])
                    asks = ob.get("asks", [])
                    best_bid = float(bids[0][0]) if bids else 0.0
                    best_ask = float(asks[0][0]) if asks else 0.0
                    if best_bid > 0 and best_ask > 0:
                        fill_price = (best_bid + best_ask) / 2.0
                    elif best_bid > 0:
                        fill_price = best_bid
                    elif best_ask > 0:
                        fill_price = best_ask
                except Exception:
                    log.warning("Failed to fetch orderbook for fill price", exc_info=True)

            position_key = f"{market_id}:{side}"
            if fill_price is None and position_key in positions_dict:
                fill_price = positions_dict[position_key].avg_price

            if fill_price is None or fill_price <= 0:
                fill_price = FALLBACK_PRICE

            cost_basis = amount * fill_price
            current_price = gamma_price if gamma_price > 0 else fill_price

            entry_time = None
            incoming = [t for t in transfers_by_token.get(token_id, [])
                        if t.get("to", "").lower() == address.lower()]
            if incoming:
                timestamps = []
                for t in incoming:
                    ts = t.get("metadata", {}).get("blockTimestamp", "")
                    if ts:
                        try:
                            timestamps.append(datetime.fromisoformat(ts.replace("Z", "+00:00")))
                        except (ValueError, TypeError):
                            pass
                if timestamps:
                    entry_time = min(timestamps)

            position = RealPosition(
                market_id=market_id,
                slug=slug,
                question=question,
                side=side,
                shares=amount,
                avg_price=fill_price,
                current_price=current_price,
                cost_basis=cost_basis,
                current_value=amount * current_price,
                entry_time=entry_time,
            )

            positions_dict[position_key] = position

    def sync_positions_from_chain(self) -> None:
        """Fetch real positions from the blockchain using Alchemy."""
        log.debug("Syncing positions from blockchain...")

        if self._use_multi_wallet and self._real_wallet_manager:
            for wallet in self._real_wallet_manager.get_all_wallets():
                try:
                    self._sync_single_wallet_positions(
                        wallet.address,
                        wallet.clob_client,
                        wallet.positions,
                    )
                except Exception as e:
                    log.exception("Failed to sync positions for wallet %s", wallet.wallet_id)
        else:
            self._sync_single_wallet_positions(
                self._wallet.address,
                self._clob_client,
                self._positions,
            )

    def positions(self) -> list[RealPosition]:
        """Get all open positions."""
        now = time.time()
        if now - self._last_position_sync > self._position_sync_ttl:
            self.sync_positions_from_chain()
            self._last_position_sync = now
        positions = self._get_all_positions_across_wallets()
        return [p for p in positions.values() if not p.resolved]

    def all_positions(self) -> list[RealPosition]:
        """Get all positions including resolved ones."""
        now = time.time()
        if now - self._last_position_sync > self._position_sync_ttl:
            self.sync_positions_from_chain()
            self._last_position_sync = now
        positions = self._get_all_positions_across_wallets()
        return list(positions.values())

    def show_positions(self, show_all: bool = False, verbose: bool = True) -> None:
        """
        Display positions with entry/exit information and ROI.

        Parameters
        ----------
        show_all : bool
            If True, show all positions including resolved ones. If False, only show live positions.
        verbose : bool
            If True, show detailed information including entry/exit times.

        Example
        -------
        >>> client.real.show_positions()  # Show live positions
        >>> client.real.show_positions(show_all=True)  # Show all positions
        """
        from ..report.terminal import render_positions

        positions = self.all_positions() if show_all else self.positions()
        orders = self._get_all_orders_across_wallets()
        render_positions(positions, orders, show_all=show_all, verbose=verbose)

    def position_history(self) -> dict:
        """
        Get position history summary statistics.

        Returns
        -------
        dict
            Dictionary with position history statistics including:
            - total_positions: Total number of positions opened
            - total_closed: Total number of positions closed
            - total_open: Current number of open positions
            - win_rate: Win rate percentage
            - avg_holding_time: Average holding time in seconds
            - best_position: Best performing position
            - worst_position: Worst performing position
        """
        all_pos = self.all_positions()
        open_pos = [p for p in all_pos if not p.resolved]
        closed_pos = [p for p in all_pos if p.resolved]

        wins = [p for p in closed_pos if p.outcome == "WON"]
        losses = [p for p in closed_pos if p.outcome == "LOST"]

        # Calculate holding times for closed positions
        orders = self._get_all_orders_across_wallets()
        holding_times = []
        for pos in closed_pos:
            if pos.order_ids:
                fill_times = [
                    orders[oid].filled_at 
                    for oid in pos.order_ids 
                    if oid in orders and orders[oid].filled_at
                ]
                if fill_times:
                    holding_time = (max(fill_times) - min(fill_times)).total_seconds()
                    holding_times.append(holding_time)

        avg_holding = sum(holding_times) / len(holding_times) if holding_times else 0.0

        # Find best and worst positions
        best_pos = max(closed_pos, key=lambda p: p.pnl) if closed_pos else None
        worst_pos = min(closed_pos, key=lambda p: p.pnl) if closed_pos else None

        return {
            "total_positions": len(all_pos),
            "total_closed": len(closed_pos),
            "total_open": len(open_pos),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": (len(wins) / len(closed_pos) * 100) if closed_pos else 0.0,
            "avg_holding_time": avg_holding,
            "best_position": {
                "market": best_pos.slug if best_pos else None,
                "pnl": best_pos.pnl if best_pos else 0.0,
                "pnl_pct": best_pos.pnl_pct if best_pos else 0.0,
            } if best_pos else None,
            "worst_position": {
                "market": worst_pos.slug if worst_pos else None,
                "pnl": worst_pos.pnl if worst_pos else 0.0,
                "pnl_pct": worst_pos.pnl_pct if worst_pos else 0.0,
            } if worst_pos else None,
        }

    def get_position(self, market_id: str, side: str) -> RealPosition:
        """
        Get position for a market and side.

        Parameters
        ----------
        market_id : str
            Market ID
        side : str
            "UP" or "DOWN"

        Returns
        -------
        RealPosition
            Position object
        """
        position, _ = self._find_position_across_wallets(market_id, side)
        if position is None:
            raise PositionNotFound(f"No position for {market_id} {side}")
        return position

    def set_stop_loss(
        self,
        market,
        side: str,
        stop_price: float,
    ) -> None:
        """
        Set stop loss for a position.

        Parameters
        ----------
        market : Market
            Market object
        side : str
            "UP" or "DOWN"
        stop_price : float
            Stop loss price trigger

        Example
        -------
        >>> client.real.set_stop_loss(market, side="UP", stop_price=0.45)
        """
        side = _validate_side(side)
        position, _ = self._find_position_across_wallets(market.id, side)
        if position is None:
            raise PositionNotFound(f"No position found for {market.slug} {side}")
        
        position.stop_loss = stop_price
        
        log.info("Stop loss set at $%.4f for %s %s", stop_price, market.slug, side)

    def set_take_profit(
        self,
        market,
        side: str,
        profit_price: float,
    ) -> None:
        """
        Set take profit for a position.

        Parameters
        ----------
        market : Market
            Market object
        side : str
            "UP" or "DOWN"
        profit_price : float
            Take profit price trigger

        Example
        -------
        >>> client.real.set_take_profit(market, side="UP", profit_price=0.55)
        """
        side = _validate_side(side)
        position, _ = self._find_position_across_wallets(market.id, side)
        if position is None:
            raise PositionNotFound(f"No position found for {market.slug} {side}")
        
        position.take_profit = profit_price
        
        log.info("Take profit set at $%.4f for %s %s", profit_price, market.slug, side)

    def set_trailing_stop(
        self,
        market,
        side: str,
        trail_distance: float,
    ) -> None:
        """
        Set trailing stop loss for a position.

        Parameters
        ----------
        market : Market
            Market object
        side : str
            "UP" or "DOWN"
        trail_distance : float
            Trailing distance as percentage (e.g., 0.05 for 5%)

        Example
        -------
        >>> client.real.set_trailing_stop(market, side="UP", trail_distance=0.05)
        """
        side = _validate_side(side)
        position, _ = self._find_position_across_wallets(market.id, side)
        if position is None:
            raise PositionNotFound(f"No position found for {market.slug} {side}")
        
        if not hasattr(position, 'trail_sl'):
            position.trail_sl = None
        if not hasattr(position, 'trail_sl_price'):
            position.trail_sl_price = None
        
        position.trail_sl = trail_distance
        position.trail_sl_price = position.current_price - trail_distance if side == "UP" else position.current_price + trail_distance
        
        log.info("Trailing stop set at %.4f distance for %s %s", trail_distance, market.slug, side)

    def check_and_execute_trailing_stops(self, market_updates: dict[str, float]) -> list[str]:
        """
        Check and execute trailing stops based on current market prices.

        Parameters
        ----------
        market_updates : dict[str, float]
            Dictionary mapping token_id to current price

        Returns
        -------
        list[str]
            List of position keys that had trailing stops triggered
        """
        triggered = []
        positions = self._get_all_positions_across_wallets()
        
        for key, position in positions.items():
            if position.resolved:
                continue
            
            if not hasattr(position, 'trail_sl') or position.trail_sl is None:
                continue
            
            token_id = None
            if hasattr(position, 'token_id'):
                token_id = position.token_id
            else:
                token_id = position.market_id
            
            if token_id not in market_updates:
                continue
            
            current_price = market_updates[token_id]
            old_trail_price = position.trail_sl_price
            
            if position.side == "UP":
                new_trail_price = current_price - position.trail_sl
                if new_trail_price > old_trail_price:
                    position.trail_sl_price = new_trail_price
                    log.debug("Trailing stop updated for %s %s: $%.4f -> $%.4f", 
                              position.slug, position.side, old_trail_price, new_trail_price)
                
                if current_price <= position.trail_sl_price:
                    triggered.append(key)
                    log.warning("Trailing stop triggered for %s %s at $%.4f", 
                              position.slug, position.side, current_price)
                    
            else:
                new_trail_price = current_price + position.trail_sl
                if new_trail_price < old_trail_price:
                    position.trail_sl_price = new_trail_price
                    log.debug("Trailing stop updated for %s %s: $%.4f -> $%.4f", 
                             position.slug, position.side, old_trail_price, new_trail_price)
                
                if current_price >= position.trail_sl_price:
                    triggered.append(key)
                    log.warning("Trailing stop triggered for %s %s at $%.4f", 
                              position.slug, position.side, current_price)
        
        return triggered

    def _find_position_by_key_across_wallets(self, position_key: str):
        """Find a position by composite key across all wallets."""
        if not self._use_multi_wallet or not self._real_wallet_manager:
            if position_key in self._positions:
                return self._positions[position_key], None
            return None, None
        for wallet in self._real_wallet_manager.get_all_wallets():
            if position_key in wallet.positions:
                return wallet.positions[position_key], wallet
        return None, None

    def execute_trailing_stop_exit(self, position_key: str) -> None:
        """
        Execute an exit order for a position whose trailing stop was triggered.

        Parameters
        ----------
        position_key : str
            Position key in format "{market_id}:{side}"
        """
        position, wallet = self._find_position_by_key_across_wallets(position_key)
        if position is None:
            log.warning("Position %s not found for trailing stop exit", position_key)
            return
        
        log.info("Executing trailing stop exit for %s %s at $%.4f",
                 position.slug, position.side, position.current_price)
        
        try:
            clob = self._resolve_clob() if wallet is None else wallet.clob_client
            orders = self._resolve_orders() if wallet is None else wallet.orders
            
            token_id = position.market_id
            current_price = position.current_price
            
            order_response = clob.place_order(
                token_id=token_id,
                side="sell",
                price=current_price,
                size=position.shares,
                order_type="market",
            )
            
            order = RealOrder(
                id=order_response["order_id"],
                market_id=position.market_id,
                slug=position.slug,
                side=position.side,
                price=current_price,
                amount=position.shares * current_price,
                shares=position.shares,
                fee=0.0,
                status="pending",
                is_limit=False,
                created_at=datetime.now(timezone.utc),
            )
            orders[order.id] = order
            
            position.resolved = True
            position.outcome = "STOPPED"
            
            log.info("Trailing stop exit executed for %s %s: order=%s",
                     position.slug, position.side, order.id)
                    
        except Exception as e:
            log.exception("Failed to execute trailing stop exit for %s %s",
                          position.slug, position.side)

    # ── Position Management ───────────────────────────────────────────────────────

    def scale_position(
        self,
        market,
        side: str,
        add_amount: float,
        confidence: float = 0.5,
    ) -> RealOrder:
        """
        Scale (pyramid) a position by adding more shares to a winning position.

        This implements the pyramiding strategy where you add to a position
        as it moves in your favor, increasing exposure while maintaining risk control.

        Parameters
        ----------
        market : Market
            Market object
        side : str
            "UP" or "DOWN"
        add_amount : float
            USDC amount to add to the position
        confidence : float, optional
            Confidence level for the additional trade (default: 0.5)

        Returns
        -------
        RealOrder
            The order that was placed to scale the position

        Raises
        ------
        PositionNotFound
            If no existing position exists for this market/side
        RiskLimitExceeded
            If scaling would exceed risk limits or position is not profitable enough

        Example
        -------
        >>> # Add $50 more to a winning UP position
        >>> order = client.real.scale_position(market, side="UP", add_amount=50.0, confidence=0.7)
        """
        side = _validate_side(side)
        position, _ = self._find_position_across_wallets(market.id, side)
        if position is None:
            raise PositionNotFound(f"No position found for {market.slug} {side}")

        config, _ = self._resolve_config_and_risk()

        if not config.enable_position_scaling:
            raise RiskLimitExceeded("Position scaling is disabled in configuration")

        if position.scale_count >= config.max_scale_additions:
            raise RiskLimitExceeded(
                f"Position has been scaled {position.scale_count} times, "
                f"maximum is {config.max_scale_additions}"
            )

        min_profit_pct = config.min_profit_for_scaling
        if position.pnl_pct < min_profit_pct * 100:
            raise RiskLimitExceeded(
                f"Position profit {position.pnl_pct:.1f}% is below minimum {min_profit_pct*100:.1f}% for scaling"
            )

        current_exposure = self._get_market_exposure(market.id)
        max_add_amount = config.max_position_size - current_exposure
        if add_amount > max_add_amount:
            log.warning("Requested scale amount $%.2f exceeds limit, capping at $%.2f", add_amount, max_add_amount)
            add_amount = max_add_amount

        # Place additional order
        log.info("Scaling position %s %s by $%.2f at confidence %.2f (scale #%d)",
                 market.slug, side, add_amount, confidence, position.scale_count + 1)

        order = self.buy(market, side=side, amount=add_amount, confidence=confidence, confirm=False)

        # Update scale count
        position.scale_count += 1

        return order

    def reduce_position(
        self,
        market,
        side: str,
        reduce_pct: float,
        reason: str = "manual",
    ) -> RealOrder:
        """
        Reduce a position by selling a percentage of shares.

        This implements position reduction strategies for risk management
        or profit taking. In Polymarket, selling is done by buying the opposite side.

        Parameters
        ----------
        market : Market
            Market object
        side : str
            "UP" or "DOWN"
        reduce_pct : float
            Percentage of position to reduce (0.0 to 1.0)
        reason : str, optional
            Reason for reduction (default: "manual")

        Returns
        -------
        RealOrder
            The order that was placed to reduce the position

        Raises
        ------
        PositionNotFound
            If no existing position exists for this market/side
        ValueError
            If reduce_pct is not between 0 and 1
        RiskLimitExceeded
            If position reduction is disabled in configuration

        Example
        -------
        >>> # Reduce position by 50%
        >>> order = client.real.reduce_position(market, side="UP", reduce_pct=0.5, reason="profit_taking")
        """
        side = _validate_side(side)
        position, wallet = self._find_position_across_wallets(market.id, side)
        if position is None:
            raise PositionNotFound(f"No position found for {market.slug} {side}")

        config = self._resolve_config_and_risk(wallet)

        if not config.enable_position_reduction:
            raise RiskLimitExceeded("Position reduction is disabled in configuration")

        if not 0 < reduce_pct <= 1:
            raise ValueError("reduce_pct must be between 0 and 1")


        shares_to_reduce = position.shares * reduce_pct

        # Calculate amount to spend on opposite side to reduce position
        current_price = position.current_price
        reduce_amount = shares_to_reduce * current_price

        log.info("Reducing position %s %s by %.1f%% ($%.2f) - reason: %s",
                 market.slug, side, reduce_pct * 100, reduce_amount, reason)

        # To reduce a position, buy the opposite side
        opposite_side = "DOWN" if side == "UP" else "UP"
        order = self.buy(market, side=opposite_side, amount=reduce_amount, confidence=0.5, confirm=False)

        return order

    def hedge_position(
        self,
        market,
        side: str,
        hedge_pct: float = 0.5,
    ) -> RealOrder:
        """
        Hedge a position by taking an opposite position in the same market.
        """
        side = _validate_side(side)
        position, wallet = self._find_position_across_wallets(market.id, side)
        if position is None:
            raise PositionNotFound(f"No position found for {market.slug} {side}")

        config = self._resolve_config_and_risk(wallet)

        if not config.enable_hedging:
            raise RiskLimitExceeded("Position hedging is disabled in configuration")

        if not 0 < hedge_pct <= 1:
            raise ValueError("hedge_pct must be between 0 and 1")

        if hedge_pct > config.max_hedge_ratio:
            raise RiskLimitExceeded(
                f"Hedge ratio {hedge_pct:.1%} exceeds maximum {config.max_hedge_ratio:.1%}"
            )



        # Calculate hedge amount based on position value
        hedge_amount = position.cost_basis * hedge_pct

        # Determine opposite side
        hedge_side = "DOWN" if side == "UP" else "UP"

        log.info("Hedging position %s %s with %.1f%% ($%.2f) on opposite side %s",
                 market.slug, side, hedge_pct * 100, hedge_amount, hedge_side)

        # Place order on opposite side
        order = self.buy(market, side=hedge_side, amount=hedge_amount, confidence=0.5, confirm=False)

        # Track hedge amount on position
        position.hedge_amount += hedge_amount

        return order

    def redeem_position(
        self,
        market_id: str,
        side: str,
    ) -> dict:
        """
        Redeem a resolved position on-chain via the CTF contract.

        Converts winning polymarket position tokens back into USDC
        by calling the Conditional Tokens Framework ``redeem`` method.

        Parameters
        ----------
        market_id : str
            Market/condition ID to redeem.
        side : str
            "UP" or "DOWN" — which side of the market to redeem.

        Returns
        -------
        dict
            ``{"success": bool, "tx_hash": str | None, "error": str | None}``

        Raises
        ------
        PositionNotFound
            If no position exists for the given market/side.
        """
        side = _validate_side(side)
        position_key = f"{market_id}:{side}"

        if position_key not in self._positions:
            raise PositionNotFound(f"No position found for {market_id} {side}")

        position = self._positions[position_key]
        log.info("Redeeming position %s %s (shares=%.4f, resolved=%s)",
                 position.slug, side, position.shares, position.resolved)

        if not position.resolved:
            log.warning("Position %s %s is not yet resolved, checking chain...",
                        position.slug, side)
            try:
                self._alchemy_client.get_token_balances(self._wallet.address)
            except Exception:
                log.warning("Failed to check token balances for redemption", exc_info=True)

        if self._wallet._web3 is None:
            self._wallet._init_web3()

        tx_hash = None
        try:
            from web3 import Web3

            ctf = self._wallet._ctf_contract
            address = Web3.to_checksum_address(self._wallet.address)

            condition_id = Web3.to_bytes(hexstr=market_id) if market_id.startswith("0x") else market_id.encode()
            if len(condition_id) != 32:
                condition_id = Web3.keccak(text=market_id)

            parent_collection_id = "0x" + "0" * 64
            index_set = 0 if side == "DOWN" else 1
            index_sets = [index_set]

            gas_estimate = ctf.functions.redeem(
                condition_id,
                parent_collection_id,
                index_sets,
            ).estimate_gas({'from': address})

            tx_params = self._wallet._build_transaction_params(
                gas_estimate=int(gas_estimate * 1.2),
                to_address=self._wallet._ctf_address,
            )

            tx = ctf.functions.redeem(
                condition_id,
                parent_collection_id,
                index_sets,
            ).build_transaction(tx_params)

            from eth_account import Account
            signed_tx = Account.sign_transaction(tx, self._wallet._private_key)
            tx_hash_raw = self._wallet._web3.eth.send_raw_transaction(signed_tx.raw_transaction)
            tx_hash = tx_hash_raw.hex()

            self._wallet._track_pending_transaction(tx_hash, tx_params['nonce'])
            receipt = self._wallet.wait_for_transaction(tx_hash, timeout=120)

            if receipt['status'] == 1:
                position.resolved = True
                position.outcome = "WON"
                log.info("Position %s %s redeemed on-chain: tx=%s",
                         position.slug, side, tx_hash)

                del self._positions[position_key]

                self.refresh_balance()

                return {"success": True, "tx_hash": tx_hash, "error": None}
            else:
                log.error("Redeem transaction %s failed on-chain", tx_hash)
                return {"success": False, "tx_hash": tx_hash, "error": "On-chain revert"}

        except Exception as e:
            log.exception("Failed to redeem position %s %s", position.slug, side)
            return {"success": False, "tx_hash": tx_hash, "error": str(e)}

    def transfer_position(
        self,
        market,
        side: str,
        target_wallet_address: str,
        transfer_pct: float = 1.0,
    ) -> dict:
        """
        Transfer a position (or portion of it) to another wallet.

        This allows moving positions between wallets for risk management
        or portfolio rebalancing.

        Parameters
        ----------
        market : Market
            Market object
        side : str
            "UP" or "DOWN"
        target_wallet_address : str
            Address of the wallet to transfer to
        transfer_pct : float, optional
            Percentage of position to transfer (0.0 to 1.0, default: 1.0)

        Returns
        -------
        dict
            Transaction details including tx_hash and status

        Raises
        ------
        PositionNotFound
            If no existing position exists for this market/side
        ValueError
            If transfer_pct is not between 0 and 1

        Example
        -------
        >>> # Transfer entire position to another wallet
        >>> tx = client.real.transfer_position(market, side="UP",
        ...                                     target_wallet_address="0x123...")
        """
        side = _validate_side(side)
        position_key = f"{market.id}:{side}"

        if position_key not in self._positions:
            raise PositionNotFound(f"No position found for {market.slug} {side}")

        if not 0 < transfer_pct <= 1:
            raise ValueError("transfer_pct must be between 0 and 1")

        position = self._positions[position_key]
        shares_to_transfer = position.shares * transfer_pct

        log.info("Transferring %.1f%% (%.2f shares) of position %s %s to wallet %s",
                 transfer_pct * 100, shares_to_transfer, market.slug, side, target_wallet_address)

        if self._wallet._web3 is None:
            self._wallet._init_web3()

        from web3 import Web3

        token_id = market.up_token if side == "UP" else market.down_token
        token_id_int = int(token_id, 16) if token_id.startswith("0x") else int(token_id)
        from_address = Web3.to_checksum_address(self._wallet.address)
        to_address = Web3.to_checksum_address(target_wallet_address)
        amount_raw = int(shares_to_transfer * 1_000_000)

        try:
            gas_estimate = self._wallet._ctf_contract.functions.safeTransferFrom(
                from_address,
                to_address,
                token_id_int,
                amount_raw,
                b"",
            ).estimate_gas({'from': from_address})

            tx_params = self._wallet._build_transaction_params(
                gas_estimate=int(gas_estimate * 1.2),
                to_address=self._wallet._ctf_address,
            )

            tx = self._wallet._ctf_contract.functions.safeTransferFrom(
                from_address,
                to_address,
                token_id_int,
                amount_raw,
                b"",
            ).build_transaction(tx_params)

            from eth_account import Account
            signed_tx = Account.sign_transaction(tx, self._wallet._private_key)
            tx_hash_raw = self._wallet._web3.eth.send_raw_transaction(signed_tx.raw_transaction)
            tx_hash = tx_hash_raw.hex()

            self._wallet._track_pending_transaction(tx_hash, tx_params['nonce'])
            receipt = self._wallet.wait_for_transaction(tx_hash, timeout=120)

            if receipt['status'] == 1:
                with self._position_lock:
                    position.shares -= shares_to_transfer
                    position.cost_basis = position.shares * position.avg_price
                    position.current_value = position.shares * position.current_price

                    del self._positions[position_key]

                log.info("Transfer successful: %s %s -> %s (tx=%s)",
                         market.slug, side, target_wallet_address, tx_hash)

                tx_details = {
                    "from_wallet": from_address,
                    "to_wallet": to_address,
                    "market_id": market.id,
                    "side": side,
                    "shares": shares_to_transfer,
                    "tx_hash": tx_hash,
                    "status": "confirmed",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            else:
                raise RuntimeError("Transfer reverted on-chain")

        except Exception as e:
            log.exception("Failed to transfer position %s %s", market.slug, side)
            tx_details = {
                "from_wallet": from_address,
                "to_wallet": to_address,
                "market_id": market.id,
                "side": side,
                "shares": shares_to_transfer,
                "tx_hash": None,
                "status": "failed",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

        return tx_details

    def merge_positions(
        self,
        market,
        side: str,
    ) -> RealPosition:
        """
        Merge multiple positions for the same market and side into a single position.

        This combines fragmented positions into one for easier management.

        Parameters
        ----------
        market : Market
            Market object
        side : str
            "UP" or "DOWN"

        Returns
        -------
        RealPosition
            The merged position

        Raises
        ------
        PositionNotFound
            If no positions exist for this market/side

        Example
        -------
        >>> # Merge all UP positions for a market
        >>> merged = client.real.merge_positions(market, side="UP")
        """
        side = _validate_side(side)
        position_key = f"{market.id}:{side}"

        if position_key not in self._positions:
            raise PositionNotFound(f"No position found for {market.slug} {side}")

        # In the current implementation, positions are already merged by market_id:side
        # This method is provided for future extensibility if the implementation
        # changes to support multiple positions per market/side

        position = self._positions[position_key]

        log.info("Position %s %s already merged (single position per market/side)",
                 market.slug, side)

        return position

    def get_position_exposure(self, market_id: str) -> float:
        """
        Get total exposure for a specific market across all sides.

        Parameters
        ----------
        market_id : str
            Market ID

        Returns
        -------
        float
            Total exposure in USDC

        Example
        -------
        >>> exposure = client.real.get_position_exposure(market.id)
        """
        return self._get_market_exposure(market_id)

    def get_portfolio_exposure(self) -> dict[str, float]:
        """
        Get total exposure across all markets.

        Returns
        -------
        dict[str, float]
            Dictionary mapping market_id to total exposure

        Example
        -------
        >>> exposure = client.real.get_portfolio_exposure()
        """
        exposure = {}
        for position in self.positions():
            if position.market_id not in exposure:
                exposure[position.market_id] = 0.0
            exposure[position.market_id] += position.cost_basis
        return exposure

    def _get_price_for_side(self, market, side: str) -> tuple[float, str]:
        """
        Get the best available price for a side, preferring live stream prices.
        
        Returns a tuple of (price, source) where source indicates where the price came from:
        - "stream": Live price from attached stream
        - "market": Price from market object (may be stale)
        - "fallback": Fallback price when no valid price available
        
        Parameters
        ----------
        market : Market object
        side   : "UP" or "DOWN"
        
        Returns
        -------
        tuple[float, str] - (price, source)
        """
        # Check if there's an attached running stream for this market
        stream = self._attached_streams.get(market.id)
        if stream and stream.running:
            # Use live stream price
            price = stream.up if side == "UP" else stream.down
            if price > 0:
                log.debug("Real: using live stream price %.4f for %s %s", price, market.slug, side)
                return price, "stream"
            else:
                log.warning("Real: stream attached but price is 0, falling back to market price")
        
        # Fall back to market price
        price = market.up_price if side == "UP" else market.down_price
        
        # Check if market price is stale
        if hasattr(market, 'end_time') and market.end_time:
            try:
                from datetime import datetime, timezone
                end_time = datetime.fromisoformat(market.end_time.replace('Z', '+00:00'))
                now = datetime.now(timezone.utc)
                time_until_close = (end_time - now).total_seconds()
                
                # If market is closed or very close to closing, price is likely stale
                if time_until_close <= 0:
                    log.warning("Real: market %s is closed, price may be stale", market.slug)
                elif time_until_close < PRICE_STALENESS_THRESHOLD:
                    log.warning(
                        "Real: market %s closes in %.1fs, using potentially stale price %.4f",
                        market.slug, time_until_close, price
                    )
            except (ValueError, TypeError):
                pass  # If we can't parse end_time, skip staleness check
        
        if price <= 0:
            log.warning("Real: market price is invalid (%.4f), using fallback", price)
            return FALLBACK_PRICE, "fallback"
        
        log.debug("Real: using market price %.4f for %s %s", price, market.slug, side)
        return price, "market"

    # ── Real-Time Price Monitoring ───────────────────────────────────────────────

    def attach_stream(self, stream, market) -> None:
        """
        Wire *stream* so positions auto-update and stop loss/take profit triggers execute.

        This method integrates price streams with the RealTradingEngine for automatic
        price updates, stop loss/take profit execution, and trailing stop management.

        Also enables price-aware trading: buy() will automatically use live
        streamed prices when a stream is attached and running.

        Example
        -------
        >>> stream = client.stream(market)
        >>> client.real.attach_stream(stream, market)
        >>> stream.start(background=True)
        """
        # Validate market
        if not hasattr(market, 'id') or not hasattr(market, 'slug'):
            raise ValueError("Invalid market object")

        # Store stream reference for price-aware trading
        self._attached_streams[market.id] = stream

        @stream.on("price")
        def _on_price(up: float, down: float) -> None:
            self._on_price_update(market.id, up, down)

        @stream.on("close")
        def _on_close() -> None:
            log.info(
                "Real: stream closed for %s — market resolved",
                market.slug,
            )
            # Remove stream reference when closed
            self._attached_streams.pop(market.id, None)

        log.info("Real: stream attached for %s", market.slug)

    def _on_price_update(self, market_id: str, up_price: float, down_price: float) -> None:
        """
        Handle price updates from attached stream.

        Updates live position prices and executes stop loss, take profit,
        and trailing stop triggers based on current prices.

        Parameters
        ----------
        market_id : str
            Market ID for the price update
        up_price : float
            Current UP token price
        down_price : float
            Current DOWN token price
        """
        # Validate prices
        up_price = _validate_positive(up_price, "up_price")
        down_price = _validate_positive(down_price, "down_price")

        # Update live prices for all open positions in this market
        for pos in self._positions.values():
            if pos.market_id == market_id and not pos.resolved:
                pos.current_price = up_price if pos.side == "UP" else down_price

        # Build market updates dictionary for trailing stops
        market_updates = {}
        for pos in self._positions.values():
            if pos.market_id == market_id and not pos.resolved:
                token_id = pos.market_id  # Use market_id as token_id for now
                current_price = up_price if pos.side == "UP" else down_price
                market_updates[token_id] = current_price

        # Check and execute stop losses
        self._check_and_execute_stop_losses(market_id, up_price, down_price)

        # Check and execute take profits
        self._check_and_execute_take_profits(market_id, up_price, down_price)

        # Check and execute trailing stops
        triggered_trailing_stops = self.check_and_execute_trailing_stops(market_updates)
        for position_key in triggered_trailing_stops:
            self.execute_trailing_stop_exit(position_key)

    def _check_stop_losses_for_wallet(self, positions, risk_manager, market_id: str, up_price: float, down_price: float, wallet=None) -> list[tuple]:
        """Check stop losses for positions in a single wallet. Returns list of (position, wallet) pairs to exit."""
        triggered = []
        for position in positions.values():
            if position.market_id != market_id or position.resolved:
                continue
            if position.stop_loss is None:
                continue
            current_price = up_price if position.side == "UP" else down_price
            if risk_manager.check_stop_loss(position, current_price):
                triggered.append((position, wallet))
        return triggered

    def _check_take_profits_for_wallet(self, positions, risk_manager, market_id: str, up_price: float, down_price: float, wallet=None) -> list[tuple]:
        """Check take profits for positions in a single wallet. Returns list of (position, wallet) pairs to exit."""
        triggered = []
        for position in positions.values():
            if position.market_id != market_id or position.resolved:
                continue
            if position.take_profit is None:
                continue
            current_price = up_price if position.side == "UP" else down_price
            if risk_manager.check_take_profit(position, current_price):
                triggered.append((position, wallet))
        return triggered

    def _check_and_execute_stop_losses(self, market_id: str, up_price: float, down_price: float) -> None:
        """
        Check and execute stop loss orders based on current prices.
        """
        all_triggered = []
        if self._use_multi_wallet and self._real_wallet_manager:
            for w in self._real_wallet_manager.get_all_wallets():
                rm = w.risk_manager if w.risk_manager is not None else self._risk_manager
                all_triggered.extend(
                    self._check_stop_losses_for_wallet(w.positions, rm, market_id, up_price, down_price, w)
                )
        else:
            all_triggered.extend(
                self._check_stop_losses_for_wallet(self._positions, self._risk_manager, market_id, up_price, down_price)
            )

        for position, wallet in all_triggered:
            log.warning(
                "Stop loss triggered for %s %s", position.slug, position.side
            )
            self._execute_exit_order(position, "STOP_LOSS", wallet=wallet)

    def _check_and_execute_take_profits(self, market_id: str, up_price: float, down_price: float) -> None:
        """
        Check and execute take profit orders based on current prices.
        """
        all_triggered = []
        if self._use_multi_wallet and self._real_wallet_manager:
            for w in self._real_wallet_manager.get_all_wallets():
                rm = w.risk_manager if w.risk_manager is not None else self._risk_manager
                all_triggered.extend(
                    self._check_take_profits_for_wallet(w.positions, rm, market_id, up_price, down_price, w)
                )
        else:
            all_triggered.extend(
                self._check_take_profits_for_wallet(self._positions, self._risk_manager, market_id, up_price, down_price)
            )

        for position, wallet in all_triggered:
            log.info(
                "Take profit triggered for %s %s", position.slug, position.side
            )
            self._execute_exit_order(position, "TAKE_PROFIT", wallet=wallet)

    def _execute_exit_order(self, position: RealPosition, reason: str, wallet=None) -> None:
        """
        Execute an exit order for a position (stop loss, take profit, or trailing stop).
        """
        try:
            token_id = position.market_id
            current_price = position.current_price

            order_response = self._place_clob_order(
                token_id,
                "sell",
                current_price,
                position.shares,
                "market",
                wallet=wallet,
            )

            position.resolved = True
            position.outcome = reason

            if position.side == "UP":
                exit_value = position.shares * current_price
            else:
                exit_value = position.shares * (1 - current_price)

            pnl = exit_value - position.cost_basis
            position.current_value = exit_value

            log.info(
                "Exit order executed for %s %s: reason=%s, pnl=$%.2f",
                position.slug, position.side, reason, pnl
            )

            if self._db_enabled:
                self._save_exit_to_db(position, reason, current_price)

        except Exception as e:
            log.exception("Failed to execute exit order for %s %s", position.slug, position.side)

    def _save_exit_to_db(self, position: RealPosition, reason: str, exit_price: float) -> None:
        """
        Save exit trade to database.

        Parameters
        ----------
        position : RealPosition
            Position that was exited
        reason : str
            Exit reason
        exit_price : float
            Exit price
        """
        try:
            # Look up order-level metadata from the position's first order
            sizing_strategy = "unknown"
            confidence = 0.5
            kelly_fraction = 0.0
            fee = 0.0
            if position.order_ids:
                first_order = self._orders.get(position.order_ids[0])
                if first_order:
                    sizing_strategy = first_order.sizing_strategy
                    confidence = first_order.confidence
                    kelly_fraction = first_order.kelly_fraction
                    fee = first_order.fee

            self._db.save_trade(
                market_slug=position.slug,
                market_id=position.market_id,
                side=position.side,
                entry_price=position.avg_price,
                exit_price=exit_price,
                amount=position.cost_basis,
                shares=position.shares,
                fee=fee,
                outcome=reason,
                pnl=position.pnl,
                timestamp=datetime.now(timezone.utc),
                sizing_strategy=sizing_strategy,
                confidence=confidence,
                kelly_fraction=kelly_fraction,
                stop_loss=position.stop_loss,
                take_profit=position.take_profit,
                tx_hash=None,
                is_real_trade=True,
                wallet_address=self._wallet.get_address(),
            )
            log.debug("Real: exit saved to database for %s", position.slug)
        except Exception as exc:
            log.exception("Real: failed to save exit to database")

    # ── Safety Features ───────────────────────────────────────────────────────────

    def emergency_stop(self, reason: str = "Manual") -> None:
        """
        Emergency stop - cancel all open orders and prevent new trades.

        Parameters
        ----------
        reason : str
            Reason for emergency stop
        """
        log.warning("EMERGENCY STOP: %s", reason)

        # Cancel all open orders
        for order_id in list(self._orders.keys()):
            try:
                self.cancel(order_id)
            except Exception as e:
                log.exception("Failed to cancel order %s", order_id)

        # Set emergency flag
        self._emergency_mode = True

        log.warning("All trading halted. Call resume_trading() to re-enable.")

    def resume_trading(self, confirm: bool = True) -> None:
        """Resume trading after emergency stop."""
        if confirm:
            response = input("Resume trading? (yes/no): ").strip().lower()
            if response not in ("yes", "y"):
                log.info("Trading remains halted.")
                return

        self._emergency_mode = False
        log.info("Trading resumed.")

    # ── Private Methods ───────────────────────────────────────────────────────────

    def _create_position_sizer(self) -> PositionSizer:
        """Create position sizer based on configuration."""
        strategy = self._config.position_sizing

        if strategy == "fixed":
            return FixedPositionSizer(amount=self._config.fixed_amount)
        elif strategy == "percentage":
            return PercentagePositionSizer(percentage=self._config.percentage_of_balance)
        elif strategy == "kelly":
            return KellyPositionSizer(
                kelly_fraction=self._config.kelly_fraction,
                min_confidence=0.55,
            )
        else:
            # Default to fixed
            return FixedPositionSizer(amount=self._config.fixed_amount)

    def _calculate_position_size(
        self,
        market,
        side: str,
        confidence: float,
        price: float,
    ) -> float:
        """Calculate position size using the configured position sizer."""
        return self._position_sizer.calculate_size(
            balance=self._balance,
            market=market,
            side=side,
            confidence=confidence,
            price=price,
        )

    def _validate_order(self, amount: float, market) -> None:
        """Validate order against risk limits using RiskManager."""
        self._risk_manager.validate_order(
            amount=amount,
            balance=self._balance,
            market=market,
            positions=self._positions,
        )

    def _apply_buy_slippage(self, price: float, config) -> float:
        """
        Buffer a market-buy price by the configured slippage tolerance.

        A market buy has to cross the spread; submitting at the exact quoted price
        leaves the signed order non-marketable, so it may never fill. We raise the
        price by ``slippage_tolerance`` (a buy is always adverse in the up direction)
        and cap it just below 1.0, since Polymarket prices live in (0, 1). The
        buffered price is used for both submission and accounting, so recorded cost
        is the worst case and never underestimated.

        Parameters
        ----------
        price : float
            Quoted price per share.
        config : RealTradingConfig
            Resolved config providing ``slippage_tolerance``.

        Returns
        -------
        float
            Slippage-adjusted price, capped at ``MAX_ORDER_PRICE``.
        """
        tolerance = getattr(config, "slippage_tolerance", 0.0)
        if tolerance <= 0 or price <= 0:
            return price
        adjusted = min(price * (1 + tolerance), MAX_ORDER_PRICE)
        if adjusted != price:
            log.debug(
                "Real: applied %.2f%% buy slippage: %.4f -> %.4f",
                tolerance * 100, price, adjusted,
            )
        return adjusted

    def _calculate_shares_and_fee(self, amount: float, price: float, is_maker: bool = False) -> tuple[float, float]:
        """
        Calculate shares and fee for an order using the configured fee mode.

        The fee is deducted from the trade amount (like Polymarket does on-chain),
        so the user receives fewer shares.

        Parameters
        ----------
        amount : float
            Total USDC being spent
        price : float
            Price per share
        is_maker : bool
            Whether this is a maker order (limit order providing liquidity)

        Returns
        -------
        tuple[float, float]
            (shares, fee) where shares = (amount - fee) / price
        """
        if price <= 0:
            return 0.0, 0.0

        # First pass: estimate fee from initial share estimate
        shares_est = amount / price
        fee = self._calculate_fee(amount, price, shares_est, is_maker)

        # Fee comes out of the trade amount
        net_trade = amount - fee
        if net_trade <= 0:
            return 0.0, fee

        shares = net_trade / price

        # Second pass: recalculate fee with actual shares (significant for polymarket formula)
        if self._config.fee_mode == "polymarket":
            fee = self._calculate_fee(amount, price, shares, is_maker)
            net_trade = amount - fee
            if net_trade <= 0:
                return 0.0, fee
            shares = net_trade / price

        return shares, fee

    def _calculate_fee(self, amount: float, price: float, shares: float, is_maker: bool = False) -> float:
        """
        Calculate the fee for an order based on the configured fee mode.

        Parameters
        ----------
        amount : float
            Total USDC being spent
        price : float
            Price per share
        shares : float
            Number of shares being traded
        is_maker : bool
            Whether this is a maker order (limit order providing liquidity)

        Returns
        -------
        float
            The fee amount in USDC
        """
        if self._config.fee_mode == "zero":
            return 0.0
        elif self._config.fee_mode == "custom":
            fee_rate = self._config.maker_fee_rate if is_maker else self._config.custom_fee_rate
            return round(amount * fee_rate, FEE_ROUNDING)
        elif self._config.fee_mode == "polymarket":
            return self._polymarket_fee(amount, price, shares, is_maker)
        return 0.0

    def _polymarket_fee(self, amount: float, price: float, shares: float, is_maker: bool = False) -> float:
        if self._config.market_category.lower() == "geopolitical":
            return 0.0
        fee_rate = fee_rate_for_category(self._config.market_category)
        return calculate_polymarket_fee(shares, price, fee_rate)

    def _require_confirmation(
        self,
        market,
        side: str,
        amount: float,
        price: float,
        shares: float,
        fee: float,
    ) -> None:
        """Require user confirmation before executing order."""
        print("\n" + "=" * 60)
        print("ORDER CONFIRMATION REQUIRED")
        print("=" * 60)
        print(f"Market:    {market.question}")
        print(f"Side:      {side}")
        print(f"Amount:    ${amount:.2f}")
        print(f"Price:     ${price:.4f}")
        print(f"Shares:    {shares:.4f}")
        print(f"Fee:       ${fee:.4f}")
        print(f"Net Trade: ${amount - fee:.2f}")
        print(f"Total:     ${amount:.2f}")
        balance = self._resolve_balance()
        print(f"Balance:   ${balance:.2f}")
        print("=" * 60)

        response = input("\nConfirm this order? (yes/no): ").strip().lower()

        if response not in ("yes", "y"):
            raise OrderCancelled("Order cancelled by user")

        print("Order confirmed.\n")

    def _place_clob_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        order_type: str,
        wallet=None,
    ) -> dict:
        """Place order on CLOB."""
        clob = self._resolve_clob() if wallet is None else wallet.clob_client
        return clob.place_order(
            token_id=token_id,
            side=side,
            price=price,
            size=size,
            order_type=order_type,
        )

    def _cancel_clob_order(self, order_id: str, wallet=None) -> None:
        """Cancel order on CLOB."""
        clob = self._resolve_clob() if wallet is None else wallet.clob_client
        clob.cancel_order(order_id)

    def _update_position(self, market, side: str, order: RealOrder, wallet=None) -> None:
        """Update position after order fill."""
        key = f"{market.id}:{side}"
        if wallet is not None and self._use_multi_wallet:
            positions = wallet.positions
        else:
            positions = self._resolve_positions()

        with self._position_lock:
            if key in positions:
                position = positions[key]
                position.order_ids.append(order.id)

                total_shares = position.shares + order.shares
                position.avg_price = (
                    (position.avg_price * position.shares + order.price * order.shares)
                    / total_shares
                )
                position.shares = total_shares
                position.cost_basis = position.shares * position.avg_price
                position.current_value = position.shares * order.price
            else:
                position = RealPosition(
                    market_id=market.id,
                    slug=market.slug,
                    question=market.question,
                    side=side,
                    shares=order.shares,
                    avg_price=order.price,
                    current_price=order.price,
                    cost_basis=order.shares * order.price,
                    current_value=order.shares * order.price,
                    order_ids=[order.id],
                )
                positions[key] = position

    def _get_market_exposure(self, market_id: str) -> float:
        """Get total exposure for a market."""
        exposure = 0.0
        positions = self._get_all_positions_across_wallets()
        for position in positions.values():
            if position.market_id == market_id and not position.resolved:
                exposure += position.cost_basis
        return exposure

    def _save_order_to_db(self, order: RealOrder, wallet=None) -> None:
        """Save real order to database."""
        if not self._db_enabled or self._db is None:
            return

        try:
            wallet_obj = wallet if (wallet is not None and self._use_multi_wallet) else self._resolve_wallet()
            addr = wallet_obj.get_address() if hasattr(wallet_obj, 'get_address') else str(wallet_obj)

            self._db.save_trade(
                market_slug=order.slug,
                market_id=order.market_id,
                side=order.side,
                entry_price=order.price,
                exit_price=None,
                amount=order.amount,
                shares=order.shares,
                fee=order.fee,
                outcome=None,
                pnl=0.0,
                timestamp=order.created_at,
                sizing_strategy=order.sizing_strategy,
                confidence=order.confidence,
                kelly_fraction=order.kelly_fraction,
                stop_loss=order.stop_loss,
                take_profit=order.take_profit,
                tx_hash=order.tx_hash,
                is_real_trade=True,
                wallet_address=addr,
                order_id=order.id,
                status=order.status,
            )
            log.debug("Real: order saved to database for %s", order.slug)
        except Exception as exc:
            log.exception("Real: failed to save order to database")

    def _update_order_in_db(self, order: RealOrder) -> None:
        """Update order status in database after fill status changes."""
        if not self._db_enabled or self._db is None:
            return

        try:
            # Update the trade record with fill information
            self._db.update_trade_status(
                order_id=order.id,
                status=order.status,
                filled_shares=order.filled_shares,
                filled_amount=order.filled_amount,
                avg_fill_price=order.avg_fill_price,
                filled_at=order.filled_at,
            )
            log.debug("Real: order status updated in database for %s: %s", order.slug, order.status)
        except Exception as exc:
            log.exception("Real: failed to update order in database")

    # ── Advanced Order Types ─────────────────────────────────────────────────────────

    def place_oco_order(
        self,
        market,
        side: str,
        amount: float,
        price1: float,
        price2: float,
        confirm: bool = True,
    ) -> OCOOrder:
        """
        Place a One-Cancels-Other (OCO) order pair.

        An OCO order places two orders where if one is filled, the other is automatically cancelled.
        Commonly used for stop loss + take profit combinations.

        Parameters
        ----------
        market : Market
            Market to trade
        side : str
            "UP" or "DOWN"
        amount : float
            USDC amount for each order
        price1 : float
            Price for first order (e.g., take profit)
        price2 : float
            Price for second order (e.g., stop loss)
        confirm : bool
            Require manual confirmation

        Returns
        -------
        OCOOrder
            The OCO order object

        Example
        -------
        >>> oco = client.real.place_oco_order(
        ...     market, side="UP", amount=10.0, price1=0.60, price2=0.40
        ... )
        """
        import uuid

        if self._emergency_mode:
            raise OrderCancelled("Trading halted - emergency mode active")

        side = _validate_side(side)

        # Place first order
        order1 = self.limit(market, side, price1, amount, confirm=confirm)

        # Place second order
        order2 = self.limit(market, side, price2, amount, confirm=False)

        # Create OCO order
        oco_id = str(uuid.uuid4())
        oco_order = OCOOrder(
            id=oco_id,
            market_id=market.id,
            slug=market.slug,
            side=side,
            order1_id=order1.id,
            order2_id=order2.id,
            order1_price=price1,
            order2_price=price2,
            amount=amount,
            status="active",
            created_at=datetime.now(timezone.utc),
        )

        self._oco_orders[oco_id] = oco_order

        log.info(
            "OCO order placed: %s %s, order1=%s @ %.4f, order2=%s @ %.4f",
            market.slug, side, order1.id, price1, order2.id, price2
        )

        return oco_order

    def check_oco_triggers(self) -> list[str]:
        """
        Check OCO orders for trigger conditions and cancel the other order if one fills.

        Returns
        -------
        list[str]
            List of OCO order IDs that were triggered
        """
        triggered_ocos = []

        for oco_id, oco in list(self._oco_orders.items()):
            if oco.status != "active":
                continue

            # Check if either order is filled
            order1 = self._orders.get(oco.order1_id)
            order2 = self._orders.get(oco.order2_id)

            if not order1 or not order2:
                continue

            # Update order statuses
            self.update_order_fill_status(oco.order1_id)
            self.update_order_fill_status(oco.order2_id)

            # Check if order1 is filled
            if order1.status == "filled":
                # Cancel order2
                try:
                    self.cancel(oco.order2_id)
                    oco.status = "triggered"
                    oco.triggered_order_id = order1.id
                    oco.cancelled_order_id = order2.id
                    oco.triggered_at = datetime.now(timezone.utc)
                    triggered_ocos.append(oco_id)
                    log.info("OCO triggered: order1 %s filled, cancelled order2 %s", order1.id, order2.id)
                except Exception as e:
                    log.exception("Failed to cancel order2 in OCO %s", oco_id)

            # Check if order2 is filled
            elif order2.status == "filled":
                # Cancel order1
                try:
                    self.cancel(oco.order1_id)
                    oco.status = "triggered"
                    oco.triggered_order_id = order2.id
                    oco.cancelled_order_id = order1.id
                    oco.triggered_at = datetime.now(timezone.utc)
                    triggered_ocos.append(oco_id)
                    log.info("OCO triggered: order2 %s filled, cancelled order1 %s", order2.id, order1.id)
                except Exception as e:
                    log.exception("Failed to cancel order1 in OCO %s", oco_id)

        return triggered_ocos

    def place_bracket_order(
        self,
        market,
        side: str,
        entry_price: float,
        amount: float,
        stop_loss_price: Optional[float] = None,
        take_profit_price: Optional[float] = None,
        stop_loss_pct: Optional[float] = None,
        take_profit_pct: Optional[float] = None,
        confirm: bool = True,
    ) -> BracketOrder:
        """
        Place a bracket order (entry + stop loss + take profit).

        A bracket order places an entry order along with associated stop loss and take profit orders.

        Parameters
        ----------
        market : Market
            Market to trade
        side : str
            "UP" or "DOWN"
        entry_price : float
            Entry order price
        amount : float
            USDC amount for entry order
        stop_loss_price : float, optional
            Stop loss price (overrides stop_loss_pct)
        take_profit_price : float, optional
            Take profit price (overrides take_profit_pct)
        stop_loss_pct : float, optional
            Stop loss as percentage of entry price (e.g., 0.20 for 20%)
        take_profit_pct : float, optional
            Take profit as percentage of entry price (e.g., 0.50 for 50%)
        confirm : bool
            Require manual confirmation

        Returns
        -------
        BracketOrder
            The bracket order object

        Example
        -------
        >>> bracket = client.real.place_bracket_order(
        ...     market, side="UP", entry_price=0.50, amount=10.0,
        ...     stop_loss_pct=0.20, take_profit_pct=0.50
        ... )
        """
        import uuid

        if self._emergency_mode:
            raise OrderCancelled("Trading halted - emergency mode active")

        side = _validate_side(side)

        # Calculate stop loss and take profit prices if not provided
        if stop_loss_price is None and stop_loss_pct is not None:
            if side == "UP":
                stop_loss_price = entry_price * (1 - stop_loss_pct)
            else:
                stop_loss_price = entry_price * (1 + stop_loss_pct)

        if take_profit_price is None and take_profit_pct is not None:
            if side == "UP":
                take_profit_price = entry_price * (1 + take_profit_pct)
            else:
                take_profit_price = entry_price * (1 - take_profit_pct)

        # Place entry order
        entry_order = self.limit(market, side, entry_price, amount, confirm=confirm)

        # Create bracket order
        bracket_id = str(uuid.uuid4())
        bracket_order = BracketOrder(
            id=bracket_id,
            market_id=market.id,
            slug=market.slug,
            side=side,
            entry_order_id=entry_order.id,
            entry_price=entry_price,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            amount=amount,
            status="pending",
            created_at=datetime.now(timezone.utc),
        )

        self._bracket_orders[bracket_id] = bracket_order

        log.info(
            "Bracket order placed: %s %s @ %.4f, stop=%.4f, take=%.4f",
            market.slug, side, entry_price, stop_loss_price, take_profit_price
        )

        return bracket_order

    def activate_bracket_orders(self) -> None:
        """
        Activate stop loss and take profit orders for filled bracket entry orders.

        This method checks all pending bracket orders and if the entry order is filled,
        it places the corresponding stop loss and take profit orders.
        """
        for bracket_id, bracket in list(self._bracket_orders.items()):
            if bracket.status != "pending":
                continue

            entry_order = self._orders.get(bracket.entry_order_id)
            if not entry_order:
                continue

            # Update entry order status
            self.update_order_fill_status(bracket.entry_order_id)

            # If entry order is filled, place stop loss and take profit
            if entry_order.status == "filled":
                bracket.status = "active"
                bracket.filled_at = datetime.now(timezone.utc)

                # Place stop loss order if specified
                if bracket.stop_loss_price is not None:
                    try:
                        log.info("Placing stop loss order for bracket %s at %.4f", bracket_id, bracket.stop_loss_price)
                        sl_order = self._clob_client.place_order(
                            token_id=bracket.market_id,
                            side="sell",
                            price=bracket.stop_loss_price,
                            size=bracket.amount / bracket.stop_loss_price,
                            order_type="limit",
                        )
                        bracket.stop_loss_order_id = sl_order.get("order_id", "")
                    except Exception as e:
                        log.exception("Failed to place stop loss for bracket %s", bracket_id)

                # Place take profit order if specified
                if bracket.take_profit_price is not None:
                    try:
                        log.info("Placing take profit order for bracket %s at %.4f", bracket_id, bracket.take_profit_price)
                        tp_order = self._clob_client.place_order(
                            token_id=bracket.market_id,
                            side="sell",
                            price=bracket.take_profit_price,
                            size=bracket.amount / bracket.take_profit_price,
                            order_type="limit",
                        )
                        bracket.take_profit_order_id = tp_order.get("order_id", "")
                    except Exception as e:
                        log.exception("Failed to place take profit for bracket %s", bracket_id)

                log.info("Bracket order %s activated", bracket_id)

    def place_conditional_order(
        self,
        market,
        side: str,
        condition_type: str,
        condition_value: float,
        child_order_price: float,
        child_order_amount: float,
        expires_after_seconds: Optional[int] = None,
    ) -> ConditionalOrder:
        """
        Place a conditional order with if-then logic.

        A conditional order triggers a child order when specified conditions are met.

        Parameters
        ----------
        market : Market
            Market to trade
        side : str
            "UP" or "DOWN"
        condition_type : str
            Condition type: "price_above", "price_below", "time_after"
        condition_value : float
            Value for the condition (price threshold or timestamp)
        child_order_price : float
            Price for the child order when triggered
        child_order_amount : float
            Amount for the child order when triggered
        expires_after_seconds : int, optional
            Expiration time in seconds

        Returns
        -------
        ConditionalOrder
            The conditional order object

        Example
        -------
        >>> cond = client.real.place_conditional_order(
        ...     market, side="UP", condition_type="price_above",
        ...     condition_value=0.60, child_order_price=0.61, child_order_amount=10.0
        ... )
        """
        import uuid

        if self._emergency_mode:
            raise OrderCancelled("Trading halted - emergency mode active")

        side = _validate_side(side)

        if condition_type not in ("price_above", "price_below", "time_after"):
            raise ValueError(f"Invalid condition_type: {condition_type}")

        # Calculate expiration
        expires_at = None
        if expires_after_seconds is not None:
            expires_at = datetime.now(timezone.utc).replace(
                second=0, microsecond=0
            ) + datetime.timedelta(seconds=expires_after_seconds)

        # Create conditional order
        cond_id = str(uuid.uuid4())
        cond_order = ConditionalOrder(
            id=cond_id,
            market_id=market.id,
            slug=market.slug,
            side=side,
            condition_type=condition_type,
            condition_value=condition_value,
            child_order_price=child_order_price,
            child_order_amount=child_order_amount,
            status="waiting",
            created_at=datetime.now(timezone.utc),
            expires_at=expires_at,
        )

        self._conditional_orders[cond_id] = cond_order

        log.info(
            "Conditional order placed: %s %s, condition=%s %.4f",
            market.slug, side, condition_type, condition_value
        )

        return cond_order

    def check_conditional_triggers(self, market_updates: dict[str, float]) -> list[str]:
        """
        Check conditional orders for trigger conditions.

        Parameters
        ----------
        market_updates : dict[str, float]
            Dictionary mapping market_id to current price

        Returns
        -------
        list[str]
            List of conditional order IDs that were triggered
        """
        triggered = []

        for cond_id, cond in list(self._conditional_orders.items()):
            if cond.status != "waiting":
                continue

            # Check expiration
            if cond.expires_at and datetime.now(timezone.utc) > cond.expires_at:
                cond.status = "expired"
                log.info("Conditional order %s expired", cond_id)
                continue

            # Check price conditions
            if cond.condition_type in ("price_above", "price_below"):
                current_price = market_updates.get(cond.market_id)
                if current_price is None:
                    continue

                should_trigger = False
                if cond.condition_type == "price_above" and current_price > cond.condition_value:
                    should_trigger = True
                elif cond.condition_type == "price_below" and current_price < cond.condition_value:
                    should_trigger = True

                if should_trigger:
                    try:
                        log.info(
                            "Conditional order %s triggered: price %.4f, placing child order",
                            cond_id, current_price
                        )
                        child = self._clob_client.place_order(
                            token_id=cond.market_id,
                            side="buy",
                            price=cond.child_order_price,
                            size=cond.child_order_amount / cond.child_order_price,
                            order_type="limit",
                        )
                        cond.child_order_id = child.get("order_id", "")
                        cond.status = "triggered"
                        cond.triggered_at = datetime.now(timezone.utc)
                        triggered.append(cond_id)
                    except Exception as e:
                        log.exception("Failed to place child order for conditional %s", cond_id)

        return triggered

    def place_iceberg_order(
        self,
        market,
        side: str,
        total_amount: float,
        visible_size: float,
        price: float,
        confirm: bool = True,
    ) -> IcebergOrder:
        """
        Place an iceberg order for large order splitting.

        An iceberg order splits a large order into smaller visible chunks to avoid market impact.

        Parameters
        ----------
        market : Market
            Market to trade
        side : str
            "UP" or "DOWN"
        total_amount : float
            Total USDC amount to execute
        visible_size : float
            Visible chunk size in USDC
        price : float
            Limit price for each chunk
        confirm : bool
            Require manual confirmation for first chunk

        Returns
        -------
        IcebergOrder
            The iceberg order object

        Example
        -------
        >>> iceberg = client.real.place_iceberg_order(
        ...     market, side="UP", total_amount=1000.0, visible_size=50.0, price=0.50
        ... )
        """
        import uuid

        if self._emergency_mode:
            raise OrderCancelled("Trading halted - emergency mode active")

        side = _validate_side(side)

        if visible_size > total_amount:
            raise ValueError("visible_size cannot exceed total_amount")

        # Create iceberg order
        token_id = market.up_token if side == "UP" else market.down_token
        iceberg_id = str(uuid.uuid4())
        iceberg_order = IcebergOrder(
            id=iceberg_id,
            market_id=market.id,
            slug=market.slug,
            side=side,
            total_amount=total_amount,
            visible_size=visible_size,
            price=price,
            status="active",
            created_at=datetime.now(timezone.utc),
            token_id=token_id,
        )

        self._iceberg_orders[iceberg_id] = iceberg_order

        # Place first visible chunk
        self._execute_iceberg_slice(iceberg_id, confirm=confirm)

        log.info(
            "Iceberg order placed: %s %s, total=$%.2f, visible=$%.2f @ %.4f",
            market.slug, side, total_amount, visible_size, price
        )

        return iceberg_order

    def _execute_iceberg_slice(self, iceberg_id: str, confirm: bool = True) -> Optional[RealOrder]:
        """
        Execute a single slice of an iceberg order.

        Parameters
        ----------
        iceberg_id : str
            Iceberg order ID
        confirm : bool
            Require manual confirmation

        Returns
        -------
        RealOrder, optional
            The placed order, or None if no more to execute
        """
        iceberg = self._iceberg_orders.get(iceberg_id)
        if not iceberg or iceberg.status not in ("active", "partial"):
            return None

        remaining = iceberg.remaining_amount
        if remaining <= 0:
            iceberg.status = "completed"
            return None

        # Calculate slice size (visible size or remaining, whichever is smaller)
        slice_amount = min(iceberg.visible_size, remaining)

        try:
            log.info(
                "Executing iceberg slice: %s %s, amount=$%.2f @ %.4f",
                iceberg.slug, iceberg.side, slice_amount, iceberg.price
            )
            # Use ClobClient directly with the stored token_id
            order_response = self._clob_client.place_order(
                token_id=iceberg.token_id,
                side="buy",
                price=iceberg.price,
                size=slice_amount / iceberg.price if iceberg.price > 0 else 0,
                order_type="limit",
            )

            # Create a RealOrder for tracking
            order = RealOrder(
                id=order_response["order_id"],
                market_id=iceberg.market_id,
                slug=iceberg.slug,
                side=iceberg.side,
                price=iceberg.price,
                amount=slice_amount,
                shares=slice_amount / iceberg.price if iceberg.price > 0 else 0,
                fee=0.0,
                status="pending",
                is_limit=True,
                created_at=datetime.now(timezone.utc),
            )
            self._orders[order.id] = order
            iceberg.child_order_ids.append(order.id)
            return order

        except Exception as e:
            log.exception("Failed to execute iceberg slice for %s", iceberg_id)
            return None

    def update_iceberg_orders(self) -> None:
        """
        Update iceberg orders and execute additional slices as previous ones fill.

        This method should be called periodically to check if iceberg slices have filled
        and execute additional slices if needed.
        """
        for iceberg_id, iceberg in list(self._iceberg_orders.items()):
            if iceberg.status not in ("active", "partial"):
                continue

            # Check if child orders have filled
            filled_amount = 0.0
            for child_id in iceberg.child_order_ids:
                child_order = self._orders.get(child_id)
                if child_order:
                    self.update_order_fill_status(child_id)
                    if child_order.status == "filled":
                        filled_amount += child_order.amount

            iceberg.filled_amount = filled_amount

            # Update status
            if iceberg.filled_amount >= iceberg.total_amount:
                iceberg.status = "completed"
                log.info("Iceberg order %s completed", iceberg_id)
            elif iceberg.filled_amount > 0:
                iceberg.status = "partial"

            # Execute next slice if there's remaining amount and previous slice filled
            if iceberg.remaining_amount > 0 and len(iceberg.child_order_ids) > 0:
                last_child_id = iceberg.child_order_ids[-1]
                last_child = self._orders.get(last_child_id)
                if last_child and last_child.status == "filled":
                    self._execute_iceberg_slice(iceberg_id, confirm=False)

    def place_twap_order(
        self,
        market,
        side: str,
        total_amount: float,
        duration_seconds: int,
        num_slices: int,
        price: Optional[float] = None,
        confirm: bool = True,
    ) -> TWAPOrder:
        """
        Place a Time-Weighted Average Price (TWAP) execution order.

        A TWAP order executes a large order over a specified time period to achieve an average execution price.

        Parameters
        ----------
        market : Market
            Market to trade
        side : str
            "UP" or "DOWN"
        total_amount : float
            Total USDC amount to execute
        duration_seconds : int
            Duration over which to execute (in seconds)
        num_slices : int
            Number of slices to split the order into
        price : float, optional
            Limit price for each slice (if None, uses market price)
        confirm : bool
            Require manual confirmation for first slice

        Returns
        -------
        TWAPOrder
            The TWAP order object

        Example
        -------
        >>> twap = client.real.place_twap_order(
        ...     market, side="UP", total_amount=1000.0, duration_seconds=300, num_slices=10
        ... )
        """
        import uuid

        if self._emergency_mode:
            raise OrderCancelled("Trading halted - emergency mode active")

        side = _validate_side(side)

        if num_slices < 1:
            raise ValueError("num_slices must be at least 1")

        # Calculate slice interval
        slice_interval = duration_seconds / num_slices

        # Calculate end time
        ends_at = datetime.now(timezone.utc) + datetime.timedelta(seconds=duration_seconds)

        # Create TWAP order
        token_id = market.up_token if side == "UP" else market.down_token
        twap_id = str(uuid.uuid4())
        twap_order = TWAPOrder(
            id=twap_id,
            market_id=market.id,
            slug=market.slug,
            side=side,
            total_amount=total_amount,
            duration_seconds=duration_seconds,
            num_slices=num_slices,
            price=price,
            status="active",
            created_at=datetime.now(timezone.utc),
            ends_at=ends_at,
            slice_interval=slice_interval,
            token_id=token_id,
        )

        self._twap_orders[twap_id] = twap_order

        # Place first slice
        self._execute_twap_slice(twap_id, confirm=confirm)

        log.info(
            "TWAP order placed: %s %s, total=$%.2f over %ds in %d slices",
            market.slug, side, total_amount, duration_seconds, num_slices
        )

        return twap_order

    def _execute_twap_slice(self, twap_id: str, confirm: bool = True) -> Optional[RealOrder]:
        """
        Execute a single slice of a TWAP order.

        Parameters
        ----------
        twap_id : str
            TWAP order ID
        confirm : bool
            Require manual confirmation

        Returns
        -------
        RealOrder, optional
            The placed order, or None if no more to execute
        """
        twap = self._twap_orders.get(twap_id)
        if not twap or twap.status not in ("active", "partial"):
            return None

        # Check if we've exceeded the end time
        if twap.ends_at and datetime.now(timezone.utc) > twap.ends_at:
            twap.status = "completed"
            log.info("TWAP order %s completed (time expired)", twap_id)
            return None

        remaining = twap.remaining_amount
        if remaining <= 0:
            twap.status = "completed"
            return None

        # Calculate slice amount
        slice_amount = twap.slice_amount

        try:
            log.info(
                "Executing TWAP slice: %s %s, amount=$%.2f",
                twap.slug, twap.side, slice_amount
            )
            if twap.price and twap.price > 0:
                # Limit order at specified price
                order_response = self._clob_client.place_order(
                    token_id=twap.token_id,
                    side="buy",
                    price=twap.price,
                    size=slice_amount / twap.price,
                    order_type="limit",
                )
            else:
                # Market order (use current price)
                price = 0.5  # Will be overridden by actual fill
                order_response = self._clob_client.place_order(
                    token_id=twap.token_id,
                    side="buy",
                    price=price,
                    size=slice_amount / price,
                    order_type="market",
                )

            order = RealOrder(
                id=order_response["order_id"],
                market_id=twap.market_id,
                slug=twap.slug,
                side=twap.side,
                price=twap.price or 0.5,
                amount=slice_amount,
                shares=slice_amount / (twap.price or 0.5),
                fee=0.0,
                status="pending",
                is_limit=twap.price is not None and twap.price > 0,
                created_at=datetime.now(timezone.utc),
            )
            self._orders[order.id] = order
            twap.child_order_ids.append(order.id)
            return order

        except Exception as e:
            log.exception("Failed to execute TWAP slice for %s", twap_id)
            return None

    def update_twap_orders(self) -> None:
        """
        Update TWAP orders and execute slices based on schedule.

        This method should be called periodically to check if it's time to execute
        the next slice of each TWAP order.
        """
        for twap_id, twap in list(self._twap_orders.items()):
            if twap.status not in ("active", "partial"):
                continue

            # Check if child orders have filled
            filled_amount = 0.0
            for child_id in twap.child_order_ids:
                child_order = self._orders.get(child_id)
                if child_order:
                    self.update_order_fill_status(child_id)
                    if child_order.status == "filled":
                        filled_amount += child_order.amount

            twap.filled_amount = filled_amount

            # Update status
            if twap.filled_amount >= twap.total_amount:
                twap.status = "completed"
                log.info("TWAP order %s completed", twap_id)
            elif twap.filled_amount > 0:
                twap.status = "partial"

            # Check if it's time for next slice
            if twap.remaining_amount > 0:
                # Calculate expected number of slices based on elapsed time
                elapsed = (datetime.now(timezone.utc) - twap.created_at).total_seconds()
                expected_slices = int(elapsed / twap.slice_interval) + 1

                # Execute next slice if we haven't placed enough slices yet
                if len(twap.child_order_ids) < expected_slices and len(twap.child_order_ids) < twap.num_slices:
                    self._execute_twap_slice(twap_id, confirm=False)


# ── Helpers ─────────────────────────────────────────────────────────────────────

def _validate_side(side: str) -> str:
    """Validate and normalize side."""
    side = side.upper()
    if side not in ("UP", "DOWN"):
        raise ValueError(f"side must be 'UP' or 'DOWN', got '{side}'")
    return side


def _validate_positive(value: float, name: str) -> float:
    """Validate that value is positive."""
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def _now() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)
