"""
Real trading engine tests — run with: pytest tests/unit/trading/test_real_engine.py
"""

import pytest
from polyalpha.trading.real_engine import RealTradingEngine
from polyalpha.trading.real_config import RealTradingConfig
from polyalpha.core.market import Market
from polyalpha.core.errors import (
    InsufficientAllowance,
    RiskLimitExceeded,
    OrderNotFound,
    PositionNotFound,
    OrderCancelled,
)


@pytest.fixture
def make_market():
    """Factory function to create test markets."""
    from datetime import datetime, timedelta, timezone
    def _make_market(**overrides) -> Market:
        now = datetime.now(timezone.utc)
        future_start = now + timedelta(minutes=5)
        future_end = now + timedelta(minutes=10)
        defaults = dict(
            id="test-id",
            question="Will BTC be higher in 5 minutes?",
            description="",
            slug="btc-updown-5m-9999999",
            active=True,
            closed=False,
            archived=False,
            start_time=future_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_time=future_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            volume=10_000.0,
            liquidity=5_000.0,
            outcomes=["UP", "DOWN"],
            prices=[0.55, 0.45],
            tokens=["tok_up", "tok_down"],
        )
        defaults.update(overrides)
        return Market(**defaults)
    return _make_market


@pytest.fixture
def engine():
    """Create a basic real trading engine with simulated balance."""
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
    )
    engine._balance = 500.0
    engine._allowance = 1000.0
    return engine


# ── RealTradingEngine tests ────────────────────────────────────────────────────

@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_initialization():
    """Test engine initialization."""
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
    )
    assert engine.config is not None
    assert engine.balance == 0.0
    assert engine.emergency_mode == False
    assert len(engine._orders) == 0
    assert len(engine._positions) == 0


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_with_config(make_market):
    """Test engine with custom config."""
    config = RealTradingConfig(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        max_order_size=500.0,
        position_sizing="percentage",
        percentage_of_balance=0.10,
    )
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        config=config,
    )
    assert engine.config.max_order_size == 500.0
    assert engine.config.position_sizing == "percentage"
    assert engine.config.percentage_of_balance == 0.10


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_buy_with_fixed_amount(engine, make_market):
    """Test buy with fixed amount position sizing."""
    market = make_market()
    order = engine.buy(market, side="UP", amount=10.0, confirm=False)

    assert order.status == "pending"
    assert order.side == "UP"
    assert order.amount == 10.0
    assert order.sizing_strategy == "fixed"
    assert order.is_limit == False


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_buy_with_limit(engine, make_market):
    """Test limit order placement."""
    market = make_market()
    order = engine.limit(market, side="UP", price=0.92, amount=10.0, confirm=False)

    assert order.status == "pending"
    assert order.side == "UP"
    assert order.price == 0.92
    assert order.is_limit == True


# ── Buy slippage (item 68: buy at correct price incl. slippage) ─────────────────
# Exercise the pure helpers directly (unbound, dummy self) so they run offline —
# the RealTradingEngine fixture requires network, unlike these.

from types import SimpleNamespace


@pytest.mark.unit
def test_apply_buy_slippage_buffers_quote():
    """Market-buy price is buffered upward by slippage_tolerance (worst-case)."""
    cfg = SimpleNamespace(slippage_tolerance=0.05)
    # 0.55 * (1 + 0.05) = 0.5775
    assert RealTradingEngine._apply_buy_slippage(None, 0.55, cfg) == pytest.approx(0.5775)


@pytest.mark.unit
def test_apply_buy_slippage_capped_below_one():
    """Slippage-adjusted price is capped at MAX_ORDER_PRICE (< 1.0)."""
    from polyalpha.core.constants import MAX_ORDER_PRICE
    cfg = SimpleNamespace(slippage_tolerance=0.10)  # 0.98 * 1.10 = 1.078 -> capped
    assert RealTradingEngine._apply_buy_slippage(None, 0.98, cfg) == pytest.approx(MAX_ORDER_PRICE)


@pytest.mark.unit
def test_apply_buy_slippage_zero_tolerance_unchanged():
    """slippage_tolerance=0 leaves the quoted price untouched."""
    cfg = SimpleNamespace(slippage_tolerance=0.0)
    assert RealTradingEngine._apply_buy_slippage(None, 0.55, cfg) == pytest.approx(0.55)


@pytest.mark.unit
def test_apply_buy_slippage_shares_consistent():
    """Shares are computed from the buffered price so cost is not underestimated."""
    cfg = SimpleNamespace(slippage_tolerance=0.05)
    buffered = RealTradingEngine._apply_buy_slippage(None, 0.55, cfg)

    fake_self = SimpleNamespace(
        _config=SimpleNamespace(fee_mode="zero"),
        _calculate_fee=lambda *a, **k: 0.0,
    )
    shares, fee = RealTradingEngine._calculate_shares_and_fee(fake_self, 10.0, buffered)
    raw_shares, _ = RealTradingEngine._calculate_shares_and_fee(fake_self, 10.0, 0.55)
    # Buffered price -> fewer shares than the raw quote, and internally consistent
    assert shares < raw_shares
    assert shares == pytest.approx((10.0 - fee) / buffered)


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_insufficient_allowance(make_market):
    """Test insufficient allowance error."""
    config = RealTradingConfig(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        max_risk_per_trade=1.0,  # 100% to allow testing allowance check
    )
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        config=config,
    )
    engine._balance = 100.0
    engine._allowance = 5.0

    market = make_market()
    with pytest.raises(InsufficientAllowance):
        engine.buy(market, side="UP", amount=10.0, confirm=False)


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_max_order_size_limit(make_market):
    """Test max order size limit."""
    config = RealTradingConfig(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        max_order_size=50.0,
        max_risk_per_trade=1.0,  # 100% to allow testing order size limit
    )
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        config=config,
    )
    engine._balance = 100.0
    engine._allowance = 1000.0

    market = make_market()
    with pytest.raises(RiskLimitExceeded):
        engine.buy(market, side="UP", amount=100.0, confirm=False)


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_max_position_size_limit(make_market):
    """Test max position size limit."""
    config = RealTradingConfig(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        max_position_size=50.0,
        max_risk_per_trade=1.0,  # 100% to allow testing position size limit
    )
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        config=config,
    )
    engine._balance = 100.0
    engine._allowance = 1000.0

    market = make_market()
    # First order
    engine.buy(market, side="UP", amount=30.0, confirm=False)
    # Second order should exceed max position size
    with pytest.raises(RiskLimitExceeded):
        engine.buy(market, side="UP", amount=30.0, confirm=False)


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_max_open_positions_limit(make_market):
    """Test max open positions limit."""
    config = RealTradingConfig(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        max_open_positions=2,
        max_risk_per_trade=1.0,  # 100% to allow testing open positions limit
    )
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        config=config,
    )
    engine._balance = 1000.0
    engine._allowance = 10000.0

    market1 = make_market(id="market-1", slug="market-1")
    market2 = make_market(id="market-2", slug="market-2")
    market3 = make_market(id="market-3", slug="market-3")

    engine.buy(market1, side="UP", amount=10.0, confirm=False)
    engine.buy(market2, side="UP", amount=10.0, confirm=False)
    # Third position should exceed max
    with pytest.raises(RiskLimitExceeded):
        engine.buy(market3, side="UP", amount=10.0, confirm=False)


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_max_risk_per_trade(make_market):
    """Test max risk per trade limit."""
    config = RealTradingConfig(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        max_risk_per_trade=0.05,  # 5%
    )
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        config=config,
    )
    engine._balance = 100.0
    engine._allowance = 1000.0

    market = make_market()
    # 10% of balance exceeds 5% max risk
    with pytest.raises(RiskLimitExceeded):
        engine.buy(market, side="UP", amount=10.0, confirm=False)


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_position_sizing_fixed(make_market):
    """Test fixed position sizing."""
    config = RealTradingConfig(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        position_sizing="fixed",
        fixed_amount=25.0,
        max_risk_per_trade=1.0,  # 100% to allow testing position sizing
    )
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        config=config,
    )
    engine._balance = 100.0
    engine._allowance = 1000.0

    market = make_market()
    order = engine.buy(market, side="UP", confirm=False)

    assert order.amount == 25.0


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_position_sizing_percentage(make_market):
    """Test percentage position sizing."""
    config = RealTradingConfig(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        position_sizing="percentage",
        percentage_of_balance=0.10,
        max_risk_per_trade=1.0,  # 100% to allow testing position sizing
    )
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        config=config,
    )
    engine._balance = 100.0
    engine._allowance = 1000.0

    market = make_market()
    order = engine.buy(market, side="UP", confirm=False)

    assert order.amount == 10.0  # 10% of 100


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_position_sizing_kelly(make_market):
    """Test Kelly position sizing."""
    config = RealTradingConfig(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        position_sizing="kelly",
        kelly_fraction=0.25,
        max_risk_per_trade=1.0,  # 100% to allow testing position sizing
    )
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        config=config,
    )
    engine._balance = 100.0
    engine._allowance = 1000.0

    market = make_market(prices=[0.55, 0.45])
    # High confidence should result in position
    order = engine.buy(market, side="UP", confidence=0.70, confirm=False)
    assert order.amount > 0

    # Low confidence should result in no position
    order2 = engine.buy(market, side="UP", confidence=0.50, confirm=False)
    assert order2.amount == 0


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_emergency_stop(engine, make_market):
    """Test emergency stop functionality."""
    market = make_market()
    engine.buy(market, side="UP", amount=10.0, confirm=False)

    engine.emergency_stop("Test emergency")
    assert engine.emergency_mode == True

    # Should not be able to place orders
    with pytest.raises(OrderCancelled):
        engine.buy(market, side="UP", amount=10.0, confirm=False)


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_resume_trading(make_market):
    """Test resume trading functionality."""
    config = RealTradingConfig(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        max_risk_per_trade=1.0,  # 100% to allow testing resume trading
    )
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        config=config,
    )
    engine._balance = 100.0
    engine._allowance = 1000.0

    engine.emergency_stop("Test emergency")
    assert engine.emergency_mode == True

    engine.resume_trading(confirm=False)
    assert engine.emergency_mode == False


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_cancel_order(engine, make_market):
    """Test order cancellation."""
    market = make_market()
    order = engine.buy(market, side="UP", amount=10.0, confirm=False)

    engine.cancel(order.id)
    assert order.status == "cancelled"


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_cancel_nonexistent_order():
    """Test canceling nonexistent order raises error."""
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
    )
    with pytest.raises(OrderNotFound):
        engine.cancel("nonexistent-id")


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_get_order(engine, make_market):
    """Test getting order by ID."""
    market = make_market()
    order = engine.buy(market, side="UP", amount=10.0, confirm=False)

    retrieved = engine.get_order(order.id)
    assert retrieved.id == order.id
    assert retrieved.side == "UP"


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_get_nonexistent_order():
    """Test getting nonexistent order raises error."""
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
    )
    with pytest.raises(OrderNotFound):
        engine.get_order("nonexistent-id")


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_open_orders(engine, make_market):
    """Test getting open orders."""
    market = make_market()
    order1 = engine.buy(market, side="UP", amount=10.0, confirm=False)
    order2 = engine.buy(market, side="DOWN", amount=10.0, confirm=False)

    open_orders = engine.open_orders()
    assert len(open_orders) == 2
    assert order1.id in [o.id for o in open_orders]
    assert order2.id in [o.id for o in open_orders]


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_positions(engine, make_market):
    """Test getting positions."""
    market = make_market()
    engine.buy(market, side="UP", amount=10.0, confirm=False)

    positions = engine.positions()
    assert len(positions) == 1
    assert positions[0].side == "UP"


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_get_position(engine, make_market):
    """Test getting position by market ID and side."""
    market = make_market()
    engine.buy(market, side="UP", amount=10.0, confirm=False)

    position = engine.get_position(market.id, "UP")
    assert position.side == "UP"
    assert position.market_id == market.id


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_get_nonexistent_position():
    """Test getting nonexistent position raises error."""
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
    )
    with pytest.raises(PositionNotFound):
        engine.get_position("nonexistent-market", "UP")


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_position_aggregation(make_market):
    """Test position aggregation."""
    config = RealTradingConfig(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        max_risk_per_trade=1.0,  # 100% to allow testing position aggregation
    )
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
        config=config,
    )
    engine._balance = 100.0
    engine._allowance = 1000.0

    market = make_market()
    engine.buy(market, side="UP", amount=10.0, confirm=False)
    engine.buy(market, side="UP", amount=10.0, confirm=False)

    positions = engine.positions()
    assert len(positions) == 1
    assert positions[0].shares > 0


@pytest.mark.requires_network
@pytest.mark.unit
def test_real_engine_refresh_balance():
    """Test balance refresh."""
    engine = RealTradingEngine(
        private_key="0x" + "1" * 64,
        rpc_url="https://polygon-rpc.com",
        polymarket_api_key="test-api-key",
    )
    engine.refresh_balance()
    # Should not raise any errors
