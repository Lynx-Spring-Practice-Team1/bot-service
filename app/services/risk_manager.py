"""Risk-rule enforcement helpers.

All functions are pure (no I/O) so they are trivial to unit-test.
The bot engine reads per-session overrides from strategy_config and
passes them in; the constants here are the defaults only.
"""

from __future__ import annotations

from decimal import ROUND_DOWN, Decimal

DEFAULT_MAX_OPEN_ORDERS: int = 3
DEFAULT_STOP_LOSS_PCT: float = 0.02    # 2 %
DEFAULT_MAX_BALANCE_PCT: float = 0.20  # 20 %
DEFAULT_DAILY_LOSS_LIMIT: float = 0.05 # 5 % of balance
DEFAULT_MAX_DRAWDOWN: float = 0.10     # 10 % from peak

# Matches OrderStatus enum in order-service/app/models.py
_OPEN_STATUSES: frozenset[str] = frozenset({"PENDING", "ACCEPTED"})


def can_place_order(orders: list[dict], max_open: int = DEFAULT_MAX_OPEN_ORDERS) -> bool:
    """Return True when the number of open orders is below the cap."""
    open_count = sum(1 for o in orders if o.get("status") in _OPEN_STATUSES)
    return open_count < max_open


def is_stop_loss_triggered(
    entry_price: float,
    current_price: float,
    position_side: str,
    stop_loss_pct: float = DEFAULT_STOP_LOSS_PCT,
) -> bool:
    """Return True when the unrealised loss exceeds the threshold."""
    if position_side == "BUY":
        loss_pct = (entry_price - current_price) / entry_price
    else:
        loss_pct = (current_price - entry_price) / entry_price
    return loss_pct >= stop_loss_pct


def is_daily_loss_exceeded(
    daily_pnl: float,
    balance: float,
    limit_pct: float = DEFAULT_DAILY_LOSS_LIMIT,
) -> bool:
    """Return True when today's realized losses exceed `limit_pct` of current balance."""
    if balance <= 0:
        return False
    return (-daily_pnl / balance) >= limit_pct


def is_max_drawdown_exceeded(
    current_balance: float,
    peak_balance: float,
    max_drawdown_pct: float = DEFAULT_MAX_DRAWDOWN,
) -> bool:
    """Return True when balance has fallen `max_drawdown_pct` below its daily peak."""
    if peak_balance <= 0:
        return False
    return (peak_balance - current_balance) / peak_balance >= max_drawdown_pct


def compute_quantity(
    available_cash: float,
    price: float,
    max_balance_pct: float = DEFAULT_MAX_BALANCE_PCT,
) -> float:
    """Return the maximum order quantity within the balance cap.

    Uses Decimal arithmetic to avoid IEEE-754 rounding errors that would
    accumulate across high-frequency order sizing calculations (L-01).
    Always rounds down (never over-spends the cap).
    """
    avail = Decimal(str(available_cash))
    p = Decimal(str(price))
    pct = Decimal(str(max_balance_pct))

    if avail <= 0 or p <= 0:
        return 0.0

    max_spend = avail * pct
    qty = (max_spend / p).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return float(qty)
