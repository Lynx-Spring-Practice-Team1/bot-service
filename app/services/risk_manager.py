"""Risk-rule enforcement helpers.

All functions are pure (no I/O) so they are trivial to unit-test.
The bot engine reads per-session overrides from strategy_config and
passes them in; the constants here are the defaults only.
"""

from __future__ import annotations

from decimal import ROUND_DOWN, Decimal

DEFAULT_MAX_OPEN_ORDERS: int = 3
DEFAULT_STOP_LOSS_PCT: float = 0.02   # 2 %
DEFAULT_MAX_BALANCE_PCT: float = 0.20  # 20 %

_OPEN_STATUSES: frozenset[str] = frozenset(
    {"open", "pending", "NEW", "PENDING", "OPEN", "submitted"}
)


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
