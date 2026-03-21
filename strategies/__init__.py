"""Shared trading strategies (backtest + live)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from strategies.base import BaseStrategy, OrderSide
from strategies.sentiment_v1 import MorningSentimentStrategy

# Registry: DynamoDB ``strategy_name`` (or default) -> strategy class
STRATEGY_MAP: dict[str, type[BaseStrategy]] = {
    "Sentiment_V1": MorningSentimentStrategy,
}

__all__ = [
    "BaseStrategy",
    "OrderSide",
    "MorningSentimentStrategy",
    "STRATEGY_MAP",
    "build_strategy_from_config",
]


def build_strategy_from_config(
    strategy_config: dict[str, Any],
    *,
    buy_threshold: Decimal,
    sell_threshold: Decimal,
    enable_shorts: bool,
) -> BaseStrategy:
    """
    Factory: instantiate the strategy class for this symbol's Dynamo config.

    Expects keys from ``STRATEGY#<SYMBOL>`` / ``LATEST`` (see trade executor).
    Each registered class must implement ``from_executor_config(...) -> BaseStrategy``.
    """
    raw = strategy_config.get("strategy_name") or "Sentiment_V1"
    key = str(raw).strip()
    cls = STRATEGY_MAP.get(key)
    if cls is None:
        raise ValueError(f"Unknown strategy_name={key!r}; known={sorted(STRATEGY_MAP)}")
    factory = getattr(cls, "from_executor_config", None)
    if not callable(factory):
        raise TypeError(f"Strategy {cls.__name__} must define callable from_executor_config for the executor")
    return factory(  # type: ignore[no-any-return]
        strategy_config,
        buy_threshold=buy_threshold,
        sell_threshold=sell_threshold,
        enable_shorts=enable_shorts,
    )
