"""
Abstract base for backtest + live trading strategies.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

import pandas as pd

try:
    from alpaca.trading.enums import OrderSide
except ImportError:  # pragma: no cover - backtest image should install alpaca-py
    from enum import Enum

    class OrderSide(str, Enum):
        """Minimal stand-in when alpaca-py is not installed."""

        BUY = "buy"
        SELL = "sell"


class BaseStrategy(ABC):
    strategy_name = "BaseStrategy"

    def set_params(self, **params: Any) -> None:
        for key, value in params.items():
            setattr(self, key, value)

    @abstractmethod
    def generate_signals(self, price_df: pd.DataFrame, news_df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        """
        Return (entries, exits) boolean Series indexed exactly like price_df.index.
        """
        raise NotImplementedError

    @abstractmethod
    def check_live_signal(
        self,
        current_prices: Any,
        current_news: Any,
        **kwargs: Any,
    ) -> Optional[OrderSide]:
        """
        Live decision at "now" using the same economic logic as the backtest where applicable.

        Args:
            current_prices: Recent OHLCV bars (typically a pandas DataFrame indexed by UTC time).
            current_news: Recent sentiment rows (DataFrame or list of row dicts); strategy-specific schema.
            **kwargs: Optional executor context (e.g. ``run_id``, ``symbol``) for logging.

        Returns:
            Alpaca ``OrderSide`` (e.g. BUY / SELL) or ``None`` for no trade.
        """
        raise NotImplementedError
