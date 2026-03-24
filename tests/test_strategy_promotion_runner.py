"""Wiring tests for ``maybe_promote_strategy_latest_after_backtest`` (no vectorbt)."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

_root = Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

# backtester on path for strategy_promotion_runner
_bt = _root / "backtester"
if str(_bt) not in sys.path:
    sys.path.insert(0, str(_bt))

from strategy_promotion_runner import maybe_promote_strategy_latest_after_backtest  # noqa: E402


class _DummyTable:
    def __init__(self):
        self.put_calls: list = []

    def get_item(self, **_kwargs):
        return {"Item": {"is_active": False}}

    def put_item(self, **kwargs):
        self.put_calls.append(kwargs)


class TestPromotionRunner(unittest.TestCase):
    def test_disabled_noop(self):
        table = _DummyTable()
        with patch.dict(os.environ, {"STRATEGY_PROMOTION_ENABLED": "false"}, clear=False):
            maybe_promote_strategy_latest_after_backtest(
                table=table,
                symbol="TSLA",
                new_threshold=0.75,
                current_threshold=0.70,
                new_sharpe=1.0,
                current_sharpe=0.5,
                article_count=25,
                strategy_name="Sentiment_V1",
                exit_type="fixed_time",
                hold_minutes=60,
                backtest_sort_key="20260101T000000Z",
            )
        self.assertEqual(table.put_calls, [])

    def test_enabled_all_gates_pass_put_item(self):
        table = _DummyTable()
        env = {
            "STRATEGY_PROMOTION_ENABLED": "true",
            "STRATEGY_PROMOTION_ALERT_SNS_TOPIC_ARN": "",
            "STRATEGY_PROMOTE_SET_ACTIVE": "false",
        }
        with patch.dict(os.environ, env, clear=False):
            maybe_promote_strategy_latest_after_backtest(
                table=table,
                symbol="TSLA",
                new_threshold=0.75,
                current_threshold=0.70,
                new_sharpe=1.2,
                current_sharpe=0.8,
                article_count=25,
                strategy_name="Sentiment_V1",
                exit_type="fixed_time",
                hold_minutes=60,
                backtest_sort_key="20260101T000000Z",
            )
        self.assertEqual(len(table.put_calls), 1)
        item = table.put_calls[0]["Item"]
        self.assertEqual(item["run_id"], "STRATEGY#TSLA")
        self.assertEqual(item["sort_key"], "LATEST")
        self.assertEqual(item["item_type"], "STRATEGY")

    def test_enabled_gate_fail_no_put(self):
        table = _DummyTable()
        env = {
            "STRATEGY_PROMOTION_ENABLED": "true",
            "STRATEGY_PROMOTION_ALERT_SNS_TOPIC_ARN": "",
        }
        with patch.dict(os.environ, env, clear=False):
            maybe_promote_strategy_latest_after_backtest(
                table=table,
                symbol="TSLA",
                new_threshold=0.50,
                current_threshold=0.70,
                new_sharpe=1.2,
                current_sharpe=0.8,
                article_count=25,
                strategy_name="Sentiment_V1",
                exit_type="fixed_time",
                hold_minutes=60,
                backtest_sort_key="20260101T000000Z",
            )
        self.assertEqual(table.put_calls, [])


if __name__ == "__main__":
    unittest.main()
