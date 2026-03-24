import importlib.util
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from tests.stub_dependencies import install_stubs

# Repo root so `strategies` resolves when loading `lambdas/trade_executor/app.py`.
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

try:
    import pandas  # noqa: F401
except ImportError:
    pandas = None


def load_module_from_path(module_name: str, path: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def _sentiment_row(*, run_id: str, symbol: str, hours_ago: float = 1.0, score: float = 0.75) -> dict:
    """GSI window uses ``now`` at handler time; keep article inside last 7d."""
    pub = (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).replace(microsecond=0)
    pub_iso = pub.isoformat().replace("+00:00", "Z")
    sym = symbol.upper()
    return {
        "run_id": run_id,
        "sort_key": f"SENTIMENT#{sym}",
        "item_type": "SENTIMENT",
        "symbol": sym,
        "sentiment_score": Decimal(str(score)),
        "sentiment_label": "positive",
        "news_published_at": pub_iso,
        "gsi_pk": sym,
        "gsi_sk": pub_iso,
    }


class DummyTable:
    """Supports base-table Query (batch) and GSI Query (7d window) returning the same logical rows."""

    def __init__(self, items, trade_keys_existing: frozenset | None = None):
        self._items = items
        self.put_calls: list = []
        self.update_calls: list = []
        self._trade_existing = trade_keys_existing or frozenset()

    def query(self, **_kwargs):
        return {"Items": list(self._items), "LastEvaluatedKey": None}

    def get_item(self, **kwargs):
        key = kwargs.get("Key") or {}
        run_id = key.get("run_id", "")
        sort_key = key.get("sort_key", "")
        if str(run_id).startswith("STRATEGY#") and sort_key == "LATEST":
            return {"Item": getattr(self, "_strategy_item", {})}
        if (run_id, sort_key) in self._trade_existing:
            return {"Item": {"run_id": run_id, "sort_key": sort_key, "status": "OPEN"}}
        return {}

    def put_item(self, *, Item=None, ConditionExpression=None):
        self.put_calls.append({"Item": Item, "ConditionExpression": ConditionExpression})

    def update_item(self, **kwargs):
        self.update_calls.append(kwargs)


@unittest.skipUnless(pandas is not None, "pandas required (trade executor uses strategies package)")
class TestTradeExecutor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        install_stubs()
        cls.mod = load_module_from_path(
            "trade_app",
            os.path.join("lambdas", "trade_executor", "app.py"),
        )

    def setUp(self):
        self._env_patches = [
            patch.dict(
                os.environ,
                {
                    "DYNAMODB_TABLE_NAME": "TradingNewsSentiment",
                    "SENTIMENT_BUY_THRESHOLD": "0.2",
                    "SENTIMENT_SELL_THRESHOLD": "-0.2",
                    "ENABLE_SHORTS": "false",
                    "TRADE_NOTIONAL_USD": "1000",
                    "TRADE_QTY": "",
                    "TRADE_ONLY_AT_CLOSE": "false",
                    "LOG_LEVEL": "INFO",
                },
                clear=False,
            )
        ]
        for p in self._env_patches:
            p.start()
            self.addCleanup(p.stop)

    def _patch_aws_and_alpaca(self, dummy_table: DummyTable):
        class DummyResource:
            def Table(self, _table_name):
                return dummy_table

        self.mod.boto3.resource = lambda _svc: DummyResource()  # type: ignore[attr-defined]
        self.mod._get_trade_client = lambda: object()  # type: ignore[attr-defined]
        self.mod._place_order = lambda **_kwargs: {"alpaca_order_id": "order-1", "submitted_at": "now"}  # type: ignore[attr-defined]
        self.mod._account_pnl_snapshot = lambda _tc: {  # type: ignore[attr-defined]
            "equity_usd": 100_000.0,
            "account_status_code": 2.0,
            "account_status": "PAPER_ONLY",
        }
        self.mod._publish_trading_metrics = lambda *_a, **_k: None  # type: ignore[attr-defined]

    def test_trade_executor_writes_decimal_sentiment_score(self):
        row = _sentiment_row(run_id="run-1", symbol="AAPL", score=0.5)
        dummy_table = DummyTable(items=[row])
        dummy_table._strategy_item = {
            "is_active": True,
            "optimized_threshold": Decimal("0.3"),
            "strategy_name": "Sentiment_V1",
            "exit_type": "fixed_time",
            "hold_minutes": 60,
        }
        self._patch_aws_and_alpaca(dummy_table)

        event = {"run_id": "run-1"}
        context = type("C", (), {"aws_request_id": "req-ctx"})()

        resp = self.mod.handler(event, context)
        self.assertEqual(resp["status"], "ok")
        self.assertIn("account_snapshot", resp)
        self.assertEqual(resp["account_snapshot"]["equity_usd"], 100_000.0)
        self.assertGreaterEqual(len(dummy_table.put_calls), 1)

        written_item = dummy_table.put_calls[-1]["Item"]
        self.assertIsInstance(written_item["sentiment_score"], Decimal)
        self.assertEqual(written_item["sentiment_score"], Decimal(str(0.5)))
        self.assertEqual(written_item.get("strategy_name"), "Sentiment_V1")
        self.assertEqual(written_item.get("exit_type"), "fixed_time")
        self.assertEqual(written_item.get("status"), "OPEN")
        self.assertIsNotNone(written_item.get("hold_minutes"))

    def test_no_sentiment_returns_zero_trades(self):
        dummy_table = DummyTable(items=[])
        dummy_table._strategy_item = {"is_active": True, "optimized_threshold": Decimal("0.1"), "strategy_name": "Sentiment_V1"}
        self._patch_aws_and_alpaca(dummy_table)

        resp = self.mod.handler({"run_id": "run-empty"}, type("C", (), {"aws_request_id": "r"})())
        self.assertEqual(resp["trades"], 0)
        self.assertEqual(dummy_table.put_calls, [])

    def test_inactive_strategy_skips_trade(self):
        row = _sentiment_row(run_id="run-2", symbol="TSLA")
        dummy_table = DummyTable(items=[row])
        dummy_table._strategy_item = {
            "is_active": False,
            "optimized_threshold": Decimal("0.3"),
            "strategy_name": "Sentiment_V1",
        }
        self._patch_aws_and_alpaca(dummy_table)

        resp = self.mod.handler({"run_id": "run-2"}, type("C", (), {"aws_request_id": "r"})())
        self.assertEqual(resp["trades"], 0)
        self.assertEqual(dummy_table.put_calls, [])

    def test_idempotent_skip_when_trade_record_exists(self):
        row = _sentiment_row(run_id="run-3", symbol="MSFT")
        dummy_table = DummyTable(
            items=[row],
            trade_keys_existing=frozenset({("run-3", "TRADE#MSFT")}),
        )
        dummy_table._strategy_item = {
            "is_active": True,
            "optimized_threshold": Decimal("0.3"),
            "strategy_name": "Sentiment_V1",
            "exit_type": "fixed_time",
            "hold_minutes": 60,
        }
        self._patch_aws_and_alpaca(dummy_table)

        resp = self.mod.handler({"run_id": "run-3"}, type("C", (), {"aws_request_id": "r"})())
        self.assertEqual(resp["trades"], 0)
        self.assertEqual(dummy_table.put_calls, [])

    def test_side_none_skips_put_item(self):
        row = _sentiment_row(run_id="run-4", symbol="NVDA", score=0.01)
        dummy_table = DummyTable(items=[row])
        dummy_table._strategy_item = {
            "is_active": True,
            "optimized_threshold": Decimal("0.95"),
            "strategy_name": "Sentiment_V1",
            "exit_type": "fixed_time",
            "hold_minutes": 60,
        }
        self._patch_aws_and_alpaca(dummy_table)

        resp = self.mod.handler({"run_id": "run-4"}, type("C", (), {"aws_request_id": "r"})())
        self.assertEqual(resp["trades"], 0)
        self.assertEqual(dummy_table.put_calls, [])

    def test_missing_run_id_raises(self):
        dummy_table = DummyTable(items=[])
        self._patch_aws_and_alpaca(dummy_table)
        with self.assertRaises(ValueError):
            self.mod.handler({}, type("C", (), {"aws_request_id": "r"})())


if __name__ == "__main__":
    unittest.main()
