import importlib.util
import os
import unittest
from decimal import Decimal

from tests.stub_dependencies import install_stubs


def load_module_from_path(module_name: str, path: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class DummyTable:
    def __init__(self, items):
        self._items = items
        self.put_calls = []

    def query(self, **_kwargs):
        return {"Items": self._items}

    def get_item(self, **_kwargs):
        # No existing trade record
        return {}

    def put_item(self, *, Item=None, ConditionExpression=None):
        self.put_calls.append({"Item": Item, "ConditionExpression": ConditionExpression})


class TestTradeExecutor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        install_stubs()
        cls.mod = load_module_from_path(
            "trade_app",
            os.path.join("lambdas", "trade_executor", "app.py"),
        )

    def test_trade_executor_writes_decimal_sentiment_score(self):
        table_items = [
            {
                "symbol": "AAPL",
                "sentiment_score": 0.5,  # float from DynamoDB when using mocks
                "sentiment_label": "positive",
            }
        ]
        dummy_table = DummyTable(items=table_items)

        # Patch boto3.resource to return our dummy table.
        class DummyResource:
            def Table(self, _table_name):
                return dummy_table

        self.mod.boto3.resource = lambda _svc: DummyResource()  # type: ignore[attr-defined]

        # Avoid Alpaca calls: patch the trade client + order placement.
        self.mod._get_trade_client = lambda: object()  # type: ignore[attr-defined]
        self.mod._place_order = lambda **_kwargs: {"alpaca_order_id": "order-1", "submitted_at": "now"}  # type: ignore[attr-defined]
        self.mod._decide_side = lambda **_kwargs: "BUY"  # type: ignore[attr-defined]

        # Environment variables used by handler thresholds/sizing.
        os.environ["DYNAMODB_TABLE_NAME"] = "TradingNewsSentiment"
        os.environ["SENTIMENT_BUY_THRESHOLD"] = "0.2"
        os.environ["SENTIMENT_SELL_THRESHOLD"] = "-0.2"
        os.environ["ENABLE_SHORTS"] = "false"
        os.environ["TRADE_NOTIONAL_USD"] = "1000"
        os.environ["TRADE_QTY"] = ""
        os.environ["LOG_LEVEL"] = "INFO"

        # Minimal event/context.
        event = {"run_id": "run-1"}
        context = type("C", (), {"aws_request_id": "req-ctx"})()

        resp = self.mod.handler(event, context)
        self.assertEqual(resp["status"], "ok")
        self.assertGreaterEqual(len(dummy_table.put_calls), 1)

        written_item = dummy_table.put_calls[-1]["Item"]
        self.assertIsInstance(written_item["sentiment_score"], Decimal)
        self.assertEqual(written_item["sentiment_score"], Decimal(str(0.5)))


if __name__ == "__main__":
    unittest.main()

