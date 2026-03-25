import importlib.util
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from tests.stub_dependencies import install_stubs

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


class ExitDummyTable:
    def __init__(self, scan_items: list):
        self._scan_items = scan_items
        self.update_calls: list = []

    def scan(self, **_kwargs):
        return {"Items": list(self._scan_items), "LastEvaluatedKey": None}

    def query(self, **_kwargs):
        return {"Items": [], "LastEvaluatedKey": None}

    def get_item(self, **_kwargs):
        return {}

    def update_item(self, **kwargs):
        self.update_calls.append(kwargs)


@unittest.skipUnless(pandas is not None, "pandas required (exit_manager loads strategies)")
class TestExitManagerPure(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        install_stubs()
        cls.mod = load_module_from_path(
            "exit_app",
            os.path.join("lambdas", "exit_manager", "app.py"),
        )

    def test_should_exit_fixed_time_when_deadline_passed(self):
        sub = datetime.now(timezone.utc) - timedelta(hours=2)
        item = {
            "submitted_at": sub.isoformat(),
            "hold_minutes": Decimal("60"),
        }
        self.assertTrue(self.mod._should_exit_fixed_time(item, now_utc=datetime.now(timezone.utc)))

    def test_should_exit_fixed_time_when_not_due(self):
        sub = datetime.now(timezone.utc) - timedelta(minutes=30)
        item = {
            "submitted_at": sub.isoformat(),
            "hold_minutes": Decimal("60"),
        }
        self.assertFalse(self.mod._should_exit_fixed_time(item, now_utc=datetime.now(timezone.utc)))

    def test_entry_is_long(self):
        self.assertTrue(self.mod._entry_is_long("buy"))
        self.assertFalse(self.mod._entry_is_long("sell"))


@unittest.skipUnless(pandas is not None, "pandas required (exit_manager loads strategies)")
class TestExitManagerHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        install_stubs()
        cls.mod = load_module_from_path(
            "exit_app",
            os.path.join("lambdas", "exit_manager", "app.py"),
        )

    def test_handler_fixed_time_closes_and_updates_ddb(self):
        sub = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        trade = {
            "run_id": "run-x",
            "sort_key": "TRADE#TSLA",
            "item_type": "TRADE",
            "status": "OPEN",
            "symbol": "TSLA",
            "exit_type": "fixed_time",
            "hold_minutes": Decimal("60"),
            "submitted_at": sub,
            "side": "BUY",
        }
        table = ExitDummyTable([trade])

        class DummyResource:
            def Table(self, _name):
                return table

        self.mod.boto3.resource = lambda _svc: DummyResource()  # type: ignore[attr-defined]

        class DummyClock:
            is_open = False
            timestamp = None
            next_close = None

        class DummyClient:
            def get_clock(self):
                return DummyClock()

            def get_all_positions(self):
                return []

            def submit_order(self, _req):
                raise AssertionError("should not submit when no position")

        self.mod._get_trade_client = lambda: DummyClient()  # type: ignore[attr-defined]
        self.mod._close_position_at_alpaca = lambda **kwargs: ("", 0.0)  # type: ignore[attr-defined]

        os.environ["DYNAMODB_TABLE_NAME"] = "T"
        os.environ.pop("STRATEGY_TABLE_NAME", None)

        resp = self.mod.handler({}, None)
        self.assertEqual(resp["closed"], 1)
        self.assertEqual(resp["errors"], 0)
        self.assertEqual(len(table.update_calls), 1)
        vals = table.update_calls[0]["ExpressionAttributeValues"]
        self.assertEqual(vals[":closed"], "CLOSED")
        self.assertIn("fixed_time", str(vals.get(":er", "")))


if __name__ == "__main__":
    unittest.main()
