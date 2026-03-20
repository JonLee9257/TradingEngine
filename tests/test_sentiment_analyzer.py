import importlib.util
import os
import sys
import types
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
    def __init__(self):
        self.last_put_item = None

    def put_item(self, *, Item=None, ConditionExpression=None):
        self.last_put_item = {"Item": Item, "ConditionExpression": ConditionExpression}


class DummyParseResp:
    def __init__(self, parsed_output):
        self.parsed_output = parsed_output


class DummyParsedOutput:
    def __init__(self, payload: dict):
        self._payload = payload

    # pydantic v2 style
    def model_dump(self):
        return self._payload


class TestSentimentAnalyzer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        install_stubs()
        cls.mod = load_module_from_path(
            "sentiment_app",
            os.path.join("lambdas", "sentiment_analyzer", "app.py"),
        )

    def test_put_sentiment_item_uses_decimal(self):
        table = DummyTable()
        run_id = "run-1"
        symbol = "AAPL"
        item = {"label": "positive", "score": 0.2, "rationale": "good news"}
        source = {"triggered_at": "t", "model": "m", "lambda_request_id": "req-1"}

        self.mod._put_sentiment_item(
            table=table,
            run_id=run_id,
            symbol=symbol,
            item=item,
            source=source,
        )

        written = table.last_put_item["Item"]
        self.assertIsInstance(written["sentiment_score"], Decimal)
        self.assertEqual(written["sentiment_score"], Decimal(str(item["score"])))
        self.assertEqual(written["sentiment_label"], "positive")
        self.assertEqual(written["rationale"], "good news")

    def test_put_sentiment_item_stores_market_price_fields(self):
        table = DummyTable()
        self.mod._put_sentiment_item(
            table=table,
            run_id="run-1",
            symbol="TSLA",
            item={
                "label": "neutral",
                "score": 0.0,
                "rationale": "flat",
                "market_price": Decimal("250.12"),
                "market_price_fetched_at": "2026-03-18T12:00:00Z",
                "market_price_source": "alpaca_latest_trade",
            },
            source={"triggered_at": "t", "model": "m", "lambda_request_id": "req-1"},
        )
        written = table.last_put_item["Item"]
        self.assertEqual(written["market_price"], Decimal("250.12"))
        self.assertEqual(written["market_price_fetched_at"], "2026-03-18T12:00:00Z")
        self.assertEqual(written["market_price_source"], "alpaca_latest_trade")

    def test_analyze_sentiment_uses_structured_parse(self):
        # Patch the Anthropic client used inside the module.
        class DummyMessages:
            def __init__(self):
                self.calls = []

            def parse(self, **kwargs):
                self.calls.append(kwargs)
                parsed = DummyParsedOutput(
                    {
                        "results": [
                            {
                                "symbol": "AAPL",
                                "label": "positive",
                                "score": 0.3,
                                "rationale": "ok",
                            }
                        ]
                    }
                )
                return DummyParseResp(parsed)

        class DummyAnthropic:
            def __init__(self, *args, **kwargs):
                self.messages = DummyMessages()

        dummy = DummyAnthropic()
        # When _analyze_sentiment_with_claude constructs Anthropic(), return our dummy.
        self.mod.Anthropic = lambda api_key: dummy  # type: ignore[attr-defined]

        os.environ["ANTHROPIC_API_KEY"] = "dummy"
        os.environ["ANTHROPIC_MODEL"] = "claude-haiku-4-5-20251001"

        results = self.mod._analyze_sentiment_with_claude(
            symbols=["AAPL"],
            articles_by_symbol={"AAPL": [{"title": "t", "description": "d", "publishedAt": "now", "url": ""}]},
        )

        self.assertIn("results", results)
        self.assertEqual(results["results"][0]["symbol"], "AAPL")

        # Ensure structured parsing was requested.
        call = dummy.messages.calls[-1]
        self.assertIn("output_format", call)


if __name__ == "__main__":
    unittest.main()

