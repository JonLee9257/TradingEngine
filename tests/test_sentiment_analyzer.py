import importlib.util
import os
import sys
import types
import unittest
from datetime import datetime, timezone
from decimal import Decimal

from tests.stub_dependencies import install_stubs


def load_module_from_path(module_name: str, path: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


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

    def test_build_sentiment_item_payload_uses_decimal(self):
        run_id = "run-1"
        symbol = "AAPL"
        item = {"label": "positive", "score": 0.2, "rationale": "good news"}
        source = {"triggered_at": "t", "model": "m", "lambda_request_id": "req-1"}

        written = self.mod._build_sentiment_item_payload(
            run_id=run_id,
            symbol=symbol,
            item=item,
            source=source,
        )

        self.assertIsInstance(written["sentiment_score"], Decimal)
        self.assertEqual(written["sentiment_score"], Decimal(str(item["score"])))
        self.assertEqual(written["sentiment_label"], "positive")
        self.assertEqual(written["rationale"], "good news")

    def test_build_sentiment_item_payload_stores_market_price_fields(self):
        written = self.mod._build_sentiment_item_payload(
            run_id="run-1",
            symbol="TSLA",
            item={
                "label": "neutral",
                "score": 0.0,
                "rationale": "flat",
                "market_price": Decimal("250.12"),
                "market_price_fetched_at": "2026-03-18T12:00:00Z",
                "market_price_source": "alpaca_latest_trade",
                "market_session": "REGULAR",
                "is_regular_hours": True,
                "market_price_lookup_start_at": "2026-03-18T12:00:00Z",
            },
            source={"triggered_at": "t", "model": "m", "lambda_request_id": "req-1"},
        )
        self.assertEqual(written["market_price"], Decimal("250.12"))
        self.assertEqual(written["market_price_fetched_at"], "2026-03-18T12:00:00Z")
        self.assertEqual(written["market_price_source"], "alpaca_latest_trade")
        self.assertEqual(written["market_session"], "REGULAR")
        self.assertEqual(written["is_regular_hours"], True)
        self.assertEqual(written["market_price_lookup_start_at"], "2026-03-18T12:00:00Z")

    def test_market_session_classifier(self):
        # 2026-03-18 14:00 UTC == 10:00 ET (weekday regular session)
        self.assertEqual(
            self.mod._market_session_from_timestamp(datetime(2026, 3, 18, 14, 0, tzinfo=timezone.utc)),
            "REGULAR",
        )
        # 2026-03-18 11:00 UTC == 07:00 ET (pre-market)
        self.assertEqual(
            self.mod._market_session_from_timestamp(datetime(2026, 3, 18, 11, 0, tzinfo=timezone.utc)),
            "PRE",
        )

    def test_next_regular_open_weekday_fallback(self):
        # 2026-03-18 22:00 UTC == 18:00 ET (post-market), should map to next day 09:30 ET.
        out = self.mod._next_regular_open_utc(datetime(2026, 3, 18, 22, 0, tzinfo=timezone.utc))
        # With the unit-test pytz stub (-04:00), 09:30 ET == 13:30 UTC.
        self.assertEqual(out.hour, 13)
        self.assertEqual(out.minute, 30)

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

