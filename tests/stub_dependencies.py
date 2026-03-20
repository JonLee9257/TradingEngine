"""
Test helper: installs minimal stub modules so unit tests can import
lambda code without requiring the full AWS/Anthropic/Alpaca SDKs locally.
"""

from __future__ import annotations

import sys
import types
from decimal import Decimal


def install_stubs() -> None:
    # ---- pydantic (only needed for model class definitions) ----
    if "pydantic" not in sys.modules:
        pydantic_stub = types.ModuleType("pydantic")

        class BaseModel:  # noqa: D401 - minimal stub
            pass

        pydantic_stub.BaseModel = BaseModel
        sys.modules["pydantic"] = pydantic_stub

    # ---- boto3 ----
    if "boto3" not in sys.modules:
        boto3_stub = types.ModuleType("boto3")

        def _not_implemented(*_args, **_kwargs):
            raise RuntimeError("boto3 stub: this should be patched/mocked in tests.")

        boto3_stub.resource = _not_implemented
        boto3_stub.client = _not_implemented
        sys.modules["boto3"] = boto3_stub

    # boto3.dynamodb.conditions for Attr/Key imports
    if "boto3.dynamodb.conditions" not in sys.modules:
        dyn_stub = types.ModuleType("boto3.dynamodb")
        cond_stub = types.ModuleType("boto3.dynamodb.conditions")

        class _Condition:
            def __init__(self, expr: str):
                self.expr = expr

            def __and__(self, other: "_Condition") -> "_Condition":
                return _Condition(f"({self.expr} AND {other.expr})")

        class Attr:
            def __init__(self, name: str):
                self.name = name

            def not_exists(self) -> _Condition:
                return _Condition(f"attribute_not_exists({self.name})")

        class Key:
            def __init__(self, name: str):
                self.name = name

            def eq(self, value) -> _Condition:
                return _Condition(f"{self.name} = {value!r}")

            def begins_with(self, value) -> _Condition:
                return _Condition(f"begins_with({self.name}, {value!r})")

        cond_stub.Attr = Attr
        cond_stub.Key = Key

        sys.modules["boto3.dynamodb"] = dyn_stub
        sys.modules["boto3.dynamodb.conditions"] = cond_stub

    # ---- botocore.exceptions ----
    if "botocore" not in sys.modules:
        botocore_stub = types.ModuleType("botocore")
        exceptions_stub = types.ModuleType("botocore.exceptions")

        class ClientError(Exception):
            pass

        exceptions_stub.ClientError = ClientError
        botocore_stub.exceptions = exceptions_stub

        sys.modules["botocore"] = botocore_stub
        sys.modules["botocore.exceptions"] = exceptions_stub

    # ---- anthropic ----
    if "anthropic" not in sys.modules:
        anthropic_stub = types.ModuleType("anthropic")

        class Anthropic:  # pragma: no cover
            def __init__(self, *args, **kwargs):
                raise RuntimeError("anthropic stub should be patched in tests")

        anthropic_stub.Anthropic = Anthropic
        sys.modules["anthropic"] = anthropic_stub

    # ---- requests ----
    if "requests" not in sys.modules:
        requests_stub = types.ModuleType("requests")

        class _DummyResponse:
            status_code = 200
            text = "{}"

            def raise_for_status(self):
                return None

            def json(self):
                return {}

        def get(*_args, **_kwargs):  # pragma: no cover
            return _DummyResponse()

        requests_stub.get = get
        sys.modules["requests"] = requests_stub

    # ---- pytz ----
    if "pytz" not in sys.modules:
        pytz_stub = types.ModuleType("pytz")

        def timezone(_name: str):  # pragma: no cover
            from datetime import timedelta, timezone as _tz

            # Minimal stub sufficient for astimezone() in unit tests.
            if _name == "America/New_York":
                return _tz(timedelta(hours=-4))
            return _tz.utc

        pytz_stub.timezone = timezone
        sys.modules["pytz"] = pytz_stub

    # ---- alpaca-py ----
    if "alpaca" not in sys.modules:
        alpaca_stub = types.ModuleType("alpaca")
        trading_stub = types.ModuleType("alpaca.trading")
        client_stub = types.ModuleType("alpaca.trading.client")
        enums_stub = types.ModuleType("alpaca.trading.enums")
        requests_stub = types.ModuleType("alpaca.trading.requests")

        class TradingClient:  # pragma: no cover
            def __init__(self, *args, **kwargs):
                raise RuntimeError("alpaca stub should be patched in tests")

        class OrderSide:
            BUY = "BUY"
            SELL = "SELL"

        class TimeInForce:
            DAY = "DAY"

        class AccountStatus:
            ACTIVE = "ACTIVE"
            PAPER_ONLY = "PAPER_ONLY"
            DISABLED = "DISABLED"

        class MarketOrderRequest:  # pragma: no cover
            def __init__(self, *args, **kwargs):
                pass

        client_stub.TradingClient = TradingClient
        enums_stub.OrderSide = OrderSide
        enums_stub.TimeInForce = TimeInForce
        enums_stub.AccountStatus = AccountStatus
        requests_stub.MarketOrderRequest = MarketOrderRequest

        sys.modules["alpaca"] = alpaca_stub
        sys.modules["alpaca.trading"] = trading_stub
        sys.modules["alpaca.trading.client"] = client_stub
        sys.modules["alpaca.trading.enums"] = enums_stub
        sys.modules["alpaca.trading.requests"] = requests_stub

