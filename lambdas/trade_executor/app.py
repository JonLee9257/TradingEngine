"""
Lambda 3: Trade Executor

What it does:
- Receives a `run_id` (invoked by the sentiment analyzer)
- Looks up all SENTIMENT items for that run in DynamoDB
- Loads STRATEGY#<SYMBOL>/LATEST from DynamoDB and instantiates a registered strategy (STRATEGY_MAP)
- Delegates buy/sell/none to ``strategy.check_live_signal(...)``
- Places paper trades via Alpaca
- Writes a TRADE record per symbol (idempotently) so retries don't duplicate orders
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import boto3
import requests
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AccountStatus, OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr, Key

# Production: ``strategies`` comes from Lambda layer at /opt/python. Local: repo ``layers/strategies/python``.
_here = Path(__file__).resolve().parent
if not (_here / "strategies").is_dir():
    _repo_root = _here.parent.parent
    _layer_python = _repo_root / "layers" / "strategies" / "python"
    if _layer_python.is_dir() and str(_layer_python) not in sys.path:
        sys.path.insert(0, str(_layer_python))

from strategies import STRATEGY_MAP, build_strategy_from_config  # noqa: E402

# Logs go to CloudWatch.
logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

try:
    # Optional local-dev support.
    from dotenv import load_dotenv

    # For local testing only: load env vars from a `.env` file if present.
    load_dotenv()
except Exception:
    pass


def _utc_now_iso() -> str:
    """Return current UTC time as ISO 8601 string for DynamoDB records."""
    return datetime.now(timezone.utc).isoformat()


def _backoff_sleep(attempt: int) -> None:
    """Sleep with exponential backoff between Alpaca API retries."""
    base = float(os.getenv("ALPACA_BACKOFF_BASE_SECONDS", "0.5"))
    max_sleep = float(os.getenv("ALPACA_BACKOFF_MAX_SECONDS", "10"))
    sleep_s = min(max_sleep, base * (2 ** (attempt - 1)))
    time.sleep(sleep_s)


def _get_env_float(name: str, default: float) -> float:
    """Read a float from environment variables, returning default if missing or invalid."""
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def _get_trade_client() -> TradingClient:
    """Create an Alpaca TradingClient using API credentials from environment variables."""
    api_key = os.environ["ALPACA_API_KEY"]
    api_secret = os.environ["ALPACA_SECRET_KEY"]
    paper = os.getenv("ALPACA_PAPER", "true").lower() == "true"
    # `paper=True` means "paper trading" (no real money) when supported by alpaca-py.
    return TradingClient(api_key=api_key, secret_key=api_secret, paper=paper)


def _parse_money_field(value: object) -> Optional[float]:
    """Alpaca returns many money fields as strings; normalize to float for math/logging."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _account_status_code(status: object) -> float:
    """
    Map Alpaca account status to a number for CloudWatch metrics (no string metric values).

    Rough scale: 0 = blocked/closed, 1 = active trading, 2 = paper-only, 5 = onboarding/pending.
    """
    if status is None:
        return 0.0
    if isinstance(status, AccountStatus):
        key = status.value
    else:
        key = str(status).strip().upper()
        if "." in key:
            key = key.split(".")[-1]

    terminal_bad = frozenset(
        {
            "DISABLED",
            "ACCOUNT_CLOSED",
            "REJECTED",
            "INACTIVE",
            "SUBMISSION_FAILED",
        }
    )
    if key in terminal_bad:
        return 0.0
    if key in ("ACTIVE", "APPROVED"):
        return 1.0
    if key == "PAPER_ONLY":
        return 2.0
    return 5.0


def _sum_positions_unrealized_pl(trading_client: TradingClient) -> Optional[float]:
    """Aggregate open-position unrealized P&L (Alpaca exposes PL per position, not on TradeAccount)."""
    try:
        positions = trading_client.get_all_positions()
    except Exception:  # noqa: BLE001
        return None
    total = 0.0
    for pos in positions:
        pl = _parse_money_field(getattr(pos, "unrealized_pl", None))
        if pl is not None:
            total += pl
    return total


def _account_pnl_snapshot(trading_client: TradingClient) -> dict:
    """
    Read Alpaca account + positions for P&L / exposure (logs + future CloudWatch custom metrics).

    Numbers-only fields are safe for PutMetricData. `account_status` is a string for logs/JSON only.
    """
    account = trading_client.get_account()
    equity = _parse_money_field(getattr(account, "equity", None))
    last_equity = _parse_money_field(getattr(account, "last_equity", None))
    day_pl: Optional[float] = None
    if equity is not None and last_equity is not None:
        day_pl = equity - last_equity

    day_pl_pct: Optional[float] = None
    if day_pl is not None and last_equity is not None and abs(last_equity) > 1e-9:
        day_pl_pct = (day_pl / last_equity) * 100.0

    # API may include these on account; SDK model might omit them — use getattr.
    realized_pl = _parse_money_field(getattr(account, "realized_pl", None))
    account_unrealized = _parse_money_field(getattr(account, "unrealized_pl", None))
    positions_unrealized = _sum_positions_unrealized_pl(trading_client)
    unrealized_pl = account_unrealized if account_unrealized is not None else positions_unrealized

    raw_status = getattr(account, "status", None)
    status_str = raw_status.value if isinstance(raw_status, AccountStatus) else str(raw_status or "")

    return {
        "equity_usd": equity,
        "last_equity_usd": last_equity,
        "day_pl_usd": day_pl,
        "day_pl_pct": day_pl_pct,
        "realized_pl_usd": realized_pl,
        "unrealized_pl_usd": unrealized_pl,
        "cash_usd": _parse_money_field(getattr(account, "cash", None)),
        "long_market_value_usd": _parse_money_field(getattr(account, "long_market_value", None)),
        "short_market_value_usd": _parse_money_field(getattr(account, "short_market_value", None)),
        "buying_power_usd": _parse_money_field(getattr(account, "buying_power", None)),
        "initial_margin_usd": _parse_money_field(getattr(account, "initial_margin", None)),
        "maintenance_margin_usd": _parse_money_field(getattr(account, "maintenance_margin", None)),
        "account_status_code": _account_status_code(raw_status),
        "account_status": status_str,
    }


def _snapshot_key_to_metric_name(key: str) -> str:
    """CloudWatch metric names: EquityUsd, DayPlPct, ... (from equity_usd, day_pl_pct)."""
    return "".join(part.capitalize() for part in key.split("_"))


def _metric_unit_for_snapshot_key(key: str) -> str:
    if key == "day_pl_pct":
        return "Percent"
    return "None"


def _publish_trading_metrics(snap: dict, *, run_id: str) -> None:
    """
    Emit numeric snapshot fields as custom CloudWatch metrics (for dashboards / alarms).

    Skips `account_status` (string). Failures are logged only — trading must not depend on CW.
    """
    raw = os.getenv("PUBLISH_CLOUDWATCH_METRICS", "true").lower()
    if raw in ("0", "false", "no", "off"):
        return

    namespace = os.getenv("CLOUDWATCH_METRIC_NAMESPACE", "Trading/Paper").strip() or "Trading/Paper"

    dimensions = [
        {
            "Name": "PaperTrading",
            "Value": "true" if os.getenv("ALPACA_PAPER", "true").lower() == "true" else "false",
        },
    ]

    metric_data: list[dict] = []
    for key, val in snap.items():
        if key == "account_status":
            continue
        if val is None or isinstance(val, bool):
            continue
        if not isinstance(val, (int, float)):
            continue
        metric_data.append(
            {
                "MetricName": _snapshot_key_to_metric_name(key),
                "Dimensions": dimensions,
                "Value": float(val),
                "Unit": _metric_unit_for_snapshot_key(key),
            }
        )

    if not metric_data:
        return

    try:
        boto3.client("cloudwatch").put_metric_data(Namespace=namespace, MetricData=metric_data)
        logger.debug(
            "Published trading metrics to CloudWatch",
            extra={"run_id": run_id, "namespace": namespace, "count": len(metric_data)},
        )
    except Exception:  # noqa: BLE001
        logger.warning(
            "Failed to publish trading metrics to CloudWatch",
            extra={"run_id": run_id, "namespace": namespace},
            exc_info=True,
        )


def _snapshot_and_publish_telemetry(trading_client: TradingClient, run_id: str) -> Optional[dict]:
    """Fetch Alpaca snapshot, log it, publish CloudWatch metrics; returns None on Alpaca failure."""
    try:
        snap = _account_pnl_snapshot(trading_client)
        logger.info("Alpaca account P&L snapshot", extra={"run_id": run_id, **snap})
        _publish_trading_metrics(snap, run_id=run_id)
        return snap
    except Exception:  # noqa: BLE001
        logger.exception("Failed to fetch Alpaca account snapshot", extra={"run_id": run_id})
        return None


def _place_order(*, trading_client: TradingClient, symbol: str, side: OrderSide) -> dict:
    """
    Place a market order. Uses either TRADE_QTY or TRADE_NOTIONAL_USD.
    """
    max_attempts = int(os.getenv("ALPACA_MAX_ATTEMPTS", "3"))
    time_in_force = TimeInForce.DAY

    # You can choose trade sizing by either:
    # - TRADE_QTY (share count)
    # - TRADE_NOTIONAL_USD (dollar amount)
    qty_raw = os.getenv("TRADE_QTY")
    notional_raw = os.getenv("TRADE_NOTIONAL_USD")

    qty = int(qty_raw) if qty_raw not in (None, "") else None
    notional = float(notional_raw) if notional_raw not in (None, "") else None

    # At least one sizing method must be provided.
    if qty is None and notional is None:
        raise ValueError("Set either TRADE_QTY or TRADE_NOTIONAL_USD.")

    last_err: Optional[Exception] = None
    # Retry order placement if Alpaca errors transiently.
    for attempt in range(1, max_attempts + 1):
        try:
            req_kwargs = {
                "symbol": symbol,
                "side": side,
                "time_in_force": time_in_force,
            }
            if qty is not None:
                req_kwargs["qty"] = qty
            else:
                req_kwargs["notional"] = notional

            req = MarketOrderRequest(**req_kwargs)
            # Submit the order to Alpaca.
            order = trading_client.submit_order(req)
            order_id = getattr(order, "id", None) or getattr(order, "order_id", None)
            order_id_str = str(order_id) if order_id is not None else ""

            return {
                # DynamoDB can't serialize UUID objects, so store as string.
                "alpaca_order_id": order_id_str,
                "submitted_at": _utc_now_iso(),
            }
        except Exception as e:  # noqa: BLE001
            last_err = e
            logger.warning(
                "Alpaca order placement failed",
                extra={"symbol": symbol, "side": str(side), "attempt": attempt, "max_attempts": max_attempts, "error": str(e)},
            )
            if attempt < max_attempts:
                _backoff_sleep(attempt)

    raise RuntimeError(f"Alpaca order placement failed after {max_attempts} attempts: {last_err}")


def _get_env_decimal(name: str, default: Decimal) -> Decimal:
    """Read a Decimal from environment variables, returning default if missing or invalid."""
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return Decimal(raw)


def _query_sentiment_window(*, table, symbol: str, now_utc: datetime, window_hours: int) -> list[dict]:
    index_name = os.getenv("TICKER_TIMESTAMP_GSI_NAME", "TickerTimestampIndex")
    start_utc = now_utc - timedelta(hours=window_hours)
    start_iso = start_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    end_iso = now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    kwargs = {
        "IndexName": index_name,
        "KeyConditionExpression": Key("gsi_pk").eq(symbol) & Key("gsi_sk").between(start_iso, end_iso),
        "FilterExpression": Attr("item_type").eq("SENTIMENT"),
    }
    items: list[dict] = []
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items") or [])
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


def _build_sentiment_news_rows(*, table, symbol: str, now_utc: datetime) -> list[dict[str, Any]]:
    """
    Load recent SENTIMENT rows for `symbol` (up to 7d) as row dicts for live strategies.
    Same fields as DynamoDB items used by time-decay logic (DataFrame-compatible shape).
    """
    items = _query_sentiment_window(table=table, symbol=symbol, now_utc=now_utc, window_hours=24 * 7)
    rows: list[dict[str, Any]] = []
    for item in items:
        rows.append(
            {
                "sentiment_score": item.get("sentiment_score"),
                "news_published_at": item.get("news_published_at"),
                "analyzed_at": item.get("analyzed_at"),
            }
        )
    return rows


def _fetch_recent_price_bars(symbol: str) -> list[dict[str, Any]]:
    """
    Recent 1-minute bars for live strategy context (OHLCV, UTC timestamp per row).
    Each row: timestamp (datetime UTC), open, high, low, close, volume.
    Returns [] on missing creds or request failure (strategies may be news-only).
    """
    key = os.environ.get("ALPACA_API_KEY")
    secret = os.environ.get("ALPACA_SECRET_KEY")
    if not key or not secret:
        logger.warning("Skipping price fetch: Alpaca data credentials missing", extra={"symbol": symbol})
        return []

    base_url = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
    feed = os.getenv("ALPACA_DATA_FEED", "iex")
    timeout_s = float(os.getenv("ALPACA_PRICE_TIMEOUT_SECONDS", "20"))
    end_utc = datetime.now(timezone.utc)
    start_utc = end_utc - timedelta(days=2)

    url = f"{base_url}/v2/stocks/{symbol}/bars"
    params = {
        "timeframe": "1Min",
        "start": start_utc.isoformat().replace("+00:00", "Z"),
        "end": end_utc.isoformat().replace("+00:00", "Z"),
        "limit": 10000,
        "sort": "asc",
        "adjustment": "raw",
        "feed": feed,
    }
    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=timeout_s)
        resp.raise_for_status()
    except Exception as e:  # noqa: BLE001
        logger.warning("Alpaca price bar fetch failed", extra={"symbol": symbol, "error": str(e)})
        return []

    bars = (resp.json() or {}).get("bars") or []
    if not bars:
        return []

    rows: list[dict[str, Any]] = []
    for b in bars:
        ts_raw = b.get("t")
        try:
            ts_norm = str(ts_raw).replace("Z", "+00:00") if ts_raw else ""
            ts = datetime.fromisoformat(ts_norm) if ts_norm else None
            if ts is not None and ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts is not None:
                ts = ts.astimezone(timezone.utc)
        except (TypeError, ValueError):
            ts = None
        if ts is None:
            continue
        try:
            rows.append(
                {
                    "timestamp": ts,
                    "open": float(b["o"]) if b.get("o") is not None else None,
                    "high": float(b["h"]) if b.get("h") is not None else None,
                    "low": float(b["l"]) if b.get("l") is not None else None,
                    "close": float(b["c"]) if b.get("c") is not None else None,
                    "volume": float(b["v"]) if b.get("v") is not None else None,
                }
            )
        except (TypeError, ValueError):
            continue
    rows.sort(key=lambda r: r["timestamp"])
    return rows


def _get_strategy_config(*, table, symbol: str) -> dict:
    """
    Read latest strategy config from DynamoDB:
      PK(run_id)=STRATEGY#<SYMBOL>, SK(sort_key)=LATEST
    Returns keys used by ``build_strategy_from_config`` / TRADE records.
    """
    key = {"run_id": f"STRATEGY#{symbol}", "sort_key": "LATEST"}
    item = table.get_item(Key=key).get("Item") or {}
    # Conservative defaults: inactive strategy, threshold 1.0 (practically no trade)
    raw_active = item.get("is_active", False)
    if isinstance(raw_active, bool):
        is_active = raw_active
    else:
        is_active = str(raw_active).strip().lower() == "true"
    raw_threshold = item.get("optimized_threshold")
    if raw_threshold is None or raw_threshold == "":
        optimized_threshold = Decimal("1")
    elif isinstance(raw_threshold, Decimal):
        optimized_threshold = raw_threshold
    else:
        optimized_threshold = Decimal(str(raw_threshold))

    default_exit = os.getenv("STRATEGY_EXIT_TYPE", "fixed_time").strip() or "fixed_time"
    raw_name = item.get("strategy_name")
    strategy_name = str(raw_name).strip() if raw_name not in (None, "") else "Sentiment_V1"
    exit_type = str(item.get("exit_type") or default_exit).strip() or default_exit
    hm_raw = item.get("hold_minutes")
    if hm_raw is None or hm_raw == "":
        hold_minutes = int(os.getenv("STRATEGY_HOLD_MINUTES", "60"))
    elif isinstance(hm_raw, Decimal):
        hold_minutes = int(hm_raw)
    else:
        hold_minutes = int(hm_raw)

    return {
        "is_active": is_active,
        "optimized_threshold": optimized_threshold,
        "strategy_name": strategy_name,
        "exit_type": exit_type,
        "hold_minutes": hold_minutes,
    }


def _should_trade_now_close_only(trading_client: TradingClient) -> bool:
    """
    If TRADE_ONLY_AT_CLOSE=true, allow trading only in the final N minutes of regular session.
    """
    if os.getenv("TRADE_ONLY_AT_CLOSE", "true").lower() != "true":
        return True

    close_window_minutes = int(os.getenv("CLOSE_WINDOW_MINUTES", "5"))
    clock = trading_client.get_clock()
    is_open = bool(getattr(clock, "is_open", False))
    if not is_open:
        return False

    now_dt = getattr(clock, "timestamp", None)
    next_close_dt = getattr(clock, "next_close", None)
    if now_dt is None or next_close_dt is None:
        return False

    mins_to_close = (next_close_dt - now_dt).total_seconds() / 60.0
    return 0 <= mins_to_close <= close_window_minutes


def handler(event, context):
    """
    Invoke with: {"run_id": "..."}.
    Queries DynamoDB for sentiment results, then executes idempotent paper trades via Alpaca.
    """
    # `run_id` tells us which sentiment analysis batch to use.
    run_id = event.get("run_id")
    if not run_id:
        raise ValueError("Missing required input: run_id")

    # DynamoDB table handles (strategy can be same table or a dedicated table).
    table = boto3.resource("dynamodb").Table(os.environ["DYNAMODB_TABLE_NAME"])
    strategy_table_name = os.getenv("STRATEGY_TABLE_NAME", os.environ["DYNAMODB_TABLE_NAME"])
    strategy_table = boto3.resource("dynamodb").Table(strategy_table_name)

    # Trading thresholds control when to place BUY/SELL orders.
    buy_threshold = _get_env_decimal("SENTIMENT_BUY_THRESHOLD", Decimal("0.2"))
    sell_threshold = _get_env_decimal("SENTIMENT_SELL_THRESHOLD", Decimal("-0.2"))
    enable_shorts = os.getenv("ENABLE_SHORTS", "false").lower() == "true"

    # Registered strategies (for observability / validation).
    logger.debug("Trade executor STRATEGY_MAP keys", extra={"strategies": sorted(STRATEGY_MAP.keys())})

    # Create Alpaca API client.
    trading_client = _get_trade_client()
    if not _should_trade_now_close_only(trading_client):
        logger.info(
            "Skipping execution: not in close-only trading window",
            extra={"run_id": run_id, "close_only": os.getenv("TRADE_ONLY_AT_CLOSE", "true")},
        )
        snap_none = _snapshot_and_publish_telemetry(trading_client, run_id)
        return {"status": "ok", "run_id": run_id, "trades": 0, "account_snapshot": snap_none}

    # Query only sentiment items for this run_id.
    # sort_key begins_with("SENTIMENT#")
    resp = table.query(
        KeyConditionExpression=Key("run_id").eq(run_id) & Key("sort_key").begins_with("SENTIMENT#"),
    )
    items = resp.get("Items") or []

    if not items:
        logger.warning("No sentiment items found; no trades executed", extra={"run_id": run_id})
        snap_none = _snapshot_and_publish_telemetry(trading_client, run_id)
        return {"status": "ok", "run_id": run_id, "trades": 0, "account_snapshot": snap_none}

    # Track whether we encountered errors; if yes, we raise so Lambda/SQS retry can happen.
    had_errors = False
    trades_submitted = 0
    now_utc = datetime.now(timezone.utc)

    for it in items:
        symbol = (it.get("symbol") or "").strip().upper()
        if not symbol:
            continue

        try:
            strategy_config = _get_strategy_config(table=strategy_table, symbol=symbol)
        except Exception:  # noqa: BLE001
            logger.exception("Failed reading strategy config", extra={"run_id": run_id, "symbol": symbol})
            had_errors = True
            continue

        if not strategy_config["is_active"]:
            logger.info("Strategy inactive; skipping symbol", extra={"run_id": run_id, "symbol": symbol})
            continue

        try:
            active_strategy = build_strategy_from_config(
                strategy_config,
                buy_threshold=buy_threshold,
                sell_threshold=sell_threshold,
                enable_shorts=enable_shorts,
            )
        except Exception:  # noqa: BLE001
            logger.exception("Failed to build strategy from config", extra={"run_id": run_id, "symbol": symbol})
            had_errors = True
            continue

        current_news = _build_sentiment_news_rows(table=table, symbol=symbol, now_utc=now_utc)
        current_prices = _fetch_recent_price_bars(symbol)

        try:
            side = active_strategy.check_live_signal(
                current_prices,
                current_news,
                run_id=run_id,
                symbol=symbol,
            )
        except Exception:  # noqa: BLE001
            logger.exception("Strategy check_live_signal failed", extra={"run_id": run_id, "symbol": symbol})
            had_errors = True
            continue

        if side is None:
            continue

        sentiment_score = active_strategy.last_sentiment_score

        trade_sort_key = f"TRADE#{symbol}"

        # Idempotency: if we've already recorded a trade for this run+symbol, skip.
        try:
            existing = table.get_item(Key={"run_id": run_id, "sort_key": trade_sort_key}).get("Item")
            if existing:
                logger.info("Trade already recorded; skipping", extra={"run_id": run_id, "symbol": symbol})
                continue
        except Exception:  # noqa: BLE001
            # If we can't read idempotency state, fail fast so SQS/Lambda retries.
            logger.exception("Failed reading idempotency record", extra={"run_id": run_id, "symbol": symbol})
            had_errors = True
            continue

        logger.info(
            "Submitting paper trade",
            extra={"run_id": run_id, "symbol": symbol, "side": str(side), "score": str(sentiment_score)},
        )

        try:
            # Actually place the order with Alpaca.
            order_result = _place_order(trading_client=trading_client, symbol=symbol, side=side)

            # Store a TRADE record in DynamoDB.
            # Conditional writes prevent duplicates if the Lambda retries.
            trade_item = {
                "run_id": run_id,
                "sort_key": trade_sort_key,
                "item_type": "TRADE",
                "symbol": symbol,
                # DynamoDB/boto3 requires numbers to be Decimal (floats are not supported).
                "sentiment_score": Decimal(str(sentiment_score)),
                "sentiment_label": it.get("sentiment_label") or "neutral",
                "side": str(side),
                "strategy_name": getattr(active_strategy, "strategy_name", type(active_strategy).__name__),
                "exit_type": str(getattr(active_strategy, "exit_type", "fixed_time")),
                "hold_minutes": Decimal(str(int(getattr(active_strategy, "hold_minutes", 60)))),
                "status": "OPEN",
                "alpaca_order_id": order_result.get("alpaca_order_id") or "",
                "submitted_at": order_result.get("submitted_at"),
                "trade_attempted_by": getattr(context, "aws_request_id", None),
            }

            table.put_item(
                Item=trade_item,
                ConditionExpression=Attr("sort_key").not_exists(),
            )

            trades_submitted += 1
        except ClientError as e:
            # Conditional write indicates another retry already recorded it; treat as success.
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                logger.info("Trade record written by concurrent run; skipping", extra={"run_id": run_id, "symbol": symbol})
                continue
            logger.exception("Failed writing trade record", extra={"run_id": run_id, "symbol": symbol})
            had_errors = True
        except Exception:  # noqa: BLE001
            logger.exception("Trade execution failed", extra={"run_id": run_id, "symbol": symbol})
            had_errors = True

    if had_errors:
        raise RuntimeError(f"Trade executor had errors for run_id={run_id}")

    snap_done = _snapshot_and_publish_telemetry(trading_client, run_id)

    return {
        "status": "ok",
        "run_id": run_id,
        "trades": trades_submitted,
        "account_snapshot": snap_done,
    }

