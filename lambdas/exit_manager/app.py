"""
Lambda 4: Exit Manager

EventBridge (rate 1 minute):
- Scan DynamoDB for TRADE rows with status == OPEN
- Exit per ``exit_type`` (fixed_time, end_of_day, signal_flip) via Alpaca
- Update TRADE item: status=CLOSED, realized P&L snapshot, exit order id
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
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

_here = Path(__file__).resolve().parent
if not (_here / "strategies").is_dir():
    _repo_root = _here.parent.parent
    _layer_python = _repo_root / "layers" / "strategies" / "python"
    if _layer_python.is_dir() and str(_layer_python) not in sys.path:
        sys.path.insert(0, str(_layer_python))

from strategies import build_strategy_from_config  # noqa: E402

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _backoff_sleep(attempt: int) -> None:
    base = float(os.getenv("ALPACA_BACKOFF_BASE_SECONDS", "0.5"))
    max_sleep = float(os.getenv("ALPACA_BACKOFF_MAX_SECONDS", "10"))
    sleep_s = min(max_sleep, base * (2 ** (attempt - 1)))
    time.sleep(sleep_s)


def _get_trade_client() -> TradingClient:
    api_key = os.environ["ALPACA_API_KEY"]
    api_secret = os.environ["ALPACA_SECRET_KEY"]
    paper = os.getenv("ALPACA_PAPER", "true").lower() == "true"
    return TradingClient(api_key=api_key, secret_key=api_secret, paper=paper)


def _get_env_decimal(name: str, default: Decimal) -> Decimal:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return Decimal(raw)


def _parse_iso8601_utc(ts: object) -> Optional[datetime]:
    if not isinstance(ts, str) or not ts.strip():
        return None
    normalized = ts.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_money_field(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _query_sentiment_window(*, table, symbol: str, now_utc: datetime, window_hours: int) -> list[dict]:
    index_name = os.getenv("TICKER_TIMESTAMP_GSI_NAME", "TickerTimestampIndex")
    start_utc = now_utc - timedelta(hours=window_hours)
    start_iso = start_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    end_iso = now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    kwargs: dict[str, Any] = {
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
    key = os.environ.get("ALPACA_API_KEY")
    secret = os.environ.get("ALPACA_SECRET_KEY")
    if not key or not secret:
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
    except Exception:
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


def _get_strategy_config(*, table, symbol: str) -> dict[str, Any]:
    key = {"run_id": f"STRATEGY#{symbol}", "sort_key": "LATEST"}
    item = table.get_item(Key=key).get("Item") or {}
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


def _within_minutes_of_market_close(trading_client: TradingClient, window_minutes: int) -> bool:
    clock = trading_client.get_clock()
    if not bool(getattr(clock, "is_open", False)):
        return False
    now_dt = getattr(clock, "timestamp", None)
    next_close_dt = getattr(clock, "next_close", None)
    if now_dt is None or next_close_dt is None:
        return False
    mins_to_close = (next_close_dt - now_dt).total_seconds() / 60.0
    return 0 <= mins_to_close <= window_minutes


def _entry_is_long(side_str: object) -> bool:
    u = str(side_str or "").upper()
    return "SELL" not in u


def _side_is_buy(side: object) -> bool:
    if side is None:
        return False
    u = str(side).upper()
    return "BUY" in u and "SELL" not in u


def _side_is_sell(side: object) -> bool:
    if side is None:
        return False
    u = str(side).upper()
    return "SELL" in u


def _find_open_position(trading_client: TradingClient, symbol: str) -> Any:
    for p in trading_client.get_all_positions():
        if str(getattr(p, "symbol", "")).upper() == symbol.upper():
            return p
    return None


def _close_position_at_alpaca(*, trading_client: TradingClient, symbol: str) -> tuple[str, float]:
    """
    Flatten position for symbol. Returns (exit_alpaca_order_id, unrealized_pl_usd snapshot before close).
    """
    pos = _find_open_position(trading_client, symbol)
    if pos is None:
        return "", 0.0

    unrealized = _parse_money_field(getattr(pos, "unrealized_pl", None)) or 0.0
    qty_raw = getattr(pos, "qty", None) or getattr(pos, "quantity", None)
    qty = float(qty_raw) if qty_raw is not None else 0.0
    if abs(qty) < 1e-12:
        return "", unrealized

    close_side = OrderSide.SELL if qty > 0 else OrderSide.BUY
    abs_qty = abs(qty)
    qty_param: int | float | str
    if abs_qty == int(abs_qty):
        qty_param = int(abs_qty)
    else:
        qty_param = str(abs_qty)

    max_attempts = int(os.getenv("ALPACA_MAX_ATTEMPTS", "3"))
    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            req = MarketOrderRequest(
                symbol=symbol,
                side=close_side,
                qty=qty_param,
                time_in_force=TimeInForce.DAY,
            )
            order = trading_client.submit_order(req)
            oid = getattr(order, "id", None) or getattr(order, "order_id", None)
            return (str(oid) if oid is not None else ""), unrealized
        except Exception as e:  # noqa: BLE001
            last_err = e
            logger.warning(
                "Alpaca exit order failed",
                extra={"symbol": symbol, "attempt": attempt, "error": str(e)},
            )
            if attempt < max_attempts:
                _backoff_sleep(attempt)

    raise RuntimeError(f"Alpaca exit failed after {max_attempts} attempts: {last_err}")


def _iter_open_trade_items(table) -> Any:
    """Paginated scan: TRADE items; caller filters status == OPEN."""
    kwargs: dict[str, Any] = {
        "FilterExpression": Attr("item_type").eq("TRADE"),
    }
    while True:
        resp = table.scan(**kwargs)
        for item in resp.get("Items") or []:
            yield item
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek


def _should_exit_fixed_time(item: dict, *, now_utc: datetime) -> bool:
    submitted = _parse_iso8601_utc(item.get("submitted_at"))
    if submitted is None:
        return False
    hm_raw = item.get("hold_minutes")
    if hm_raw is None or hm_raw == "":
        hold = int(os.getenv("STRATEGY_HOLD_MINUTES", "60"))
    elif isinstance(hm_raw, Decimal):
        hold = int(hm_raw)
    else:
        hold = int(hm_raw)
    deadline = submitted + timedelta(minutes=hold)
    return now_utc >= deadline


def _should_exit_signal_flip(
    *,
    strategy: Any,
    item: dict,
    current_prices: list[dict[str, Any]],
    current_news: list[dict[str, Any]],
) -> bool:
    """
    Call ``check_live_signal`` and exit when the actionable side flipped vs entry (long vs short).
    """
    run_id = str(item.get("run_id") or "")
    symbol = str(item.get("symbol") or "").upper()
    new_side = strategy.check_live_signal(
        current_prices,
        current_news,
        run_id=run_id,
        symbol=symbol,
    )
    entry_long = _entry_is_long(item.get("side", ""))
    if entry_long and _side_is_sell(new_side):
        return True
    if (not entry_long) and _side_is_buy(new_side):
        return True
    return False


def handler(event, context):
    table = boto3.resource("dynamodb").Table(os.environ["DYNAMODB_TABLE_NAME"])
    strategy_table_name = os.getenv("STRATEGY_TABLE_NAME", os.environ["DYNAMODB_TABLE_NAME"])
    strategy_table = boto3.resource("dynamodb").Table(strategy_table_name)

    buy_threshold = _get_env_decimal("SENTIMENT_BUY_THRESHOLD", Decimal("0.2"))
    sell_threshold = _get_env_decimal("SENTIMENT_SELL_THRESHOLD", Decimal("-0.2"))
    enable_shorts = os.getenv("ENABLE_SHORTS", "false").lower() == "true"
    eod_window = int(os.getenv("EXIT_EOD_WINDOW_MINUTES", "5"))

    trading_client = _get_trade_client()
    now_utc = datetime.now(timezone.utc)

    closed = 0
    errors = 0

    for item in _iter_open_trade_items(table):
        if item.get("status") != "OPEN":
            continue

        symbol = (item.get("symbol") or "").strip().upper()
        if not symbol:
            continue

        exit_type = str(item.get("exit_type") or "fixed_time").strip().lower()
        should_exit = False
        exit_reason = ""

        try:
            if exit_type == "fixed_time":
                if _should_exit_fixed_time(item, now_utc=now_utc):
                    should_exit = True
                    exit_reason = "fixed_time"
            elif exit_type == "end_of_day":
                if _within_minutes_of_market_close(trading_client, eod_window):
                    should_exit = True
                    exit_reason = "end_of_day"
            elif exit_type == "signal_flip":
                strat_cfg = _get_strategy_config(table=strategy_table, symbol=symbol)
                strategy = build_strategy_from_config(
                    strat_cfg,
                    buy_threshold=buy_threshold,
                    sell_threshold=sell_threshold,
                    enable_shorts=enable_shorts,
                )
                news = _build_sentiment_news_rows(table=table, symbol=symbol, now_utc=now_utc)
                prices = _fetch_recent_price_bars(symbol)
                if _should_exit_signal_flip(strategy=strategy, item=item, current_prices=prices, current_news=news):
                    should_exit = True
                    exit_reason = "signal_flip"
            else:
                logger.warning("Unknown exit_type on TRADE row", extra={"symbol": symbol, "exit_type": exit_type})
                continue

            if not should_exit:
                continue

            run_id = item["run_id"]
            sk = item["sort_key"]

            try:
                exit_oid, pnl_snapshot = _close_position_at_alpaca(trading_client=trading_client, symbol=symbol)
            except Exception:  # noqa: BLE001
                logger.exception("Failed to submit Alpaca exit", extra={"symbol": symbol, "run_id": run_id})
                errors += 1
                continue

            if not exit_oid:
                logger.info("No open Alpaca position; marking TRADE closed in DDB", extra={"symbol": symbol, "run_id": run_id})
                exit_reason = f"{exit_reason}_no_position"

            try:
                table.update_item(
                    Key={"run_id": run_id, "sort_key": sk},
                    UpdateExpression=(
                        "SET #st = :closed, closed_at = :ca, exit_reason = :er, "
                        "realized_pnl_usd = :pnl, exit_alpaca_order_id = :eoid"
                    ),
                    ExpressionAttributeNames={"#st": "status"},
                    ExpressionAttributeValues={
                        ":closed": "CLOSED",
                        ":ca": _utc_now_iso(),
                        ":er": exit_reason,
                        ":pnl": Decimal(str(round(pnl_snapshot, 6))),
                        ":eoid": exit_oid or "",
                    },
                    ConditionExpression=Attr("status").eq("OPEN"),
                )
                closed += 1
                logger.info(
                    "Closed trade",
                    extra={
                        "symbol": symbol,
                        "run_id": run_id,
                        "exit_reason": exit_reason,
                        "realized_pnl_usd": pnl_snapshot,
                        "exit_alpaca_order_id": exit_oid,
                    },
                )
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                    logger.info("TRADE already closed by concurrent run", extra={"run_id": run_id, "symbol": symbol})
                    continue
                logger.exception("DynamoDB update failed after exit", extra={"run_id": run_id, "symbol": symbol})
                errors += 1
        except Exception:  # noqa: BLE001
            logger.exception("Exit manager error for item", extra={"symbol": symbol})
            errors += 1

    if errors:
        logger.error("Exit manager finished with errors", extra={"errors": errors, "closed": closed})

    return {"status": "ok", "closed": closed, "errors": errors}
