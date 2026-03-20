"""
Backtest helper:
- Scan SENTIMENT items from DynamoDB
- Fetch Alpaca historical close at +15m / +60m after news timestamp
- Compute returns and print bucketed averages by sentiment score

Usage:
  export AWS_REGION=us-east-1
  export DYNAMODB_TABLE_NAME=TradingNewsSentiment
  export ALPACA_API_KEY=...
  export ALPACA_SECRET_KEY=...
  python3 analysis/backtest_report.py
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
import pandas as pd
import requests
from boto3.dynamodb.conditions import Attr


def _parse_iso8601_utc(ts: str) -> datetime | None:
    if not ts:
        return None
    normalized = ts.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso8601_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_float(v) -> float | None:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _scan_sentiment_items(table) -> list[dict]:
    items: list[dict] = []
    kwargs = {"FilterExpression": Attr("item_type").eq("SENTIMENT")}
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items") or [])
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


def _fetch_close_near(symbol: str, target_utc: datetime, api_key: str, api_secret: str) -> float | None:
    base_url = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
    url = f"{base_url}/v2/stocks/{symbol}/bars"
    start = _iso8601_z(target_utc)
    end = _iso8601_z(target_utc + timedelta(minutes=5))
    params = {
        "timeframe": "1Min",
        "start": start,
        "end": end,
        "limit": 1,
        "sort": "asc",
        "adjustment": "raw",
        "feed": os.getenv("ALPACA_DATA_FEED", "iex"),
    }
    headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        bars = (resp.json() or {}).get("bars") or []
        if not bars:
            return None
        return _to_float(bars[0].get("c"))
    except Exception:
        return None


def main() -> None:
    table_name = os.getenv("DYNAMODB_TABLE_NAME", "TradingNewsSentiment")
    api_key = os.environ["ALPACA_API_KEY"]
    api_secret = os.environ["ALPACA_SECRET_KEY"]
    table = boto3.resource("dynamodb").Table(table_name)
    items = _scan_sentiment_items(table)

    rows: list[dict] = []
    for it in items:
        symbol = (it.get("symbol") or "").strip().upper()
        score = _to_float(it.get("sentiment_score"))
        entry_price = _to_float(it.get("market_price_at_news") or it.get("market_price"))
        ts = it.get("market_price_timestamp") or it.get("news_published_at") or it.get("analyzed_at")
        ts_utc = _parse_iso8601_utc(ts or "")
        if not symbol or score is None or entry_price is None or entry_price <= 0 or ts_utc is None:
            continue

        close_15 = _fetch_close_near(symbol, ts_utc + timedelta(minutes=15), api_key, api_secret)
        close_60 = _fetch_close_near(symbol, ts_utc + timedelta(minutes=60), api_key, api_secret)
        ret_15 = ((close_15 / entry_price) - 1.0) if close_15 is not None else None
        ret_60 = ((close_60 / entry_price) - 1.0) if close_60 is not None else None
        rows.append(
            {
                "run_id": it.get("run_id"),
                "symbol": symbol,
                "sentiment_score": score,
                "entry_price": entry_price,
                "ret_15m": ret_15,
                "ret_60m": ret_60,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        print("No usable SENTIMENT rows found.")
        return

    # Score buckets for quick signal-quality check.
    bins = [-1.01, -0.5, -0.2, 0.2, 0.5, 1.01]
    labels = ["<-0.5", "-0.5~-0.2", "-0.2~0.2", "0.2~0.5", ">0.5"]
    df["score_bucket"] = pd.cut(df["sentiment_score"], bins=bins, labels=labels)
    report = (
        df.groupby("score_bucket", dropna=False)
        .agg(
            samples=("sentiment_score", "count"),
            avg_ret_15m=("ret_15m", "mean"),
            avg_ret_60m=("ret_60m", "mean"),
        )
        .reset_index()
    )

    print("\n=== Sentiment bucket average returns ===")
    print(report.to_string(index=False))


if __name__ == "__main__":
    main()

