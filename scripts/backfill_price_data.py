"""
Backfill 1-minute Alpaca bars into S3 parquet by day.

Usage:
  python3 scripts/backfill_price_data.py \
    --symbol TSLA \
    --start-date 2026-01-01 \
    --end-date 2026-01-31

Default destination path:
  s3://trading-data/bars/{symbol}/{date}.parquet
"""

from __future__ import annotations

import argparse
import io
import os
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
import requests

EASTERN_TZ = ZoneInfo("America/New_York")
UTC = timezone.utc


def _parse_yyyy_mm_dd(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _iter_days(start_date: date, end_date: date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def _s3_key_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def _fetch_day_bars(symbol: str, trading_day: date) -> pd.DataFrame:
    """
    Fetch one full day in one Alpaca batch call.
    """
    alpaca_key = os.environ["ALPACA_API_KEY"]
    alpaca_secret = os.environ["ALPACA_SECRET_KEY"]
    base_url = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
    feed = os.getenv("ALPACA_DATA_FEED", "iex")
    timeout_s = float(os.getenv("ALPACA_PRICE_TIMEOUT_SECONDS", "20"))

    start_utc = datetime.combine(trading_day, datetime.min.time(), tzinfo=UTC)
    end_utc = start_utc + timedelta(days=1)

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
    headers = {
        "APCA-API-KEY-ID": alpaca_key,
        "APCA-API-SECRET-KEY": alpaca_secret,
    }
    resp = requests.get(url, headers=headers, params=params, timeout=timeout_s)
    resp.raise_for_status()
    bars = (resp.json() or {}).get("bars") or []
    if not bars:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "symbol"])

    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime([b.get("t") for b in bars], utc=True, errors="coerce"),
            "open": pd.to_numeric([b.get("o") for b in bars], errors="coerce"),
            "high": pd.to_numeric([b.get("h") for b in bars], errors="coerce"),
            "low": pd.to_numeric([b.get("l") for b in bars], errors="coerce"),
            "close": pd.to_numeric([b.get("c") for b in bars], errors="coerce"),
            "volume": pd.to_numeric([b.get("v") for b in bars], errors="coerce"),
        }
    )

    # Keep regular-session bars only (09:30 - 16:00 ET) for consistent "trading day" snapshots.
    local = df["timestamp"].dt.tz_convert(EASTERN_TZ)
    # Regular session: 9:30 ET through 16:00 ET only (hour<=16, but minute==0 when hour==16).
    in_regular_hours = ((local.dt.hour > 9) | ((local.dt.hour == 9) & (local.dt.minute >= 30))) & (
        (local.dt.hour <= 16) & ((local.dt.hour < 16) | (local.dt.minute == 0))
    )
    df = df[in_regular_hours].dropna(subset=["timestamp", "open", "high", "low", "close"]).copy()
    if df.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "symbol"])
    df["symbol"] = symbol
    return df


def _write_parquet_to_s3(s3, df: pd.DataFrame, *, bucket: str, key: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(), ContentType="application/octet-stream")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Alpaca 1-minute bars to S3 parquet by day.")
    parser.add_argument("--symbol", required=True, help="Ticker symbol, e.g. TSLA")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--s3-bucket", default="trading-data")
    parser.add_argument("--s3-prefix", default="bars")
    args = parser.parse_args()

    symbol = args.symbol.strip().upper()
    start_date = _parse_yyyy_mm_dd(args.start_date)
    end_date = _parse_yyyy_mm_dd(args.end_date)
    if end_date < start_date:
        raise ValueError("end-date must be >= start-date")

    s3 = boto3.client("s3")

    for d in _iter_days(start_date, end_date):
        try:
            key = f"{args.s3_prefix.strip('/')}/{symbol}/{d.isoformat()}.parquet"

            if _s3_key_exists(s3, args.s3_bucket, key):
                print(f"SKIP exists: s3://{args.s3_bucket}/{key}")
                continue

            try:
                bars_df = _fetch_day_bars(symbol, d)
            except Exception as exc:  # noqa: BLE001
                print(f"ERROR fetch {d.isoformat()} symbol={symbol}: {exc}")
                continue

            # Weekend / market holiday handling:
            # if Alpaca returns zero bars, do not write an empty file.
            if bars_df.empty:
                print(f"SKIP no-bars: {symbol} {d.isoformat()} (weekend/holiday/closed)")
                continue

            try:
                _write_parquet_to_s3(s3, bars_df, bucket=args.s3_bucket, key=key)
                print(f"WROTE {len(bars_df)} rows -> s3://{args.s3_bucket}/{key}")
            except Exception as exc:  # noqa: BLE001
                print(f"ERROR write {d.isoformat()} symbol={symbol}: {exc}")
        finally:
            time.sleep(0.2)


if __name__ == "__main__":
    main()
