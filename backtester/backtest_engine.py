"""
Fargate backtest job:
- Load latest SENTIMENT parquet from S3 lake
- Sweep threshold range and optimize Sharpe ratio with vectorbt
- Compare against current STRATEGY#<SYMBOL>/LATEST
- Write BACKTEST#<TIMESTAMP> result to DynamoDB when significantly better
"""

from __future__ import annotations

import gzip
import io
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from urllib.parse import urlparse

import boto3
import numpy as np
import pandas as pd
import requests
import vectorbt as vbt
from boto3.dynamodb.types import TypeDeserializer

_DESERIALIZER = TypeDeserializer()


def _load_latest_data(path: str) -> pd.DataFrame:
    # Requires pyarrow + s3fs (installed by vectorbt[full]/awswrangler deps).
    return pd.read_parquet(path)


def _parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {s3_uri}")
    remainder = s3_uri[5:]
    bucket, _, key = remainder.partition("/")
    return bucket, key


def _ddb_unmarshal(item: dict) -> dict:
    # Native export payload is DynamoDB JSON, e.g. {"symbol":{"S":"TSLA"}, ...}
    return {k: _DESERIALIZER.deserialize(v) for k, v in item.items()}


def _load_native_export_data(export_s3_prefix: str) -> pd.DataFrame:
    """
    Load SENTIMENT rows from DynamoDB native export files (DYNAMODB_JSON).
    Expected line format in gz files: {"Item": { ...ddb-json... }}
    """
    s3 = boto3.client("s3")
    bucket, prefix = _parse_s3_uri(export_s3_prefix.rstrip("/") + "/")
    data_prefix = f"{prefix}data/"
    paginator = s3.get_paginator("list_objects_v2")
    rows: list[dict] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=data_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json.gz"):
                continue
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
                for raw_line in gz:
                    line = raw_line.decode("utf-8").strip()
                    if not line:
                        continue
                    payload = json.loads(line)
                    raw_item = payload.get("Item") or payload
                    item = _ddb_unmarshal(raw_item)
                    if item.get("item_type") == "SENTIMENT":
                        rows.append(item)

    return pd.DataFrame(rows)


def _parse_s3_parts(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def _fetch_week_bars_from_alpaca(symbol: str, week_start_utc: pd.Timestamp) -> pd.DataFrame:
    api_key = os.environ["ALPACA_API_KEY"]
    api_secret = os.environ["ALPACA_SECRET_KEY"]
    base_url = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
    feed = os.getenv("ALPACA_DATA_FEED", "iex")
    timeout_s = float(os.getenv("ALPACA_PRICE_TIMEOUT_SECONDS", "10"))

    # Include +2h buffer at week end to support +60m forward lookup on late Friday signals.
    start = week_start_utc.tz_convert("UTC")
    end = start + pd.Timedelta(days=7, hours=2)
    url = f"{base_url}/v2/stocks/{symbol}/bars"
    params = {
        "timeframe": "1Min",
        "start": start.isoformat().replace("+00:00", "Z"),
        "end": end.isoformat().replace("+00:00", "Z"),
        "limit": 10000,
        "sort": "asc",
        "adjustment": "raw",
        "feed": feed,
    }
    headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret}
    resp = requests.get(url, headers=headers, params=params, timeout=timeout_s)
    resp.raise_for_status()
    bars = (resp.json() or {}).get("bars") or []
    if not bars:
        return pd.DataFrame(columns=["ts", "close"])
    out = pd.DataFrame(
        {
            "ts": pd.to_datetime([b.get("t") for b in bars], utc=True, errors="coerce"),
            "close": pd.to_numeric([b.get("c") for b in bars], errors="coerce"),
        }
    ).dropna(subset=["ts", "close"])
    return out.sort_values("ts")


def _load_or_build_week_bars_cache(symbol: str, week_start_utc: pd.Timestamp, cache_prefix: str) -> pd.DataFrame:
    """
    S3 cache pattern:
    1) Check weekly parquet cache in S3
    2) If cache miss, fetch from Alpaca and write parquet cache
    """
    s3 = boto3.client("s3")
    cache_path = (
        f"{cache_prefix.rstrip('/')}/"
        f"symbol={symbol}/week_start={week_start_utc.strftime('%Y-%m-%d')}/bars.parquet"
    )
    bucket, key = _parse_s3_parts(cache_path)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return pd.read_parquet(cache_path)
    except Exception:
        pass

    bars = _fetch_week_bars_from_alpaca(symbol, week_start_utc)
    if not bars.empty:
        bars.to_parquet(cache_path, index=False)
    return bars


def _ensure_forward_returns(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if "ret_60m" in df.columns and df["ret_60m"].notna().any():
        return df

    cache_prefix = os.getenv("S3_BARS_CACHE_PREFIX", "")
    if not cache_prefix:
        raise ValueError("Missing S3_BARS_CACHE_PREFIX for Alpaca bars cache.")

    work = df.copy()
    ts_primary = work.get("market_price_timestamp")
    if ts_primary is None:
        ts_primary = pd.Series([None] * len(work), index=work.index)
    ts_fallback = work.get("news_published_at")
    if ts_fallback is None:
        ts_fallback = pd.Series([None] * len(work), index=work.index)
    work["ts"] = pd.to_datetime(ts_primary.fillna(ts_fallback), utc=True, errors="coerce")
    work["entry_price"] = pd.to_numeric(
        work.get("entry_price", work.get("market_price_at_news", work.get("market_price"))), errors="coerce"
    )
    if "symbol" not in work.columns:
        return work.iloc[0:0]
    work = work[work["symbol"].astype(str).str.upper() == symbol]
    work = work.dropna(subset=["ts", "entry_price"]).sort_values("ts")
    if work.empty:
        return work

    week_starts = work["ts"].dt.tz_convert("UTC").dt.to_period("W-MON").dt.start_time.dt.tz_localize("UTC").unique()
    all_bars: list[pd.DataFrame] = []
    for ws in week_starts:
        all_bars.append(_load_or_build_week_bars_cache(symbol=symbol, week_start_utc=pd.Timestamp(ws), cache_prefix=cache_prefix))
    bars_df = pd.concat(all_bars, ignore_index=True) if all_bars else pd.DataFrame(columns=["ts", "close"])
    bars_df = bars_df.dropna(subset=["ts", "close"]).sort_values("ts")
    if bars_df.empty:
        work["ret_60m"] = np.nan
        return work

    targets = work[["ts"]].copy()
    targets["target_ts"] = targets["ts"] + pd.Timedelta(minutes=60)
    merged = pd.merge_asof(
        targets.sort_values("target_ts"),
        bars_df.rename(columns={"ts": "bar_ts", "close": "close_60m"}),
        left_on="target_ts",
        right_on="bar_ts",
        direction="forward",
        tolerance=pd.Timedelta(minutes=5),
    )
    work = work.reset_index(drop=True)
    work["close_60m"] = merged["close_60m"].values
    work["ret_60m"] = (work["close_60m"] / work["entry_price"]) - 1.0
    return work


def _optimize_threshold(df: pd.DataFrame, symbol: str) -> tuple[float, float]:
    d = df[df["symbol"] == symbol].copy()
    if d.empty:
        raise RuntimeError(f"No parquet rows for symbol={symbol}")

    d = _ensure_forward_returns(d, symbol)
    d["ts"] = pd.to_datetime(d["ts"], utc=True, errors="coerce")
    d = d.dropna(subset=["ts", "entry_price", "ret_60m", "sentiment_score"]).sort_values("ts")
    if d.empty:
        raise RuntimeError(f"No usable rows after cleaning for symbol={symbol}")

    close = pd.Series((1.0 + d["ret_60m"].astype(float)).cumprod().values, index=d["ts"])
    thresholds = np.round(np.arange(0.5, 0.951, 0.01), 2)
    best_thr = 0.5
    best_sharpe = float("-inf")

    for thr in thresholds:
        entries = pd.Series((d["sentiment_score"].astype(float) >= thr).values, index=d["ts"])
        exits = entries.shift(1).fillna(False) & (~entries)
        pf = vbt.Portfolio.from_signals(close=close, entries=entries, exits=exits, freq="1h")
        sharpe = float(pf.sharpe_ratio())
        if np.isfinite(sharpe) and sharpe > best_sharpe:
            best_sharpe = sharpe
            best_thr = float(thr)

    return best_thr, best_sharpe


def _current_strategy_threshold(table, symbol: str) -> float | None:
    key = {"run_id": f"STRATEGY#{symbol}", "sort_key": "LATEST"}
    item = table.get_item(Key=key).get("Item") or {}
    raw = item.get("optimized_threshold")
    if raw in (None, ""):
        return None
    return float(raw)


def main() -> None:
    table_name = os.getenv("DYNAMODB_TABLE_NAME", "TradingNewsSentiment")
    symbol = os.getenv("BACKTEST_SYMBOL", "TSLA").upper()
    parquet_path = os.getenv("S3_PARQUET_PATH", "")
    native_export_prefix = os.getenv("S3_DDB_EXPORT_PREFIX", "")
    min_improvement = float(os.getenv("BACKTEST_MIN_SHARPE_IMPROVEMENT", "0.1"))

    if parquet_path:
        df = _load_latest_data(parquet_path)
        source_path = parquet_path
    elif native_export_prefix:
        df = _load_native_export_data(native_export_prefix)
        source_path = native_export_prefix
    else:
        raise ValueError("Set either S3_PARQUET_PATH or S3_DDB_EXPORT_PREFIX.")

    # Normalize numeric fields from either parquet or native-export source.
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.upper()
    if "sentiment_score" in df.columns:
        df["sentiment_score"] = pd.to_numeric(df["sentiment_score"], errors="coerce")
    if "entry_price" not in df.columns:
        df["entry_price"] = pd.to_numeric(
            df.get("market_price_at_news", df.get("market_price")),
            errors="coerce",
        )

    table = boto3.resource("dynamodb").Table(table_name)
    new_threshold, new_sharpe = _optimize_threshold(df, symbol)
    current_threshold = _current_strategy_threshold(table, symbol)

    # Evaluate the existing threshold using the same result set if available.
    improved = True
    if current_threshold is not None:
        improved = abs(new_threshold - current_threshold) >= 0.01 and new_sharpe >= min_improvement

    if not improved:
        print(
            f"Skip write: symbol={symbol} new_threshold={new_threshold:.2f} "
            f"new_sharpe={new_sharpe:.4f} current_threshold={current_threshold}"
        )
        return

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    item = {
        "run_id": f"BACKTEST#{symbol}",
        "sort_key": ts,
        "item_type": "BACKTEST",
        "symbol": symbol,
        "candidate_threshold": Decimal(str(round(new_threshold, 2))),
        "candidate_sharpe": Decimal(str(round(new_sharpe, 6))),
        "current_threshold": Decimal(str(current_threshold if current_threshold is not None else -1)),
        "source_parquet_path": source_path,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    table.put_item(Item=item)
    print(f"Wrote backtest result: {item['run_id']} / {item['sort_key']}")


if __name__ == "__main__":
    main()
