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
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import boto3
import numpy as np
import pandas as pd
import requests
import vectorbt as vbt
from boto3.dynamodb.types import TypeDeserializer

# Repo layout: `backtester/backtest_engine.py` vs Docker `/app/backtest_engine.py`
_bt_dir = Path(__file__).resolve().parent
if _bt_dir.name == "backtester":
    _repo_root = str(_bt_dir.parent)
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)

from strategies.base import BaseStrategy  # noqa: E402
from strategies.sentiment_v1 import MorningSentimentStrategy  # noqa: E402

_DESERIALIZER = TypeDeserializer()
EASTERN_TZ = ZoneInfo("America/New_York")


def _load_latest_data(path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Universal data provider for parquet inputs.
    Returns world state:
      - price_df: MultiIndex [symbol, ts] with raw 1m OHLCV bars
      - news_df: MultiIndex [symbol, article_ts] with raw sentiment score fields
    """
    raw = pd.read_parquet(path)
    return _build_world_state(raw)


def _parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {s3_uri}")
    remainder = s3_uri[5:]
    bucket, _, key = remainder.partition("/")
    return bucket, key


def _ddb_unmarshal(item: dict) -> dict:
    return {k: _DESERIALIZER.deserialize(v) for k, v in item.items()}


def _load_native_export_data(export_s3_prefix: str) -> tuple[pd.DataFrame, pd.DataFrame]:
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

    raw = pd.DataFrame(rows)
    return _build_world_state(raw)


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
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    out = pd.DataFrame(
        {
            "ts": pd.to_datetime([b.get("t") for b in bars], utc=True, errors="coerce"),
            "open": pd.to_numeric([b.get("o") for b in bars], errors="coerce"),
            "high": pd.to_numeric([b.get("h") for b in bars], errors="coerce"),
            "low": pd.to_numeric([b.get("l") for b in bars], errors="coerce"),
            "close": pd.to_numeric([b.get("c") for b in bars], errors="coerce"),
            "volume": pd.to_numeric([b.get("v") for b in bars], errors="coerce"),
        }
    ).dropna(subset=["ts", "open", "high", "low", "close"])
    return out.sort_values("ts")


def _load_or_build_week_bars_cache(symbol: str, week_start_utc: pd.Timestamp, cache_prefix: str) -> pd.DataFrame:
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


def _build_world_state(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build strategy-agnostic world state:
      - price_df: MultiIndex [symbol, ts], raw 1m OHLCV in regular session
      - news_df: MultiIndex [symbol, article_ts], raw sentiment fields
    """
    if "symbol" in raw_df.columns:
        raw_df["symbol"] = raw_df["symbol"].astype(str).str.upper()
    if "sentiment_score" in raw_df.columns:
        raw_df["sentiment_score"] = pd.to_numeric(raw_df["sentiment_score"], errors="coerce")

    # Build normalized sentiment/events table.
    ts_cols = [c for c in ["published_at", "news_published_at", "market_price_timestamp", "analyzed_at"] if c in raw_df.columns]
    news = raw_df.copy()
    if "symbol" not in news.columns or not ts_cols:
        empty_prices = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        empty_prices.index = pd.MultiIndex.from_arrays([[], []], names=["symbol", "ts"])
        empty_news = pd.DataFrame(columns=["article_score", "sentiment_score"])
        empty_news.index = pd.MultiIndex.from_arrays([[], []], names=["symbol", "article_ts"])
        return empty_prices, empty_news

    article_ts = None
    for c in ts_cols:
        parsed = pd.to_datetime(news[c], utc=True, errors="coerce")
        article_ts = parsed if article_ts is None else article_ts.fillna(parsed)
    news["article_ts"] = article_ts
    news["article_score"] = pd.to_numeric(news.get("sentiment_score"), errors="coerce")
    news = news.dropna(subset=["symbol", "article_ts", "article_score"]).sort_values(["symbol", "article_ts"])
    if news.empty:
        empty_prices = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        empty_prices.index = pd.MultiIndex.from_arrays([[], []], names=["symbol", "ts"])
        empty_news = pd.DataFrame(columns=["article_score", "sentiment_score"])
        empty_news.index = pd.MultiIndex.from_arrays([[], []], names=["symbol", "article_ts"])
        return empty_prices, empty_news

    news_world = news.set_index(["symbol", "article_ts"]).sort_index()

    # Build raw OHLCV bars for each symbol and date range seen in sentiment.
    cache_prefix = os.getenv("S3_BARS_CACHE_PREFIX", "")
    if not cache_prefix:
        raise ValueError("Missing S3_BARS_CACHE_PREFIX for bars cache.")
    price_parts: list[pd.DataFrame] = []
    for symbol, grp in news.groupby("symbol"):
        start_ts = grp["article_ts"].min().floor("D")
        end_ts = grp["article_ts"].max().ceil("D")
        week_starts = pd.date_range(start=start_ts, end=end_ts + pd.Timedelta(days=7), freq="W-MON", tz="UTC")
        symbol_parts: list[pd.DataFrame] = []
        for ws in week_starts:
            symbol_parts.append(
                _load_or_build_week_bars_cache(symbol=symbol, week_start_utc=pd.Timestamp(ws), cache_prefix=cache_prefix)
            )
        bars = pd.concat(symbol_parts, ignore_index=True) if symbol_parts else pd.DataFrame()
        if bars.empty:
            continue
        bars = bars.dropna(subset=["ts", "open", "high", "low", "close"]).drop_duplicates(subset=["ts"]).sort_values("ts")
        bars["ts"] = pd.to_datetime(bars["ts"], utc=True, errors="coerce")
        bars = bars[~bars["ts"].isna()]
        # Keep regular session bars so any strategy exit between 9:30 and 16:00 has coverage.
        local = bars["ts"].dt.tz_convert(EASTERN_TZ)
        # Regular session: 9:30 ET through 16:00 ET only (hour<=16, but minute==0 when hour==16).
        regular = ((local.dt.hour > 9) | ((local.dt.hour == 9) & (local.dt.minute >= 30))) & (
            (local.dt.hour <= 16) & ((local.dt.hour < 16) | (local.dt.minute == 0))
        )
        bars = bars[regular].copy()
        if bars.empty:
            continue
        bars["symbol"] = symbol
        price_parts.append(bars[["symbol", "ts", "open", "high", "low", "close", "volume"]])

    if not price_parts:
        empty_prices = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        empty_prices.index = pd.MultiIndex.from_arrays([[], []], names=["symbol", "ts"])
        return empty_prices, news_world

    price_world = pd.concat(price_parts, ignore_index=True).set_index(["symbol", "ts"]).sort_index()
    return price_world, news_world


def _apply_strategy_params(strategy: BaseStrategy, params: dict) -> None:
    # Prefer explicit strategy hook, then fallback to direct attribute set.
    if hasattr(strategy, "set_params"):
        strategy.set_params(**params)
    else:
        for k, v in params.items():
            setattr(strategy, k, v)


def _sharpe_for_params(price_df: pd.DataFrame, news_df: pd.DataFrame, strategy: BaseStrategy, params: dict) -> float:
    _apply_strategy_params(strategy, params)
    entries, exits = strategy.generate_signals(price_df, news_df)
    if entries.empty:
        return float("nan")
    pf = vbt.Portfolio.from_signals(
        close=price_df["close"],
        entries=entries,
        exits=exits,
        freq="1min",
    )
    return float(pf.sharpe_ratio())


def _iter_param_grid(param_space: dict) -> list[dict]:
    # Requested: use vbt.ParamGrid for universal parameter sweeps.
    if hasattr(vbt, "ParamGrid"):
        grid = vbt.ParamGrid(param_space)
        # vectorbt ParamGrid is iterable in recent versions; fallback below if not.
        try:
            return [dict(p) for p in grid]
        except Exception:
            pass
    # Fallback cartesian product if ParamGrid is unavailable in runtime version.
    keys = list(param_space.keys())
    vals = [list(v) for v in param_space.values()]
    if not keys:
        return [{}]
    out: list[dict] = [{}]
    for k, arr in zip(keys, vals):
        nxt: list[dict] = []
        for d in out:
            for v in arr:
                nd = dict(d)
                nd[k] = v
                nxt.append(nd)
        out = nxt
    return out


def _optimize_threshold(
    price_df: pd.DataFrame,
    news_df: pd.DataFrame,
    strategy: BaseStrategy,
    *,
    param_space: dict | None = None,
) -> tuple[dict, float]:
    grid_space = param_space or {"threshold": np.round(np.arange(0.5, 0.951, 0.01), 2).tolist()}
    params_grid = _iter_param_grid(grid_space)
    if not params_grid:
        raise RuntimeError("Empty parameter grid for optimization")

    best_params = params_grid[0]
    best_sharpe = float("-inf")
    for params in params_grid:
        sharpe = _sharpe_for_params(price_df, news_df, strategy, params)
        if np.isfinite(sharpe) and sharpe > best_sharpe:
            best_sharpe = sharpe
            best_params = dict(params)
    return best_params, best_sharpe


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
    hold_minutes = int(os.getenv("BACKTEST_HOLD_MINUTES", "60"))
    exit_type = os.getenv("BACKTEST_EXIT_TYPE", "fixed_time")

    if parquet_path:
        price_world, news_world = _load_latest_data(parquet_path)
        source_path = parquet_path
    elif native_export_prefix:
        price_world, news_world = _load_native_export_data(native_export_prefix)
        source_path = native_export_prefix
    else:
        raise ValueError("Set either S3_PARQUET_PATH or S3_DDB_EXPORT_PREFIX.")

    if news_world.empty:
        raise RuntimeError(f"No usable sentiment rows for symbol={symbol}")

    try:
        news_df = news_world.xs(symbol, level="symbol", drop_level=True).copy()
    except Exception:
        news_df = pd.DataFrame(columns=["article_score", "sentiment_score"])
    try:
        price_df = price_world.xs(symbol, level="symbol", drop_level=True).copy()
    except Exception:
        price_df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    if news_df.empty:
        raise RuntimeError(f"No usable sentiment rows for symbol={symbol}")
    if price_df.empty:
        raise RuntimeError(f"No bars loaded for symbol={symbol}")

    # Strategy reads close + index, but world-state keeps full OHLCV.
    price_df = price_df.sort_index()

    strategy: BaseStrategy = MorningSentimentStrategy(
        threshold=0.5,
        exit_type=exit_type,
        hold_minutes=hold_minutes,
    )
    table = boto3.resource("dynamodb").Table(table_name)
    best_params, new_sharpe = _optimize_threshold(
        price_df,
        news_df,
        strategy,
        param_space={"threshold": np.round(np.arange(0.5, 0.951, 0.01), 2).tolist()},
    )
    new_threshold = float(best_params.get("threshold", 0.5))
    current_threshold = _current_strategy_threshold(table, symbol)

    current_sharpe: float | None = None
    if current_threshold is not None:
        current_sharpe = _sharpe_for_params(price_df, news_df, strategy, {"threshold": float(current_threshold)})

    improved = True if current_sharpe is None else (new_sharpe >= (current_sharpe + min_improvement))

    if not improved:
        print(
            f"Skip write: symbol={symbol} new_threshold={new_threshold:.2f} "
            f"new_sharpe={new_sharpe:.4f} current_threshold={current_threshold} current_sharpe={current_sharpe}"
        )
        return

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    item = {
        "run_id": f"BACKTEST#{symbol}",
        "sort_key": ts,
        "item_type": "BACKTEST",
        "symbol": symbol,
        "strategy_name": getattr(strategy, "strategy_name", strategy.__class__.__name__),
        "candidate_threshold": Decimal(str(round(new_threshold, 2))),
        "candidate_sharpe": Decimal(str(round(new_sharpe, 6))),
        "current_sharpe": Decimal(str(round(current_sharpe, 6))) if current_sharpe is not None else Decimal("-1"),
        "current_threshold": Decimal(str(current_threshold if current_threshold is not None else -1)),
        "source_parquet_path": source_path,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    table.put_item(Item=item)
    print(f"Wrote backtest result: {item['run_id']} / {item['sort_key']}")


if __name__ == "__main__":
    main()
