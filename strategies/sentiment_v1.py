"""
Morning sentiment strategy (Sentiment_V1): shared backtest + live decayed-sentiment math.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Optional

import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo

from strategies.base import BaseStrategy, OrderSide

logger = logging.getLogger(__name__)

EASTERN_TZ = ZoneInfo("America/New_York")


def _window_stats_vectorized(
    *,
    decision_ts: pd.Series,
    article_ts: pd.Series,
    article_scores: pd.Series,
    window_hours: int,
    decay_lambda: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Vectorized decayed sentiment over ``[decision - window_hours, decision]`` per decision timestamp.

    Must stay in sync with historical backtest behavior.
    """
    # Use DatetimeIndex.asi8 (int64 ns) — Series.view("int64") breaks on pandas 2.x.
    dec_ns = pd.DatetimeIndex(pd.to_datetime(decision_ts, utc=True, errors="coerce")).asi8
    art_ns = pd.DatetimeIndex(pd.to_datetime(article_ts, utc=True, errors="coerce")).asi8
    scores = article_scores.to_numpy(dtype=float)

    right = np.searchsorted(art_ns, dec_ns, side="left")
    left = np.searchsorted(art_ns, dec_ns - int(pd.Timedelta(hours=window_hours).value), side="left")

    raw_prefix = np.concatenate(([0.0], np.cumsum(scores)))
    counts = (right - left).astype(int)
    raw_sum = raw_prefix[right] - raw_prefix[left]
    raw_avg = np.where(counts > 0, raw_sum / np.maximum(counts, 1), 0.0)

    t_hours_art = art_ns / 3.6e12
    t_hours_dec = dec_ns / 3.6e12
    exp_pos = np.exp(decay_lambda * t_hours_art)
    score_exp = scores * exp_pos
    score_exp_prefix = np.concatenate(([0.0], np.cumsum(score_exp)))
    exp_prefix = np.concatenate(([0.0], np.cumsum(exp_pos)))

    weighted_num = np.exp(-decay_lambda * t_hours_dec) * (score_exp_prefix[right] - score_exp_prefix[left])
    weighted_den = np.exp(-decay_lambda * t_hours_dec) * (exp_prefix[right] - exp_prefix[left])
    signal = np.where(weighted_den > 0, weighted_num / weighted_den, 0.0)
    return counts, raw_avg, signal


def _ensure_news_dataframe(current_news: Any) -> pd.DataFrame:
    """Normalize live inputs (e.g. list of Dynamo-like dicts) to a DataFrame."""
    if isinstance(current_news, pd.DataFrame):
        return current_news.copy()
    if current_news is None:
        return pd.DataFrame()
    if isinstance(current_news, list):
        return pd.DataFrame(current_news)
    raise TypeError(f"current_news must be DataFrame or list of dicts, got {type(current_news)!r}")


def _coerce_article_ts_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Match backtester ``_build_world_state`` timing: derive ``article_ts`` from published / analyzed columns.
    """
    if df.empty or "article_ts" in df.columns:
        return df
    out = df.copy()
    ts: pd.Series | None = None
    for c in ("news_published_at", "analyzed_at", "published_at"):
        if c not in out.columns:
            continue
        parsed = pd.to_datetime(out[c], utc=True, errors="coerce")
        ts = parsed if ts is None else ts.fillna(parsed)
    if ts is None:
        return out
    out["article_ts"] = ts
    return out


class MorningSentimentStrategy(BaseStrategy):
    strategy_name = "Sentiment_V1"
    """
    Entry logic (backtest):
    - Decision points are 09:30 ET bars.
    - Signal is time-decayed sentiment:
      * 24h window lambda=0.1
      * 7d fallback lambda=0.5 if 24h has no articles
    - Enter when |signal| >= threshold

    Live:
    - ``check_live_signal`` uses the same ``_decayed_signal_at_decisions`` path with decision time = now (UTC).
    """

    def __init__(
        self,
        *,
        threshold: float,
        exit_type: str = "fixed_time",
        hold_minutes: int = 60,
        enable_shorts: bool = False,
        live_buy_threshold: float | None = None,
        live_sell_threshold: float | None = None,
        live_optimized_threshold: float | None = None,
    ) -> None:
        self.threshold = float(threshold)
        self.exit_type = exit_type
        self.hold_minutes = int(hold_minutes)
        self.enable_shorts = bool(enable_shorts)
        self._latest_signal_series: pd.Series | None = None
        self._live_buy_threshold = live_buy_threshold
        self._live_sell_threshold = live_sell_threshold
        self._live_optimized_threshold = live_optimized_threshold
        self.last_sentiment_score: Decimal = Decimal("0")

    @classmethod
    def from_executor_config(
        cls,
        cfg: dict[str, Any],
        *,
        buy_threshold: Decimal,
        sell_threshold: Decimal,
        enable_shorts: bool,
    ) -> MorningSentimentStrategy:
        """
        Build from DynamoDB STRATEGY#<SYMBOL>/LATEST fields + env thresholds (trade executor).
        """
        raw_opt = cfg.get("optimized_threshold", Decimal("1"))
        if isinstance(raw_opt, Decimal):
            opt_f = float(raw_opt)
        else:
            opt_f = float(raw_opt) if raw_opt not in (None, "") else 1.0

        exit_type = str(cfg.get("exit_type") or "fixed_time").strip() or "fixed_time"
        hm_raw = cfg.get("hold_minutes")
        if hm_raw is None or hm_raw == "":
            hold_minutes = 60
        elif isinstance(hm_raw, Decimal):
            hold_minutes = int(hm_raw)
        else:
            hold_minutes = int(hm_raw)

        mag = max(abs(float(buy_threshold)), abs(float(sell_threshold)))
        return cls(
            threshold=mag,
            exit_type=exit_type,
            hold_minutes=hold_minutes,
            enable_shorts=enable_shorts,
            live_buy_threshold=float(buy_threshold),
            live_sell_threshold=float(sell_threshold),
            live_optimized_threshold=opt_f,
        )

    def set_threshold(self, threshold: float) -> None:
        self.threshold = float(threshold)

    def generate_signals(self, price_df: pd.DataFrame, news_df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        if price_df.empty:
            empty = pd.Series([], dtype=bool)
            return empty, empty
        idx = price_df.index
        entries = pd.Series(False, index=idx)
        exits = pd.Series(False, index=idx)

        decision_mask = (idx.tz_convert(EASTERN_TZ).hour == 9) & (idx.tz_convert(EASTERN_TZ).minute == 30)
        decision_ts = idx[decision_mask]
        if len(decision_ts) == 0:
            self._latest_signal_series = pd.Series(0.0, index=idx)
            return entries, exits

        signal_at_decisions = self._decayed_signal_at_decisions(pd.Series(decision_ts), news_df)
        self._latest_signal_series = pd.Series(0.0, index=idx)
        self._latest_signal_series.loc[decision_ts] = signal_at_decisions.values

        decision_entries = signal_at_decisions.abs() >= self.threshold
        entry_ts = decision_ts[decision_entries.to_numpy()]
        if len(entry_ts) == 0:
            return entries, exits
        entries.loc[entry_ts] = True

        if self.exit_type == "fixed_time":
            target = pd.DatetimeIndex(entry_ts + pd.Timedelta(minutes=self.hold_minutes))
            exit_ts = idx.searchsorted(target, side="left")
            valid = exit_ts < len(idx)
            exits.iloc[exit_ts[valid]] = True
            return entries, exits

        if self.exit_type == "end_of_day":
            local = idx.tz_convert(EASTERN_TZ)
            eod_mask = (local.hour == 15) & (local.minute == 59)
            eod_ix = idx[eod_mask]
            if len(eod_ix) == 0:
                return entries, exits
            for ts in entry_ts:
                day_match = eod_ix[(eod_ix.tz_convert(EASTERN_TZ).date == ts.tz_convert(EASTERN_TZ).date)]
                if len(day_match):
                    exits.loc[day_match[0]] = True
            return entries, exits

        if self.exit_type == "signal_flip":
            open_dir = 0
            for ts in decision_ts:
                sig = float(signal_at_decisions.loc[ts])
                if entries.loc[ts] and open_dir == 0:
                    open_dir = 1 if sig >= 0 else -1
                    continue
                if open_dir > 0 and sig < 0:
                    exits.loc[ts] = True
                    open_dir = 0
                elif open_dir < 0 and sig > 0:
                    exits.loc[ts] = True
                    open_dir = 0
            return entries, exits

        raise ValueError(f"Unsupported exit_type={self.exit_type}. Use fixed_time|end_of_day|signal_flip")

    def _decayed_signal_at_decisions(self, decision_ts: pd.Series, news_df: pd.DataFrame) -> pd.Series:
        if news_df.empty:
            return pd.Series(0.0, index=pd.DatetimeIndex(decision_ts))
        work = news_df.copy()
        if "article_ts" not in work.columns:
            if isinstance(work.index, pd.DatetimeIndex):
                work = work.reset_index().rename(columns={work.index.name or "index": "article_ts"})
            else:
                work["article_ts"] = pd.NaT
        if "article_score" not in work.columns:
            work["article_score"] = pd.to_numeric(work.get("sentiment_score"), errors="coerce")
        work["article_ts"] = pd.to_datetime(work["article_ts"], utc=True, errors="coerce")
        work["article_score"] = pd.to_numeric(work["article_score"], errors="coerce")
        work = work.dropna(subset=["article_ts", "article_score"]).sort_values("article_ts")
        if work.empty:
            return pd.Series(0.0, index=pd.DatetimeIndex(decision_ts))
        c24, _raw24, sig24 = _window_stats_vectorized(
            decision_ts=decision_ts,
            article_ts=work["article_ts"],
            article_scores=work["article_score"],
            window_hours=24,
            decay_lambda=0.1,
        )
        c7d, _raw7d, sig7d = _window_stats_vectorized(
            decision_ts=decision_ts,
            article_ts=work["article_ts"],
            article_scores=work["article_score"],
            window_hours=24 * 7,
            decay_lambda=0.5,
        )
        use_24 = c24 > 0
        use_7d = (~use_24) & (c7d > 0)
        signal = np.where(use_24, sig24, np.where(use_7d, sig7d, 0.0))
        return pd.Series(signal, index=pd.DatetimeIndex(decision_ts))

    def _decayed_signal_scalar(self, current_news: Any) -> float:
        """Same vectorized decay as backtest, decision time = now (UTC)."""
        news_df = _coerce_article_ts_column(_ensure_news_dataframe(current_news))
        now_utc = pd.Timestamp.now(tz="UTC")
        decision_ts = pd.Series([now_utc])
        signal_series = self._decayed_signal_at_decisions(decision_ts, news_df)
        return float(signal_series.iloc[0])

    def decayed_live_signal(self, current_news: Any) -> float:
        """Public alias for exit logic / diagnostics (same math as ``check_live_signal`` decay step)."""
        return self._decayed_signal_scalar(current_news)

    def check_live_signal(
        self,
        current_prices: Any,
        current_news: Any,
        **kwargs: Any,
    ) -> Optional[OrderSide]:
        """
        Single decision at current UTC time using the same decay pipeline as ``generate_signals``.

        When constructed via ``from_executor_config`` (live), applies magnitude + optimized + buy/sell gates.
        Otherwise uses simple ``|signal| >= threshold`` long/short rules (standalone / tests).
        """
        _ = current_prices
        run_id = str(kwargs.get("run_id") or "")
        symbol = str(kwargs.get("symbol") or "")

        signal = self._decayed_signal_scalar(current_news)
        self.last_sentiment_score = Decimal(str(signal))

        if self._live_optimized_threshold is not None:
            buy_th = Decimal(str(self._live_buy_threshold))
            sell_th = Decimal(str(self._live_sell_threshold))
            opt_th = Decimal(str(self._live_optimized_threshold))
            mag = max(abs(float(buy_th)), abs(float(sell_th)))

            if abs(float(self.last_sentiment_score)) < mag:
                logger.info(
                    "Decayed signal below trade threshold; skipping symbol",
                    extra={"run_id": run_id, "symbol": symbol, "decayed_weighted_signal": float(self.last_sentiment_score)},
                )
                return None
            if self.last_sentiment_score < opt_th:
                logger.info(
                    "Sentiment score below optimized threshold; skipping symbol",
                    extra={
                        "run_id": run_id,
                        "symbol": symbol,
                        "score": str(self.last_sentiment_score),
                        "optimized_threshold": str(opt_th),
                    },
                )
                return None
            if self.last_sentiment_score >= buy_th:
                return OrderSide.BUY
            if self.last_sentiment_score <= sell_th:
                if not self.enable_shorts:
                    logger.info(
                        "No trade decision for symbol",
                        extra={"run_id": run_id, "symbol": symbol, "score": str(self.last_sentiment_score)},
                    )
                    return None
                return OrderSide.SELL
            logger.info(
                "No trade decision for symbol",
                extra={"run_id": run_id, "symbol": symbol, "score": str(self.last_sentiment_score)},
            )
            return None

        if abs(signal) < self.threshold:
            return None
        if signal >= self.threshold:
            return OrderSide.BUY
        if signal <= -self.threshold:
            if self.enable_shorts:
                return OrderSide.SELL
            return None
        return None
