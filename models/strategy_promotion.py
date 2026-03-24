"""
Hybrid strategy promotion gates (backtester → STRATEGY#<SYMBOL>/LATEST).

All four conditions must pass; otherwise callers should log, skip Dynamo updates, and alert.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


class PromotionGateConfig(BaseModel):
    """Four-key safety rules (env-tunable)."""

    min_threshold_exclusive: float = Field(
        0.60,
        description="Candidate threshold must be strictly greater than this (anti-noise).",
    )
    max_threshold_exclusive: float = Field(
        0.90,
        description="Candidate threshold must be strictly less than this (anti-stall).",
    )
    max_jump_absolute: float = Field(
        0.15,
        description="If prior threshold exists, abs(new - old) must be strictly less than this.",
    )
    min_article_count_exclusive: int = Field(
        20,
        description="Article count must be strictly greater than this (statistical power).",
    )

    model_config = {"frozen": True}

    @field_validator("min_article_count_exclusive")
    @classmethod
    def _non_negative_articles(cls, v: int) -> int:
        if v < 0:
            raise ValueError("min_article_count_exclusive must be >= 0")
        return v

    @classmethod
    def from_env(cls) -> PromotionGateConfig:
        return cls(
            min_threshold_exclusive=float(os.getenv("STRATEGY_PROMOTION_MIN_THRESHOLD", "0.60")),
            max_threshold_exclusive=float(os.getenv("STRATEGY_PROMOTION_MAX_THRESHOLD", "0.90")),
            max_jump_absolute=float(os.getenv("STRATEGY_PROMOTION_MAX_JUMP", "0.15")),
            min_article_count_exclusive=int(os.getenv("STRATEGY_PROMOTION_MIN_ARTICLES", "20")),
        )


class PromotionEvaluationResult(BaseModel):
    approved: bool
    failures: list[str] = Field(default_factory=list)
    new_threshold: float
    old_threshold: Optional[float] = None
    article_count: int = 0

    model_config = {"frozen": True}


def evaluate_promotion_gates(
    *,
    new_threshold: float,
    old_threshold: Optional[float],
    article_count: int,
    config: PromotionGateConfig,
) -> PromotionEvaluationResult:
    failures: list[str] = []

    if not (new_threshold > config.min_threshold_exclusive):
        failures.append(
            f"min_threshold: new {new_threshold:.4f} must be > {config.min_threshold_exclusive} (anti-noise)"
        )
    if not (new_threshold < config.max_threshold_exclusive):
        failures.append(
            f"max_threshold: new {new_threshold:.4f} must be < {config.max_threshold_exclusive} (anti-stall)"
        )
    if old_threshold is not None:
        jump = abs(new_threshold - old_threshold)
        if not (jump < config.max_jump_absolute):
            failures.append(
                f"max_jump: abs(new-old)={jump:.4f} must be < {config.max_jump_absolute} "
                f"(old={old_threshold:.4f}, new={new_threshold:.4f})"
            )
    if not (article_count > config.min_article_count_exclusive):
        failures.append(
            f"sample_size: articles {article_count} must be > {config.min_article_count_exclusive}"
        )

    approved = len(failures) == 0
    return PromotionEvaluationResult(
        approved=approved,
        failures=failures,
        new_threshold=new_threshold,
        old_threshold=old_threshold,
        article_count=article_count,
    )


def parse_ddb_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == "true"


def resolve_is_active_after_promotion(*, table: Any, symbol: str) -> bool:
    """
    If STRATEGY_PROMOTE_SET_ACTIVE=true, force active; else preserve existing LATEST flag or false.
    """
    if os.getenv("STRATEGY_PROMOTE_SET_ACTIVE", "false").lower() == "true":
        return True
    key = {"run_id": f"STRATEGY#{symbol}", "sort_key": "LATEST"}
    existing = table.get_item(Key=key).get("Item") or {}
    return parse_ddb_bool(existing.get("is_active", False))


def build_strategy_latest_item(
    *,
    symbol: str,
    optimized_threshold: float,
    strategy_name: str,
    exit_type: str,
    hold_minutes: int,
    is_active: bool,
    source_backtest_sk: str,
) -> dict[str, Any]:
    """Full item for STRATEGY#<SYMBOL> / LATEST (validated shape)."""
    return {
        "run_id": f"STRATEGY#{symbol.upper()}",
        "sort_key": "LATEST",
        "item_type": "STRATEGY",
        "symbol": symbol.upper(),
        "is_active": is_active,
        "optimized_threshold": Decimal(str(round(optimized_threshold, 4))),
        "strategy_name": strategy_name,
        "exit_type": exit_type,
        "hold_minutes": Decimal(str(int(hold_minutes))),
        "promoted_at": datetime.now(timezone.utc).isoformat(),
        "source_backtest_sort_key": source_backtest_sk,
    }


def send_promotion_rejection_alert(
    *,
    symbol: str,
    result: PromotionEvaluationResult,
    new_sharpe: float,
    current_sharpe: Optional[float],
    topic_arn: Optional[str],
) -> None:
    if not topic_arn or not topic_arn.strip():
        logger.warning(
            "Strategy promotion rejected (no SNS topic configured)",
            extra={
                "symbol": symbol,
                "failures": result.failures,
                "new_threshold": result.new_threshold,
                "old_threshold": result.old_threshold,
                "article_count": result.article_count,
            },
        )
        return

    try:
        import boto3

        sns = boto3.client("sns")
        body = {
            "symbol": symbol,
            "status": "STRATEGY_PROMOTION_REJECTED",
            "message": "Sharpe improved but hybrid gates failed; LATEST unchanged.",
            "failures": result.failures,
            "new_threshold": result.new_threshold,
            "old_threshold": result.old_threshold,
            "article_count": result.article_count,
            "new_sharpe": new_sharpe,
            "current_sharpe": current_sharpe,
        }
        sns.publish(
            TopicArn=topic_arn.strip(),
            Subject=f"[Trading] Strategy promotion blocked: {symbol}",
            Message=json.dumps(body, indent=2),
        )
    except Exception:
        logger.exception("Failed to publish SNS promotion rejection alert", extra={"symbol": symbol})


def send_promotion_success_alert(
    *,
    symbol: str,
    new_threshold: float,
    old_threshold: Optional[float],
    new_sharpe: float,
    topic_arn: Optional[str],
) -> None:
    if not topic_arn or not topic_arn.strip():
        return
    try:
        import boto3

        sns = boto3.client("sns")
        body = {
            "symbol": symbol,
            "status": "STRATEGY_PROMOTION_APPLIED",
            "new_threshold": new_threshold,
            "old_threshold": old_threshold,
            "new_sharpe": new_sharpe,
        }
        sns.publish(
            TopicArn=topic_arn.strip(),
            Subject=f"[Trading] Strategy promoted: {symbol}",
            Message=json.dumps(body, indent=2),
        )
    except Exception:
        logger.exception("Failed to publish SNS promotion success alert", extra={"symbol": symbol})
