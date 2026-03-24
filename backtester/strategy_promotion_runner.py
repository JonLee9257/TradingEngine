"""
Post-backtest STRATEGY#LATEST promotion (no vectorbt/pandas dependency).

Called from ``backtest_engine`` after a BACKTEST row is written when Sharpe improved.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)


def maybe_promote_strategy_latest_after_backtest(
    *,
    table: Any,
    symbol: str,
    new_threshold: float,
    current_threshold: Optional[float],
    new_sharpe: float,
    current_sharpe: Optional[float],
    article_count: int,
    strategy_name: str,
    exit_type: str,
    hold_minutes: int,
    backtest_sort_key: str,
) -> None:
    """
    When ``STRATEGY_PROMOTION_ENABLED=true``, evaluate hybrid gates; promote or alert.
    """
    if os.getenv("STRATEGY_PROMOTION_ENABLED", "false").lower() != "true":
        return

    from models.strategy_promotion import (
        PromotionGateConfig,
        build_strategy_latest_item,
        evaluate_promotion_gates,
        resolve_is_active_after_promotion,
        send_promotion_rejection_alert,
        send_promotion_success_alert,
    )

    gate_cfg = PromotionGateConfig.from_env()
    promo = evaluate_promotion_gates(
        new_threshold=new_threshold,
        old_threshold=current_threshold,
        article_count=article_count,
        config=gate_cfg,
    )
    topic = os.getenv("STRATEGY_PROMOTION_ALERT_SNS_TOPIC_ARN", "").strip() or None

    if not promo.approved:
        logger.error(
            "Hybrid strategy promotion rejected; STRATEGY#LATEST unchanged",
            extra={"symbol": symbol, "failures": promo.failures},
        )
        for line in promo.failures:
            logger.error("promotion_gate_failed %s", line, extra={"symbol": symbol})
        send_promotion_rejection_alert(
            symbol=symbol,
            result=promo,
            new_sharpe=new_sharpe,
            current_sharpe=current_sharpe,
            topic_arn=topic,
        )
        return

    is_active = resolve_is_active_after_promotion(table=table, symbol=symbol)
    ddb_item = build_strategy_latest_item(
        symbol=symbol,
        optimized_threshold=new_threshold,
        strategy_name=strategy_name,
        exit_type=exit_type,
        hold_minutes=hold_minutes,
        is_active=is_active,
        source_backtest_sk=backtest_sort_key,
    )
    table.put_item(Item=ddb_item)
    logger.info(
        "Promoted STRATEGY#LATEST after hybrid gates",
        extra={"symbol": symbol, "new_threshold": new_threshold, "is_active": is_active},
    )
    send_promotion_success_alert(
        symbol=symbol,
        new_threshold=new_threshold,
        old_threshold=current_threshold,
        new_sharpe=new_sharpe,
        topic_arn=topic,
    )
