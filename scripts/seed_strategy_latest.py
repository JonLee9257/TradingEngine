#!/usr/bin/env python3
"""
Phase 1: Create or replace STRATEGY#<SYMBOL>/LATEST rows (bootstrap per ticker).

Usage:
  DYNAMODB_TABLE_NAME=TradingNewsSentiment python3 scripts/seed_strategy_latest.py --symbols TSLA,MSFT
  # or
  TOP_SYMBOLS=TSLA,MSFT python3 scripts/seed_strategy_latest.py

Does not enable trading unless --active is passed (default is_active=false).
"""

from __future__ import annotations

import argparse
import os
import sys
from decimal import Decimal
from pathlib import Path

# Repo root on path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import boto3
from pydantic import BaseModel, Field, field_validator


class SeedStrategyLatestItem(BaseModel):
    """Validated DynamoDB item shape for STRATEGY#<SYMBOL>/LATEST."""

    run_id: str
    sort_key: str = "LATEST"
    item_type: str = "STRATEGY"
    symbol: str
    is_active: bool = False
    optimized_threshold: Decimal
    strategy_name: str = "Sentiment_V1"
    exit_type: str = Field(default="fixed_time")
    hold_minutes: Decimal = Field(default=Decimal("60"))

    @field_validator("exit_type")
    @classmethod
    def _exit(cls, v: str) -> str:
        allowed = frozenset({"fixed_time", "end_of_day", "signal_flip"})
        if v not in allowed:
            raise ValueError(f"exit_type must be one of {sorted(allowed)}")
        return v

    def to_dynamo_item(self) -> dict:
        return {
            "run_id": self.run_id,
            "sort_key": self.sort_key,
            "item_type": self.item_type,
            "symbol": self.symbol.upper(),
            "is_active": self.is_active,
            "optimized_threshold": self.optimized_threshold,
            "strategy_name": self.strategy_name,
            "exit_type": self.exit_type,
            "hold_minutes": self.hold_minutes,
            "seeded_by": "scripts/seed_strategy_latest.py",
        }


def _parse_symbols(symbols_arg: str | None) -> list[str]:
    raw = symbols_arg or os.getenv("TOP_SYMBOLS", "")
    parts = [p.strip().upper() for p in raw.replace(" ", "").split(",") if p.strip()]
    return parts


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed STRATEGY#<SYMBOL>/LATEST in DynamoDB.")
    parser.add_argument(
        "--symbols",
        default="",
        help="Comma-separated tickers (else env TOP_SYMBOLS).",
    )
    parser.add_argument("--table", default=os.getenv("DYNAMODB_TABLE_NAME", "TradingNewsSentiment"))
    parser.add_argument(
        "--threshold",
        type=float,
        default=float(os.getenv("SEED_STRATEGY_THRESHOLD", "0.65")),
        help="Initial optimized_threshold (Decimal).",
    )
    parser.add_argument("--strategy-name", default=os.getenv("SEED_STRATEGY_NAME", "Sentiment_V1"))
    parser.add_argument("--exit-type", default=os.getenv("SEED_STRATEGY_EXIT_TYPE", "fixed_time"))
    parser.add_argument("--hold-minutes", type=int, default=int(os.getenv("SEED_STRATEGY_HOLD_MINUTES", "60")))
    parser.add_argument(
        "--active",
        action="store_true",
        help="Set is_active=true (default false for safe bootstrap).",
    )
    args = parser.parse_args()

    symbols = _parse_symbols(args.symbols or None)
    if not symbols:
        print("No symbols: pass --symbols TSLA,AAPL or set TOP_SYMBOLS.", file=sys.stderr)
        sys.exit(1)

    table = boto3.resource("dynamodb").Table(args.table)
    th = Decimal(str(round(args.threshold, 4)))

    for sym in symbols:
        model = SeedStrategyLatestItem(
            run_id=f"STRATEGY#{sym}",
            symbol=sym,
            is_active=args.active,
            optimized_threshold=th,
            strategy_name=args.strategy_name.strip(),
            exit_type=args.exit_type.strip(),
            hold_minutes=Decimal(str(args.hold_minutes)),
        )
        table.put_item(Item=model.to_dynamo_item())
        print(f"Seeded {model.run_id} / LATEST (is_active={model.is_active})")


if __name__ == "__main__":
    main()
