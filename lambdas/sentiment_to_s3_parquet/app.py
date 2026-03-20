"""
DynamoDB Streams -> S3 Parquet sink for SENTIMENT items.

This Lambda listens to stream records and writes INSERTed SENTIMENT items
to an S3 partitioned dataset using awswrangler.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import awswrangler as wr
import pandas as pd
from boto3.dynamodb.types import TypeDeserializer

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

_DESERIALIZER = TypeDeserializer()


def _to_native_item(ddb_image: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {k: _DESERIALIZER.deserialize(v) for k, v in ddb_image.items()}


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _partition_date(item: dict[str, Any]) -> str:
    raw = item.get("analyzed_at") or item.get("triggered_at")
    if isinstance(raw, str) and raw:
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def handler(event, context):
    bucket = os.environ["S3_LAKE_BUCKET"]
    prefix = os.getenv("S3_LAKE_PREFIX", "sentiment-buffered").strip("/")
    s3_path = f"s3://{bucket}/{prefix}/"
    database = os.getenv("GLUE_DATABASE")
    table = os.getenv("GLUE_TABLE", "sentiment_buffered")

    rows: list[dict[str, Any]] = []
    for rec in event.get("Records", []):
        if rec.get("eventName") != "INSERT":
            continue
        image = ((rec.get("dynamodb") or {}).get("NewImage")) or {}
        if not image:
            continue
        item = _to_native_item(image)
        if item.get("item_type") != "SENTIMENT":
            continue

        item["partition_date"] = _partition_date(item)
        rows.append(_json_safe(item))

    if not rows:
        return {"status": "ok", "written_rows": 0}

    df = pd.DataFrame(rows)
    # "append to daily buffered parquet" in lake style: append row groups/files
    # under date partitions for efficient backtesting scans.
    wr.s3.to_parquet(
        df=df,
        path=s3_path,
        dataset=True,
        mode="append",
        partition_cols=["partition_date"],
        database=database,
        table=table if database else None,
        compression="snappy",
    )

    logger.info(
        "Wrote buffered sentiment parquet records",
        extra={"rows": len(df), "s3_path": s3_path, "partition_dates": sorted(df["partition_date"].unique())},
    )
    return {"status": "ok", "written_rows": int(len(df))}

