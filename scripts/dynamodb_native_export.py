"""
Enable PITR and start native DynamoDB export to S3 (Parquet).

Usage:
  export AWS_REGION=us-east-1
  python3 scripts/dynamodb_native_export.py \
    --table-name TradingNewsSentiment \
    --s3-bucket my-backtest-lake-bucket \
    --s3-prefix dynamodb-exports/tradingnews
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

import boto3


def _table_arn(dynamodb, table_name: str) -> str:
    resp = dynamodb.describe_table(TableName=table_name)
    return resp["Table"]["TableArn"]


def _enable_pitr(dynamodb, table_name: str) -> None:
    dynamodb.update_continuous_backups(
        TableName=table_name,
        PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
    )


def _start_export(dynamodb, *, table_arn: str, s3_bucket: str, s3_prefix: str) -> dict:
    # Pick a recent "export time" so the export can start immediately after PITR.
    export_time = datetime.now(timezone.utc) - timedelta(minutes=5)
    return dynamodb.export_table_to_point_in_time(
        TableArn=table_arn,
        S3Bucket=s3_bucket,
        S3Prefix=s3_prefix,
        ExportFormat="DYNAMODB_JSON",
        S3SseAlgorithm="AES256",
        ExportType="FULL_EXPORT",
        ExportTime=export_time,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Enable PITR and trigger native DynamoDB export to S3.")
    parser.add_argument("--table-name", required=True)
    parser.add_argument("--s3-bucket", required=True)
    parser.add_argument("--s3-prefix", required=True)
    args = parser.parse_args()

    dynamodb = boto3.client("dynamodb")
    print(f"Enabling PITR on table={args.table_name} ...")
    _enable_pitr(dynamodb, args.table_name)

    table_arn = _table_arn(dynamodb, args.table_name)
    print(f"Starting export for arn={table_arn} to s3://{args.s3_bucket}/{args.s3_prefix}")
    resp = _start_export(
        dynamodb,
        table_arn=table_arn,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix.rstrip("/"),
    )

    export_desc = resp.get("ExportDescription", {})
    print("Export started.")
    print(f"  ExportArn: {export_desc.get('ExportArn')}")
    print(f"  ExportStatus: {export_desc.get('ExportStatus')}")


if __name__ == "__main__":
    main()
