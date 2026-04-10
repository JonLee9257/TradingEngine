"""
FastMCP gateway to the three trading Lambdas (news_fetcher, sentiment_analyzer, trade_executor).

Install: pip install -r requirements-mcp.txt
Run:     python mcp_server.py

Override Lambda names via MCP_* environment variables after redeploys (suffixes may change).
"""

from __future__ import annotations

import json
import logging
import os
from decimal import Decimal
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Placeholders — search/replace these or override via environment variables.
# ---------------------------------------------------------------------------

# Lambda function names (short name, not ARN — pass to Lambda Invoke API).
PLACEHOLDER_NEWS_FETCHER_FUNCTION_NAME = "trading-system-paper-NewsFetcherFunction-etCL3vwkDP9w"
PLACEHOLDER_SENTIMENT_ANALYZER_FUNCTION_NAME = "trading-system-paper-SentimentAnalyzerFunction-bveJEh2pDu0f"
PLACEHOLDER_TRADE_EXECUTOR_FUNCTION_NAME = "trading-system-paper-TradeExecutorFunction-pHR0f5UUbcgT"

# Env var names (optional overrides)
ENV_NEWS_FETCHER = "MCP_NEWS_FETCHER_FUNCTION_NAME"
ENV_SENTIMENT_ANALYZER = "MCP_SENTIMENT_ANALYZER_FUNCTION_NAME"
ENV_TRADE_EXECUTOR = "MCP_TRADE_EXECUTOR_FUNCTION_NAME"
ENV_AWS_REGION = "AWS_REGION"

logger = logging.getLogger(__name__)


def _env_or_default(env_name: str, default: str) -> str:
    v = os.getenv(env_name, "").strip()
    return v if v else default


def _json_safe(obj: Any) -> Any:
    """Normalize Lambda JSON payloads (e.g. Decimal) for MCP-friendly output."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(x) for x in obj]
    return obj


def _read_lambda_invoke_payload(payload_stream: Any) -> dict[str, Any]:
    raw = payload_stream.read() if payload_stream is not None else b""
    if isinstance(raw, (bytes, bytearray)):
        text = raw.decode("utf-8", errors="replace")
    else:
        text = str(raw)
    if not text.strip():
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw": text}


def _invoke_lambda(
    *,
    lambda_client: Any,
    function_name: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """
    Synchronously invoke a Lambda and return a structured result.

    On success: {"ok": True, "function_name": ..., "response": parsed_or_raw}
    On Lambda or boto error: {"ok": False, "error": "...", "function_name": ...}
    """
    try:
        resp = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )
    except (ClientError, BotoCoreError) as e:
        err = f"{type(e).__name__}: {e}"
        logger.exception("Lambda invoke failed", extra={"function_name": function_name})
        return {"ok": False, "function_name": function_name, "error": err}

    out: dict[str, Any] = {
        "ok": True,
        "function_name": function_name,
        "status_code": resp.get("StatusCode"),
    }
    parsed = _read_lambda_invoke_payload(resp.get("Payload"))

    if resp.get("FunctionError"):
        out["ok"] = False
        msg = parsed.get("errorMessage") or parsed.get("error") or json.dumps(parsed)
        out["error"] = f"Lambda FunctionError ({resp.get('FunctionError')}): {msg}"
        out["response"] = _json_safe(parsed)
        return out

    out["response"] = _json_safe(parsed)
    return out


mcp = FastMCP("Trading AWS Gateway")


@mcp.tool
def fetch_market_news(
    event: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Invoke the news_fetcher Lambda to pull recent headlines for configured symbols
    and enqueue a batch message for sentiment analysis (typically to SQS).

    Pass an optional EventBridge-shaped payload, e.g. {"time": "<ISO8601>"}.
    An empty object {} is valid; the Lambda generates its own run_id and reads
    TOP_SYMBOLS from its environment.

    Returns the Lambda JSON response (e.g. status, run_id, symbols) or an error payload.
    """
    function_name = _env_or_default(ENV_NEWS_FETCHER, PLACEHOLDER_NEWS_FETCHER_FUNCTION_NAME)
    region = _env_or_default(ENV_AWS_REGION, "us-east-1")
    payload = dict(event) if event else {}
    lambda_client = boto3.client("lambda", region_name=region)
    return _invoke_lambda(lambda_client=lambda_client, function_name=function_name, payload=payload)


@mcp.tool
def analyze_sentiment(message_body: dict[str, Any]) -> dict[str, Any]:
    """
    Invoke the sentiment_analyzer Lambda with a synthetic SQS event.

    `message_body` must match what news_fetcher places on the queue, including at least:
    - run_id (str)
    - symbols (list[str])
    - articles_by_symbol (dict mapping symbol -> list of article dicts)

    Optional fields: triggered_at, lookback_hours, source.

    The Lambda runs Claude sentiment, writes SENTIMENT rows to DynamoDB, and may invoke
    trade_executor when configured. Returns the Lambda response or a structured error.
    """
    function_name = _env_or_default(ENV_SENTIMENT_ANALYZER, PLACEHOLDER_SENTIMENT_ANALYZER_FUNCTION_NAME)
    region = _env_or_default(ENV_AWS_REGION, "us-east-1")

    sqs_event = {
        "Records": [
            {
                "messageId": "mcp-synthetic-id",
                "receiptHandle": "mcp-placeholder-receipt",
                "body": json.dumps(message_body),
                "attributes": {"ApproximateReceiveCount": "1"},
                "messageAttributes": {},
                "md5OfBody": "mcp-placeholder",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:REGION:ACCOUNT:QUEUE_NAME",
                "awsRegion": region,
            }
        ]
    }

    lambda_client = boto3.client("lambda", region_name=region)
    return _invoke_lambda(lambda_client=lambda_client, function_name=function_name, payload=sqs_event)


@mcp.tool
def execute_trade(run_id: str) -> dict[str, Any]:
    """
    Invoke the trade_executor Lambda for a completed sentiment batch.

    Parameters
    ----------
    run_id
        Correlation id for the pipeline; must match SENTIMENT rows written for that run.

    The Lambda loads strategy config, queries recent sentiment from DynamoDB, and places
    idempotent paper trades via Alpaca. Returns the Lambda JSON response or an error payload.
    """
    function_name = _env_or_default(ENV_TRADE_EXECUTOR, PLACEHOLDER_TRADE_EXECUTOR_FUNCTION_NAME)
    region = _env_or_default(ENV_AWS_REGION, "us-east-1")
    lambda_client = boto3.client("lambda", region_name=region)
    return _invoke_lambda(lambda_client=lambda_client, function_name=function_name, payload={"run_id": run_id})


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    mcp.run()
