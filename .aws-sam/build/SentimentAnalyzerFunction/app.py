"""
Lambda 2: Sentiment Analyzer

What it does:
- Reads one SQS message (which contains news articles per symbol)
- Calls Claude (Anthropic) to score sentiment per symbol
- Stores results in DynamoDB (idempotently)
- Invokes Lambda 3 (trade_executor) with the `run_id`
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Literal, Optional

from decimal import Decimal

import boto3
from anthropic import Anthropic
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr
from pydantic import BaseModel


# `logger` writes logs to CloudWatch.
logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

try:
    # Optional local-dev support.
    from dotenv import load_dotenv

    # For local testing: load env vars from a `.env` file if present.
    load_dotenv()
except Exception:
    pass


def _utc_now_iso() -> str:
    # Use timezone-aware UTC timestamp.
    return datetime.now(timezone.utc).isoformat()


def _truncate(s: str, max_chars: int) -> str:
    # LLMs have prompt-size limits; truncate long fields.
    s = s or ""
    if len(s) <= max_chars:
        return s.strip()
    return s[:max_chars].rstrip() + "…"


def _extract_first_json_object(text: str) -> dict:
    """
    Best-effort extraction for models that wrap JSON in additional text.
    """
    # Normalize whitespace first.
    text = (text or "").strip()

# Claude sometimes wraps JSON in markdown fences like:
# ```json
# { ... }
# ```
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

    # Step 1: If the model returned JSON only, parse it directly.
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    # Step 2: Otherwise, extract the first complete JSON object by matching braces.
    # This avoids the common pitfall of using regex like `\{.*?\}` which can stop
    # at the first closing brace inside nested objects/arrays.
    start_idx = text.find("{")
    if start_idx == -1:
        raise ValueError("No JSON object found in model output.")

    depth = 0
    in_string = False
    escape = False
    end_idx: Optional[int] = None

    for i in range(start_idx, len(text)):
        ch = text[i]

        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        # Not in a JSON string
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end_idx = i
                break

    if end_idx is None:
        raise ValueError("Could not find a complete JSON object in model output.")

    candidate = text[start_idx : end_idx + 1]
    parsed = json.loads(candidate)
    if not isinstance(parsed, dict):
        raise ValueError("Extracted JSON is not an object.")
    return parsed


def _backoff_sleep(attempt: int) -> None:
    # Exponential backoff for Anthropic API failures.
    base = float(os.getenv("ANTHROPIC_BACKOFF_BASE_SECONDS", "0.5"))
    max_sleep = float(os.getenv("ANTHROPIC_BACKOFF_MAX_SECONDS", "15"))
    sleep_s = min(max_sleep, base * (2 ** (attempt - 1)))
    time.sleep(sleep_s)


def _build_prompt(symbols: list[str], articles_by_symbol: dict) -> str:
    # Build a compact prompt describing the latest news for each symbol.
    articles_per_symbol = int(os.getenv("ARTICLES_PER_SYMBOL_PROMPT", "3"))
    per_article_max_chars = int(os.getenv("ARTICLE_TEXT_MAX_CHARS", "500"))

    parts: list[str] = []
    for sym in symbols:
        articles = articles_by_symbol.get(sym) or []
        articles = articles[:articles_per_symbol]

        lines: list[str] = []
        if articles:
            for a in articles:
                title = _truncate(a.get("title") or "", per_article_max_chars)
                desc = _truncate(a.get("description") or "", per_article_max_chars)
                published_at = a.get("publishedAt") or ""
                url = a.get("url") or ""
                # Keep this compact: Claude doesn't need URLs for sentiment.
                line = f"- {published_at} {title}. {desc}".strip()
                if url:
                    line += f" ({_truncate(url, 120)})"
                lines.append(line)

        if not lines:
            lines = ["- (no articles provided)"]

        parts.append(f"Symbol: {sym}\nRecent articles:\n" + "\n".join(lines))

    return "\n\n".join(parts)


class SentimentItem(BaseModel):
    symbol: str
    label: Literal["positive", "neutral", "negative"]
    score: Decimal
    rationale: str


class SentimentResponse(BaseModel):
    results: list[SentimentItem]


def _analyze_sentiment_with_claude(*, symbols: list[str], articles_by_symbol: dict) -> dict:
    # Claude API key + model name come from environment variables.
    api_key = os.environ["ANTHROPIC_API_KEY"]
    model = os.getenv("ANTHROPIC_MODEL", "")
    if not model:
        raise ValueError("ANTHROPIC_MODEL is not set. Example: claude-3-5-sonnet-latest")

    # Control inference behavior.
    max_attempts = int(os.getenv("ANTHROPIC_MAX_ATTEMPTS", "3"))
    max_tokens = int(os.getenv("ANTHROPIC_MAX_TOKENS", "900"))
    temperature = float(os.getenv("ANTHROPIC_TEMPERATURE", "0.2"))

    # Create the client (Anthropic SDK).
    client = Anthropic(api_key=api_key)

    # `system_prompt` tells the model how to respond (JSON-only).
    system_prompt = (
        "You are a trading sentiment analyst. "
        "You must respond with valid JSON only, matching the requested schema."
    )

    # Build the user prompt from the articles received in SQS.
    user_payload = _build_prompt(symbols=symbols, articles_by_symbol=articles_by_symbol)
    user_prompt = f"""
Given news articles for each symbol, assign a sentiment score from -1 to 1 and a label.

Rules:
- Score > 0.2 => positive, Score < -0.2 => negative, otherwise neutral.
- If there are no articles for a symbol, set score=0 and label="neutral".
- Sentiment should reflect the overall tone of the provided articles.
- Keep rationale to 1 short sentence.

Return JSON ONLY in this shape:
{{
  "results": [
    {{
      "symbol": "AAPL",
      "label": "positive|neutral|negative",
      "score": 0.0,
      "rationale": "..."
    }}
  ]
}}

Articles:
{user_payload}
""".strip()

    last_err: Optional[Exception] = None
    # Retry the Claude request if something goes wrong.
    for attempt in range(1, max_attempts + 1):
        try:
            # Structured outputs: Claude will be constrained to return JSON
            # matching our Pydantic schema (no fragile JSON parsing).
            resp = client.messages.parse(
                model=model,
                max_tokens=max_tokens,
                temperature=temperature,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
                output_format=SentimentResponse,
            )

            parsed = resp.parsed_output
            # pydantic v2 uses `model_dump`; v1 uses `dict()`.
            if hasattr(parsed, "model_dump"):
                return parsed.model_dump()
            return parsed.dict()
        except Exception as e:  # noqa: BLE001
            last_err = e
            logger.warning(
                "Claude sentiment analysis failed",
                extra={"attempt": attempt, "max_attempts": max_attempts, "error": str(e)},
            )
            if attempt < max_attempts:
                _backoff_sleep(attempt)

    raise RuntimeError(f"Claude sentiment analysis failed after {max_attempts} attempts: {last_err}")


def _put_sentiment_item(*, table, run_id: str, symbol: str, item: dict, source: dict):
    """
    Idempotent write: if the item already exists for this symbol+run_id, ignore.
    """
    # DynamoDB uses a composite key: (run_id, sort_key).
    sort_key = f"SENTIMENT#{symbol}"
    now_iso = _utc_now_iso()
    payload = {
        "run_id": run_id,
        "sort_key": sort_key,
        "item_type": "SENTIMENT",
        "symbol": symbol,
        "sentiment_label": item["label"],
        # DynamoDB/boto3 requires numbers to be Decimal (floats are not supported).
        "sentiment_score": Decimal(str(item["score"])),
        "rationale": item.get("rationale") or "",
        "analyzed_at": now_iso,
        "triggered_at": source.get("triggered_at"),
        "model": source.get("model"),
        "lambda_request_id": source.get("lambda_request_id"),
    }

    try:
        # ConditionalExpression ensures we only write once.
        table.put_item(
            Item=payload,
            ConditionExpression=Attr("sort_key").not_exists(),
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            logger.info("Sentiment item already exists; skipping", extra={"run_id": run_id, "symbol": symbol})
            return
        raise


def _put_run_metadata(*, table, run_id: str, source: dict):
    # Store basic metadata about this run (trigger time, lookback, model).
    try:
        table.put_item(
            Item={
                "run_id": run_id,
                "sort_key": "RUN_META",
                "item_type": "RUN_META",
                "triggered_at": source.get("triggered_at"),
                "lookback_hours": source.get("lookback_hours"),
                "analyzed_at": _utc_now_iso(),
                "lambda_request_id": source.get("lambda_request_id"),
                "model": source.get("model"),
            },
            ConditionExpression=Attr("sort_key").not_exists(),
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return
        raise


def _invoke_trade_executor(*, lambda_client, trade_executor_arn: str, run_id: str) -> None:
    # Call Lambda 3 synchronously (RequestResponse) so we can fail the SQS batch on errors.
    resp = lambda_client.invoke(
        FunctionName=trade_executor_arn,
        InvocationType="RequestResponse",
        Payload=json.dumps({"run_id": run_id}),
    )
    payload_bytes = resp.get("Payload").read() if resp.get("Payload") else b"{}"
    payload_text = payload_bytes.decode("utf-8") if isinstance(payload_bytes, (bytes, bytearray)) else str(payload_bytes)
    try:
        payload = json.loads(payload_text) if payload_text else {}
    except json.JSONDecodeError:
        payload = {"raw": payload_text}

    if "errorMessage" in payload or resp.get("FunctionError"):
        raise RuntimeError(f"Trade executor failed: {payload.get('errorMessage') or payload}")

    # If trade executor returned {status:"ok"} etc, we don't need details here.


def _process_one_record(record: dict, *, table, trade_executor_arn: str, lambda_request_id: str):
    # Each SQS message is handled independently.
    # `record["body"]` is a JSON string.
    body = json.loads(record["body"])
    run_id = body["run_id"]
    triggered_at = body.get("triggered_at")
    symbols = body.get("symbols") or []
    if not symbols or not isinstance(symbols, list):
        raise ValueError("Invalid SQS message: missing/invalid 'symbols'.")
    articles_by_symbol = body.get("articles_by_symbol") or {}

    logger.info("Analyzing sentiment", extra={"run_id": run_id, "symbols": symbols})

    model = os.getenv("ANTHROPIC_MODEL", "")
    # Ask Claude for sentiment results.
    results = _analyze_sentiment_with_claude(symbols=symbols, articles_by_symbol=articles_by_symbol)
    raw_results = results.get("results") or []

    # Normalize the model response into a dict keyed by symbol.
    by_symbol: dict[str, dict] = {}
    for r in raw_results:
        sym = (r.get("symbol") or "").strip().upper()
        if not sym:
            continue
        by_symbol[sym] = r

    source = {
        "triggered_at": triggered_at,
        "lookback_hours": body.get("lookback_hours"),
        "lambda_request_id": lambda_request_id,
        "model": model,
    }

    # Store run metadata + sentiment items, then trigger the trade executor.
    _put_run_metadata(table=table, run_id=run_id, source=source)
    for sym in symbols:
        item = by_symbol.get(sym)
        if not item:
            item = {"symbol": sym, "label": "neutral", "score": 0.0, "rationale": "No analysis returned by model."}
        # Coerce label/score.
        label = (item.get("label") or "neutral").strip().lower()
        if label not in ("positive", "neutral", "negative"):
            label = "neutral"
        score_raw = item.get("score") or 0
        if isinstance(score_raw, Decimal):
            score = score_raw
        else:
            # Ensure we never pass floats into DynamoDB/boto3.
            score = Decimal(str(score_raw))
        if score > Decimal("1"):
            score = Decimal("1")
        if score < Decimal("-1"):
            score = Decimal("-1")
        _put_sentiment_item(
            table=table,
            run_id=run_id,
            symbol=sym,
            item={"label": label, "score": score, "rationale": item.get("rationale") or ""},
            source=source,
        )

    lambda_client = boto3.client("lambda")
    _invoke_trade_executor(lambda_client=lambda_client, trade_executor_arn=trade_executor_arn, run_id=run_id)


def handler(event, context):
    """
    SQS consumer -> analyze sentiment with Claude -> store in DynamoDB -> invoke trade executor.
    """
    # DynamoDB Table resource gives higher-level convenience methods.
    table = boto3.resource("dynamodb").Table(os.environ["DYNAMODB_TABLE_NAME"])
    trade_executor_arn = os.environ["TRADE_EXECUTOR_FUNCTION_ARN"]
    lambda_request_id = getattr(context, "aws_request_id", None)

    records = event.get("Records") or []
    # If any single record fails, we fail the entire invocation.
    # With SAM config `BatchSize: 1`, that means a single SQS message is retried.
    for record in records:
        message_id = record.get("messageId")
        try:
            _process_one_record(
                record,
                table=table,
                trade_executor_arn=trade_executor_arn,
                lambda_request_id=lambda_request_id,
            )
        except Exception:  # noqa: BLE001
            logger.exception("Failed to process SQS record; failing invocation", extra={"messageId": message_id})
            raise

    return {"status": "ok", "processed": len(records)}

