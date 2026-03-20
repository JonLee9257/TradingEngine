"""
Lambda 1: News Fetcher

What it does:
- Triggered by EventBridge (cron)
- Fetches recent news for the configured top symbols (NewsAPI)
- Sends the results to SQS so the next Lambda can process them
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
import requests


# `logger` lets us write structured logs to CloudWatch.
logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

try:
    # Optional local-dev support (SAM/pytest often don't auto-load .env).
    from dotenv import load_dotenv

    # In local runs, `.env` may be used to load environment variables.
    load_dotenv()
except Exception:
    pass


def _utc_now_iso() -> str:
    # Use timezone-aware UTC time so timestamps are consistent across services.
    return datetime.now(timezone.utc).isoformat()


def _parse_symbols() -> list[str]:
    # Read a comma-separated env var like: "AAPL,MSFT,NVDA,..."
    raw = os.getenv("TOP_SYMBOLS", "")
    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
    if not symbols:
        raise ValueError("TOP_SYMBOLS is empty; expected comma-separated stock symbols.")
    # Keep the Lambda bounded: the architecture says "top 10 stocks".
    return symbols[:10]


def _backoff_sleep(attempt: int) -> None:
    # Simple exponential backoff:
    # attempt=1 => base seconds, attempt=2 => base*2, attempt=3 => base*4, ...
    base = float(os.getenv("NEWS_API_BACKOFF_BASE_SECONDS", "0.5"))
    max_sleep = float(os.getenv("NEWS_API_BACKOFF_MAX_SECONDS", "10"))
    sleep_s = min(max_sleep, base * (2 ** (attempt - 1)))
    time.sleep(sleep_s)


def _fetch_news_api(symbol: str, from_date_utc: datetime) -> list[dict]:
    """
    Fetch recent news for a symbol from NewsAPI.

    Returns a list of normalized articles:
      [{title, description, url, publishedAt}, ...]
    """
    # Environment variables are injected by SAM/Lambda configuration.
    news_api_key = os.environ["NEWS_API_KEY"]
    base_url = os.getenv("NEWS_API_BASE_URL", "https://newsapi.org")
    endpoint = os.getenv("NEWS_API_ENDPOINT", "/v2/everything")

    # Controls how many articles to fetch and how the request behaves.
    articles_per_symbol = int(os.getenv("ARTICLES_PER_SYMBOL", "3"))
    max_attempts = int(os.getenv("NEWS_API_MAX_ATTEMPTS", "3"))
    timeout_s = float(os.getenv("NEWS_API_TIMEOUT_SECONDS", "15"))
    language = os.getenv("NEWS_API_LANGUAGE", "en")

    # Build the request URL and the query parameters.
    url = f"{base_url.rstrip('/')}{endpoint}"
    from_date_str = from_date_utc.date().isoformat()  # NewsAPI expects YYYY-MM-DD

    params = {
        "q": symbol,
        "language": language,
        "sortBy": "publishedAt",
        "pageSize": articles_per_symbol,
        "from": from_date_str,
        "apiKey": news_api_key,
    }

    session = requests.Session()

    last_err: Optional[Exception] = None
    # Retry external calls (NewsAPI can be flaky).
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout_s)
            if 500 <= resp.status_code:
                raise RuntimeError(f"NewsAPI server error: {resp.status_code} {resp.text[:200]}")
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != "ok":
                raise RuntimeError(f"NewsAPI returned status={data.get('status')}: {data}")

            articles = data.get("articles") or []
            normalized: list[dict] = []
            for a in articles[:articles_per_symbol]:
                normalized.append(
                    {
                        "title": (a.get("title") or "").strip(),
                        "description": (a.get("description") or "").strip(),
                        "url": a.get("url") or "",
                        "publishedAt": a.get("publishedAt") or "",
                    }
                )
            return normalized
        except Exception as e:  # noqa: BLE001 - we want a robust retry boundary
            last_err = e
            # Log why we failed, then wait and retry (until max_attempts).
            logger.warning(
                "NewsAPI fetch failed",
                extra={"symbol": symbol, "attempt": attempt, "max_attempts": max_attempts, "error": str(e)},
            )
            if attempt < max_attempts:
                _backoff_sleep(attempt)

    raise RuntimeError(f"NewsAPI fetch failed after {max_attempts} attempts: {last_err}")


def handler(event, context):
    """
    This is the Lambda entry point.

    EventBridge cron -> fetch NewsAPI headlines for top 10 stocks -> send to SQS.
    """
    # `run_id` is our "job id". Downstream Lambdas use it to correlate records.
    # Keep run_id stable for downstream processing/retries.
    run_id = str(uuid.uuid4())

    # The symbol list comes from env var.
    symbols = _parse_symbols()

    # EventBridge sometimes passes a `time` field; if not, we fall back to current UTC.
    triggered_at = event.get("time") or event.get("detail", {}).get("time") or _utc_now_iso()

    # How far back we look for articles.
    lookback_hours = int(os.getenv("NEWS_LOOKBACK_HOURS", "24"))
    from_date_utc = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

    # Create an SQS client to send a message.
    sqs_queue_url = os.environ["SQS_QUEUE_URL"]
    sqs = boto3.client("sqs")

    logger.info(
        "Starting news fetcher",
        extra={"run_id": run_id, "symbols": symbols, "triggered_at": triggered_at},
    )

    # Collect results so we can send one SQS message containing all symbols.
    articles_by_symbol: dict[str, list[dict]] = {}
    for symbol in symbols:
        try:
            # Fetch a small list of articles for this symbol.
            articles = _fetch_news_api(symbol=symbol, from_date_utc=from_date_utc)
        except Exception as e:  # noqa: BLE001
            # Allow partial progress: downstream can decide how to handle empty articles.
            logger.exception("NewsAPI fetch failed for symbol; sending empty list", extra={"symbol": symbol})
            articles = []
        articles_by_symbol[symbol] = articles

    # This JSON message is what `sentiment_analyzer` reads from SQS.
    message = {
        "run_id": run_id,
        "triggered_at": triggered_at,
        "symbols": symbols,
        "lookback_hours": lookback_hours,
        "articles_by_symbol": articles_by_symbol,
        "source": {
            "lambda_request_id": getattr(context, "aws_request_id", None),
            "fetched_at": _utc_now_iso(),
        },
    }

    try:
        resp = sqs.send_message(
            # QueueUrl identifies the queue; message body is a JSON string.
            QueueUrl=sqs_queue_url,
            MessageBody=json.dumps(message),
        )
    except Exception as e:  # noqa: BLE001
        logger.exception("Failed to send message to SQS", extra={"run_id": run_id})
        raise

    logger.info("SQS message sent", extra={"run_id": run_id, "messageId": resp.get("MessageId")})
    return {"status": "ok", "run_id": run_id, "symbols": symbols}

