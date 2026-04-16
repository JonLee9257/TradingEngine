"""
Bedrock Agent action-group router: maps OpenAPI operations (or function-style calls)
to trading Lambdas via boto3 invoke, and shapes responses per Amazon Bedrock docs.

Success (OpenAPI action group): httpStatusCode 200 + responseBody.application/json.body
as a JSON string (FULFILLED / successful completion).

Reference: https://docs.aws.amazon.com/bedrock/latest/userguide/agents-lambda.html

Environment (same semantics as mcp_server.py):
  NEWS_FETCHER_FUNCTION_NAME, SENTIMENT_ANALYZER_FUNCTION_NAME, TRADE_EXECUTOR_FUNCTION_NAME
Optional overrides:
  MCP_NEWS_FETCHER_FUNCTION_NAME, MCP_SENTIMENT_ANALYZER_FUNCTION_NAME, MCP_TRADE_EXECUTOR_FUNCTION_NAME
  AWS_REGION (default us-east-1)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Defaults match mcp_server.py placeholders; override via env in Lambda configuration.
_DEFAULT_NEWS = "trading-system-paper-NewsFetcherFunction-etCL3vwkDP9w"
_DEFAULT_SENTIMENT = "trading-system-paper-SentimentAnalyzerFunction-bveJEh2pDu0f"
_DEFAULT_TRADE = "trading-system-paper-TradeExecutorFunction-pHR0f5UUbcgT"


def _env(name: str, fallback: str) -> str:
    v = os.getenv(name, "").strip()
    return v if v else fallback


def _lambda_name_news() -> str:
    return _env("MCP_NEWS_FETCHER_FUNCTION_NAME", _env("NEWS_FETCHER_FUNCTION_NAME", _DEFAULT_NEWS))


def _lambda_name_sentiment() -> str:
    return _env(
        "MCP_SENTIMENT_ANALYZER_FUNCTION_NAME",
        _env("SENTIMENT_ANALYZER_FUNCTION_NAME", _DEFAULT_SENTIMENT),
    )


def _lambda_name_trade() -> str:
    return _env("MCP_TRADE_EXECUTOR_FUNCTION_NAME", _env("TRADE_EXECUTOR_FUNCTION_NAME", _DEFAULT_TRADE))


def _region() -> str:
    return _env("AWS_REGION", "us-east-1")


def _coerce_session_for_bedrock(raw: Any) -> dict[str, str]:
    """
    Bedrock Agents expect sessionAttributes / promptSessionAttributes as string→string maps.
    Non-strings (nested dicts, numbers) cause: "error processing the Lambda response".
    """
    if not isinstance(raw, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in raw.items():
        key = str(k)
        if v is None:
            continue
        if isinstance(v, str):
            out[key] = v
        elif isinstance(v, (bool, int, float)):
            out[key] = str(v)
        else:
            out[key] = json.dumps(v, default=str)
    return out


def _props_list_to_dict(properties: list[dict[str, Any]] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for p in properties or []:
        name = p.get("name")
        if not name:
            continue
        out[str(name)] = p.get("value")
    return out


def _parse_json_property(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return val
    return val


def _normalize_bedrock_api_path(raw: str) -> str:
    """
    Bedrock usually sends apiPath as in the OpenAPI file (e.g. /fetch_market_news).
    Some integrations send no leading slash or a full URL — normalize so routing matches.
    """
    s = (raw or "").strip().split("?")[0].strip()
    if not s:
        return "/"
    if "://" in s:
        s = urlparse(s).path or "/"
    if not s.startswith("/"):
        s = f"/{s}"
    s = s.rstrip("/")
    return s if s else "/"


def _extract_openapi_body_dict(event: dict[str, Any]) -> dict[str, Any]:
    """Bedrock OpenAPI: requestBody.content['application/json'].properties OR .body JSON string."""
    rb = event.get("requestBody")
    if not isinstance(rb, dict):
        return {}
    content = rb.get("content")
    if not isinstance(content, dict):
        return {}
    for _ct, body in content.items():
        if not isinstance(body, dict):
            continue
        props = body.get("properties")
        if isinstance(props, list) and props:
            return _props_list_to_dict(props)
        raw_body = body.get("body")
        if isinstance(raw_body, str) and raw_body.strip():
            try:
                parsed = json.loads(raw_body)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                continue
    return {}


def _invoke_lambda_sync(function_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    client = boto3.client("lambda", region_name=_region())
    try:
        resp = client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload, default=str).encode("utf-8"),
        )
    except (ClientError, BotoCoreError) as e:
        logger.exception("Lambda invoke failed", extra={"function_name": function_name})
        return {"ok": False, "error": str(e), "function_name": function_name}

    raw_bytes = resp.get("Payload").read() if resp.get("Payload") else b""
    text = raw_bytes.decode("utf-8", errors="replace") if raw_bytes else ""
    status_code = resp.get("StatusCode")
    try:
        parsed = json.loads(text) if text.strip() else {}
    except json.JSONDecodeError:
        parsed = {"raw": text}

    if resp.get("FunctionError"):
        return {
            "ok": False,
            "function_name": function_name,
            "status_code": status_code,
            "error": parsed.get("errorMessage") or json.dumps(parsed, default=str),
            "response": parsed,
        }

    return {"ok": True, "function_name": function_name, "status_code": status_code, "response": parsed}


def _openapi_action_body(*, inv: dict[str, Any], step: str, next_obj: dict[str, Any] | None = None) -> dict[str, Any]:
    """Payload inside responseBody.application/json.body — matches OpenAPI LambdaInvokeEnvelope + step/next."""
    body: dict[str, Any] = {
        "ok": bool(inv.get("ok")),
        "function_name": inv.get("function_name") or "",
    }
    if inv.get("status_code") is not None:
        body["status_code"] = inv["status_code"]
    if inv.get("error"):
        body["error"] = str(inv["error"])
    inner = inv.get("response")
    if isinstance(inner, dict):
        body["response"] = inner
    body["step"] = step
    if next_obj is not None:
        body["next"] = next_obj
    return body


def _build_message_body_after_fetch(
    *,
    run_id: str,
    symbols: list[str],
    lookback_hours: int = 24,
) -> dict[str, Any]:
    """SQS-shaped body for sentiment_analyzer; articles empty — same as MCP chaining."""
    sym_upper = [str(s).strip().upper() for s in symbols if str(s).strip()]
    return {
        "run_id": run_id,
        "triggered_at": datetime.now(timezone.utc).isoformat(),
        "symbols": sym_upper,
        "lookback_hours": lookback_hours,
        "articles_by_symbol": {s: [] for s in sym_upper},
        "source": {"bedrock_router": "chained_after_fetch_market_news"},
    }


def _bedrock_openapi_success(
    event: dict[str, Any],
    *,
    http_status: int,
    payload_obj: dict[str, Any],
    session_attributes: dict[str, Any],
    prompt_session_attributes: dict[str, Any],
) -> dict[str, Any]:
    """Amazon Bedrock OpenAPI action group success: body must be a JSON *string*."""
    # ASCII-safe JSON string: some Bedrock paths are strict about response body encoding.
    body_str = json.dumps(payload_obj, default=str, ensure_ascii=True)
    response_body = {
        "application/json": {
            "body": body_str,
        }
    }
    action_response = {
        "actionGroup": event.get("actionGroup") or "",
        "apiPath": event.get("apiPath") or "",
        "httpMethod": event.get("httpMethod") or "",
        "httpStatusCode": int(http_status),
        "responseBody": response_body,
    }
    return {
        "messageVersion": "1.0",
        "response": action_response,
        "sessionAttributes": _coerce_session_for_bedrock(session_attributes),
        "promptSessionAttributes": _coerce_session_for_bedrock(prompt_session_attributes),
    }


def _bedrock_openapi_error(
    event: dict[str, Any],
    *,
    http_status: int,
    message: str,
    session_attributes: dict[str, Any],
    prompt_session_attributes: dict[str, Any],
) -> dict[str, Any]:
    err_obj = {"ok": False, "error": message}
    return _bedrock_openapi_success(
        event,
        http_status=http_status,
        payload_obj=err_obj,
        session_attributes=session_attributes,
        prompt_session_attributes=prompt_session_attributes,
    )


def _bedrock_function_success(
    event: dict[str, Any],
    *,
    payload_obj: dict[str, Any],
    session_attributes: dict[str, Any],
    prompt_session_attributes: dict[str, Any],
) -> dict[str, Any]:
    """
    Function-details action group success: TEXT body with JSON string (docs allow TEXT content type).
    Omit responseState on success (only FAILURE | REPROMPT are documented for errors).
    """
    # ASCII-safe JSON string: some Bedrock paths are strict about response body encoding.
    body_str = json.dumps(payload_obj, default=str, ensure_ascii=True)
    function_response = {
        "actionGroup": event.get("actionGroup") or "",
        "function": event.get("function") or "",
        "functionResponse": {
            "responseBody": {
                "TEXT": {
                    "body": body_str,
                }
            }
        },
    }
    return {
        "messageVersion": "1.0",
        "response": function_response,
        "sessionAttributes": _coerce_session_for_bedrock(session_attributes),
        "promptSessionAttributes": _coerce_session_for_bedrock(prompt_session_attributes),
    }


def _route_openapi(event: dict[str, Any], context: Any) -> dict[str, Any]:
    session_attrs = dict(event.get("sessionAttributes") or {})
    prompt_attrs = dict(event.get("promptSessionAttributes") or {})

    raw_path = event.get("apiPath")
    api_path = _normalize_bedrock_api_path(str(raw_path) if raw_path is not None else "")
    method = (event.get("httpMethod") or "POST").upper()
    body_fields = _extract_openapi_body_dict(event)

    # --- fetch_market_news ---
    if api_path == "/fetch_market_news" and method == "POST":
        event_payload: dict[str, Any] = {}
        raw_event = body_fields.get("event")
        parsed_event = _parse_json_property(raw_event)
        if isinstance(parsed_event, dict):
            event_payload = parsed_event

        inv = _invoke_lambda_sync(_lambda_name_news(), event_payload)
        if not inv.get("ok"):
            return _bedrock_openapi_error(
                event,
                http_status=502,
                message=inv.get("error", "Lambda error"),
                session_attributes=session_attrs,
                prompt_session_attributes=prompt_attrs,
            )

        lr = inv.get("response") or {}
        run_id = lr.get("run_id")
        symbols = lr.get("symbols") or []

        message_body = None
        if run_id and isinstance(symbols, list):
            message_body = _build_message_body_after_fetch(run_id=str(run_id), symbols=symbols)
            session_attrs["pending_analyze_sentiment_message_body"] = json.dumps(message_body, default=str)
            session_attrs["last_trading_run_id"] = str(run_id)
            session_attrs["last_symbols_json"] = json.dumps(symbols, default=str)

        out = _openapi_action_body(
            inv=inv,
            step="fetch_market_news",
            next_obj={
                "description": (
                    "Call POST /analyze_sentiment with message_body "
                    "(or rely on session pending_analyze_sentiment_message_body)."
                ),
                "message_body": message_body,
            },
        )
        return _bedrock_openapi_success(
            event,
            http_status=200,
            payload_obj=out,
            session_attributes=session_attrs,
            prompt_session_attributes=prompt_attrs,
        )

    # --- analyze_sentiment ---
    if api_path == "/analyze_sentiment" and method == "POST":
        mb_raw = body_fields.get("message_body")
        message_body = _parse_json_property(mb_raw)

        if not isinstance(message_body, dict) or not message_body.get("run_id"):
            pending = session_attrs.get("pending_analyze_sentiment_message_body")
            if pending:
                try:
                    message_body = json.loads(pending)
                except json.JSONDecodeError:
                    message_body = None

        if not isinstance(message_body, dict) or not message_body.get("run_id"):
            return _bedrock_openapi_error(
                event,
                http_status=400,
                message="Missing message_body.run_id; run fetch_market_news first or pass message_body.",
                session_attributes=session_attrs,
                prompt_session_attributes=prompt_attrs,
            )

        sqs_event = {
            "Records": [
                {
                    "messageId": "bedrock-router-synthetic",
                    "receiptHandle": "bedrock-placeholder",
                    "body": json.dumps(message_body, default=str),
                    "attributes": {"ApproximateReceiveCount": "1"},
                    "messageAttributes": {},
                    "md5OfBody": "placeholder",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:*:*:*",
                    "awsRegion": _region(),
                }
            ]
        }

        inv = _invoke_lambda_sync(_lambda_name_sentiment(), sqs_event)
        if not inv.get("ok"):
            return _bedrock_openapi_error(
                event,
                http_status=502,
                message=inv.get("error", "Lambda error"),
                session_attributes=session_attrs,
                prompt_session_attributes=prompt_attrs,
            )

        run_id = message_body.get("run_id")
        if run_id:
            session_attrs["last_trading_run_id"] = str(run_id)

        out = _openapi_action_body(
            inv=inv,
            step="analyze_sentiment",
            next_obj={
                "description": "Call POST /execute_trade with run_id when ready.",
                "run_id": str(run_id) if run_id else None,
            },
        )
        return _bedrock_openapi_success(
            event,
            http_status=200,
            payload_obj=out,
            session_attributes=session_attrs,
            prompt_session_attributes=prompt_attrs,
        )

    # --- execute_trade ---
    if api_path == "/execute_trade" and method == "POST":
        rid_raw = body_fields.get("run_id")
        run_id: Optional[str] = None
        if rid_raw is not None:
            run_id = str(rid_raw).strip() or None
        if not run_id:
            run_id = (session_attrs.get("last_trading_run_id") or "").strip() or None

        if not run_id:
            return _bedrock_openapi_error(
                event,
                http_status=400,
                message="Missing run_id; pass run_id or set session from prior steps.",
                session_attributes=session_attrs,
                prompt_session_attributes=prompt_attrs,
            )

        inv = _invoke_lambda_sync(_lambda_name_trade(), {"run_id": run_id})
        if not inv.get("ok"):
            return _bedrock_openapi_error(
                event,
                http_status=502,
                message=inv.get("error", "Lambda error"),
                session_attributes=session_attrs,
                prompt_session_attributes=prompt_attrs,
            )

        out = _openapi_action_body(inv=inv, step="execute_trade", next_obj=None)
        return _bedrock_openapi_success(
            event,
            http_status=200,
            payload_obj=out,
            session_attributes=session_attrs,
            prompt_session_attributes=prompt_attrs,
        )

    logger.warning(
        "Unknown Bedrock OpenAPI route",
        extra={"httpMethod": method, "apiPath_raw": raw_path, "apiPath_normalized": api_path},
    )
    return _bedrock_openapi_error(
        event,
        http_status=404,
        message=f"Unknown route {method} {api_path}",
        session_attributes=session_attrs,
        prompt_session_attributes=prompt_attrs,
    )


def _route_function(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Function-details schema: route by operationId-style function name."""
    session_attrs = dict(event.get("sessionAttributes") or {})
    prompt_attrs = dict(event.get("promptSessionAttributes") or {})
    fn = (event.get("function") or "").strip()
    params = _props_list_to_dict(event.get("parameters"))

    if fn == "fetch_market_news":
        ev = _parse_json_property(params.get("event"))
        payload = ev if isinstance(ev, dict) else {}
        inv = _invoke_lambda_sync(_lambda_name_news(), payload)
        if not inv.get("ok"):
            return _bedrock_function_error(event, inv.get("error", "error"), session_attrs, prompt_attrs)
        lr = inv.get("response") or {}
        run_id = lr.get("run_id")
        symbols = lr.get("symbols") or []
        message_body = None
        if run_id and isinstance(symbols, list):
            message_body = _build_message_body_after_fetch(run_id=str(run_id), symbols=symbols)
            session_attrs["pending_analyze_sentiment_message_body"] = json.dumps(message_body, default=str)
            session_attrs["last_trading_run_id"] = str(run_id)
        out = _openapi_action_body(
            inv=inv,
            step="fetch_market_news",
            next_obj={"message_body": message_body},
        )
        return _bedrock_function_success(event, payload_obj=out, session_attributes=session_attrs, prompt_session_attributes=prompt_attrs)

    if fn == "analyze_sentiment":
        mb = _parse_json_property(params.get("message_body"))
        if not isinstance(mb, dict) or not mb.get("run_id"):
            pending = session_attrs.get("pending_analyze_sentiment_message_body")
            if pending:
                try:
                    mb = json.loads(pending)
                except json.JSONDecodeError:
                    mb = None
        if not isinstance(mb, dict) or not mb.get("run_id"):
            return _bedrock_function_error(
                event, "Missing message_body / run_id", session_attrs, prompt_attrs
            )
        sqs_event = {
            "Records": [
                {
                    "messageId": "bedrock-router-synthetic",
                    "receiptHandle": "bedrock-placeholder",
                    "body": json.dumps(mb, default=str),
                    "attributes": {"ApproximateReceiveCount": "1"},
                    "messageAttributes": {},
                    "md5OfBody": "placeholder",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:*:*:*",
                    "awsRegion": _region(),
                }
            ]
        }
        inv = _invoke_lambda_sync(_lambda_name_sentiment(), sqs_event)
        if not inv.get("ok"):
            return _bedrock_function_error(event, inv.get("error", "error"), session_attrs, prompt_attrs)
        run_id = mb.get("run_id")
        if run_id:
            session_attrs["last_trading_run_id"] = str(run_id)
        out = _openapi_action_body(
            inv=inv,
            step="analyze_sentiment",
            next_obj={
                "description": "Call execute_trade with run_id when ready.",
                "run_id": str(run_id) if run_id else None,
            },
        )
        return _bedrock_function_success(event, payload_obj=out, session_attributes=session_attrs, prompt_session_attributes=prompt_attrs)

    if fn == "execute_trade":
        run_id = (params.get("run_id") or session_attrs.get("last_trading_run_id") or "").strip()
        if not run_id:
            return _bedrock_function_error(event, "Missing run_id", session_attrs, prompt_attrs)
        inv = _invoke_lambda_sync(_lambda_name_trade(), {"run_id": run_id})
        if not inv.get("ok"):
            return _bedrock_function_error(event, inv.get("error", "error"), session_attrs, prompt_attrs)
        out = _openapi_action_body(inv=inv, step="execute_trade", next_obj=None)
        return _bedrock_function_success(event, payload_obj=out, session_attributes=session_attrs, prompt_session_attributes=prompt_attrs)

    return _bedrock_function_error(event, f"Unknown function {fn}", session_attrs, prompt_attrs)


def _bedrock_function_error(
    event: dict[str, Any],
    message: str,
    session_attributes: dict[str, Any],
    prompt_session_attributes: dict[str, Any],
) -> dict[str, Any]:
    """Use documented FAILURE / REPROMPT for function-style errors."""
    body_str = json.dumps({"ok": False, "error": message}, default=str)
    function_response = {
        "actionGroup": event.get("actionGroup") or "",
        "function": event.get("function") or "",
        "functionResponse": {
            "responseState": "FAILURE",
            "responseBody": {
                "TEXT": {
                    "body": body_str,
                }
            },
        },
    }
    return {
        "messageVersion": "1.0",
        "response": function_response,
        "sessionAttributes": _coerce_session_for_bedrock(session_attributes),
        "promptSessionAttributes": _coerce_session_for_bedrock(prompt_session_attributes),
    }


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Entry point for Bedrock action group Lambda.
    Prefer OpenAPI (apiPath + httpMethod); fall back to function name.
    """
    try:
        if "apiPath" in event:
            return _route_openapi(event, context)
        if event.get("function"):
            return _route_function(event, context)
        session_attrs = dict(event.get("sessionAttributes") or {})
        prompt_attrs = dict(event.get("promptSessionAttributes") or {})
        return _bedrock_openapi_error(
            event,
            http_status=400,
            message="Expected apiPath (OpenAPI) or function (function-details) in Bedrock event.",
            session_attributes=session_attrs,
            prompt_session_attributes=prompt_attrs,
        )
    except Exception as e:  # noqa: BLE001
        logger.exception("Router error")
        session_attrs = dict(event.get("sessionAttributes") or {})
        prompt_attrs = dict(event.get("promptSessionAttributes") or {})
        return _bedrock_openapi_error(
            event,
            http_status=500,
            message=str(e),
            session_attributes=session_attrs,
            prompt_session_attributes=prompt_attrs,
        )


# Lambda console / zip uses "handler" by default — alias for SAM.
lambda_handler = handler
