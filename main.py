import asyncio
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
import pika
from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ALERT_EXCHANGE = "space_weather"
ALERT_QUEUE = "space_weather_alerts"
ALERT_ROUTING_KEY = "space_weather_alerts"
OPENROUTER_CHAT_COMPLETIONS_URL = "https://openrouter.ai/api/v1/chat/completions"
RISK_LEVEL_PATTERN = re.compile(r"\b(CRITICAL|G[0-5]|UNKNOWN)\b", re.IGNORECASE)
RISK_ORDER = {
    "UNKNOWN": -1,
    "G0": 0,
    "G1": 1,
    "G2": 2,
    "G3": 3,
    "G4": 4,
    "G5": 5,
    "CRITICAL": 6,
}
RISK_LABELS = {
    "G0": "Below storm levels",
    "G1": "Minor",
    "G2": "Moderate",
    "G3": "Strong",
    "G4": "Severe",
    "G5": "Extreme",
    "CRITICAL": "Critical",
    "UNKNOWN": "Unknown",
}


class Settings(BaseSettings):
    openrouter_api_key: str = ""
    openrouter_model: str = "meta-llama/llama-3-8b-instruct:free"
    mcp_transport: str = "grpc"
    mcp_grpc_host: str = "localhost"
    mcp_grpc_port: int = 50051
    mcp_grpc_timeout_seconds: float = 30.0
    mcp_base_url: str = "http://localhost:6274"
    mcp_context_path: str = "/context"
    mcp_timeout_seconds: float = 30.0
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "guest"
    rabbitmq_password: str = "guest"
    rabbitmq_timeout_seconds: float = 5.0
    cors_allowed_origins: str = ""
    cors_origins: str = "http://localhost:3000,http://127.0.0.1:3000"
    prompt_max_length: int = 2000

    class Config:
        env_file = ".env"


settings = Settings()

app = FastAPI(title="GeoStorm AI Insight Engine", redirect_slashes=True)

configured_cors_origins = settings.cors_allowed_origins or settings.cors_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        origin.strip() for origin in configured_cors_origins.split(",") if origin.strip()
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


class InsightRequest(BaseModel):
    prompt: str = Field(min_length=1, max_length=settings.prompt_max_length)


class QueueStatus(BaseModel):
    published: bool
    message: str


class InsightResponse(BaseModel):
    analysis_id: str
    summary: str
    risk_level: str
    current_risk_level: str
    forecast_risk_level: str
    risk_basis: str
    context: dict[str, Any]
    mcp_transport: str
    alert_published: bool
    queue_status: QueueStatus
    timestamp: str


def utc_now_rfc3339() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def mcp_context_url() -> str:
    base_url = settings.mcp_base_url.rstrip("/")
    path = settings.mcp_context_path
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base_url}{path}"


def empty_context(error_message: str) -> dict[str, Any]:
    now = utc_now_rfc3339()
    return {
        "source": "geostorm-mcp-server",
        "fetched_at": now,
        "date_window": {
            "startDate": now[:10],
            "endDate": now[:10],
        },
        "noaa_swpc_alerts": [],
        "nasa_donki_cmes": [],
        "esa_source_status": "unavailable",
        "esa_data_json": "[]",
        "esa_dataset_id": "",
        "esa_error": error_message,
        "esa_summary": {
            "source": "ESA SWE HAPI",
            "status": "unavailable",
            "dataset_id": "",
            "record_count": 0,
            "error": error_message,
        },
        "risk_signals": {
            "has_noaa_alerts": False,
            "cme_count": 0,
            "highest_detected_level": "UNKNOWN",
        },
        "errors": [error_message],
    }


async def fetch_mcp_context() -> dict[str, Any]:
    if settings.mcp_transport.lower() == "http":
        return await fetch_mcp_context_http()

    return await fetch_mcp_context_grpc()


async def fetch_mcp_context_http() -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=settings.mcp_timeout_seconds) as client:
            response = await client.get(mcp_context_url())
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                return payload
            return empty_context("MCP service returned a non-object context payload.")
    except Exception as error:
        logger.warning("MCP context fetch failed: %s", error)
        return empty_context(f"MCP context unavailable: {error}")


async def fetch_mcp_context_grpc() -> dict[str, Any]:
    return await asyncio.to_thread(fetch_mcp_context_grpc_sync)


def fetch_mcp_context_grpc_sync() -> dict[str, Any]:
    try:
        import grpc

        from app.grpc import space_weather_pb2, space_weather_pb2_grpc

        target = f"{settings.mcp_grpc_host}:{settings.mcp_grpc_port}"
        with grpc.insecure_channel(target) as channel:
            stub = space_weather_pb2_grpc.SpaceWeatherServiceStub(channel)
            response = stub.GetContext(
                space_weather_pb2.GetContextRequest(),
                timeout=settings.mcp_grpc_timeout_seconds,
            )

        return context_from_grpc_response(response)
    except Exception as error:
        logger.warning("MCP gRPC context fetch failed: %s", error)
        return empty_context(f"MCP gRPC context unavailable: {error}")


def context_from_grpc_response(response: Any) -> dict[str, Any]:
    errors = list(getattr(response, "errors", []))
    noaa_alerts, noaa_error = parse_json_field(response.noaa_swpc_alerts_json, [])
    nasa_cmes, nasa_error = parse_json_field(response.nasa_donki_cmes_json, [])
    esa_data, esa_json_error = parse_json_field(getattr(response, "esa_data_json", "[]"), [])
    risk_signals, risk_error = parse_json_field(
        response.risk_signals_json,
        {
            "has_noaa_alerts": False,
            "cme_count": 0,
            "highest_detected_level": "UNKNOWN",
        },
    )

    esa_source_status = getattr(response, "esa_source_status", "") or "disabled"
    esa_dataset_id = getattr(response, "esa_dataset_id", "") or ""
    esa_error = getattr(response, "esa_error", "") or ""

    for error in (noaa_error, nasa_error, esa_json_error, risk_error):
        if error:
            errors.append(error)
    if esa_error and esa_source_status not in {"disabled", "ok"}:
        errors.append(esa_error)

    return {
        "source": response.source or "geostorm-mcp-server",
        "fetched_at": response.fetched_at or utc_now_rfc3339(),
        "date_window": {
            "startDate": response.date_window.start_date,
            "endDate": response.date_window.end_date,
        },
        "noaa_swpc_alerts": noaa_alerts,
        "nasa_donki_cmes": nasa_cmes,
        "esa_source_status": esa_source_status,
        "esa_data_json": json.dumps(esa_data, ensure_ascii=False),
        "esa_dataset_id": esa_dataset_id,
        "esa_error": esa_error,
        "esa_summary": summarize_esa_context(esa_source_status, esa_dataset_id, esa_data, esa_error),
        "risk_signals": risk_signals,
        "errors": errors,
    }


def parse_json_field(value: str, fallback: Any) -> tuple[Any, str | None]:
    try:
        return json.loads(value or json.dumps(fallback)), None
    except json.JSONDecodeError as error:
        return fallback, f"Failed to parse gRPC JSON field: {error}"


def context_as_prompt_json(context: dict[str, Any]) -> str:
    return json.dumps(context, ensure_ascii=False, indent=2)


def summarize_esa_context(
    status: str,
    dataset_id: str,
    esa_data: Any,
    error: str,
) -> dict[str, Any]:
    records = esa_data if isinstance(esa_data, list) else []
    return {
        "source": "ESA SWE HAPI",
        "status": status or "disabled",
        "dataset_id": dataset_id,
        "record_count": len(records),
        "error": error,
    }


def generate_fallback_summary(prompt: str, context: dict[str, Any], reason: str) -> str:
    risk_profile = build_risk_profile(context)
    return (
        "OpenRouter inference could not be completed, so GeoStorm-AI returned "
        "a structured fallback analysis.\n"
        f"Requested analysis: {prompt}\n"
        f"Fallback reason: {reason}\n"
        f"{format_risk_briefing(risk_profile)}\n"
        "Operational note: Space-weather context was collected when available, "
        "and the downstream RabbitMQ alert pipeline was still attempted."
    )


async def generate_llm_summary(prompt: str, context: dict[str, Any]) -> str:
    if not settings.openrouter_api_key.strip():
        logger.warning("OPENROUTER_API_KEY is missing; using fallback analysis")
        return generate_fallback_summary(
            prompt,
            context,
            "OPENROUTER_API_KEY is not configured.",
        )

    system_prompt = (
        "You are a senior space-weather operations analyst. Interpret NASA DONKI "
        "CME records, NOAA SWPC alerts, and optional ESA SWE/HAPI supplementary "
        "context from the provided JSON telemetry. "
        "Answer clearly, summarize operational implications, and do not invent "
        "risk tiers that conflict with the provided canonical risk classification. "
        "Distinguish current observed risk from forecast/watch risk. "
        "ESA data is supplementary and must not override the canonical risk basis. "
        "Do not include raw JSON in the final answer."
    )
    risk_profile = build_risk_profile(context)
    payload = {
        "model": settings.openrouter_model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": (
                    f"User question:\n{prompt}\n\n"
                    f"Canonical risk classification:\n"
                    f"{format_risk_briefing(risk_profile)}\n\n"
                    f"Normalized space-weather context:\n{context_as_prompt_json(context)}"
                ),
            },
        ],
    }
    headers = {
        "Authorization": f"Bearer {settings.openrouter_api_key}",
        "HTTP-Referer": "http://localhost:3000",
        "X-Title": "GeoStorm AI Console",
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                OPENROUTER_CHAT_COMPLETIONS_URL,
                headers=headers,
                json=payload,
            )
            response.raise_for_status()

        response_payload = response.json()
        summary = (
            response_payload.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
        )
        if not summary:
            raise ValueError("OpenRouter returned an empty completion")

        return ensure_risk_briefing(summary, risk_profile)
    except Exception as error:
        logger.exception("OpenRouter inference failed: %s", error)
        return generate_fallback_summary(prompt, context, str(error))


def find_first_key_value(data: Any, keys: tuple[str, ...]) -> str | None:
    if isinstance(data, dict):
        for key in keys:
            value = data.get(key)
            if value is not None and str(value).strip():
                return str(value)

        for value in data.values():
            found = find_first_key_value(value, keys)
            if found:
                return found

    if isinstance(data, list):
        for item in data:
            found = find_first_key_value(item, keys)
            if found:
                return found

    return None


def extract_activity_id(context: dict[str, Any]) -> str:
    nasa_activity_id = find_first_key_value(
        context.get("nasa_donki_cmes"),
        ("activityID", "activity_id", "id"),
    )
    if nasa_activity_id:
        return nasa_activity_id

    return f"GST-{uuid.uuid4()}"


def extract_risk_level_from_context(context: dict[str, Any]) -> str:
    risk_signals = context.get("risk_signals")
    if isinstance(risk_signals, dict):
        level = risk_signals.get("highest_detected_level")
        if isinstance(level, str) and RISK_LEVEL_PATTERN.fullmatch(level.upper()):
            return level.upper()
    return "UNKNOWN"


def build_risk_profile(context: dict[str, Any]) -> dict[str, str]:
    current_level = extract_current_observed_risk_level(context)
    forecast_level = extract_forecast_risk_level(context)
    fallback_level = extract_risk_level_from_context(context)
    canonical_level = highest_risk_level(current_level, forecast_level)
    if canonical_level != "UNKNOWN":
        basis = "highest_current_or_forecast"
    elif fallback_level != "UNKNOWN":
        canonical_level = fallback_level
        basis = "highest_detected"
    else:
        basis = "unknown"

    return {
        "risk_level": canonical_level,
        "current_risk_level": current_level,
        "forecast_risk_level": forecast_level,
        "risk_basis": basis,
    }


def extract_current_observed_risk_level(context: dict[str, Any]) -> str:
    for alert in normalized_noaa_alerts(context):
        text = alert_text(alert)
        if not text or is_cancelled_product(text):
            continue
        if is_observed_alert(text):
            level = extract_highest_risk_from_text(text)
            if level != "UNKNOWN":
                return level
    return "UNKNOWN"


def extract_forecast_risk_level(context: dict[str, Any]) -> str:
    levels: list[str] = []
    for alert in normalized_noaa_alerts(context):
        text = alert_text(alert)
        if not text or is_cancelled_product(text):
            continue
        if is_forecast_product(text):
            level = extract_highest_risk_from_text(text)
            if level != "UNKNOWN":
                levels.append(level)
    return highest_risk_level(*levels)


def normalized_noaa_alerts(context: dict[str, Any]) -> list[Any]:
    alerts = context.get("noaa_swpc_alerts")
    return alerts if isinstance(alerts, list) else []


def alert_text(alert: Any) -> str:
    return json.dumps(alert, ensure_ascii=False) if not isinstance(alert, str) else alert


def is_cancelled_product(text: str) -> bool:
    normalized = text.upper()
    return "CANCELLED WATCH" in normalized or "CANCELLED ALERT" in normalized


def is_observed_alert(text: str) -> bool:
    normalized = text.upper()
    return "ALERT:" in normalized and "CANCELLED ALERT" not in normalized


def is_forecast_product(text: str) -> bool:
    normalized = text.upper()
    return any(keyword in normalized for keyword in ("WATCH:", "WARNING:", "PREDICTED", "EXPECTED"))


def extract_highest_risk_from_text(text: str) -> str:
    normalized = text.upper()
    if "CRITICAL" in normalized or "EXTREME" in normalized:
        return "CRITICAL"
    matches = [match.group(0).upper() for match in RISK_LEVEL_PATTERN.finditer(normalized)]
    return highest_risk_level(*matches)


def highest_risk_level(*levels: str) -> str:
    valid_levels = [
        level.upper()
        for level in levels
        if isinstance(level, str) and level.upper() in RISK_ORDER
    ]
    if not valid_levels:
        return "UNKNOWN"
    return max(valid_levels, key=lambda level: RISK_ORDER[level])


def format_risk_level(level: str) -> str:
    normalized = level.upper() if level else "UNKNOWN"
    label = RISK_LABELS.get(normalized, "Unknown")
    return f"{normalized} - {label}"


def format_risk_briefing(risk_profile: dict[str, str]) -> str:
    return "\n".join(
        [
            f"Canonical Risk Level: {format_risk_level(risk_profile['risk_level'])}",
            f"Current Observed Level: {format_risk_level(risk_profile['current_risk_level'])}",
            f"Forecast Watch Level: {format_risk_level(risk_profile['forecast_risk_level'])}",
            f"Risk Basis: {risk_profile['risk_basis']}",
        ]
    )


def ensure_risk_briefing(summary: str, risk_profile: dict[str, str]) -> str:
    briefing = format_risk_briefing(risk_profile)
    if "Canonical Risk Level:" in summary:
        return summary
    return f"{briefing}\n\n{summary}"


def extract_alert_level(summary: str, context: dict[str, Any]) -> str:
    normalized_summary = summary.upper()
    match = RISK_LEVEL_PATTERN.search(normalized_summary)
    if match:
        return match.group(1).upper()

    context_level = extract_risk_level_from_context(context)
    if context_level != "UNKNOWN":
        return context_level

    if any(keyword in normalized_summary for keyword in ("SEVERE", "EXTREME", "CRITICAL")):
        return "CRITICAL"

    if "MODERATE" in normalized_summary:
        return "G2"

    if "LOW" in normalized_summary or "NOMINAL" in normalized_summary:
        return "G0"

    return "UNKNOWN"


def build_alert_event(
    analysis_id: str,
    context: dict[str, Any],
    summary: str,
    risk_profile: dict[str, str],
    timestamp: str,
) -> dict[str, str]:
    event = {
        "schema_version": "1.0",
        "event_id": analysis_id,
        "activity_id": extract_activity_id(context),
        "alert_level": risk_profile["risk_level"],
        "current_risk_level": risk_profile["current_risk_level"],
        "forecast_risk_level": risk_profile["forecast_risk_level"],
        "risk_basis": risk_profile["risk_basis"],
        "details": summary,
        "timestamp": timestamp,
    }

    for key in ("esa_source_status", "esa_dataset_id", "esa_error"):
        value = context.get(key)
        if isinstance(value, str) and value.strip():
            event[key] = value.strip()

    return event


def publish_to_rabbitmq_sync(message: dict[str, str]) -> bool:
    connection = None

    try:
        credentials = pika.PlainCredentials(
            settings.rabbitmq_user,
            settings.rabbitmq_password,
        )
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=settings.rabbitmq_host,
                port=settings.rabbitmq_port,
                credentials=credentials,
                socket_timeout=settings.rabbitmq_timeout_seconds,
                blocked_connection_timeout=settings.rabbitmq_timeout_seconds,
                connection_attempts=1,
            )
        )
        channel = connection.channel()

        channel.exchange_declare(
            exchange=ALERT_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )
        channel.queue_declare(queue=ALERT_QUEUE, durable=True)
        channel.queue_bind(
            exchange=ALERT_EXCHANGE,
            queue=ALERT_QUEUE,
            routing_key=ALERT_ROUTING_KEY,
        )

        channel.basic_publish(
            exchange=ALERT_EXCHANGE,
            routing_key=ALERT_ROUTING_KEY,
            body=json.dumps(message, ensure_ascii=False),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
        )

        logger.info(
            "Published alert event_id=%s activity_id=%s level=%s",
            message["event_id"],
            message["activity_id"],
            message["alert_level"],
        )
        return True
    except Exception as error:
        logger.error("RabbitMQ publish failed: %s", error)
        return False
    finally:
        if connection and connection.is_open:
            connection.close()


async def publish_to_rabbitmq(message: dict[str, str]) -> bool:
    return await asyncio.to_thread(publish_to_rabbitmq_sync, message)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "geostorm-ai-engine"}


@app.get("/ready")
async def ready():
    mcp_ready = False
    if settings.mcp_transport.lower() == "http":
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                response = await client.get(f"{settings.mcp_base_url.rstrip('/')}/health")
                mcp_ready = response.status_code == 200
        except Exception:
            mcp_ready = False
    else:
        try:
            mcp_ready = await asyncio.to_thread(check_mcp_grpc_health)
        except Exception:
            mcp_ready = False

    openrouter_configured = is_configured_value(settings.openrouter_api_key)
    rabbitmq_configured = all(
        [
            settings.rabbitmq_host,
            settings.rabbitmq_port,
            settings.rabbitmq_user,
            is_configured_value(settings.rabbitmq_password),
        ]
    )
    checks = {
        "openrouter_api_key_configured": openrouter_configured,
        "mcp_transport": settings.mcp_transport.lower(),
        "mcp_grpc_target": f"{settings.mcp_grpc_host}:{settings.mcp_grpc_port}",
        "mcp_reachable": mcp_ready,
        "rabbitmq_configured": rabbitmq_configured,
    }

    return {
        "status": "ready" if mcp_ready and openrouter_configured and rabbitmq_configured else "degraded",
        "service": "geostorm-ai-engine",
        "checks": checks,
        "mcp_reachable": mcp_ready,
        "mcp_transport": settings.mcp_transport.lower(),
        "openrouter_configured": openrouter_configured,
    }


def is_configured_value(value: str) -> bool:
    normalized = value.strip()
    return bool(normalized and normalized not in {"replace_me", "REPLACE_ME"})


def check_mcp_grpc_health() -> bool:
    import grpc

    from app.grpc import space_weather_pb2, space_weather_pb2_grpc

    target = f"{settings.mcp_grpc_host}:{settings.mcp_grpc_port}"
    with grpc.insecure_channel(target) as channel:
        stub = space_weather_pb2_grpc.SpaceWeatherServiceStub(channel)
        response = stub.Health(
            space_weather_pb2.HealthRequest(),
            timeout=min(settings.mcp_grpc_timeout_seconds, 2.0),
        )
    return response.status == "ok"


@app.options("/api/v1/insight", include_in_schema=False)
@app.options("/api/v1/insight/", include_in_schema=False)
async def insight_options():
    return Response(status_code=204)


@app.post("/api/v1/insight", response_model=InsightResponse)
@app.post("/api/v1/insight/", response_model=InsightResponse, include_in_schema=False)
async def get_insight(request: InsightRequest):
    try:
        analysis_id = str(uuid.uuid4())
        timestamp = utc_now_rfc3339()
        context = await fetch_mcp_context()
        summary = await generate_llm_summary(request.prompt.strip(), context)
        risk_profile = build_risk_profile(context)
        summary = ensure_risk_briefing(summary, risk_profile)
        risk_level = risk_profile["risk_level"]
        alert_event = build_alert_event(analysis_id, context, summary, risk_profile, timestamp)
        published = await publish_to_rabbitmq(alert_event)

        return InsightResponse(
            analysis_id=analysis_id,
            summary=summary,
            risk_level=risk_level,
            current_risk_level=risk_profile["current_risk_level"],
            forecast_risk_level=risk_profile["forecast_risk_level"],
            risk_basis=risk_profile["risk_basis"],
            context=context,
            mcp_transport=settings.mcp_transport.lower(),
            alert_published=published,
            queue_status=QueueStatus(
                published=published,
                message=(
                    "Alert event published to RabbitMQ."
                    if published
                    else "Analysis completed, but RabbitMQ publishing failed."
                ),
            ),
            timestamp=timestamp,
        )
    except Exception as error:
        logger.error("Error processing insight: %s", error)
        raise HTTPException(status_code=500, detail=str(error)) from error


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
