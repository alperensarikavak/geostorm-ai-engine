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
    cors_origins: str = "http://localhost:3000,http://127.0.0.1:3000"
    prompt_max_length: int = 2000

    class Config:
        env_file = ".env"


settings = Settings()

app = FastAPI(title="GeoStorm AI Insight Engine", redirect_slashes=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        origin.strip() for origin in settings.cors_origins.split(",") if origin.strip()
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
    risk_signals, risk_error = parse_json_field(
        response.risk_signals_json,
        {
            "has_noaa_alerts": False,
            "cme_count": 0,
            "highest_detected_level": "UNKNOWN",
        },
    )

    for error in (noaa_error, nasa_error, risk_error):
        if error:
            errors.append(error)

    return {
        "source": response.source or "geostorm-mcp-server",
        "fetched_at": response.fetched_at or utc_now_rfc3339(),
        "date_window": {
            "startDate": response.date_window.start_date,
            "endDate": response.date_window.end_date,
        },
        "noaa_swpc_alerts": noaa_alerts,
        "nasa_donki_cmes": nasa_cmes,
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


def generate_fallback_summary(prompt: str, context: dict[str, Any], reason: str) -> str:
    risk_level = extract_risk_level_from_context(context)
    return (
        "OpenRouter inference could not be completed, so GeoStorm-AI returned "
        "a structured fallback analysis.\n"
        f"Requested analysis: {prompt}\n"
        f"Fallback reason: {reason}\n"
        f"Risk tier: {risk_level}\n"
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
        "CME records and NOAA SWPC alerts from the provided JSON telemetry. "
        "Answer clearly, summarize operational implications, and explicitly state "
        "one risk tier using G0, G1, G2, G3, G4, G5, CRITICAL, or UNKNOWN. "
        "Do not include raw JSON in the final answer."
    )
    payload = {
        "model": settings.openrouter_model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": (
                    f"User question:\n{prompt}\n\n"
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

        return summary
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
    risk_level: str,
    timestamp: str,
) -> dict[str, str]:
    return {
        "schema_version": "1.0",
        "event_id": analysis_id,
        "activity_id": extract_activity_id(context),
        "alert_level": risk_level,
        "details": summary,
        "timestamp": timestamp,
    }


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

    return {
        "status": "ready" if mcp_ready else "degraded",
        "service": "geostorm-ai-engine",
        "mcp_reachable": mcp_ready,
        "mcp_transport": settings.mcp_transport.lower(),
        "openrouter_configured": bool(settings.openrouter_api_key.strip()),
    }


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
        risk_level = extract_alert_level(summary, context)
        alert_event = build_alert_event(analysis_id, context, summary, risk_level, timestamp)
        published = await publish_to_rabbitmq(alert_event)

        return InsightResponse(
            analysis_id=analysis_id,
            summary=summary,
            risk_level=risk_level,
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
