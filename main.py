import json
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pika
from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from google import genai
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from pydantic import BaseModel
from pydantic_settings import BaseSettings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ALERT_EXCHANGE = "space_weather"
ALERT_QUEUE = "space_weather_alerts"
ALERT_ROUTING_KEY = "space_weather_alerts"
MCP_CME_LOOKBACK_DAYS = 7
AI_ENGINE_DIR = Path(__file__).resolve().parent


class Settings(BaseSettings):
    gemini_api_key: str = "DUMMY_KEY"
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "guest"
    rabbitmq_password: str = "guest"
    mcp_command: str = "node"
    mcp_server_path: str = "../geostorm-mcp-server/dist/index.js"
    mcp_working_dir: str = str(AI_ENGINE_DIR)

    class Config:
        env_file = ".env"


settings = Settings()

app = FastAPI(title="GeoStorm AI Insight Engine", redirect_slashes=True)

origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class InsightRequest(BaseModel):
    prompt: str


class InsightResponse(BaseModel):
    summary: str
    queue_status: str


def utc_now_rfc3339() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_tool_content(content: str) -> Any:
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return content


def extract_mcp_payload(result: Any) -> Any:
    if getattr(result, "isError", False):
        error_text = "\n".join(
            getattr(content, "text", str(content)) for content in result.content
        )
        raise RuntimeError(f"MCP tool call failed: {error_text}")

    structured_content = getattr(result, "structuredContent", None)
    if structured_content:
        return structured_content

    text_blocks = [
        content.text
        for content in result.content
        if getattr(content, "type", None) == "text" and hasattr(content, "text")
    ]

    if len(text_blocks) == 1:
        return parse_tool_content(text_blocks[0])

    return [parse_tool_content(text) for text in text_blocks]


async def fetch_mcp_data() -> str:
    """Fetch live space-weather telemetry from the Node MCP server over stdio."""
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=MCP_CME_LOOKBACK_DAYS)

    server_params = StdioServerParameters(
        command=settings.mcp_command,
        args=[settings.mcp_server_path],
        cwd=settings.mcp_working_dir,
    )

    logger.info(
        "Starting MCP stdio client command=%s args=%s",
        settings.mcp_command,
        [settings.mcp_server_path],
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            noaa_alerts = await session.call_tool(
                "get_noaa_swpc_alerts",
                arguments={},
            )
            cme_records = await session.call_tool(
                "get_coronal_mass_ejections",
                arguments={
                    "startDate": start_date.isoformat(),
                    "endDate": end_date.isoformat(),
                },
            )

    telemetry = {
        "source": "geostorm-mcp-server",
        "fetched_at": utc_now_rfc3339(),
        "date_window": {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        },
        "noaa_swpc_alerts": extract_mcp_payload(noaa_alerts),
        "nasa_donki_cmes": extract_mcp_payload(cme_records),
    }

    return json.dumps(telemetry, ensure_ascii=False)


def generate_dummy_summary(prompt: str, mcp_data: str) -> str:
    return (
        "Bu analiz Gemini AI (DUMMY) tarafindan uretilmistir.\n"
        f"Isteminiz: '{prompt}'\n"
        "MCP server uzerinden NASA DONKI CME ve NOAA SWPC alert verileri alindi.\n"
        "Ornek risk degeri: G3.\n"
        f"Telemetry context: {mcp_data[:2000]}"
    )


def generate_llm_summary(prompt: str, mcp_data: str) -> str:
    if settings.gemini_api_key == "DUMMY_KEY":
        return generate_dummy_summary(prompt, mcp_data)

    client = genai.Client(api_key=settings.gemini_api_key)
    system_prompt = (
        "Sen bir uzay hava durumu analiz uzmanisin. "
        "Asagidaki JSON formatindaki NASA DONKI CME ve NOAA SWPC alert "
        "telemetrisini incele. Kullanicinin sorusunu yanitla ve risk seviyesini "
        "metinde acikca G1, G2, G3 veya CRITICAL olarak belirt."
    )
    full_prompt = f"{system_prompt}\n\nTelemetry JSON:\n{mcp_data}\n\nSoru: {prompt}"

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=full_prompt,
    )

    return response.text or "Gemini bos bir analiz yaniti dondurdu."


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


def extract_activity_id(mcp_data: str) -> str:
    try:
        telemetry = json.loads(mcp_data)
    except json.JSONDecodeError:
        telemetry = {}

    nasa_activity_id = find_first_key_value(
        telemetry.get("nasa_donki_cmes"),
        ("activityID", "activity_id", "id"),
    )
    if nasa_activity_id:
        return nasa_activity_id

    return f"GST-{uuid.uuid4()}"


def extract_alert_level(summary: str) -> str:
    normalized_summary = summary.upper()
    match = re.search(r"\b(CRITICAL|G[1-5])\b", normalized_summary)
    if match:
        return match.group(1)

    if any(keyword in normalized_summary for keyword in ("SEVERE", "EXTREME", "CRITICAL")):
        return "CRITICAL"

    if "MODERATE" in normalized_summary:
        return "G2"

    return "G1"


def build_alert_event(mcp_data: str, summary: str) -> dict[str, str]:
    return {
        "activity_id": extract_activity_id(mcp_data),
        "alert_level": extract_alert_level(summary),
        "details": summary,
        "timestamp": utc_now_rfc3339(),
    }


def publish_to_rabbitmq(message: dict[str, str]) -> bool:
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
            "Published alert activity_id=%s level=%s exchange=%s routing_key=%s",
            message["activity_id"],
            message["alert_level"],
            ALERT_EXCHANGE,
            ALERT_ROUTING_KEY,
        )
        return True
    except Exception as error:
        logger.error("RabbitMQ publish failed: %s", error)
        return False
    finally:
        if connection and connection.is_open:
            connection.close()


@app.options("/api/v1/insight", include_in_schema=False)
@app.options("/api/v1/insight/", include_in_schema=False)
async def insight_options():
    return Response(status_code=204)


@app.post("/api/v1/insight", response_model=InsightResponse)
@app.post("/api/v1/insight/", response_model=InsightResponse, include_in_schema=False)
async def get_insight(request: InsightRequest):
    try:
        mcp_data = await fetch_mcp_data()
        llm_summary = generate_llm_summary(request.prompt, mcp_data)
        alert_event = build_alert_event(mcp_data, llm_summary)
        published = publish_to_rabbitmq(alert_event)

        return InsightResponse(
            summary=llm_summary,
            queue_status=(
                "RabbitMQ'ya basarili bir sekilde iletildi."
                if published
                else "RabbitMQ'ya baglanilamadi, ancak analiz tamam."
            ),
        )
    except Exception as error:
        logger.error("Error processing insight: %s", error)
        raise HTTPException(status_code=500, detail=str(error)) from error


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
