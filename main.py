import json
import logging
import uuid
import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from google import genai
import mcp_pb2
import mcp_pb2_grpc
import grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    gemini_api_key: str = "DUMMY_KEY"
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    grpc_target: str = "localhost:50051"

    class Config:
        env_file = ".env"

settings = Settings()

app = FastAPI(title="GeoStorm AI Insight Engine")

class InsightRequest(BaseModel):
    prompt: str

class InsightResponse(BaseModel):
    summary: str
    queue_status: str

def fetch_mcp_data() -> str:
    """Mock the gRPC call to the MCP Node.js server to get space data."""
    try:
        # We simulate the gRPC connection, but use a dummy return if no real server
        channel = grpc.insecure_channel(settings.grpc_target)
        stub = mcp_pb2_grpc.SpaceDataServiceStub(channel)
        
        # Here we would normally make the call:
        # req = mcp_pb2.AlertsRequest()
        # res = stub.GetNoaaSwpcAlerts(req, timeout=5)
        # return res.data_json
        
        # Since this is a skeleton without the real Node server running yet, we return dummy
        logger.info("Faking gRPC call to MCP server...")
        return json.dumps({
            "alerts": [
                {"id": 1, "type": "Geomagnetic Storm", "severity": "G3"},
                {"id": 2, "type": "Solar Flare", "severity": "X1.2"}
            ]
        })
    except Exception as e:
        logger.error(f"gRPC failed: {e}")
        return json.dumps({"error": "No data from MCP"})

def publish_to_rabbitmq(message: dict):
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=settings.rabbitmq_host, 
                port=settings.rabbitmq_port
            )
        )
        channel = connection.channel()
        channel.queue_declare(queue="geostorm_alerts", durable=True)
        
        channel.basic_publish(
            exchange='',
            routing_key='geostorm_alerts',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            )
        )
        connection.close()
        logger.info("Successfully published to RabbitMQ")
        return True
    except Exception as e:
        logger.error(f"RabbitMQ publish failed: {e}")
        return False

@app.post("/api/v1/insight", response_model=InsightResponse)
async def get_insight(request: InsightRequest):
    try:
        # 1. Fetch data from MCP via gRPC
        mcp_data = fetch_mcp_data()
        
        # 2. Ask Gemini for analysis
        # Using dummy response if key is dummy
        if settings.gemini_api_key == "DUMMY_KEY":
            llm_summary = (
                f"Bu analiz Gemini AI (DUMMY) tarafindan uretilmistir.\n"
                f"Isteminiz: '{request.prompt}'\n"
                f"Sistem tarafindan su an G3 seviyesinde bir Jeomanyetik Firtina tespit edildi."
            )
        else:
            client = genai.Client(api_key=settings.gemini_api_key)
            system_prompt = "Sen bir uzay hava durumu analiz uzmanısın. Aşağıdaki JSON formatındaki telemetri verilerine bakarak kullanıcının sorusunu yanıtla."
            full_prompt = f"{system_prompt}\nVeri: {mcp_data}\nSoru: {request.prompt}"
            
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=full_prompt,
            )
            llm_summary = response.text
            
        # 3. Publish to RabbitMQ
        report_event = {
            "id": str(uuid.uuid4()),
            "prompt": request.prompt,
            "analysis": llm_summary,
            "raw_data": mcp_data,
            "status": "ANALYZED"
        }
        
        published = publish_to_rabbitmq(report_event)
        
        # 4. Return to Gateway
        return InsightResponse(
            summary=llm_summary,
            queue_status="RabbitMQ'ya başarılı bir şekilde iletildi." if published else "RabbitMQ'ya bağlanılamadı (Lokalde kapalı olabilir), ancak analiz tamam."
        )
        
    except Exception as e:
        logger.error(f"Error processing insight: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
