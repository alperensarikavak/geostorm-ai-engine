FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MCP_COMMAND=node \
    MCP_SERVER_PATH=../geostorm-mcp-server/dist/index.js \
    MCP_WORKING_DIR=/app/geostorm-ai-engine

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends nodejs npm ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY geostorm-ai-engine/requirements.txt ./geostorm-ai-engine/requirements.txt
RUN pip install --upgrade pip \
    && pip install -r ./geostorm-ai-engine/requirements.txt

COPY geostorm-mcp-server/package*.json ./geostorm-mcp-server/
WORKDIR /app/geostorm-mcp-server
RUN npm ci

COPY geostorm-mcp-server/tsconfig.json ./tsconfig.json
COPY geostorm-mcp-server/src ./src
RUN npm run build \
    && npm prune --omit=dev

WORKDIR /app
COPY geostorm-ai-engine ./geostorm-ai-engine

WORKDIR /app/geostorm-ai-engine
EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
