# Viper Scaling POC

Queue-driven worker architecture for AI agent runs. Standalone workers behind RabbitMQ, per-thread ordering, live SSE, crash recovery, auto-scaling.

## Quick Start

```bash
# Infra + workers
docker compose up -d rabbitmq mongo redis
docker compose up -d --build --scale worker=3 worker

# API (from backend/)
cd backend && npm install && CHAOS_ENABLED=1 REDIS_ENABLED=1 npm run api

# Frontend (from frontend/)
cd frontend && npm install && npm run dev
```

Open **http://localhost:3001**.

## What's Here

```
frontend/            Next.js chat UI                          :3001
backend/
  src/api/server.ts  API tier (HTTP + SSE, no agent work)    :3000
  src/worker.ts      Worker (runs agent loop, scaled)
  src/lib/           Shared: queue, mongo, guards, reaper, rate limiter, etc.
  src/mock/          Fake LLM + fake tool (only mocked things)
docker-compose.yml   Local orchestration
docs/                ARCHITECTURE.md · OPERATIONS.md
```

## Docs

- **[Architecture](docs/ARCHITECTURE.md)** — system diagram, mechanisms, safety matrix, scaling
- **[Operations](docs/OPERATIONS.md)** — run, test, deploy, config, rebuild, troubleshoot
- **[Scaling Internals](docs/SCALING-INTERNALS.md)** — auto-scaling, RabbitMQ dispatch algorithms, Node.js worker concurrency model
