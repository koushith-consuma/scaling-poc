# Operations

## Run Locally

### Prerequisites
- Docker Desktop running
- Node 20+
- Free ports: 3000, 3001, 5672, 15672, 27017, 6379

### Quick Start (3 terminals)

```bash
# Terminal 1 — infra + workers
docker compose up -d rabbitmq mongo redis
docker compose up -d --build --scale worker=3 worker

# Terminal 2 — API (from backend/)
cd backend && npm install
CHAOS_ENABLED=1 REDIS_ENABLED=1 npm run api

# Terminal 3 — Frontend (from frontend/)
cd frontend && npm install
npm run dev
```

Open **http://localhost:3001**.

### One-command mode (no chaos simulator)

```bash
docker compose up -d --build --scale worker=3
```

### Rebuild Containers (after code changes)

```bash
# Rebuild everything (workers + backend + frontend) with current code
docker compose up -d --build

# Rebuild + scale workers
docker compose up -d --build --scale worker=20

# Rebuild only workers (e.g. after changing backend/src/)
docker compose up -d --build worker
docker compose up -d --build --scale worker=20 worker

# Rebuild only backend API
docker compose up -d --build backend

# Rebuild only frontend
docker compose up -d --build frontend
```

`--build` forces Docker to rebuild the image from the Dockerfile instead of using the cached image. Without it, code changes won't take effect — containers run the old binary.

**After scaling:** verify workers are running the new code:
```bash
docker compose logs --tail=5 worker | head -20   # check startup logs
docker compose ps                                 # verify replica count
```

### Stop

```bash
docker compose down        # keep data
docker compose down -v     # wipe everything
```

---

## URLs

| What | URL |
|------|-----|
| Chat UI | http://localhost:3001 |
| Data inspector | http://localhost:3001/data |
| API health | http://localhost:3000/api/health |
| API stats | http://localhost:3000/api/stats |
| RabbitMQ UI | http://localhost:15672 (guest/guest) |

---

## Test Scenarios

### Interactive (in the UI)

| Do | Proves |
|----|--------|
| Send a message | Full pipeline round-trip |
| Send 3 to same thread | Per-thread ordering (sequential) |
| Use /btw command | Parallel threads across workers |
| Kill a worker (chaos button) | Crash recovery |
| Stop Redis, send message | Mongo poll fallback works |
| Stop RabbitMQ, send | Graceful failure (503) |

### CLI Scripts (from `backend/`)

```bash
npm run publish:one              # single run e2e
npm run burst:parallel -- --threads 10 --messages 3   # parallel processing
npm run test:crash               # crash + reaper recovery
npm run demo:prefetch            # prove prefetch is hard limit
npm run verify:ordering -- <thread>   # prove no interleaving
npm run loadtest -- --users 100 --label test1   # load test
bash scripts/durability-test.sh  # kill worker mid-run → PASS
```

---

## Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672` | Queue connection |
| `MONGO_URL` | `mongodb://localhost:27017` | Database |
| `REDIS_URL` | `redis://localhost:6379` | Live events |
| `REDIS_ENABLED` | `0` | Enable live push + heartbeats |
| `WORKER_PREFETCH` | `1` | Concurrent runs per worker |
| `RUN_TIMEOUT_MS` | `120000` | Max run duration (2 min) |
| `RATE_LIMIT_WINDOW_MS` | `60000` | Rate limit window |
| `RATE_LIMIT_MAX` | `60` | Max requests per window |
| `REAPER_ENABLED` | `0` | Enable crash recovery |
| `REAPER_LEASE_MS` | `15000` | Stale run threshold |
| `SANDBOX_ENABLED` | `0` | Real Docker tool execution |
| `CHAOS_ENABLED` | `1` | Failure simulator endpoints |
| `MODEL_MIN_DELAY_MS` | `500` | Mock LLM min latency |
| `MODEL_MAX_DELAY_MS` | `3000` | Mock LLM max latency |

---

## Deploy to Production

### Build images

```bash
docker build -f backend/Dockerfile.api    -t registry/viper-api:v1    backend/
docker build -f backend/Dockerfile.worker -t registry/viper-worker:v1 backend/
docker build -t registry/viper-frontend:v1 frontend/
```

### Topology

| Service | Scale on | Min replicas |
|---------|----------|-------------|
| frontend | CPU | 2 |
| api | Request rate | 2 |
| worker | Queue depth | 10+ |

### Production checklist

- [ ] Managed RabbitMQ, MongoDB (replica set), Redis
- [ ] `REDIS_ENABLED=1` on all services
- [ ] `REAPER_ENABLED=1` on at least one worker
- [ ] Worker autoscaler on queue depth (KEDA recommended)
- [ ] `CHAOS_ENABLED=0` in production
- [ ] Real LLM wired in place of mockModel

### Auto-scaling (KEDA)

```bash
helm install keda kedacore/keda
kubectl apply -f k8s/worker-deployment.yaml
```

Workers auto-scale 10→1000 based on queue depth. Scale-down sends SIGTERM → graceful shutdown → no orphaned runs.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| Health dots all red | API not running, or missing `REDIS_ENABLED=1` |
| Workers show 0 | Workers crashed — check `docker compose logs worker` |
| Runs stuck "in progress" | Enable reaper: `REAPER_ENABLED=1`, or restart workers |
| Port in use | `lsof -ti:3000 :3001 | xargs kill` |
| Full reset | `docker compose down -v` |
