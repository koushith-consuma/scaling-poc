import 'dotenv/config';

/** Centralized env-driven config. All services read from here. */
export const config = {
  rabbitUrl: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672',
  rabbitMgmtUrl: process.env.RABBITMQ_MGMT_URL ?? 'http://guest:guest@localhost:15672',
  runQueue: process.env.RUN_QUEUE ?? 'agent-run-queue',

  mongoUrl: process.env.MONGO_URL ?? 'mongodb://localhost:27017',
  mongoDb: process.env.MONGO_DB ?? 'viper',
  mongoSkipIndexes: process.env.MONGO_SKIP_INDEXES === '1',

  redisUrl: process.env.REDIS_URL ?? 'redis://localhost:6379',
  // Live event layer is opt-in. Off for the Step 2/3 slice (no redis service).
  redisEnabled: process.env.REDIS_ENABLED === '1',

  webPort: Number(process.env.WEB_PORT ?? 3000),

  workerPrefetch: Number(process.env.WORKER_PREFETCH ?? 1),
  poolSize: Number(process.env.POOL_SIZE ?? 3),
  sandboxImage: process.env.SANDBOX_IMAGE ?? 'alpine:3.20',
  // Disable real docker (Step 2/3 run without a sandbox). Step 4+ turns it on.
  sandboxEnabled: process.env.SANDBOX_ENABLED === '1',

  model: {
    minDelayMs: Number(process.env.MODEL_MIN_DELAY_MS ?? 500),
    maxDelayMs: Number(process.env.MODEL_MAX_DELAY_MS ?? 3000),
    avgTurns: Number(process.env.MODEL_AVG_TURNS ?? 4),
    toolCallProbability: Number(process.env.MODEL_TOOL_CALL_PROB ?? 0.6),
    seed: process.env.MOCK_SEED ? Number(process.env.MOCK_SEED) : undefined,
  },

  tool: {
    minDelayMs: Number(process.env.TOOL_MIN_DELAY_MS ?? 200),
    maxDelayMs: Number(process.env.TOOL_MAX_DELAY_MS ?? 1500),
  },

  // Per-run timeout — abort a run if it exceeds this duration.
  runTimeoutMs: Number(process.env.RUN_TIMEOUT_MS ?? 120000), // 2 minutes default

  rateLimit: {
    windowMs: Number(process.env.RATE_LIMIT_WINDOW_MS ?? 60000),
    maxRequests: Number(process.env.RATE_LIMIT_MAX ?? 60),
  },

  // Interactive web app / chaos controls (Step 9 — testability).
  // Chaos endpoints shell out to docker; enabled by default for the POC.
  chaosEnabled: process.env.CHAOS_ENABLED !== '0',
  composeProject: process.env.COMPOSE_PROJECT ?? 'poss',
} as const;

export type Config = typeof config;
