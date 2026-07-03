import type { Request, Response, NextFunction } from 'express';

/**
 * In-memory sliding-window rate limiter.
 * Tracks request counts per key (IP or user ID) within a time window.
 *
 * For production, swap this with Redis-backed limiter for multi-instance support.
 */
export interface RateLimitConfig {
  windowMs: number;      // Time window in ms (default: 60000 = 1 minute)
  maxRequests: number;   // Max requests per window (default: 60)
}

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  resetAt: number;       // Unix timestamp when window resets
}

export class RateLimiter {
  private windows: Map<string, { count: number; startedAt: number }> = new Map();
  private config: RateLimitConfig;

  constructor(config: Partial<RateLimitConfig> = {}) {
    this.config = {
      windowMs: config.windowMs ?? 60000,
      maxRequests: config.maxRequests ?? 60,
    };
    // Cleanup stale entries every minute
    setInterval(() => this.cleanup(), 60000).unref();
  }

  check(key: string): RateLimitResult {
    const now = Date.now();
    const entry = this.windows.get(key);

    if (!entry || now - entry.startedAt > this.config.windowMs) {
      // New window
      this.windows.set(key, { count: 1, startedAt: now });
      return { allowed: true, remaining: this.config.maxRequests - 1, resetAt: now + this.config.windowMs };
    }

    entry.count++;
    const remaining = Math.max(0, this.config.maxRequests - entry.count);
    const resetAt = entry.startedAt + this.config.windowMs;

    if (entry.count > this.config.maxRequests) {
      return { allowed: false, remaining: 0, resetAt };
    }

    return { allowed: true, remaining, resetAt };
  }

  private cleanup() {
    const now = Date.now();
    for (const [key, entry] of this.windows) {
      if (now - entry.startedAt > this.config.windowMs) {
        this.windows.delete(key);
      }
    }
  }
}

/** Express middleware factory */
export function rateLimitMiddleware(config: Partial<RateLimitConfig> = {}) {
  const limiter = new RateLimiter(config);

  return (req: Request, res: Response, next: NextFunction) => {
    const key = req.ip || req.socket?.remoteAddress || 'unknown';
    const result = limiter.check(key);

    res.setHeader('X-RateLimit-Limit', config.maxRequests ?? 60);
    res.setHeader('X-RateLimit-Remaining', result.remaining);
    res.setHeader('X-RateLimit-Reset', Math.ceil(result.resetAt / 1000));

    if (!result.allowed) {
      return res.status(429).json({
        error: 'Too many requests',
        retryAfter: Math.ceil((result.resetAt - Date.now()) / 1000),
      });
    }

    next();
  };
}
