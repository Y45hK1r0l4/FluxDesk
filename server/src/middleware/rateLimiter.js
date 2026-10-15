const { getRedisClient } = require('../db/redis');
const config = require('../config/env');
const logger = require('../config/logger');

/**
 * Distributed Sliding-Window Rate Limiter using Redis Sorted Sets (ZSET)
 * Strictly follows RFC-001 Section 4.2:
 * Key format: ratelimit:{tenantId}:{endpoint}
 * Sliding Window: 60,000 ms (60s)
 */
function createRateLimiter(options = {}) {
  const endpointName = options.endpoint || 'tickets';
  const windowMs = options.windowMs || config.rateLimit.windowMs;

  return async function rateLimiter(req, res, next) {
    const tenantId = req.tenantId || (req.tenant && req.tenant.tenantId) || 'anonymous';
    const limit = (req.tenant && req.tenant.rateLimitPerMinute) || config.rateLimit.defaultPerMinute;
    const redisKey = `ratelimit:${tenantId}:${endpointName}`;

    const now = Date.now();
    const windowStart = now - windowMs;
    const member = `${now}-${Math.random().toString(36).substring(2, 9)}`;

    try {
      const redis = getRedisClient();

      // Pipeline execution for atomicity and sub-millisecond execution
      const pipeline = redis.pipeline();
      pipeline.zremrangebyscore(redisKey, 0, windowStart);
      pipeline.zcard(redisKey);

      const results = await pipeline.exec();
      const currentCount = results[1][1]; // ZCARD result

      const limitResetSeconds = Math.ceil((now + windowMs) / 1000);

      if (currentCount < limit) {
        // Quota available: Record current request timestamp
        const addPipeline = redis.pipeline();
        addPipeline.zadd(redisKey, now, member);
        addPipeline.expire(redisKey, Math.ceil(windowMs / 1000));
        await addPipeline.exec();

        const remaining = Math.max(0, limit - currentCount - 1);
        res.setHeader('X-RateLimit-Limit', limit);
        res.setHeader('X-RateLimit-Remaining', remaining);
        res.setHeader('X-RateLimit-Reset', limitResetSeconds);

        return next();
      }

      // Quota exceeded: Calculate time until oldest entry leaves window
      const oldestEntries = await redis.zrange(redisKey, 0, 0, 'WITHSCORES');
      let retryAfterSeconds = 60;
      if (oldestEntries && oldestEntries.length >= 2) {
        const oldestScore = parseFloat(oldestEntries[1]);
        const timeUntilOldestExpires = (oldestScore + windowMs) - now;
        retryAfterSeconds = Math.max(1, Math.ceil(timeUntilOldestExpires / 1000));
      }

      res.setHeader('Retry-After', retryAfterSeconds);
      res.setHeader('X-RateLimit-Limit', limit);
      res.setHeader('X-RateLimit-Remaining', 0);
      res.setHeader('X-RateLimit-Reset', limitResetSeconds);

      logger.warn('Tenant rate limit exceeded', {
        tenantId,
        endpoint: endpointName,
        currentCount,
        limit,
        retryAfterSeconds,
      });

      return res.status(429).json({
        error: 'Too Many Requests',
        message: 'Tenant sliding-window rate limit exceeded. Please wait before submitting more requests.',
        tenantId,
        limit,
        windowSeconds: windowMs / 1000,
        retryAfterSeconds,
      });
    } catch (err) {
      logger.error('Rate limiter Redis error, failing open to preserve availability', {
        tenantId,
        error: err.message,
      });
      res.setHeader('X-RateLimit-Status', 'bypassed');
      return next();
    }
  };
}

module.exports = {
  createRateLimiter,
};
