const Tenant = require('../models/Tenant');
const { getRedisClient } = require('../db/redis');
const config = require('../config/env');

async function getTenantQuota(req, res, next) {
  try {
    const tenantId = req.tenantId;
    const redis = getRedisClient();
    const redisKey = `ratelimit:${tenantId}:tickets`;

    const now = Date.now();
    const windowStart = now - config.rateLimit.windowMs;

    // Prune stale timestamps and count current window
    const pipeline = redis.pipeline();
    pipeline.zremrangebyscore(redisKey, 0, windowStart);
    pipeline.zcard(redisKey);
    const results = await pipeline.exec();
    const currentUsage = results[1][1];

    const tenant = await Tenant.findOne({ tenantId }).lean();
    const limit = tenant ? tenant.rateLimitPerMinute : config.rateLimit.defaultPerMinute;
    const remaining = Math.max(0, limit - currentUsage);

    return res.json({
      success: true,
      tenantId,
      quota: {
        limit,
        used: currentUsage,
        remaining,
        windowSeconds: config.rateLimit.windowMs / 1000,
        resetInSeconds: 60,
      },
    });
  } catch (err) {
    next(err);
  }
}

async function listTenants(req, res, next) {
  try {
    const tenants = await Tenant.find({}, 'tenantId name plan rateLimitPerMinute createdAt').lean();
    return res.json({
      success: true,
      tenants,
    });
  } catch (err) {
    next(err);
  }
}

module.exports = {
  getTenantQuota,
  listTenants,
};
