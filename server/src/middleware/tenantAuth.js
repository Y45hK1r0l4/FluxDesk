const Tenant = require('../models/Tenant');
const logger = require('../config/logger');

// In-memory cache for fast tenant lookup (TTL 60s) to keep latency < 5ms
const tenantCache = new Map();
const CACHE_TTL_MS = 60 * 1000;

function getCachedTenant(cacheKey) {
  const cached = tenantCache.get(cacheKey);
  if (cached && Date.now() - cached.cachedAt < CACHE_TTL_MS) {
    return cached.tenant;
  }
  return null;
}

function setCachedTenant(cacheKey, tenant) {
  tenantCache.set(cacheKey, {
    tenant,
    cachedAt: Date.now(),
  });
}

/**
 * Tenant Auth Middleware
 * Supports:
 * - x-api-key: flx_live_...
 * - x-tenant-id: org_...
 */
async function tenantAuth(req, res, next) {
  const apiKey = req.headers['x-api-key'];
  const headerTenantId = req.headers['x-tenant-id'];

  if (!apiKey && !headerTenantId) {
    return res.status(401).json({
      error: 'Unauthorized',
      message: 'Missing authentication credentials. Provide "x-api-key" or "x-tenant-id" header.',
    });
  }

  const cacheKey = apiKey ? `key:${apiKey}` : `id:${headerTenantId}`;
  let tenant = getCachedTenant(cacheKey);

  if (!tenant) {
    try {
      const query = apiKey ? { apiKey } : { tenantId: headerTenantId };
      tenant = await Tenant.findOne(query).lean();

      if (!tenant) {
        return res.status(401).json({
          error: 'Unauthorized',
          message: 'Invalid API key or Tenant ID.',
        });
      }

      setCachedTenant(cacheKey, tenant);
    } catch (err) {
      logger.error('Error verifying tenant credentials', { error: err.message });
      return res.status(500).json({
        error: 'Internal Server Error',
        message: 'Failed to verify tenant credentials.',
      });
    }
  }

  // Cross-check if both headers are provided
  if (headerTenantId && tenant.tenantId !== headerTenantId) {
    return res.status(403).json({
      error: 'Forbidden',
      message: 'API key does not belong to the specified Tenant ID.',
    });
  }

  req.tenant = tenant;
  req.tenantId = tenant.tenantId;
  next();
}

module.exports = {
  tenantAuth,
  tenantCache,
};
