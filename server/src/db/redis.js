const Redis = require('ioredis');
const config = require('../config/env');
const logger = require('../config/logger');

let redisClient = null;

function getRedisClient() {
  if (!redisClient) {
    redisClient = new Redis(config.redis.url, {
      maxRetriesPerRequest: 3,
      retryStrategy(times) {
        const delay = Math.min(times * 200, 2000);
        return delay;
      },
      lazyConnect: false,
    });

    redisClient.on('connect', () => {
      logger.info('Redis client connected', { host: config.redis.host, port: config.redis.port });
    });

    redisClient.on('ready', () => {
      logger.info('Redis client ready to accept commands');
    });

    redisClient.on('error', (err) => {
      logger.error('Redis client error', { error: err.message });
    });

    redisClient.on('close', () => {
      logger.warn('Redis client connection closed');
    });
  }

  return redisClient;
}

async function pingRedis() {
  const client = getRedisClient();
  const start = Date.now();
  const pong = await client.ping();
  const latencyMs = Date.now() - start;
  return { pong, latencyMs };
}

async function disconnectRedis() {
  if (redisClient) {
    await redisClient.quit();
    redisClient = null;
    logger.info('Redis client disconnected cleanly');
  }
}

async function getRedisHealth() {
  try {
    const { pong, latencyMs } = await pingRedis();
    return {
      status: pong === 'PONG' ? 'UP' : 'DEGRADED',
      latencyMs,
    };
  } catch (err) {
    return {
      status: 'DOWN',
      error: err.message,
    };
  }
}

module.exports = {
  getRedisClient,
  disconnectRedis,
  getRedisHealth,
};
