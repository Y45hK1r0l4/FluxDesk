const Redis = require('ioredis');
const config = require('../config/env');
const logger = require('../config/logger');

let publisherClient = null;
let subscriberClient = null;
let isSubscribed = false;
let messageHandlers = new Set();

const metrics = {
  messagesPublished: 0,
  messagesReceived: 0,
  lastPublishedAt: null,
  lastReceivedAt: null,
};

/**
 * Get or create dedicated Redis Publisher client
 */
function getPublisherClient() {
  if (!publisherClient) {
    publisherClient = new Redis(config.redis.url, {
      maxRetriesPerRequest: 3,
      retryStrategy(times) {
        return Math.min(times * 200, 2000);
      },
    });

    publisherClient.on('connect', () => {
      logger.info('Redis PubSub Publisher connected', { host: config.redis.host, port: config.redis.port });
    });

    publisherClient.on('error', (err) => {
      logger.error('Redis PubSub Publisher error', { error: err.message });
    });
  }
  return publisherClient;
}

/**
 * Get or create dedicated Redis Subscriber client
 */
function getSubscriberClient() {
  if (!subscriberClient) {
    subscriberClient = new Redis(config.redis.url, {
      maxRetriesPerRequest: 3,
      retryStrategy(times) {
        return Math.min(times * 200, 2000);
      },
    });

    subscriberClient.on('connect', () => {
      logger.info('Redis PubSub Subscriber connected', { host: config.redis.host, port: config.redis.port });
    });

    subscriberClient.on('error', (err) => {
      logger.error('Redis PubSub Subscriber error', { error: err.message });
    });
  }
  return subscriberClient;
}

/**
 * Publish event to tenant-specific channel: tenant:{tenantId}
 */
async function publishTenantEvent(tenantId, eventData) {
  if (!tenantId) {
    throw new Error('tenantId is required to publish tenant event');
  }

  const pub = getPublisherClient();
  const channel = `tenant:${tenantId}`;
  const serialized = typeof eventData === 'string' ? eventData : JSON.stringify(eventData);

  try {
    const receiverCount = await pub.publish(channel, serialized);
    metrics.messagesPublished += 1;
    metrics.lastPublishedAt = new Date().toISOString();

    logger.debug('Published event to Redis channel', {
      channel,
      tenantId,
      receiverCount,
      eventType: eventData?.eventType || eventData?.type,
    });

    return receiverCount;
  } catch (err) {
    logger.error('Failed to publish event to Redis channel', {
      channel,
      tenantId,
      error: err.message,
    });
    throw err;
  }
}

/**
 * Register a listener for Redis Pub/Sub messages across tenant channels
 * @param {Function} handler - callback function (tenantId, eventData, channel)
 */
function onTenantMessage(handler) {
  messageHandlers.add(handler);
  return () => {
    messageHandlers.delete(handler);
  };
}

/**
 * Initialize Redis Pub/Sub subscriber listening to pattern tenant:*
 */
async function initRedisPubSub() {
  if (isSubscribed) return;

  const sub = getSubscriberClient();

  // Pattern subscribe to all tenant channels: tenant:*
  await sub.psubscribe('tenant:*');
  isSubscribed = true;
  logger.info('Redis PubSub subscriber initialized on pattern: tenant:*');

  sub.on('pmessage', (pattern, channel, message) => {
    metrics.messagesReceived += 1;
    metrics.lastReceivedAt = new Date().toISOString();

    // Extract tenantId from channel: tenant:<tenantId>
    const match = channel.match(/^tenant:(.+)$/);
    const tenantId = match ? match[1] : null;

    let parsed = null;
    try {
      parsed = JSON.parse(message);
    } catch {
      parsed = { raw: message };
    }

    // Dispatch to registered handlers
    for (const handler of messageHandlers) {
      try {
        handler(tenantId, parsed, channel);
      } catch (err) {
        logger.error('Error in Redis PubSub message handler', { error: err.message, channel });
      }
    }
  });
}

/**
 * Disconnect Redis Pub/Sub clients cleanly
 */
async function disconnectRedisPubSub() {
  messageHandlers.clear();
  isSubscribed = false;

  const promises = [];
  if (subscriberClient) {
    promises.push(
      subscriberClient.punsubscribe('tenant:*')
        .then(() => subscriberClient.quit())
        .catch(() => {})
        .finally(() => { subscriberClient = null; })
    );
  }
  if (publisherClient) {
    promises.push(
      publisherClient.quit()
        .catch(() => {})
        .finally(() => { publisherClient = null; })
    );
  }

  await Promise.all(promises);
  logger.info('Redis PubSub clients disconnected cleanly');
}

/**
 * Get Pub/Sub health and operational metrics
 */
function getPubSubMetrics() {
  return {
    status: isSubscribed ? 'UP' : 'INITIALIZING',
    pattern: 'tenant:*',
    ...metrics,
    activeHandlers: messageHandlers.size,
  };
}

module.exports = {
  getPublisherClient,
  getSubscriberClient,
  publishTenantEvent,
  onTenantMessage,
  initRedisPubSub,
  disconnectRedisPubSub,
  getPubSubMetrics,
};
