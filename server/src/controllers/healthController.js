const { getMongoHealth } = require('../db/mongo');
const { getRedisHealth } = require('../db/redis');
const { getKafkaHealth } = require('../db/kafka');
const { getWebSocketStats } = require('../realtime/websocketGateway');
const { getPubSubMetrics } = require('../realtime/pubsub');
const { getProcessedConsumerMetrics } = require('../realtime/processedConsumer');

async function getHealth(req, res) {
  const startTime = Date.now();
  const [redisHealth, kafkaHealth] = await Promise.all([
    getRedisHealth(),
    getKafkaHealth(),
  ]);

  const mongoHealth = getMongoHealth();
  const wsStats = getWebSocketStats();
  const pubsubMetrics = getPubSubMetrics();
  const consumerMetrics = getProcessedConsumerMetrics();

  const services = {
    mongodb: mongoHealth,
    redis: redisHealth,
    kafka: kafkaHealth,
    websocket: wsStats,
    redisPubSub: pubsubMetrics,
    kafkaProcessedConsumer: consumerMetrics,
  };

  const isHealthy =
    mongoHealth.status === 'UP' &&
    redisHealth.status === 'UP' &&
    kafkaHealth.status !== 'DOWN';

  const statusCode = isHealthy ? 200 : 503;

  return res.status(statusCode).json({
    status: isHealthy ? 'UP' : 'DEGRADED',
    timestamp: new Date().toISOString(),
    uptimeSeconds: process.uptime(),
    checkDurationMs: Date.now() - startTime,
    services,
  });
}

module.exports = {
  getHealth,
};
