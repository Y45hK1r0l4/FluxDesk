const http = require('http');
const app = require('./app');
const config = require('./config/env');
const logger = require('./config/logger');
const { connectMongo, disconnectMongo } = require('./db/mongo');
const { getRedisClient, disconnectRedis } = require('./db/redis');
const { connectKafkaProducer, disconnectKafkaProducer } = require('./db/kafka');
const { initRedisPubSub, disconnectRedisPubSub } = require('./realtime/pubsub');
const { initWebSocketGateway, closeWebSocketGateway } = require('./realtime/websocketGateway');
const { startProcessedConsumer, disconnectProcessedConsumer } = require('./realtime/processedConsumer');

let server = null;

async function startServer() {
  try {
    logger.info('Starting FluxDesk Ingestion Gateway & Real-Time Fabric...');

    // 1. Initialize MongoDB connection
    await connectMongo();

    // 2. Initialize Redis connection
    const redis = getRedisClient();
    await redis.ping();

    // 3. Initialize Kafka producer
    await connectKafkaProducer();

    // 4. Initialize Redis Pub/Sub multiplexer
    await initRedisPubSub();

    // 5. Initialize Kafka consumer for ticket.processed events
    await startProcessedConsumer();

    // 6. Start HTTP server
    server = http.createServer(app);

    // 7. Attach WebSocket Gateway to HTTP server
    initWebSocketGateway(server);

    server.listen(config.port, () => {
      logger.info(`⚡ FluxDesk Ingestion Gateway is listening on port ${config.port}`);
      logger.info(`   - Ingestion Endpoint: POST http://localhost:${config.port}/api/v1/tickets`);
      logger.info(`   - Real-Time WebSocket: ws://localhost:${config.port}/ws`);
      logger.info(`   - Health Check: GET http://localhost:${config.port}/health`);
    });
  } catch (err) {
    logger.error('Fatal startup error, shutting down', { error: err.message, stack: err.stack });
    process.exit(1);
  }
}

async function gracefulShutdown(signal) {
  logger.info(`Received ${signal}, initiating graceful shutdown...`);

  try {
    await closeWebSocketGateway();
  } catch (err) {
    logger.warn('Error closing WebSocket Gateway', { error: err.message });
  }

  if (server) {
    server.close(() => {
      logger.info('HTTP server closed');
    });
  }

  try {
    await disconnectProcessedConsumer();
    await disconnectRedisPubSub();
    await disconnectKafkaProducer();
    await disconnectRedis();
    await disconnectMongo();
    logger.info('All database & streaming connections closed cleanly');
    process.exit(0);
  } catch (err) {
    logger.error('Error during shutdown', { error: err.message });
    process.exit(1);
  }
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

process.on('unhandledRejection', (reason) => {
  logger.error('Unhandled Promise Rejection', { reason });
});

process.on('uncaughtException', (err) => {
  logger.error('Uncaught Exception', { error: err.message, stack: err.stack });
  process.exit(1);
});

startServer();
