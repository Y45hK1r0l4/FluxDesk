const { connectMongo, disconnectMongo } = require('./db/mongo');
const { connectProducer, disconnectAll } = require('./db/kafka');
const { startHealthServer, stopHealthServer } = require('./health/healthServer');
const { startTicketConsumer, stopTicketConsumer } = require('./consumer/ticketConsumer');
const config = require('./config/env');
const logger = require('./config/logger');

let isShuttingDown = false;

async function startWorker() {
  try {
    logger.info('======================================================');
    logger.info(`⚡ FluxDesk Asynchronous AI Worker Fleet [${config.workerId}]`);
    logger.info('======================================================');

    // 1. Connect MongoDB
    await connectMongo();

    // 2. Connect Kafka Producer (for ticket.processed and ticket.dlq)
    await connectProducer();

    // 3. Start Health & Metrics HTTP Server
    await startHealthServer();

    // 4. Start Kafka Consumer for incoming tickets
    await startTicketConsumer();

    logger.info(`🚀 Worker [${config.workerId}] is actively consuming from topic "${config.kafka.incomingTopic}"`);
  } catch (err) {
    logger.error('Fatal startup error in AI worker', {
      error: err.message,
      stack: err.stack,
    });
    process.exit(1);
  }
}

async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;

  logger.info(`Received ${signal}, initiating graceful shutdown of worker...`);

  try {
    // 1. Stop consuming new messages
    await stopTicketConsumer();

    // 2. Stop health server
    await stopHealthServer();

    // 3. Cleanly disconnect Kafka & MongoDB
    await disconnectAll();
    await disconnectMongo();

    logger.info('Worker shutdown completed cleanly');
    process.exit(0);
  } catch (err) {
    logger.error('Error during worker shutdown', { error: err.message });
    process.exit(1);
  }
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

process.on('unhandledRejection', (reason) => {
  logger.error('Unhandled Promise Rejection in worker', { reason: reason?.message || reason });
});

process.on('uncaughtException', (err) => {
  logger.error('Uncaught Exception in worker', { error: err.message, stack: err.stack });
  process.exit(1);
});

startWorker();
