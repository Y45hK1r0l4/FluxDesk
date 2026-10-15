const { getKafka } = require('../db/kafka');
const config = require('../config/env');
const logger = require('../config/logger');
const { publishTenantEvent } = require('./pubsub');

let kafkaConsumer = null;
let isRunning = false;

const metrics = {
  eventsConsumed: 0,
  lastEventReceivedAt: null,
  errorsCount: 0,
};

/**
 * Start Kafka consumer on ticket.processed topic
 */
async function startProcessedConsumer() {
  if (isRunning) return;

  const kafka = getKafka();
  const groupId = `${config.kafka.clientId}-realtime-group`;

  kafkaConsumer = kafka.consumer({
    groupId,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
  });

  try {
    await kafkaConsumer.connect();
    await kafkaConsumer.subscribe({
      topic: config.kafka.processedTopic,
      fromBeginning: false,
    });

    isRunning = true;
    logger.info('Gateway Real-Time Kafka Consumer subscribed to ticket.processed', {
      groupId,
      topic: config.kafka.processedTopic,
    });

    await kafkaConsumer.run({
      autoCommit: true,
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const rawValue = message.value.toString();
          const event = JSON.parse(rawValue);

          const tenantId = event.tenantId || (message.key ? message.key.toString() : null);

          metrics.eventsConsumed += 1;
          metrics.lastEventReceivedAt = new Date().toISOString();

          logger.info('Consumed TICKET_PROCESSED event from Kafka', {
            topic,
            partition,
            offset: message.offset,
            tenantId,
            ticketId: event.payload?.ticketId,
            urgency: event.payload?.urgency,
            status: event.payload?.status,
          });

          if (!tenantId) {
            logger.warn('Event missing tenantId, cannot broadcast to tenant channel', { eventId: event.eventId });
            return;
          }

          // Format standardized real-time broadcast event
          const broadcastEvent = {
            type: 'TICKET_PROCESSED',
            specVersion: event.specVersion || '1.0',
            eventId: event.eventId,
            timestamp: event.timestamp || new Date().toISOString(),
            tenantId,
            data: event.payload || {},
          };

          // Step 10 & 11: Publish to Redis Pub/Sub channel "tenant:<tenantId>"
          await publishTenantEvent(tenantId, broadcastEvent);
        } catch (err) {
          metrics.errorsCount += 1;
          logger.error('Error handling ticket.processed message', {
            error: err.message,
            partition,
            offset: message.offset,
          });
        }
      },
    });
  } catch (err) {
    isRunning = false;
    logger.error('Failed to start Kafka processedConsumer', { error: err.message });
    throw err;
  }
}

/**
 * Stop consumer cleanly
 */
async function disconnectProcessedConsumer() {
  if (kafkaConsumer) {
    try {
      await kafkaConsumer.disconnect();
      logger.info('Gateway Real-Time Kafka Consumer disconnected cleanly');
    } catch (err) {
      logger.warn('Error disconnecting processedConsumer', { error: err.message });
    } finally {
      kafkaConsumer = null;
      isRunning = false;
    }
  }
}

/**
 * Consumer health status & metrics
 */
function getProcessedConsumerMetrics() {
  return {
    status: isRunning ? 'UP' : 'DOWN',
    topic: config.kafka.processedTopic,
    isRunning,
    ...metrics,
  };
}

module.exports = {
  startProcessedConsumer,
  disconnectProcessedConsumer,
  getProcessedConsumerMetrics,
};
