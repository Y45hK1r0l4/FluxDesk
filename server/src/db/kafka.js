const { Kafka, Partitioners } = require('kafkajs');
const config = require('../config/env');
const logger = require('../config/logger');

let kafkaInstance = null;
let kafkaProducer = null;
let isConnected = false;
let failureCount = 0;
const FAILURE_THRESHOLD = 5;
let circuitOpenUntil = 0;

function getKafka() {
  if (!kafkaInstance) {
    kafkaInstance = new Kafka({
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
      retry: {
        initialRetryTime: 300,
        retries: 5,
      },
    });
  }
  return kafkaInstance;
}

async function connectKafkaProducer() {
  if (kafkaProducer && isConnected) return kafkaProducer;

  const kafka = getKafka();
  kafkaProducer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
    allowAutoTopicCreation: false,
    transactionTimeout: 30000,
  });

  try {
    await kafkaProducer.connect();
    isConnected = true;
    failureCount = 0;
    circuitOpenUntil = 0;
    logger.info('Kafka producer connected successfully', { brokers: config.kafka.brokers });
    return kafkaProducer;
  } catch (err) {
    isConnected = false;
    logger.error('Failed to connect Kafka producer', { error: err.message });
    throw err;
  }
}

async function sendTicketIncomingEvent(tenantId, eventData) {
  const now = Date.now();
  if (circuitOpenUntil > now) {
    const retryAfter = Math.ceil((circuitOpenUntil - now) / 1000);
    const err = new Error('Kafka circuit breaker is OPEN due to broker errors');
    err.status = 503;
    err.retryAfter = retryAfter;
    throw err;
  }

  if (!kafkaProducer || !isConnected) {
    await connectKafkaProducer();
  }

  try {
    const payload = typeof eventData === 'string' ? eventData : JSON.stringify(eventData);
    
    // Strict invariant: key MUST be tenantId for partition ordering per tenant
    const record = {
      topic: config.kafka.incomingTopic,
      messages: [
        {
          key: String(tenantId),
          value: payload,
          timestamp: String(Date.now()),
        },
      ],
    };

    const recordMetadata = await kafkaProducer.send(record);
    failureCount = 0;
    return recordMetadata;
  } catch (err) {
    failureCount += 1;
    logger.error('Failed to produce event to Kafka', {
      topic: config.kafka.incomingTopic,
      tenantId,
      error: err.message,
      failureCount,
    });

    if (failureCount >= FAILURE_THRESHOLD) {
      circuitOpenUntil = Date.now() + 10000; // Open circuit for 10s
      logger.warn('Kafka circuit breaker TRIPPED! Fast failing for 10 seconds');
    }

    const serviceError = new Error('Streaming broker currently unavailable');
    serviceError.status = 503;
    serviceError.retryAfter = 5;
    throw serviceError;
  }
}

async function disconnectKafkaProducer() {
  if (kafkaProducer) {
    try {
      await kafkaProducer.disconnect();
      logger.info('Kafka producer disconnected cleanly');
    } catch (err) {
      logger.warn('Error disconnecting Kafka producer', { error: err.message });
    } finally {
      kafkaProducer = null;
      isConnected = false;
    }
  }
}

async function getKafkaHealth() {
  const circuitOpen = circuitOpenUntil > Date.now();
  if (circuitOpen) {
    return {
      status: 'DOWN',
      circuitBreaker: 'OPEN',
      failureCount,
    };
  }

  return {
    status: isConnected ? 'UP' : 'DEGRADED',
    circuitBreaker: 'CLOSED',
    connected: isConnected,
  };
}

module.exports = {
  getKafka,
  connectKafkaProducer,
  sendTicketIncomingEvent,
  disconnectKafkaProducer,
  getKafkaHealth,
};
