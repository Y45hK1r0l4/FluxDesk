const { Kafka, Partitioners } = require('kafkajs');
const config = require('../config/env');
const logger = require('../config/logger');

let kafkaInstance = null;
let kafkaProducer = null;
let kafkaConsumer = null;
let isProducerConnected = false;
let isConsumerConnected = false;

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

async function connectProducer() {
  if (kafkaProducer && isProducerConnected) return kafkaProducer;

  const kafka = getKafka();
  kafkaProducer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
    allowAutoTopicCreation: false,
    transactionTimeout: 30000,
  });

  try {
    await kafkaProducer.connect();
    isProducerConnected = true;
    logger.info('Kafka producer connected successfully in worker fleet');
    return kafkaProducer;
  } catch (err) {
    isProducerConnected = false;
    logger.error('Failed to connect Kafka producer in worker', { error: err.message });
    throw err;
  }
}

async function connectConsumer() {
  if (kafkaConsumer && isConsumerConnected) return kafkaConsumer;

  const kafka = getKafka();
  kafkaConsumer = kafka.consumer({
    groupId: config.kafka.groupId,
    allowAutoTopicCreation: false,
    sessionTimeout: 45000,
    heartbeatInterval: 3000,
  });

  try {
    await kafkaConsumer.connect();
    isConsumerConnected = true;
    logger.info(`Kafka consumer connected in worker fleet (Group: ${config.kafka.groupId})`);
    return kafkaConsumer;
  } catch (err) {
    isConsumerConnected = false;
    logger.error('Failed to connect Kafka consumer in worker', { error: err.message });
    throw err;
  }
}

async function sendProcessedEvent(tenantId, eventData) {
  if (!kafkaProducer || !isProducerConnected) {
    await connectProducer();
  }

  const payload = typeof eventData === 'string' ? eventData : JSON.stringify(eventData);
  const record = {
    topic: config.kafka.processedTopic,
    messages: [
      {
        key: String(tenantId),
        value: payload,
        timestamp: String(Date.now()),
      },
    ],
  };

  return await kafkaProducer.send(record);
}

async function sendDlqEvent(tenantId, dlqData) {
  if (!kafkaProducer || !isProducerConnected) {
    await connectProducer();
  }

  const payload = typeof dlqData === 'string' ? dlqData : JSON.stringify(dlqData);
  const record = {
    topic: config.kafka.dlqTopic,
    messages: [
      {
        key: String(tenantId || 'unknown'),
        value: payload,
        timestamp: String(Date.now()),
      },
    ],
  };

  return await kafkaProducer.send(record);
}

async function disconnectAll() {
  if (kafkaConsumer) {
    try {
      await kafkaConsumer.disconnect();
      logger.info('Kafka consumer disconnected');
    } catch (e) {
      logger.warn('Error disconnecting Kafka consumer', { error: e.message });
    } finally {
      kafkaConsumer = null;
      isConsumerConnected = false;
    }
  }

  if (kafkaProducer) {
    try {
      await kafkaProducer.disconnect();
      logger.info('Kafka producer disconnected');
    } catch (e) {
      logger.warn('Error disconnecting Kafka producer', { error: e.message });
    } finally {
      kafkaProducer = null;
      isProducerConnected = false;
    }
  }
}

function getKafkaHealth() {
  return {
    status: isConsumerConnected && isProducerConnected ? 'UP' : isConsumerConnected || isProducerConnected ? 'DEGRADED' : 'DOWN',
    consumerConnected: isConsumerConnected,
    producerConnected: isProducerConnected,
    groupId: config.kafka.groupId,
    brokers: config.kafka.brokers,
  };
}

module.exports = {
  getKafka,
  connectProducer,
  connectConsumer,
  sendProcessedEvent,
  sendDlqEvent,
  disconnectAll,
  getKafkaHealth,
};
