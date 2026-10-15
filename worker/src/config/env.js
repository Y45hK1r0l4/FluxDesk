const path = require('path');
const dotenv = require('dotenv');

dotenv.config({ path: path.resolve(__dirname, '../../../.env') });

const config = {
  env: process.env.NODE_ENV || 'development',
  workerId: process.env.WORKER_ID || `worker-${process.pid}`,
  port: parseInt(process.env.WORKER_PORT || '4001', 10),

  kafka: {
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    clientId: (process.env.KAFKA_CLIENT_ID || 'fluxdesk-cluster') + '-worker',
    groupId: process.env.KAFKA_GROUP_ID || 'fluxdesk-worker-group',
    incomingTopic: process.env.KAFKA_TOPIC_INCOMING || 'ticket.incoming',
    processedTopic: process.env.KAFKA_TOPIC_PROCESSED || 'ticket.processed',
    dlqTopic: process.env.KAFKA_TOPIC_DLQ || 'ticket.dlq',
  },

  mongo: {
    uri: process.env.MONGO_URI || 'mongodb://localhost:27017/fluxdesk',
    dbName: process.env.MONGO_DB_NAME || 'fluxdesk',
  },

  gemini: {
    apiKey: process.env.GEMINI_API_KEY,
    model: process.env.GEMINI_MODEL || 'gemini-3.5-flash-lite',
    fallbackModels: ['gemini-3.5-flash-lite', 'gemini-flash-latest', 'gemini-3.8-flash'],
    timeoutMs: 25000,
  },

  retry: {
    maxAttempts: 3,
    initialDelayMs: 1000,
    backoffFactor: 2,
  },
};

module.exports = config;
