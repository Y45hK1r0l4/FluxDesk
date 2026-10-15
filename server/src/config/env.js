const path = require('path');
const dotenv = require('dotenv');

// Load environment variables from the root .env file
dotenv.config({ path: path.resolve(__dirname, '../../../.env') });

const config = {
  env: process.env.NODE_ENV || 'development',
  port: parseInt(process.env.PORT || '4000', 10),
  
  kafka: {
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    clientId: process.env.KAFKA_CLIENT_ID || 'fluxdesk-gateway',
    incomingTopic: process.env.KAFKA_TOPIC_INCOMING || 'ticket.incoming',
    processedTopic: process.env.KAFKA_TOPIC_PROCESSED || 'ticket.processed',
    dlqTopic: process.env.KAFKA_TOPIC_DLQ || 'ticket.dlq',
  },
  
  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
    url: process.env.REDIS_URL || 'redis://localhost:6379',
  },
  
  mongo: {
    uri: process.env.MONGO_URI || 'mongodb://localhost:27017/fluxdesk',
    dbName: process.env.MONGO_DB_NAME || 'fluxdesk',
  },
  
  jwt: {
    secret: process.env.JWT_SECRET || 'P{?]$3_O</KU)hM(z/,W(e0a5eJuX2.K0@+Lc|zUE;lAN(*;PMgy-%F>*4=%MDz+',
    expiresIn: process.env.JWT_EXPIRES_IN || '7d',
  },
  
  workerPort: parseInt(process.env.WORKER_PORT || '4001', 10),

  gemini: {
    apiKey: process.env.GEMINI_API_KEY,
    model: process.env.GEMINI_MODEL || 'gemini-3.5-flash-lite',
  },

  rateLimit: {
    defaultPerMinute: parseInt(process.env.DEFAULT_TENANT_RATE_LIMIT_PER_MINUTE || '60', 10),
    windowMs: 60 * 1000, // 60 seconds
  },
};

module.exports = config;
