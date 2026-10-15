const express = require('express');
const config = require('../config/env');
const logger = require('../config/logger');
const metrics = require('./metrics');
const { getKafkaHealth } = require('../db/kafka');
const { getMongoHealth } = require('../db/mongo');

let server = null;

function createHealthApp() {
  const app = express();
  app.use(express.json());

  app.get('/health', async (req, res) => {
    const kafkaHealth = getKafkaHealth();
    const mongoHealth = getMongoHealth();

    const isHealthy = kafkaHealth.status !== 'DOWN' && mongoHealth.status === 'UP';

    const response = {
      status: isHealthy ? 'UP' : 'DEGRADED',
      workerId: config.workerId,
      timestamp: new Date().toISOString(),
      services: {
        kafka: kafkaHealth,
        mongodb: mongoHealth,
        gemini: {
          status: config.gemini.apiKey ? 'UP' : 'MISSING_API_KEY',
          configuredModel: config.gemini.model,
        },
      },
      metrics: metrics.getSummary(),
    };

    return res.status(isHealthy ? 200 : 503).json(response);
  });

  app.get('/metrics', (req, res) => {
    return res.json({
      workerId: config.workerId,
      metrics: metrics.getSummary(),
    });
  });

  return app;
}

function startHealthServer(initialPort = config.port) {
  return new Promise((resolve, reject) => {
    const app = createHealthApp();
    let currentPort = initialPort;

    function tryListen() {
      server = app.listen(currentPort, () => {
        logger.info(`⚡ FluxDesk Worker [${config.workerId}] Health API listening on port ${currentPort}`);
        logger.info(`   - Health check: http://localhost:${currentPort}/health`);
        logger.info(`   - Metrics: http://localhost:${currentPort}/metrics`);
        resolve(server);
      });

      server.on('error', (err) => {
        if (err.code === 'EADDRINUSE' && currentPort < initialPort + 20) {
          logger.warn(`Port ${currentPort} already in use by another worker, trying port ${currentPort + 1}...`);
          currentPort += 1;
          tryListen();
        } else {
          logger.error('Health server listen error', { error: err.message });
          reject(err);
        }
      });
    }

    tryListen();
  });
}

function stopHealthServer() {
  return new Promise((resolve) => {
    if (server) {
      server.close(() => {
        logger.info('Health server stopped');
        resolve();
      });
    } else {
      resolve();
    }
  });
}

module.exports = {
  startHealthServer,
  stopHealthServer,
};
