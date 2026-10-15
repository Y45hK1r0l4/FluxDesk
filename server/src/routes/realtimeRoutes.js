const express = require('express');
const router = express.Router();
const { getWebSocketStats } = require('../realtime/websocketGateway');
const { getPubSubMetrics } = require('../realtime/pubsub');
const { getProcessedConsumerMetrics } = require('../realtime/processedConsumer');
const { anyAuth } = require('../middleware/anyAuth');

// Real-Time Fabric telemetry endpoint
router.get('/stats', anyAuth, (req, res) => {
  const websocket = getWebSocketStats();
  const pubsub = getPubSubMetrics();
  const kafkaConsumer = getProcessedConsumerMetrics();

  return res.json({
    success: true,
    timestamp: new Date().toISOString(),
    realtime: {
      websocket,
      pubsub,
      kafkaConsumer,
    },
  });
});

module.exports = router;
