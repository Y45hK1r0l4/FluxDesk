const { WebSocketServer, WebSocket } = require('ws');
const url = require('url');
const jwt = require('jsonwebtoken');
const config = require('../config/env');
const logger = require('../config/logger');
const Tenant = require('../models/Tenant');
const User = require('../models/User');
const { generateEventId } = require('../utils/idGenerator');
const { onTenantMessage, publishTenantEvent } = require('./pubsub');

let wss = null;
let heartbeatInterval = null;

// Map: tenantId -> Set of WebSockets
const tenantRooms = new Map();

// Map: socketId -> WebSocket metadata
const socketRegistry = new Map();

const metrics = {
  totalConnections: 0,
  activeConnections: 0,
  eventsDispatched: 0,
  authFailures: 0,
};

/**
 * Authenticate incoming WebSocket connection request
 */
async function authenticateConnection(req) {
  const parsedUrl = url.parse(req.url, true);
  const query = parsedUrl.query || {};

  // 1. Check for JWT Bearer token in query string (?token=...) or Authorization header
  let token = query.token;
  const authHeader = req.headers['authorization'];
  if (!token && authHeader && authHeader.startsWith('Bearer ')) {
    token = authHeader.split(' ')[1];
  }

  if (token) {
    try {
      const decoded = jwt.verify(token, config.jwt.secret);
      const user = await User.findById(decoded.userId).lean();
      if (user) {
        return {
          authenticated: true,
          authType: 'JWT_AGENT',
          tenantId: user.tenantId,
          user: {
            id: user._id.toString(),
            name: user.name,
            email: user.email,
            role: user.role,
          },
        };
      }
    } catch (err) {
      logger.warn('WebSocket JWT auth failed', { error: err.message });
    }
  }

  // 2. Check for Tenant API key (?apiKey=... or x-api-key header)
  const apiKey = query.apiKey || req.headers['x-api-key'];
  if (apiKey) {
    const tenant = await Tenant.findOne({ apiKey }).lean();
    if (tenant) {
      return {
        authenticated: true,
        authType: 'API_KEY',
        tenantId: tenant.tenantId,
        user: {
          id: `apikey_${tenant.tenantId}`,
          name: tenant.name,
          role: 'TENANT_INTEGRATION',
        },
      };
    }
  }

  // 3. Check for direct Tenant ID in development/testing mode (?tenantId=... or x-tenant-id header)
  const tenantId = query.tenantId || req.headers['x-tenant-id'];
  if (tenantId) {
    const tenant = await Tenant.findOne({ tenantId }).lean();
    if (tenant) {
      return {
        authenticated: true,
        authType: 'TENANT_DIRECT',
        tenantId: tenant.tenantId,
        user: {
          id: `tenant_${tenant.tenantId}`,
          name: tenant.name,
          role: 'AGENT',
        },
      };
    }
  }

  return { authenticated: false };
}

/**
 * Initialize WebSocket Gateway attached to HTTP server
 */
function initWebSocketGateway(httpServer) {
  wss = new WebSocketServer({
    server: httpServer,
    path: '/ws',
  });

  logger.info('WebSocket Gateway initialized on path /ws');

  // Wire Redis Pub/Sub message stream directly to tenant WebSocket rooms
  onTenantMessage((tenantId, eventData) => {
    if (!tenantId) return;
    broadcastToTenant(tenantId, eventData);
  });

  wss.on('connection', async (ws, req) => {
    metrics.totalConnections += 1;

    // Authenticate the connection
    const authResult = await authenticateConnection(req);
    if (!authResult.authenticated) {
      metrics.authFailures += 1;
      logger.warn('Rejecting unauthorized WebSocket connection attempt', {
        ip: req.socket.remoteAddress,
        url: req.url,
      });

      ws.send(JSON.stringify({
        type: 'ERROR',
        error: 'Unauthorized',
        message: 'Invalid or missing authentication credentials. Provide ?token=, ?apiKey=, or ?tenantId=',
      }));
      ws.close(1008, 'Unauthorized');
      return;
    }

    const { tenantId, user, authType } = authResult;
    const socketId = `ws_${generateEventId()}`;

    // Tag socket instance
    ws.id = socketId;
    ws.tenantId = tenantId;
    ws.user = user;
    ws.authType = authType;
    ws.isAlive = true;
    ws.connectedAt = new Date().toISOString();

    // Register into tenant room
    if (!tenantRooms.has(tenantId)) {
      tenantRooms.set(tenantId, new Set());
    }
    tenantRooms.get(tenantId).add(ws);
    socketRegistry.set(socketId, ws);
    metrics.activeConnections += 1;

    logger.info('WebSocket client connected and joined tenant room', {
      socketId,
      tenantId,
      authType,
      activeInTenant: tenantRooms.get(tenantId).size,
      totalActive: metrics.activeConnections,
    });

    // Send connection established confirmation
    ws.send(JSON.stringify({
      type: 'CONNECTION_ESTABLISHED',
      socketId,
      tenantId,
      user,
      authType,
      timestamp: new Date().toISOString(),
      channels: [`tenant:${tenantId}`],
    }));

    // Listen for pong messages for heartbeat
    ws.on('pong', () => {
      ws.isAlive = true;
    });

    // Handle incoming client messages
    ws.on('message', (messageRaw) => {
      try {
        const messageStr = messageRaw.toString();
        const data = JSON.parse(messageStr);

        if (data.type === 'PING') {
          ws.send(JSON.stringify({
            type: 'PONG',
            timestamp: new Date().toISOString(),
          }));
          return;
        }

        if (data.type === 'GET_STATUS') {
          ws.send(JSON.stringify({
            type: 'STATUS_RESPONSE',
            socketId: ws.id,
            tenantId: ws.tenantId,
            connectedAt: ws.connectedAt,
            activePeersInTenant: tenantRooms.get(ws.tenantId)?.size || 1,
            serverTimestamp: new Date().toISOString(),
          }));
          return;
        }

        logger.debug('Received client WebSocket message', { socketId: ws.id, type: data.type });
      } catch (err) {
        logger.warn('Failed to parse WebSocket message from client', { error: err.message, socketId: ws.id });
      }
    });

    // Handle socket disconnect
    ws.on('close', (code, reason) => {
      cleanUpSocket(ws);
      logger.info('WebSocket client disconnected', {
        socketId,
        tenantId,
        code,
        reason: reason?.toString(),
        remainingInTenant: tenantRooms.get(tenantId)?.size || 0,
      });
    });

    // Handle socket errors
    ws.on('error', (err) => {
      logger.error('WebSocket connection error', { socketId, tenantId, error: err.message });
      cleanUpSocket(ws);
    });
  });

  // Setup periodic heartbeat to detect broken connections
  heartbeatInterval = setInterval(() => {
    if (!wss) return;

    wss.clients.forEach((ws) => {
      if (ws.isAlive === false) {
        logger.warn('Terminating unresponsive WebSocket client', { socketId: ws.id, tenantId: ws.tenantId });
        cleanUpSocket(ws);
        return ws.terminate();
      }

      ws.isAlive = false;
      ws.ping();
    });
  }, 30000);

  return wss;
}

/**
 * Clean up socket reference from tenant rooms and registry
 */
function cleanUpSocket(ws) {
  if (ws.id && socketRegistry.has(ws.id)) {
    socketRegistry.delete(ws.id);
    metrics.activeConnections = Math.max(0, metrics.activeConnections - 1);
  }

  if (ws.tenantId && tenantRooms.has(ws.tenantId)) {
    const room = tenantRooms.get(ws.tenantId);
    room.delete(ws);
    if (room.size === 0) {
      tenantRooms.delete(ws.tenantId);
    }
  }
}

/**
 * Broadcast event payload strictly to all connected clients in a tenant room
 */
function broadcastToTenant(tenantId, eventData) {
  if (!tenantId || !tenantRooms.has(tenantId)) {
    return 0;
  }

  const room = tenantRooms.get(tenantId);
  if (room.size === 0) return 0;

  const payload = typeof eventData === 'string' ? eventData : JSON.stringify(eventData);
  let sentCount = 0;

  for (const client of room) {
    if (client.readyState === WebSocket.OPEN) {
      try {
        client.send(payload);
        sentCount += 1;
      } catch (err) {
        logger.error('Error sending message to client socket', {
          socketId: client.id,
          tenantId,
          error: err.message,
        });
      }
    }
  }

  metrics.eventsDispatched += sentCount;
  return sentCount;
}

/**
 * Close WebSocket Server cleanly
 */
async function closeWebSocketGateway() {
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
    heartbeatInterval = null;
  }

  if (wss) {
    return new Promise((resolve) => {
      wss.close(() => {
        logger.info('WebSocket Gateway closed cleanly');
        tenantRooms.clear();
        socketRegistry.clear();
        metrics.activeConnections = 0;
        wss = null;
        resolve();
      });
    });
  }
}

/**
 * Get operational stats for monitoring & health checks
 */
function getWebSocketStats() {
  const tenantSummary = {};
  for (const [tenantId, sockets] of tenantRooms.entries()) {
    tenantSummary[tenantId] = sockets.size;
  }

  return {
    status: wss ? 'UP' : 'DOWN',
    activeConnections: metrics.activeConnections,
    totalConnections: metrics.totalConnections,
    eventsDispatched: metrics.eventsDispatched,
    authFailures: metrics.authFailures,
    activeTenantsCount: tenantRooms.size,
    tenantConnections: tenantSummary,
  };
}

module.exports = {
  initWebSocketGateway,
  broadcastToTenant,
  closeWebSocketGateway,
  getWebSocketStats,
  authenticateConnection,
};
