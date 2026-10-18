const path = require('path');
const http = require('http');
const { spawn } = require('child_process');
const dotenv = (() => { try { return require('dotenv'); } catch { return require('../server/node_modules/dotenv'); } })();
dotenv.config({ path: path.resolve(__dirname, '../.env') });

const WebSocket = (() => { try { return require('ws'); } catch { return require('../server/node_modules/ws'); } })();
const mongoose = (() => { try { return require('mongoose'); } catch { return require('../server/node_modules/mongoose'); } })();

const GATEWAY_URL = 'http://localhost:4000';
const WORKER_URL = 'http://localhost:4001';
const WS_GATEWAY_URL = 'ws://localhost:4000/ws';

let spawnedServer = null;
let spawnedWorker = null;

function request(urlStr, method = 'GET', headers = {}, body = null) {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const options = {
      method,
      hostname: url.hostname,
      port: url.port,
      path: url.pathname + url.search,
      headers: {
        'Content-Type': 'application/json',
        ...headers,
      },
    };

    const startTime = Date.now();
    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        let json = null;
        try {
          json = JSON.parse(data);
        } catch {
          json = data;
        }
        resolve({
          status: res.statusCode,
          headers: res.headers,
          data: json,
          durationMs: Date.now() - startTime,
        });
      });
    });

    req.on('error', reject);

    if (body) {
      req.write(typeof body === 'string' ? body : JSON.stringify(body));
    }
    req.end();
  });
}

function assert(condition, message) {
  if (!condition) {
    console.error(`❌ FAILED: ${message}`);
    throw new Error(message);
  }
  console.log(`   ✅ PASS: ${message}`);
}

async function isServiceResponding(urlStr) {
  try {
    const res = await request(urlStr);
    return res.status === 200;
  } catch {
    return false;
  }
}

async function waitForService(urlStr, name, maxWaitMs = 15000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    if (await isServiceResponding(urlStr)) return true;
    await new Promise((r) => setTimeout(r, 500));
  }
  throw new Error(`Timed out waiting for ${name} at ${urlStr}`);
}

function connectWsClient(queryParams = '') {
  return new Promise((resolve, reject) => {
    const wsUrl = `${WS_GATEWAY_URL}${queryParams}`;
    const ws = new WebSocket(wsUrl);
    const messages = [];

    const timeout = setTimeout(() => {
      reject(new Error(`WebSocket connection timed out to ${wsUrl}`));
    }, 8000);

    ws.on('open', () => {
      clearTimeout(timeout);
      resolve({ ws, messages });
    });

    ws.on('message', (data) => {
      try {
        const parsed = JSON.parse(data.toString());
        messages.push(parsed);
      } catch {
        messages.push(data.toString());
      }
    });

    ws.on('error', (err) => {
      clearTimeout(timeout);
      reject(err);
    });
  });
}

async function waitForWsMessage(messagesArray, filterFn, timeoutMs = 15000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const found = messagesArray.find(filterFn);
    if (found) return found;
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error(`Timed out waiting for expected WebSocket message after ${timeoutMs}ms`);
}

async function runPhase4Verification() {
  console.log('\n======================================================');
  console.log('⚡ FluxDesk Phase 4: Real-Time Fabric & WebSocket Gateway');
  console.log('======================================================\n');

  const openSockets = [];

  try {
    // -------------------------------------------------------------------------
    // STEP 0: Auto-start Server and Worker if not already running
    // -------------------------------------------------------------------------
    const serverRunning = await isServiceResponding(`${GATEWAY_URL}/health`);
    if (!serverRunning) {
      console.log('📦 Ingestion Gateway not detected on port 4000. Auto-starting...');
      spawnedServer = spawn('node', ['server/src/index.js'], {
        cwd: path.resolve(__dirname, '..'),
        stdio: 'inherit',
      });
      await waitForService(`${GATEWAY_URL}/health`, 'Ingestion Gateway');
      console.log('   ✅ Ingestion Gateway is online!\n');
    } else {
      console.log('   ℹ️ Using already running Ingestion Gateway on port 4000\n');
    }

    const workerRunning = await isServiceResponding(`${WORKER_URL}/health`);
    if (!workerRunning) {
      console.log('🤖 AI Worker Fleet not detected on port 4001. Auto-starting...');
      spawnedWorker = spawn('node', ['worker/src/index.js'], {
        cwd: path.resolve(__dirname, '..'),
        stdio: 'inherit',
      });
      await waitForService(`${WORKER_URL}/health`, 'AI Worker Fleet');
      console.log('   ✅ AI Worker Fleet is online!\n');
    } else {
      console.log('   ℹ️ Using already running AI Worker Fleet on port 4001\n');
    }

    // -------------------------------------------------------------------------
    // TEST 1: Real-Time Fabric Health Verification
    // -------------------------------------------------------------------------
    console.log('🔹 [Test 1] Real-Time Fabric Gateway Health (GET /health on :4000)');
    const health = await request(`${GATEWAY_URL}/health`);
    assert(health.status === 200, `Gateway health returned 200 (got ${health.status})`);
    assert(health.data.status === 'UP', `Gateway overall status is UP`);
    assert(health.data.services.websocket.status === 'UP', `WebSocket Gateway status is UP`);
    assert(health.data.services.redisPubSub.status === 'UP', `Redis Pub/Sub multiplexer is UP`);
    assert(health.data.services.kafkaProcessedConsumer.status === 'UP', `Kafka ticket.processed consumer is UP`);
    console.log(`   ℹ️ Active WebSocket Connections: ${health.data.services.websocket.activeConnections}`);
    console.log(`   ℹ️ PubSub Pattern: ${health.data.services.redisPubSub.pattern}`);

    // -------------------------------------------------------------------------
    // TEST 2: WebSocket Authentication & Access Control
    // -------------------------------------------------------------------------
    console.log('\n🔹 [Test 2] WebSocket Authentication & Security Guards');

    // 2a. Unauthenticated connection rejection
    let unauthRejected = false;
    try {
      const { ws } = await connectWsClient('');
      openSockets.push(ws);
      // Wait for socket to be closed by server with 1008
      await new Promise((resolve) => ws.on('close', resolve));
      unauthRejected = true;
    } catch {
      unauthRejected = true;
    }
    assert(unauthRejected, `Unauthenticated WebSocket connection correctly rejected`);

    // 2b. Authenticate with Stripe Alpha API Key
    const stripeClient = await connectWsClient('?apiKey=flx_live_stripe_77b31c99');
    openSockets.push(stripeClient.ws);
    const stripeWelcome = await waitForWsMessage(
      stripeClient.messages,
      (m) => m.type === 'CONNECTION_ESTABLISHED'
    );
    assert(stripeWelcome.tenantId === 'org_stripe_alpha', `Stripe client connected to org_stripe_alpha`);
    assert(Boolean(stripeWelcome.socketId), `Assigned unique socketId: ${stripeWelcome.socketId}`);

    // 2c. Authenticate with Acme Corp API Key
    const acmeClient = await connectWsClient('?apiKey=flx_live_acme_98f12a34');
    openSockets.push(acmeClient.ws);
    const acmeWelcome = await waitForWsMessage(
      acmeClient.messages,
      (m) => m.type === 'CONNECTION_ESTABLISHED'
    );
    assert(acmeWelcome.tenantId === 'org_acme_corp', `Acme client connected to org_acme_corp`);

    // 2d. Authenticate with Agent JWT token (Sarah Connor - Acme Corp)
    const loginRes = await request(`${GATEWAY_URL}/api/v1/auth/login`, 'POST', {}, {
      email: 'sarah@acme.com',
      password: 'password123',
    });
    assert(loginRes.status === 200, `Agent login succeeded`);
    const agentToken = loginRes.data.token;

    const agentClient = await connectWsClient(`?token=${agentToken}`);
    openSockets.push(agentClient.ws);
    const agentWelcome = await waitForWsMessage(
      agentClient.messages,
      (m) => m.type === 'CONNECTION_ESTABLISHED'
    );
    assert(agentWelcome.tenantId === 'org_acme_corp', `Agent connected to org_acme_corp via JWT`);
    assert(agentWelcome.user.email === 'sarah@acme.com', `Agent user context preserved`);

    // -------------------------------------------------------------------------
    // TEST 3: Strict Multi-Tenant Channel Isolation
    // -------------------------------------------------------------------------
    console.log('\n🔹 [Test 3] Strict Multi-Tenant Channel Isolation');
    // Clear message buffers
    stripeClient.messages.length = 0;
    acmeClient.messages.length = 0;

    // Use direct Redis Pub/Sub publishing from server module
    const { publishTenantEvent } = require('../server/src/realtime/pubsub');

    const isolatedEventStripe = {
      type: 'SECURITY_TEST_STRIPE',
      tenantId: 'org_stripe_alpha',
      data: { secret: 'stripe_confidential_token_999' },
    };

    await publishTenantEvent('org_stripe_alpha', isolatedEventStripe);

    // Verify Stripe client receives it
    const receivedByStripe = await waitForWsMessage(
      stripeClient.messages,
      (m) => m.type === 'SECURITY_TEST_STRIPE'
    );
    assert(
      receivedByStripe.data.secret === 'stripe_confidential_token_999',
      `Stripe client received tenant-targeted event`
    );

    // Wait 500ms to ensure no leaked events arrive at Acme
    await new Promise((r) => setTimeout(r, 500));
    const leakedToAcme = acmeClient.messages.find((m) => m.type === 'SECURITY_TEST_STRIPE');
    assert(leakedToAcme === undefined, `Zero cross-tenant leakage: Acme client received 0 Stripe events`);

    // -------------------------------------------------------------------------
    // TEST 4: Immediate Ticket Ingestion Push (TICKET_INGESTED)
    // -------------------------------------------------------------------------
    console.log('\n🔹 [Test 4] Real-Time Ticket Ingestion Push (TICKET_INGESTED)');
    stripeClient.messages.length = 0;

    const newTicketPayload = {
      title: 'Latency degradation on API checkout endpoint',
      description: 'Customer reports 3.2s latency spikes when initiating credit card charges in APAC region.',
      customer: {
        name: 'David Kim',
        email: 'david@apacpay.com',
      },
      channel: 'API',
    };

    const ingestStart = Date.now();
    const ingestRes = await request(
      `${GATEWAY_URL}/api/v1/tickets`,
      'POST',
      { 'x-api-key': 'flx_live_stripe_77b31c99' },
      newTicketPayload
    );
    assert(ingestRes.status === 202, `Ticket accepted with 202`);
    const ingestedTicketId = ingestRes.data.ticketId;
    console.log(`   📨 Ingested Ticket ID: ${ingestedTicketId}`);

    // Stripe client should immediately receive TICKET_INGESTED
    const ingestedEvent = await waitForWsMessage(
      stripeClient.messages,
      (m) => m.type === 'TICKET_INGESTED' && m.data.ticketId === ingestedTicketId,
      3000
    );
    const ingestPushDuration = Date.now() - ingestStart;
    assert(Boolean(ingestedEvent), `Received real-time TICKET_INGESTED event in ${ingestPushDuration}ms`);
    assert(ingestedEvent.data.status === 'QUEUED', `Ticket initial status is QUEUED`);
    assert(ingestedEvent.data.title === newTicketPayload.title, `Ticket title preserved`);
    assert(ingestedEvent.data.customer.email === newTicketPayload.customer.email, `Customer email preserved`);

    // -------------------------------------------------------------------------
    // TEST 5: Full Reactive Loop: Worker -> Kafka -> Redis PubSub -> WebSocket
    // -------------------------------------------------------------------------
    console.log('\n🔹 [Test 5] Full End-to-End Reactive Loop (TICKET_PROCESSED)');
    console.log(`   ⏳ Awaiting AI Worker triage and Kafka ticket.processed broadcast...`);

    const processedEvent = await waitForWsMessage(
      stripeClient.messages,
      (m) => m.type === 'TICKET_PROCESSED' && m.data.ticketId === ingestedTicketId,
      25000
    );

    assert(Boolean(processedEvent), `WebSocket received TICKET_PROCESSED event!`);
    assert(
      ['NEEDS_REVIEW', 'READY_FOR_APPROVAL', 'AUTO_RESOLVED'].includes(processedEvent.data.status),
      `Processed ticket status is valid (${processedEvent.data.status})`
    );
    assert(
      ['P0_CRITICAL', 'P1_HIGH', 'P2_MEDIUM'].includes(processedEvent.data.urgency),
      `AI Urgency classified (${processedEvent.data.urgency})`
    );
    assert(Boolean(processedEvent.data.category), `AI Category assigned: ${processedEvent.data.category}`);
    assert(
      Boolean(processedEvent.data.aiDraft && processedEvent.data.aiDraft.suggestedReply),
      `AI generated suggested reply draft present`
    );
    console.log(`   💡 AI Reasoning: "${processedEvent.data.aiDraft.reasoning}"`);
    console.log(`   🎯 AI Confidence: ${processedEvent.data.aiDraft.confidenceScore}`);

    // Verify Acme client NEVER received Stripe's processed event
    const acmeReceivedStripeProcessed = acmeClient.messages.find(
      (m) => m.type === 'TICKET_PROCESSED' && m.data?.ticketId === ingestedTicketId
    );
    assert(
      acmeReceivedStripeProcessed === undefined,
      `Cross-tenant isolation maintained during Kafka -> Redis -> WS broadcast`
    );

    // -------------------------------------------------------------------------
    // TEST 6: Real-Time Ticket Status & Review Updates (TICKET_UPDATED)
    // -------------------------------------------------------------------------
    console.log('\n🔹 [Test 6] Real-Time Ticket Review & Status Updates (TICKET_UPDATED)');
    stripeClient.messages.length = 0;

    const patchRes = await request(
      `${GATEWAY_URL}/api/v1/tickets/${ingestedTicketId}`,
      'PATCH',
      { 'x-api-key': 'flx_live_stripe_77b31c99' },
      {
        status: 'RESOLVED',
        humanReview: {
          reviewedBy: 'Senior On-Call Engineer',
          finalReply: ' APAC edge router cache flushed. Latency restored to 18ms.',
        },
      }
    );
    assert(patchRes.status === 200, `PATCH /api/v1/tickets/:id returned 200`);

    const updatedEvent = await waitForWsMessage(
      stripeClient.messages,
      (m) => m.type === 'TICKET_UPDATED' && m.data.ticketId === ingestedTicketId,
      3000
    );

    assert(Boolean(updatedEvent), `Received real-time TICKET_UPDATED event`);
    assert(updatedEvent.data.status === 'RESOLVED', `Ticket status updated to RESOLVED in real-time`);
    assert(
      updatedEvent.data.humanReview.reviewedBy === 'Senior On-Call Engineer',
      `Human review metadata updated in real-time`
    );

    // -------------------------------------------------------------------------
    // TEST 7: Bidirectional Heartbeat & Real-Time Stats API
    // -------------------------------------------------------------------------
    console.log('\n🔹 [Test 7] Bidirectional WebSocket Messages & Telemetry Stats');

    // 7a. Ping / Pong
    const pingPromise = waitForWsMessage(stripeClient.messages, (m) => m.type === 'PONG');
    stripeClient.ws.send(JSON.stringify({ type: 'PING' }));
    const pongResponse = await pingPromise;
    assert(Boolean(pongResponse), `Client ping received server PONG response`);

    // 7b. Status request
    const statusPromise = waitForWsMessage(stripeClient.messages, (m) => m.type === 'STATUS_RESPONSE');
    stripeClient.ws.send(JSON.stringify({ type: 'GET_STATUS' }));
    const statusResponse = await statusPromise;
    assert(statusResponse.tenantId === 'org_stripe_alpha', `Status response confirmed tenant`);
    assert(statusResponse.activePeersInTenant >= 1, `Reported active peers in tenant`);

    // 7c. Real-Time Telemetry REST Endpoint
    const statsRes = await request(
      `${GATEWAY_URL}/api/v1/realtime/stats`,
      'GET',
      { 'x-api-key': 'flx_live_stripe_77b31c99' }
    );
    assert(statsRes.status === 200, `GET /api/v1/realtime/stats returned 200`);
    assert(statsRes.data.realtime.websocket.activeConnections >= 3, `Recorded active WebSocket connections`);
    assert(statsRes.data.realtime.websocket.eventsDispatched >= 3, `Recorded events dispatched over WebSockets`);
    assert(statsRes.data.realtime.pubsub.messagesPublished >= 3, `Recorded Redis Pub/Sub messages published`);

    console.log('\n======================================================');
    console.log('🎉 ALL PHASE 4 TESTS PASSED SUCCESSFULLY! ✅');
    console.log('   - Redis Pub/Sub multi-tenant multiplexer verified');
    console.log('   - WebSocket Gateway with JWT & API-Key auth verified');
    console.log('   - Strict multi-tenant channel isolation verified');
    console.log('   - Live ticket ingestion broadcast verified (TICKET_INGESTED)');
    console.log('   - End-to-end reactive AI triage loop verified (TICKET_PROCESSED)');
    console.log('   - Human review & status update streaming verified (TICKET_UPDATED)');
    console.log('   - Bidirectional heartbeat & real-time telemetry verified');
    console.log('======================================================\n');
  } catch (err) {
    console.error('\n❌ Phase 4 Verification Failed:', err.message);
    process.exitCode = 1;
  } finally {
    // Close open sockets
    for (const ws of openSockets) {
      try {
        ws.close();
      } catch {}
    }

    try {
      const { disconnectRedisPubSub } = require('../server/src/realtime/pubsub');
      await disconnectRedisPubSub();
    } catch {}

    if (spawnedWorker) {
      console.log('Stopping auto-spawned worker...');
      try { spawnedWorker.kill(); } catch {}
    }
    if (spawnedServer) {
      console.log('Stopping auto-spawned server...');
      try { spawnedServer.kill(); } catch {}
    }

    // Ensure test process terminates cleanly
    setTimeout(() => {
      process.exit(process.exitCode || 0);
    }, 1000);
  }
}

runPhase4Verification();
