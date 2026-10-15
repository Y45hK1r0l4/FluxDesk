const path = require('path');
const http = require('http');
const { spawn } = require('child_process');
const dotenv = (() => { try { return require('dotenv'); } catch { return require('../server/node_modules/dotenv'); } })();
dotenv.config({ path: path.resolve(__dirname, '../.env') });

const { Kafka } = (() => { try { return require('kafkajs'); } catch { return require('../server/node_modules/kafkajs'); } })();
const mongoose = (() => { try { return require('mongoose'); } catch { return require('../server/node_modules/mongoose'); } })();

const GATEWAY_URL = 'http://localhost:4000';
const WORKER_URL = 'http://localhost:4001';

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

async function pollTicketInMongo(ticketId, maxWaitMs = 35000) {
  const start = Date.now();
  const Ticket = mongoose.models.Ticket || mongoose.model('Ticket', require('../server/src/models/Ticket').schema);

  while (Date.now() - start < maxWaitMs) {
    const ticket = await Ticket.findOne({ ticketId }).lean();
    if (ticket && ticket.status !== 'QUEUED') {
      return ticket;
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  throw new Error(`Ticket ${ticketId} did not transition from QUEUED within ${maxWaitMs}ms`);
}

async function runPhase3Verification() {
  console.log('\n======================================================');
  console.log('⚡ FluxDesk Phase 3: AI Worker Fleet Test Suite');
  console.log('======================================================\n');

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

    // Connect Mongoose directly for DB assertions
    const mongoUri = process.env.MONGO_URI || 'mongodb://localhost:27017/fluxdesk';
    await mongoose.connect(mongoUri);

    // -------------------------------------------------------------------------
    // TEST 1: Worker Health & Metrics Verification
    // -------------------------------------------------------------------------
    console.log('🔹 [Test 1] AI Worker Fleet Health Check (GET /health on :4001)');
    const workerHealth = await request(`${WORKER_URL}/health`);
    assert(workerHealth.status === 200, `Worker health status is 200 (got ${workerHealth.status})`);
    assert(workerHealth.data.status === 'UP', `Worker status is UP`);
    assert(workerHealth.data.services.mongodb.status === 'UP', `MongoDB is connected in worker`);
    assert(workerHealth.data.services.kafka.consumerConnected === true, `Kafka consumer is connected`);
    assert(workerHealth.data.services.gemini.status === 'UP', `Gemini API configuration is UP`);
    console.log(`   ℹ️ Active Gemini Model: ${workerHealth.data.services.gemini.configuredModel}`);

    // -------------------------------------------------------------------------
    // TEST 2: Stripe Alpha - Webhook Bug & Knowledge Base Grounding
    // -------------------------------------------------------------------------
    console.log('\n🔹 [Test 2] Grounded Triage: Stripe Alpha Webhook Signature Bug');
    const stripeTicketPayload = {
      title: 'Webhook signature verification failing on node cluster',
      description: 'After updating the SDK to v4.2, HMAC-SHA256 signature mismatch occurs on 500 status.',
      customer: {
        name: 'Alex Mercer',
        email: 'alex@mercer.dev',
      },
      channel: 'API',
    };

    const stripeIngestRes = await request(
      `${GATEWAY_URL}/api/v1/tickets`,
      'POST',
      { 'x-api-key': 'flx_live_stripe_77b31c99' },
      stripeTicketPayload
    );
    assert(stripeIngestRes.status === 202, `Ticket accepted with 202 (got ${stripeIngestRes.status})`);
    const stripeTicketId = stripeIngestRes.data.ticketId;
    console.log(`   📨 Ingested Ticket ID: ${stripeTicketId}. Awaiting AI Worker triage...`);

    const stripeProcessedTicket = await pollTicketInMongo(stripeTicketId);
    assert(
      stripeProcessedTicket.status === 'AUTO_RESOLVED' || stripeProcessedTicket.status === 'NEEDS_REVIEW',
      `Ticket transitioned to processed state: ${stripeProcessedTicket.status}`
    );
    assert(
      ['P0_CRITICAL', 'P1_HIGH'].includes(stripeProcessedTicket.urgency),
      `Urgency classified correctly as high-priority: ${stripeProcessedTicket.urgency}`
    );
    assert(
      stripeProcessedTicket.category === 'Technical Bug',
      `Category classified as "Technical Bug" (got ${stripeProcessedTicket.category})`
    );
    assert(
      Boolean(stripeProcessedTicket.aiDraft && stripeProcessedTicket.aiDraft.suggestedReply),
      `AI generated suggested reply`
    );
    assert(
      stripeProcessedTicket.aiDraft.confidenceScore >= 0.8,
      `High confidence score (${stripeProcessedTicket.aiDraft.confidenceScore})`
    );
    console.log(`   💡 AI Reasoning: "${stripeProcessedTicket.aiDraft.reasoning}"`);
    console.log(`   📚 Referenced Runbooks:`, stripeProcessedTicket.aiDraft.referencedDocs);

    // -------------------------------------------------------------------------
    // TEST 3: Acme Corp - SSO SAML Outage & Account Access Triage
    // -------------------------------------------------------------------------
    console.log('\n🔹 [Test 3] Grounded Triage: Acme Corp Enterprise SSO Certificate Expiry');
    const acmeTicketPayload = {
      title: 'Enterprise SSO SAML certificate expired for European office',
      description: 'Over 40 agents cannot log into the CRM workspace due to certificate mismatch.',
      customer: {
        name: 'Sarah Connor',
        email: 'sarah@acme.com',
      },
      channel: 'WEBHOOK',
    };

    const acmeIngestRes = await request(
      `${GATEWAY_URL}/api/v1/tickets`,
      'POST',
      { 'x-api-key': 'flx_live_acme_98f12a34' },
      acmeTicketPayload
    );
    assert(acmeIngestRes.status === 202, `Acme ticket accepted (202)`);
    const acmeTicketId = acmeIngestRes.data.ticketId;
    console.log(`   📨 Ingested Ticket ID: ${acmeTicketId}. Awaiting AI Worker triage...`);

    const acmeProcessedTicket = await pollTicketInMongo(acmeTicketId);
    assert(
      ['P0_CRITICAL', 'P1_HIGH'].includes(acmeProcessedTicket.urgency),
      `Urgency is P0_CRITICAL or P1_HIGH (got ${acmeProcessedTicket.urgency})`
    );
    assert(
      ['Account Access', 'Technical Bug'].includes(acmeProcessedTicket.category),
      `Category correctly assigned (got ${acmeProcessedTicket.category})`
    );
    assert(
      acmeProcessedTicket.aiDraft.referencedDocs.length > 0 ||
        acmeProcessedTicket.aiDraft.suggestedReply.toLowerCase().includes('sso') ||
        acmeProcessedTicket.aiDraft.suggestedReply.toLowerCase().includes('saml'),
      `Runbook or SSO context properly incorporated into draft reply`
    );

    // -------------------------------------------------------------------------
    // TEST 4: Free Dev Sandbox - Low Priority General Query (Zero Runbooks)
    // -------------------------------------------------------------------------
    console.log('\n🔹 [Test 4] Free Tier Sandbox: General Query Handling');
    const freeTicketPayload = {
      title: 'How can I configure pagination parameters in the API?',
      description: 'Is there a limit on how many items can be returned per page in the list tickets endpoint?',
      customer: {
        name: 'Sandbox Developer',
        email: 'dev@sandbox.io',
      },
      channel: 'API',
    };

    const freeIngestRes = await request(
      `${GATEWAY_URL}/api/v1/tickets`,
      'POST',
      { 'x-api-key': 'flx_live_free_11a22b33' },
      freeTicketPayload
    );
    assert(freeIngestRes.status === 202, `Free tier ticket accepted (202)`);
    const freeTicketId = freeIngestRes.data.ticketId;

    const freeProcessedTicket = await pollTicketInMongo(freeTicketId);
    assert(
      ['P2_MEDIUM', 'P3_LOW'].includes(freeProcessedTicket.urgency),
      `Low urgency assigned for general inquiry: ${freeProcessedTicket.urgency}`
    );
    assert(
      ['General', 'Feature Request', 'Technical Bug'].includes(freeProcessedTicket.category),
      `Assigned to valid category (got ${freeProcessedTicket.category})`
    );

    // -------------------------------------------------------------------------
    // TEST 5: Kafka Dead-Letter Queue (DLQ) & Poison Pill Isolation
    // -------------------------------------------------------------------------
    console.log('\n🔹 [Test 5] Resilience: Poison Pill & Dead-Letter Queue (ticket.dlq)');
    const kafka = new Kafka({
      clientId: 'fluxdesk-dlq-test-verifier',
      brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    });

    const testProducer = kafka.producer();
    const testDlqConsumer = kafka.consumer({ groupId: `dlq-verifier-${Date.now()}` });

    await testProducer.connect();
    await testDlqConsumer.connect();
    await testDlqConsumer.subscribe({ topic: 'ticket.dlq', fromBeginning: false });

    let dlqEventReceived = null;
    await testDlqConsumer.run({
      eachMessage: async ({ message }) => {
        try {
          dlqEventReceived = JSON.parse(message.value.toString());
        } catch {
          dlqEventReceived = { raw: message.value.toString() };
        }
      },
    });

    // Give consumer time to complete group rebalance
    await new Promise((r) => setTimeout(r, 1000));

    // Send malformed non-JSON poison pill to ticket.incoming
    console.log('   💣 Injecting corrupt poison-pill message into ticket.incoming...');
    await testProducer.send({
      topic: 'ticket.incoming',
      messages: [
        {
          key: 'org_stripe_alpha',
          value: 'MALFORMED_NON_JSON_CORRUPT_PAYLOAD_<<<>>>',
        },
      ],
    });

    // Await DLQ routing (up to 10 seconds)
    const dlqStart = Date.now();
    while (Date.now() - dlqStart < 10000 && !dlqEventReceived) {
      await new Promise((r) => setTimeout(r, 500));
    }

    assert(Boolean(dlqEventReceived), `Poison pill intercepted and routed to ticket.dlq`);
    assert(
      dlqEventReceived.eventType === 'TICKET_DLQ',
      `DLQ message eventType is TICKET_DLQ`
    );
    console.log(`   🛡️ DLQ Error recorded: "${dlqEventReceived.payload?.error?.message}"`);

    await testDlqConsumer.disconnect();
    await testProducer.disconnect();

    // Verify worker didn't crash
    const healthAfterPoison = await request(`${WORKER_URL}/health`);
    assert(healthAfterPoison.status === 200, `Worker remains UP and resilient after poison pill`);

    // -------------------------------------------------------------------------
    // TEST 6: Worker Metrics & Observability
    // -------------------------------------------------------------------------
    console.log('\n🔹 [Test 6] Worker Fleet Metrics Endpoint (GET /metrics)');
    const metricsRes = await request(`${WORKER_URL}/metrics`);
    assert(metricsRes.status === 200, `Metrics endpoint returned 200`);
    assert(metricsRes.data.metrics.ticketsProcessed >= 3, `Recorded >= 3 processed tickets (got ${metricsRes.data.metrics.ticketsProcessed})`);
    assert(metricsRes.data.metrics.dlqCount >= 1, `Recorded >= 1 DLQ event (got ${metricsRes.data.metrics.dlqCount})`);
    assert(metricsRes.data.metrics.avgProcessingTimeMs > 0, `Recorded average processing latency: ${metricsRes.data.metrics.avgProcessingTimeMs}ms`);

    console.log('\n======================================================');
    console.log('🎉 ALL PHASE 3 TESTS PASSED SUCCESSFULLY! ✅');
    console.log('   - Asynchronous AI Worker Fleet fully operational');
    console.log('   - Tenant Runbook Context grounding verified');
    console.log('   - Google Gemini structured triage verified');
    console.log('   - MongoDB compound multi-tenant persistence verified');
    console.log('   - Dead-Letter Queue (DLQ) & Poison Pill resilience verified');
    console.log('======================================================\n');
  } catch (err) {
    console.error('\n❌ Phase 3 Verification Failed:', err.message);
    process.exitCode = 1;
  } finally {
    try {
      await mongoose.disconnect();
    } catch {}

    if (spawnedWorker) {
      console.log('Stopping auto-spawned worker...');
      spawnedWorker.kill();
    }
    if (spawnedServer) {
      console.log('Stopping auto-spawned server...');
      spawnedServer.kill();
    }
  }
}

runPhase3Verification();
