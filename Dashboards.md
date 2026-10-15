Start - docker compose up -d

---

Kafka Dashboard - http://localhost:8080/

---

Redis Dashboard - http://localhost:8081/

---

Whenever you start working or want to verify if your local databases/brokers are running before starting the app, you simply run in terminal:

```bash
node scripts/verify-infra.js
```

If everything says ✅ [READY], you know your backend has all its supporting services healthy!

---

### Phase 2: Ingestion API Service & Verification

1. **Seed Tenant Organizations & Agents:**
   ```bash
   node scripts/seed-tenants.js
   ```

2. **Start Edge Ingestion Gateway Server:**
   ```bash
   npm run start:server
   # or for live reloading:
   npm run dev:server
   ```
   * Health Check: http://localhost:4000/health
   * Ingestion API: `POST http://localhost:4000/api/v1/tickets`

3. **Run Phase 2 End-to-End Test Suite:**
   ```bash
   npm run test:phase2
   ```

---

### Phase 3: AI Worker Fleet & Intelligent Triage

1. **Start AI Worker Fleet:**
   ```bash
   npm run start:worker
   # or for development live reloading:
   npm run dev:worker
   ```
   * Worker Health Check: http://localhost:4001/health
   * Worker Metrics: http://localhost:4001/metrics

2. **Run Phase 3 End-to-End Test Suite:**
   ```bash
   npm run test:phase3
   ```
   * Automatically validates multi-tenant ticket consumption from Kafka `ticket.incoming`.
   * Verifies Google Gemini structured output triage (`urgency`, `category`, `reasoning`, `suggestedReply`, `confidenceScore`).
   * Validates tenant runbook grounding and referenced documentation.
   * Verifies MongoDB compound indexed persistence and `ticket.processed` event publishing.
   * Tests Dead-Letter Queue (DLQ) routing on `ticket.dlq` for poison-pill messages.

---

### Phase 4: Real-Time Event Fabric & WebSockets Gateway

1. **WebSocket Gateway Connection:**
   * URL: `ws://localhost:4000/ws`
   * Authentication Parameters:
     * Support Agent JWT: `ws://localhost:4000/ws?token=<jwt_token>`
     * Tenant API Key: `ws://localhost:4000/ws?apiKey=flx_live_stripe_77b31c99`
     * Headers supported: `Authorization: Bearer <token>`, `x-api-key`, `x-tenant-id`

2. **Real-Time Telemetry & Metrics:**
   * Health Check: http://localhost:4000/health (includes WebSocket, Redis Pub/Sub, Kafka consumer status)
   * Real-Time Stats API: `GET http://localhost:4000/api/v1/realtime/stats`

3. **Run Phase 4 End-to-End Test Suite:**
   ```bash
   npm run test:phase4
   ```
   * Validates Redis Pub/Sub multiplexer on pattern `tenant:*`.
   * Validates WebSocket connection authentication & token enforcement.
   * Proves zero cross-tenant event leakage and strict room isolation.
   * Tests instant ticket ingestion push (`TICKET_INGESTED`).
   * Verifies full end-to-end reactive AI triage loop (`TICKET_PROCESSED`).
   * Verifies human review & ticket status updates streaming (`TICKET_UPDATED`).
   * Verifies bidirectional ping/pong heartbeats and real-time telemetry stats.



