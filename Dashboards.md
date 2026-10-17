# FluxDesk Operations & Observability Guide

### Quick Start Infrastructure

```bash
docker compose up -d
```

Verify supporting databases and message brokers:
```bash
node scripts/verify-infra.js
```

---

### Management Dashboards

| Dashboard | URL | Purpose |
| :--- | :--- | :--- |
| **Kafka UI** | http://localhost:8080/ | Inspect topics (`ticket.incoming`, `ticket.processed`, `ticket.dlq`), partition distribution, and consumer group lag. |
| **Redis Commander** | http://localhost:8081/ | Inspect tenant sliding-window rate limit sorted sets (`rate:tenant:*`) and live pub/sub channels. |

---

### Running the Services

1. **Seed Tenant Organizations & Agents:**
   ```bash
   npm run seed
   ```

2. **Start Edge Ingestion Gateway & WebSocket Server:**
   ```bash
   npm run dev:server
   ```
   * Health Check: http://localhost:4000/health
   * Ingestion API: `POST http://localhost:4000/api/v1/tickets`
   * Real-Time Stats: `GET http://localhost:4000/api/v1/realtime/stats`
   * WebSocket Gateway: `ws://localhost:4000/ws`

3. **Start Asynchronous AI Worker Fleet:**
   ```bash
   npm run dev:worker
   ```
   * Worker Health Check: http://localhost:4001/health
   * Worker Prometheus Metrics: http://localhost:4001/metrics

---

### End-to-End Verification

To run the complete automated test suite validating the entire distributed pipeline:

```bash
npm test
```

This suite verifies:
* Redis Pub/Sub multiplexer on pattern `tenant:*`.
* WebSocket connection authentication & token enforcement.
* Strict multi-tenant room isolation with zero cross-tenant event leakage.
* Edge ticket ingestion push (`TICKET_INGESTED`).
* Asynchronous AI triage loop via Kafka and Gemini (`TICKET_PROCESSED`).
* Bidirectional ping/pong heartbeats and telemetry stats.
