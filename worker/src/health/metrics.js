const metrics = {
  startTime: Date.now(),
  ticketsProcessed: 0,
  ticketsFailed: 0,
  dlqCount: 0,
  totalLatencyMs: 0,
  avgProcessingTimeMs: 0,

  recordSuccess(latencyMs) {
    this.ticketsProcessed += 1;
    this.totalLatencyMs += latencyMs;
    this.avgProcessingTimeMs = Math.round(this.totalLatencyMs / this.ticketsProcessed);
  },

  recordFailure() {
    this.ticketsFailed += 1;
  },

  recordDlq() {
    this.dlqCount += 1;
  },

  getSummary() {
    return {
      uptimeSeconds: Math.floor((Date.now() - this.startTime) / 1000),
      ticketsProcessed: this.ticketsProcessed,
      ticketsFailed: this.ticketsFailed,
      dlqCount: this.dlqCount,
      avgProcessingTimeMs: this.avgProcessingTimeMs,
    };
  },
};

module.exports = metrics;
