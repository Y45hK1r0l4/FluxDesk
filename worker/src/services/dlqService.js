const { sendDlqEvent } = require('../db/kafka');
const { generateEventId } = require('../utils/idGenerator');
const logger = require('../config/logger');

/**
 * Route poison-pill or failed event to Dead Letter Queue (ticket.dlq)
 */
async function routeToDlq(originalEvent, error, context = {}) {
  const eventId = generateEventId();
  const nowIso = new Date().toISOString();
  const tenantId = originalEvent?.tenantId || context.tenantId || 'unknown';
  const ticketId = originalEvent?.payload?.ticketId || context.ticketId || null;

  const dlqPayload = {
    specVersion: '1.0',
    eventId,
    eventType: 'TICKET_DLQ',
    timestamp: nowIso,
    tenantId,
    payload: {
      ticketId,
      originalEvent,
      error: {
        message: error ? error.message : 'Unknown error',
        stack: error ? error.stack : null,
        code: error?.code || 'UNPROCESSABLE_ENTITY',
      },
      context,
      deadLetteredAt: nowIso,
    },
  };

  try {
    await sendDlqEvent(tenantId, dlqPayload);
    logger.error('CRITICAL: Event routed to DLQ (ticket.dlq)', {
      dlqEventId: eventId,
      ticketId,
      tenantId,
      errorMessage: error?.message,
    });
    return dlqPayload;
  } catch (dlqErr) {
    logger.error('FATAL: Failed to write to DLQ Kafka topic', {
      error: dlqErr.message,
      originalTicketId: ticketId,
    });
    throw dlqErr;
  }
}

module.exports = {
  routeToDlq,
};
