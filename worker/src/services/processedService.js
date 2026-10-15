const { sendProcessedEvent } = require('../db/kafka');
const { generateEventId } = require('../utils/idGenerator');
const logger = require('../config/logger');

/**
 * Publish enriched ticket event to ticket.processed
 */
async function publishTicketProcessed(tenantId, processedPayload) {
  const eventId = generateEventId();
  const nowIso = new Date().toISOString();

  const event = {
    specVersion: '1.0',
    eventId,
    eventType: 'TICKET_PROCESSED',
    timestamp: nowIso,
    tenantId,
    payload: {
      ticketId: processedPayload.ticketId,
      status: processedPayload.status,
      urgency: processedPayload.urgency,
      category: processedPayload.category,
      customer: processedPayload.customer,
      title: processedPayload.title,
      aiDraft: processedPayload.aiDraft,
      updatedAt: processedPayload.updatedAt || nowIso,
    },
  };

  await sendProcessedEvent(tenantId, event);
  logger.info('Published TICKET_PROCESSED event to Kafka', {
    eventId,
    ticketId: processedPayload.ticketId,
    tenantId,
    status: processedPayload.status,
  });

  return event;
}

module.exports = {
  publishTicketProcessed,
};
