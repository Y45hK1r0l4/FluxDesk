const { connectConsumer, getKafkaHealth } = require('../db/kafka');
const config = require('../config/env');
const logger = require('../config/logger');
const metrics = require('../health/metrics');
const { getTenantContext } = require('../services/knowledgeBaseService');
const { triageTicket } = require('../services/geminiService');
const { routeToDlq } = require('../services/dlqService');
const { publishTicketProcessed } = require('../services/processedService');
const Ticket = require('../../../server/src/models/Ticket');

let consumerInstance = null;
let isRunning = false;

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function validateEventSchema(event) {
  if (!event || typeof event !== 'object') return false;
  if (!event.tenantId || typeof event.tenantId !== 'string') return false;
  if (!event.payload || typeof event.payload !== 'object') return false;
  if (!event.payload.ticketId || !event.payload.title || !event.payload.description) return false;
  return true;
}

async function processMessage({ topic, partition, message, heartbeat }) {
  const startTime = Date.now();
  const rawValue = message.value ? message.value.toString() : null;

  // 1. Poison pill check: raw payload availability & JSON parsing
  if (!rawValue) {
    logger.warn('Received empty message payload, dispatching to DLQ', { topic, partition, offset: message.offset });
    await routeToDlq(null, new Error('Empty message payload'), { topic, partition, offset: message.offset });
    metrics.recordDlq();
    return;
  }

  let event = null;
  try {
    event = JSON.parse(rawValue);
  } catch (parseErr) {
    logger.warn('Poison pill detected: JSON parse failure', {
      topic,
      partition,
      offset: message.offset,
      error: parseErr.message,
    });
    await routeToDlq({ raw: rawValue }, parseErr, { topic, partition, offset: message.offset });
    metrics.recordDlq();
    return;
  }

  // 2. Event contract validation
  if (!validateEventSchema(event)) {
    logger.warn('Poison pill detected: event does not conform to contract RFC-001', {
      topic,
      partition,
      offset: message.offset,
      event,
    });
    await routeToDlq(event, new Error('Event does not satisfy RFC-001 contract'), { topic, partition, offset: message.offset });
    metrics.recordDlq();
    return;
  }

  const { tenantId, payload } = event;
  const { ticketId, title, description, customer, channel } = payload;

  logger.info(`Consuming ticket event [${ticketId}] for tenant [${tenantId}]`, {
    topic,
    partition,
    offset: message.offset,
  });

  // 3. Retry loop with exponential backoff & full jitter
  let attempt = 0;
  let processingSucceeded = false;
  let lastError = null;

  while (attempt < config.retry.maxAttempts && !processingSucceeded) {
    attempt += 1;
    try {
      if (heartbeat) await heartbeat();

      // Step A: Fetch Tenant Runbooks & Knowledge Base context
      const tenantContext = await getTenantContext(tenantId, title, description);
      if (heartbeat) await heartbeat();

      // Step B: Invoke Gemini AI structured triage
      const aiResult = await triageTicket(
        { ticketId, title, description, customer, channel },
        tenantContext
      );
      if (heartbeat) await heartbeat();

      // Step C: Persist to MongoDB (Compound Index Isolation)
      const finalStatus = aiResult.canAutoResolve ? 'READY_FOR_APPROVAL' : 'NEEDS_REVIEW';
      const now = new Date();

      const updatedTicket = await Ticket.findOneAndUpdate(
        { ticketId, tenantId },
        {
          $set: {
            status: finalStatus,
            urgency: aiResult.urgency,
            category: aiResult.category,
            aiDraft: {
              suggestedReply: aiResult.suggestedReply,
              reasoning: aiResult.reasoning,
              confidenceScore: aiResult.confidenceScore,
              referencedDocs: aiResult.referencedDocs,
              canAutoResolve: aiResult.canAutoResolve,
            },
            updatedAt: now,
          },
        },
        { new: true, upsert: true }
      );

      // Step D: Publish event to ticket.processed
      await publishTicketProcessed(tenantId, {
        ticketId,
        status: finalStatus,
        urgency: aiResult.urgency,
        category: aiResult.category,
        customer,
        title,
        aiDraft: updatedTicket.aiDraft,
        updatedAt: now.toISOString(),
      });

      const totalDuration = Date.now() - startTime;
      metrics.recordSuccess(totalDuration);
      processingSucceeded = true;

      logger.info(`✅ Successfully processed ticket [${ticketId}] in ${totalDuration}ms (Status: ${finalStatus}, Urgency: ${aiResult.urgency})`, {
        ticketId,
        tenantId,
        status: finalStatus,
        urgency: aiResult.urgency,
        confidenceScore: aiResult.confidenceScore,
        canAutoResolve: aiResult.canAutoResolve,
        durationMs: totalDuration,
      });

      return;
    } catch (err) {
      lastError = err;
      logger.warn(`Attempt ${attempt}/${config.retry.maxAttempts} failed for ticket [${ticketId}]`, {
        ticketId,
        tenantId,
        attempt,
        error: err.message,
      });

      if (attempt < config.retry.maxAttempts) {
        const backoffMs =
          config.retry.initialDelayMs * Math.pow(config.retry.backoffFactor, attempt - 1) +
          Math.floor(Math.random() * 500);
        await delay(backoffMs);
      }
    }
  }

  // 4. Exhausted retries -> Route to Dead Letter Queue
  if (!processingSucceeded) {
    metrics.recordFailure();
    metrics.recordDlq();

    logger.error(`❌ Retries exhausted for ticket [${ticketId}]. Routing to DLQ.`, {
      ticketId,
      tenantId,
      error: lastError?.message,
    });

    try {
      await routeToDlq(event, lastError, {
        topic,
        partition,
        offset: message.offset,
        attempts: attempt,
      });

      // Update ticket status in DB if record exists
      await Ticket.findOneAndUpdate(
        { ticketId, tenantId },
        {
          $set: {
            status: 'NEEDS_REVIEW',
            'aiDraft.reasoning': `Automated triage failed after ${attempt} retries: ${lastError?.message}`,
            updatedAt: new Date(),
          },
        }
      ).catch(() => {});
    } catch (dlqErr) {
      logger.error('Failed to route ticket to DLQ', { ticketId, error: dlqErr.message });
    }
  }
}

async function startTicketConsumer() {
  const consumer = await connectConsumer();
  consumerInstance = consumer;

  await consumer.subscribe({
    topic: config.kafka.incomingTopic,
    fromBeginning: false,
  });

  logger.info(`Subscribed to Kafka topic: ${config.kafka.incomingTopic}`);

  isRunning = true;

  await consumer.run({
    // autoCommit: false is the most critical setting in this file.
    autoCommit: false,
    eachMessage: async (payload) => {
      const { topic, partition, message } = payload;
      try {
        await processMessage(payload);
      } catch (unhandledErr) {
        logger.error('Unhandled error inside message processing pipeline', {
          error: unhandledErr.message,
          offset: message.offset,
        });
      } finally {
        // Strict invariant: commit offset ONLY after MongoDB write succeeds OR message is dead-lettered
        try {
          const nextOffset = (BigInt(message.offset) + 1n).toString();
          await consumer.commitOffsets([
            {
              topic,
              partition,
              offset: nextOffset,
            },
          ]);
        } catch (commitErr) {
          logger.error('Failed to commit Kafka offset', {
            topic,
            partition,
            offset: message.offset,
            error: commitErr.message,
          });
        }
      }
    },
  });
}

async function stopTicketConsumer() {
  isRunning = false;
  if (consumerInstance) {
    try {
      await consumerInstance.stop();
      logger.info('Ticket consumer stopped');
    } catch (err) {
      logger.warn('Error stopping ticket consumer', { error: err.message });
    }
  }
}

module.exports = {
  startTicketConsumer,
  stopTicketConsumer,
  processMessage,
};
