const { generateTicketId, generateEventId } = require('../utils/idGenerator');
const { sendTicketIncomingEvent } = require('../db/kafka');
const { publishTenantEvent } = require('../realtime/pubsub');
const Ticket = require('../models/Ticket');
const logger = require('../config/logger');

/**
 * High-Speed Ticket Ingestion Handler
 * Sub-25ms response time target
 * Returns HTTP 202 Accepted
 */
async function submitTicket(req, res, next) {
  const startTime = Date.now();
  const tenantId = req.tenantId;
  const { title, description, customer, channel, metadata } = req.validatedBody;

  const ticketId = generateTicketId();
  const eventId = generateEventId();
  const nowIso = new Date().toISOString();

  const ipAddress = req.ip || req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  const userAgent = req.headers['user-agent'] || 'unknown';

  const clientMetadata = {
    ipAddress,
    userAgent,
    ...(metadata || {}),
  };

  // Event Contract strictly conforms to RFC-001 Section 5.1
  const eventPayload = {
    specVersion: '1.0',
    eventId,
    eventType: 'TICKET_SUBMITTED',
    timestamp: nowIso,
    tenantId,
    payload: {
      ticketId,
      customer: {
        name: customer.name || 'Anonymous Customer',
        email: customer.email,
      },
      title,
      description,
      channel: channel || 'API',
      metadata: clientMetadata,
    },
  };

  try {
    // 1. Initialise ticket document in MongoDB with QUEUED status
    const ticketDoc = new Ticket({
      ticketId,
      tenantId,
      customer: {
        name: customer.name || 'Anonymous Customer',
        email: customer.email,
      },
      title,
      description,
      channel: channel || 'API',
      status: 'QUEUED',
      urgency: 'UNASSIGNED',
      category: 'General',
      metadata: clientMetadata,
    });

    // Execute Kafka publish and MongoDB persistence concurrently
    const [kafkaResult] = await Promise.all([
      sendTicketIncomingEvent(tenantId, eventPayload),
      ticketDoc.save(),
    ]);

    // Real-Time Fabric: Broadcast TICKET_INGESTED event so agent dashboards populate immediately
    publishTenantEvent(tenantId, {
      type: 'TICKET_INGESTED',
      specVersion: '1.0',
      eventId,
      timestamp: nowIso,
      tenantId,
      data: {
        ticketId,
        status: 'QUEUED',
        urgency: 'UNASSIGNED',
        category: 'General',
        customer: ticketDoc.customer,
        title,
        description,
        channel: channel || 'API',
        createdAt: nowIso,
      },
    }).catch((err) => logger.warn('Failed to publish real-time TICKET_INGESTED event', { error: err.message }));

    const processingMs = Date.now() - startTime;
    logger.info('Ticket accepted and published to Kafka', {
      ticketId,
      tenantId,
      latencyMs: processingMs,
      partition: kafkaResult && kafkaResult[0] ? kafkaResult[0].partition : undefined,
    });

    res.setHeader('X-Ingestion-Latency-Ms', processingMs);
    return res.status(202).json({
      success: true,
      ticketId,
      status: 'QUEUED',
      tenantId,
      message: 'Ticket accepted and queued for processing',
      latencyMs: processingMs,
    });
  } catch (err) {
    next(err);
  }
}

/**
 * List tenant-isolated tickets
 */
async function listTickets(req, res, next) {
  try {
    const tenantId = req.tenantId;
    const page = Math.max(1, parseInt(req.query.page || '1', 10));
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit || '20', 10)));
    const skip = (page - 1) * limit;

    const filter = { tenantId };
    if (req.query.status) {
      filter.status = req.query.status;
    }
    if (req.query.urgency) {
      filter.urgency = req.query.urgency;
    }

    const [tickets, total] = await Promise.all([
      Ticket.find(filter)
        .sort({ createdAt: -1 })
        .skip(skip)
        .limit(limit)
        .lean(),
      Ticket.countDocuments(filter),
    ]);

    return res.json({
      success: true,
      tenantId,
      tickets,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit),
      },
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Get single ticket by ticketId with strict tenant isolation
 */
async function getTicketById(req, res, next) {
  try {
    const tenantId = req.tenantId;
    const { id } = req.params;

    const ticket = await Ticket.findOne({ ticketId: id, tenantId }).lean();
    if (!ticket) {
      return res.status(404).json({
        error: 'Not Found',
        message: `Ticket with ID ${id} not found in this tenant workspace.`,
      });
    }

    return res.json({
      success: true,
      ticket,
    });
  } catch (err) {
    next(err);
  }
}

/**
 * Update ticket status, review or assignment with real-time broadcast
 */
async function updateTicket(req, res, next) {
  try {
    const tenantId = req.tenantId;
    const { id } = req.params;
    const { status, urgency, category, humanReview } = req.body;

    const ticket = await Ticket.findOne({ ticketId: id, tenantId });
    if (!ticket) {
      return res.status(404).json({
        error: 'Not Found',
        message: `Ticket with ID ${id} not found in this tenant workspace.`,
      });
    }

    if (status) ticket.status = status;
    if (urgency) ticket.urgency = urgency;
    if (category) ticket.category = category;
    if (humanReview) {
      ticket.humanReview = {
        ...ticket.humanReview?.toObject(),
        ...humanReview,
        reviewedAt: humanReview.reviewedAt || new Date(),
        reviewedBy: humanReview.reviewedBy || req.user?.name || req.user?.email || 'Support Agent',
      };
    }

    await ticket.save();
    const updatedData = ticket.toObject();

    // Broadcast TICKET_UPDATED via Redis Pub/Sub to all connected agents for this tenant
    publishTenantEvent(tenantId, {
      type: 'TICKET_UPDATED',
      timestamp: new Date().toISOString(),
      tenantId,
      data: updatedData,
    }).catch((err) => logger.warn('Failed to publish real-time TICKET_UPDATED event', { error: err.message }));

    return res.json({
      success: true,
      ticket: updatedData,
    });
  } catch (err) {
    next(err);
  }
}

module.exports = {
  submitTicket,
  listTickets,
  getTicketById,
  updateTicket,
};
