const express = require('express');
const router = express.Router();

const { tenantAuth } = require('../middleware/tenantAuth');
const { anyAuth } = require('../middleware/anyAuth');
const { createRateLimiter } = require('../middleware/rateLimiter');
const { validateBody, createTicketSchema } = require('../middleware/validate');
const ticketController = require('../controllers/ticketController');

// Ticket Ingestion (POST /api/v1/tickets)
// Strict pipeline: Tenant Auth -> Redis Sliding-Window Rate Limiter -> Zod Validation -> Kafka Publish
router.post(
  '/',
  tenantAuth,
  createRateLimiter({ endpoint: 'tickets' }),
  validateBody(createTicketSchema),
  ticketController.submitTicket
);

// List tickets with pagination & filters
router.get('/', anyAuth, ticketController.listTickets);

// Get single ticket details
router.get('/:id', anyAuth, ticketController.getTicketById);

// Update ticket status or human review
router.patch('/:id', anyAuth, ticketController.updateTicket);

module.exports = router;
