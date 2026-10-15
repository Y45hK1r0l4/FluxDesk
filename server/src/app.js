const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');

const apiRoutes = require('./routes');
const { getHealth } = require('./controllers/healthController');
const { errorHandler, notFoundHandler } = require('./middleware/errorHandler');

const app = express();

// Security headers
app.use(helmet());

// Cross-origin resource sharing
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'x-api-key', 'x-tenant-id'],
}));

// Request body parser
app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true }));

// HTTP logger (skip verbose output in testing)
if (process.env.NODE_ENV !== 'test') {
  app.use(morgan(':method :url :status :res[content-length] - :response-time ms'));
}

// Top-level health check endpoint for container orchestrators & load balancers
app.get('/health', getHealth);

// API v1 routes
app.use('/api/v1', apiRoutes);

// 404 handler
app.use(notFoundHandler);

// Central error handler
app.use(errorHandler);

module.exports = app;
