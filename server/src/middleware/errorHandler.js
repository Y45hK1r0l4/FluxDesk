const logger = require('../config/logger');

function errorHandler(err, req, res, next) {
  const status = err.status || err.statusCode || 500;

  if (err.retryAfter) {
    res.setHeader('Retry-After', err.retryAfter);
  }

  logger.error('Unhandled request error', {
    url: req.originalUrl,
    method: req.method,
    status,
    message: err.message,
    stack: process.env.NODE_ENV === 'development' ? err.stack : undefined,
  });

  res.status(status).json({
    error: status === 503 ? 'Service Unavailable' : status === 400 ? 'Bad Request' : 'Internal Server Error',
    message: err.message || 'An unexpected error occurred.',
    ...(err.retryAfter ? { retryAfterSeconds: err.retryAfter } : {}),
  });
}

function notFoundHandler(req, res) {
  res.status(404).json({
    error: 'Not Found',
    message: `Cannot ${req.method} ${req.originalUrl}`,
  });
}

module.exports = {
  errorHandler,
  notFoundHandler,
};
