const config = require('./env');

const LOG_LEVELS = {
  DEBUG: 0,
  INFO: 1,
  WARN: 2,
  ERROR: 3,
};

const currentLevel = LOG_LEVELS[process.env.LOG_LEVEL?.toUpperCase()] || LOG_LEVELS.INFO;

function formatMessage(level, message, meta = {}) {
  const logEntry = {
    timestamp: new Date().toISOString(),
    service: 'fluxdesk-worker',
    workerId: config.workerId,
    level,
    message,
    ...meta,
  };
  return JSON.stringify(logEntry);
}

const logger = {
  debug(message, meta) {
    if (currentLevel <= LOG_LEVELS.DEBUG) {
      console.log(formatMessage('DEBUG', message, meta));
    }
  },
  info(message, meta) {
    if (currentLevel <= LOG_LEVELS.INFO) {
      console.log(formatMessage('INFO', message, meta));
    }
  },
  warn(message, meta) {
    if (currentLevel <= LOG_LEVELS.WARN) {
      console.warn(formatMessage('WARN', message, meta));
    }
  },
  error(message, meta) {
    if (currentLevel <= LOG_LEVELS.ERROR) {
      console.error(formatMessage('ERROR', message, meta));
    }
  },
};

module.exports = logger;
