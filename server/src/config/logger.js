const logger = {
  info: (msg, meta = {}) => {
    console.log(`[INFO]  [${new Date().toISOString()}] ${msg}`, Object.keys(meta).length ? JSON.stringify(meta) : '');
  },
  warn: (msg, meta = {}) => {
    console.warn(`[WARN]  [${new Date().toISOString()}] ${msg}`, Object.keys(meta).length ? JSON.stringify(meta) : '');
  },
  error: (msg, meta = {}) => {
    console.error(`[ERROR] [${new Date().toISOString()}] ${msg}`, Object.keys(meta).length ? JSON.stringify(meta) : '');
  },
  debug: (msg, meta = {}) => {
    if (process.env.DEBUG || process.env.NODE_ENV === 'development') {
      console.log(`[DEBUG] [${new Date().toISOString()}] ${msg}`, Object.keys(meta).length ? JSON.stringify(meta) : '');
    }
  }
};

module.exports = logger;
