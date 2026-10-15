const jwt = require('jsonwebtoken');
const config = require('../config/env');
const User = require('../models/User');
const logger = require('../config/logger');

async function agentAuth(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({
      error: 'Unauthorized',
      message: 'Missing or invalid Authorization header. Expected Bearer token.',
    });
  }

  const token = authHeader.split(' ')[1];
  try {
    const decoded = jwt.verify(token, config.jwt.secret);
    const user = await User.findById(decoded.userId).lean();

    if (!user) {
      return res.status(401).json({
        error: 'Unauthorized',
        message: 'User belonging to this token no longer exists.',
      });
    }

    req.user = user;
    req.tenantId = user.tenantId;
    next();
  } catch (err) {
    if (err.name === 'TokenExpiredError') {
      return res.status(401).json({
        error: 'Unauthorized',
        message: 'Token has expired. Please log in again.',
      });
    }
    return res.status(401).json({
      error: 'Unauthorized',
      message: 'Invalid token.',
    });
  }
}

module.exports = {
  agentAuth,
};
