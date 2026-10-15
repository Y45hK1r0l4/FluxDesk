const { tenantAuth } = require('./tenantAuth');
const { agentAuth } = require('./agentAuth');

function anyAuth(req, res, next) {
  const authHeader = req.headers.authorization;
  if (authHeader && authHeader.startsWith('Bearer ')) {
    return agentAuth(req, res, next);
  }
  return tenantAuth(req, res, next);
}

module.exports = {
  anyAuth,
};
