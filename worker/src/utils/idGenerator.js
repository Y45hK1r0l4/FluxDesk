const crypto = require('crypto');

function generateEventId() {
  return `evt_${crypto.randomUUID()}`;
}

function generateTicketId() {
  const chars = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
  let randomPart = '';
  for (let i = 0; i < 8; i++) {
    randomPart += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  const timestampPart = Date.now().toString(36).toUpperCase();
  return `tkt_${timestampPart}${randomPart}`.slice(0, 16);
}

module.exports = {
  generateEventId,
  generateTicketId,
};
