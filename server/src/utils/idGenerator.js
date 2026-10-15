const { v4: uuidv4 } = require('uuid');

function generateEventId() {
  return `evt_${uuidv4()}`;
}

function generateTicketId() {
  const chars = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'; // Crockford Base32-like
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
