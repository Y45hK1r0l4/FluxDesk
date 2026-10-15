const http = require('http');

const tenants = [
  { apiKey: 'flx_live_stripe_77b31c99', name: 'Stripe Alpha' },
  { apiKey: 'flx_live_acme_98f12a34', name: 'Acme Corp' },
  { apiKey: 'flx_live_free_11a22b33', name: 'Free Tier Sandbox' },
];

let counter = 1;

function sendTicket(tenant) {
  const payload = JSON.stringify({
    title: `Live Ticket #${counter} from ${tenant.name}`,
    description: `Continuous background simulation event #${counter} for live dashboard inspection.`,
    customer: {
      name: `User ${counter}`,
      email: `user${counter}@example.com`,
    },
    channel: 'API',
  });

  const req = http.request(
    'http://localhost:4000/api/v1/tickets',
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': tenant.apiKey,
      },
    },
    (res) => {
      // Consume response
      res.resume();
    }
  );

  req.on('error', () => {});
  req.write(payload);
  req.end();
  counter++;
}

console.log('⚡ Live traffic generator started. Sending tickets every 4 seconds...');

// Send immediately
tenants.forEach(sendTicket);

// Keep sending every 4 seconds so Redis keys stay alive and Kafka UI gets fresh messages
setInterval(() => {
  const tenant = tenants[counter % tenants.length];
  sendTicket(tenant);
}, 4000);
