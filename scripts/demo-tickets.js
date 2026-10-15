const http = require('http');

function postTicket(apiKey, ticket) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(ticket);
    const req = http.request(
      'http://localhost:4000/api/v1/tickets',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': apiKey,
        },
      },
      (res) => {
        let body = '';
        res.on('data', (c) => (body += c));
        res.on('end', () => {
          resolve({ status: res.statusCode, body: JSON.parse(body) });
        });
      }
    );
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

async function sendDemo() {
  console.log('Sending demo tickets to populate Kafka & Redis...');

  // Ticket 1: Stripe Alpha - Webhook Bug
  const t1 = await postTicket('flx_live_stripe_77b31c99', {
    title: 'Webhook signature verification failing on node cluster',
    description: 'After updating the SDK to v4.2, HMAC-SHA256 signature mismatch occurs on 500 status.',
    customer: { name: 'Alex Mercer', email: 'alex@mercer.dev' },
    channel: 'API',
  });
  console.log(`✅ Tenant [org_stripe_alpha] Ticket 1:`, t1.body.ticketId);

  // Ticket 2: Stripe Alpha - Payout Delayed
  const t2 = await postTicket('flx_live_stripe_77b31c99', {
    title: 'Payout balance transfer delayed past 48 hour window',
    description: 'Our daily automated payout batch #8912 is stuck in pending state since Monday morning.',
    customer: { name: 'Elena Rostova', email: 'elena@fintechpay.io' },
    channel: 'PORTAL',
  });
  console.log(`✅ Tenant [org_stripe_alpha] Ticket 2:`, t2.body.ticketId);

  // Ticket 3: Acme Corp - SSO SAML failure
  const t3 = await postTicket('flx_live_acme_98f12a34', {
    title: 'Enterprise SSO SAML certificate expired for European office',
    description: 'Over 40 agents cannot log into the CRM workspace due to certificate mismatch.',
    customer: { name: 'David Kim', email: 'david.kim@acme.com' },
    channel: 'WEBHOOK',
  });
  console.log(`✅ Tenant [org_acme_corp] Ticket 3:`, t3.body.ticketId);
}

sendDemo();
