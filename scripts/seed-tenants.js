const path = require('path');
const dotenv = (() => { try { return require('dotenv'); } catch { return require('../server/node_modules/dotenv'); } })();
dotenv.config({ path: path.resolve(__dirname, '../.env') });

const bcrypt = (() => { try { return require('bcryptjs'); } catch { return require('../server/node_modules/bcryptjs'); } })();
const { connectMongo, disconnectMongo } = require('../server/src/db/mongo');
const Tenant = require('../server/src/models/Tenant');
const User = require('../server/src/models/User');

const SEED_TENANTS = [
  {
    tenantId: 'org_acme_corp',
    name: 'Acme Corporation',
    apiKey: 'flx_live_acme_98f12a34',
    plan: 'ENTERPRISE',
    rateLimitPerMinute: 60,
    knowledgeBase: [
      {
        title: 'Billing and Refund Policy',
        content: 'Enterprise refunds must be processed within 30 days of billing cycle if SLA uptime is below 99.9%. Credit notes can be issued immediately.',
        tags: ['billing', 'refund', 'sla'],
      },
      {
        title: 'Password & SSO Reset Guide',
        content: 'SSO login errors are typically caused by SAML certificate expiry or clock drift exceeding 5 minutes. Direct users to the admin dashboard.',
        tags: ['auth', 'sso', 'security'],
      },
    ],
  },
  {
    tenantId: 'org_stripe_alpha',
    name: 'Stripe Alpha Payments',
    apiKey: 'flx_live_stripe_77b31c99',
    plan: 'PRO',
    rateLimitPerMinute: 30,
    knowledgeBase: [
      {
        title: 'Webhook Signature Verification',
        content: 'In SDK v4.2+, raw request buffers must be preserved before JSON parsers mutate headers or body. Verify signatures using HMAC-SHA256.',
        tags: ['webhook', 'api', 'signatures'],
      },
    ],
  },
  {
    tenantId: 'org_free_tier',
    name: 'Free Dev Sandbox',
    apiKey: 'flx_live_free_11a22b33',
    plan: 'FREE',
    rateLimitPerMinute: 5,
    knowledgeBase: [],
  },
];

const SEED_USERS = [
  {
    tenantId: 'org_acme_corp',
    name: 'Sarah Connor (Support Lead)',
    email: 'sarah@acme.com',
    password: 'password123',
    role: 'ADMIN',
  },
  {
    tenantId: 'org_stripe_alpha',
    name: 'Alex Mercer (Payment Specialist)',
    email: 'alex@stripe.com',
    password: 'password123',
    role: 'AGENT',
  },
  {
    tenantId: 'org_free_tier',
    name: 'Dev Tester',
    email: 'tester@sandbox.com',
    password: 'password123',
    role: 'AGENT',
  },
];

async function seed() {
  console.log('\n=============================================');
  console.log('⚡ FluxDesk Database Seeder (Tenants & Users)');
  console.log('=============================================\n');

  try {
    await connectMongo();

    // Upsert tenants
    for (const t of SEED_TENANTS) {
      await Tenant.findOneAndUpdate(
        { tenantId: t.tenantId },
        { $set: t },
        { upsert: true, new: true }
      );
      console.log(`✅ Seeded Tenant: [${t.tenantId}] "${t.name}" (Limit: ${t.rateLimitPerMinute}/min, API Key: ${t.apiKey})`);
    }

    // Upsert users
    for (const u of SEED_USERS) {
      const passwordHash = await bcrypt.hash(u.password, 10);
      await User.findOneAndUpdate(
        { email: u.email },
        {
          $set: {
            tenantId: u.tenantId,
            name: u.name,
            email: u.email,
            passwordHash,
            role: u.role,
          },
        },
        { upsert: true, new: true }
      );
      console.log(`✅ Seeded User:   [${u.role}] ${u.email} -> Tenant: ${u.tenantId}`);
    }

    console.log('\n🎉 Seeding complete! Database is primed for testing.');
  } catch (err) {
    console.error('❌ Seeding failed:', err);
    process.exit(1);
  } finally {
    await disconnectMongo();
    process.exit(0);
  }
}

seed();
