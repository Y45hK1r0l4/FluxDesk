const config = require('../config/env');
const logger = require('../config/logger');

const GEMINI_TRIAGE_SCHEMA = {
  type: 'OBJECT',
  properties: {
    urgency: {
      type: 'STRING',
      enum: ['P0_CRITICAL', 'P1_HIGH', 'P2_MEDIUM', 'P3_LOW'],
    },
    category: {
      type: 'STRING',
      enum: ['Billing', 'Technical Bug', 'Account Access', 'Feature Request', 'General'],
    },
    reasoning: {
      type: 'STRING',
    },
    suggestedReply: {
      type: 'STRING',
    },
    confidenceScore: {
      type: 'NUMBER',
    },
    referencedDocs: {
      type: 'ARRAY',
      items: {
        type: 'STRING',
      },
    },
    canAutoResolve: {
      type: 'BOOLEAN',
    },
  },
  required: [
    'urgency',
    'category',
    'reasoning',
    'suggestedReply',
    'confidenceScore',
    'referencedDocs',
    'canAutoResolve',
  ],
};

function buildPrompt({ tenantName, tenantId, customer, title, description, channel, formattedContext }) {
  return `You are FluxDesk AI Support Copilot, an enterprise-grade customer support triage and intelligence engine.
Your role is to analyze incoming customer support tickets, classify urgency and category, ground answers in tenant runbooks, and draft professional enterprise replies.

### Tenant Organization
Name: ${tenantName || tenantId}
Tenant ID: ${tenantId}

### Customer Profile
Name: ${customer?.name || 'Customer'}
Email: ${customer?.email || 'unknown'}
Channel: ${channel || 'API'}

### Ticket Details
Title: ${title}
Description:
${description}

### Organization Knowledge Base & Runbooks
${formattedContext || 'No runbooks available.'}

### Guidelines:
1. Urgency Classification:
   - P0_CRITICAL: Complete system outage, data breach, security vulnerability, entire team locked out, severe payment processing outage.
   - P1_HIGH: Production bugs affecting business operations, webhook/API integration failures, active payment/payout delays, SAML SSO failures for offices.
   - P2_MEDIUM: Standard bug reports with workarounds, billing invoice questions, minor configuration errors.
   - P3_LOW: General product questions, feedback, documentation requests, feature requests.
2. Grounding: If tenant runbooks provide guidance on the reported issue, apply their technical instructions in your suggestedReply and include the exact runbook title(s) in referencedDocs.
3. Approval Criteria: Set canAutoResolve=true ONLY if confidenceScore >= 0.95, the solution is complete and standard, and no manual human investigation or dangerous permissions are required. Otherwise false.
4. Professional Tone: Maintain an empathetic, direct, enterprise-grade engineering or support tone.`;
}

async function callGeminiApi(modelName, prompt) {
  const apiKey = config.gemini.apiKey;
  if (!apiKey) {
    throw new Error('GEMINI_API_KEY is not configured in environment');
  }

  const url = `https://generativelanguage.googleapis.com/v1beta/models/${modelName}:generateContent?key=${apiKey}`;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), config.gemini.timeoutMs);

  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      signal: controller.signal,
      body: JSON.stringify({
        contents: [
          {
            parts: [
              {
                text: prompt,
              },
            ],
          },
        ],
        generationConfig: {
          responseMimeType: 'application/json',
          responseSchema: GEMINI_TRIAGE_SCHEMA,
          temperature: 0.2,
        },
      }),
    });

    const data = await response.json();

    if (!response.ok) {
      const errorMsg = data?.error?.message || `HTTP ${response.status} from Gemini API`;
      const err = new Error(errorMsg);
      err.status = response.status;
      err.code = data?.error?.code || response.status;
      throw err;
    }

    const candidate = data.candidates && data.candidates[0];
    if (!candidate || !candidate.content || !candidate.content.parts || !candidate.content.parts[0]) {
      throw new Error('Invalid Gemini API response: no candidate text returned');
    }

    const rawText = candidate.content.parts[0].text;
    const parsed = JSON.parse(rawText);
    return parsed;
  } finally {
    clearTimeout(timeoutId);
  }
}

/**
 * Triage ticket with retries, model fallbacks and schema normalization
 */
async function triageTicket(ticketData, tenantContext) {
  const prompt = buildPrompt({
    tenantName: tenantContext.tenantName,
    tenantId: tenantContext.tenantId,
    customer: ticketData.customer,
    title: ticketData.title,
    description: ticketData.description,
    channel: ticketData.channel,
    formattedContext: tenantContext.formattedContext,
  });

  const modelsToTry = [
    config.gemini.model,
    ...(config.gemini.fallbackModels || []).filter((m) => m !== config.gemini.model),
  ];

  let lastError = null;

  for (const model of modelsToTry) {
    try {
      logger.debug(`Invoking Gemini triage model: ${model}`, {
        ticketId: ticketData.ticketId,
        tenantId: tenantContext.tenantId,
      });

      const startTime = Date.now();
      const rawResult = await callGeminiApi(model, prompt);
      const latencyMs = Date.now() - startTime;

      // Normalize & Validate Output
      const validUrgencies = ['P0_CRITICAL', 'P1_HIGH', 'P2_MEDIUM', 'P3_LOW'];
      const validCategories = ['Billing', 'Technical Bug', 'Account Access', 'Feature Request', 'General'];

      const urgency = validUrgencies.includes(rawResult.urgency) ? rawResult.urgency : 'P2_MEDIUM';
      const category = validCategories.includes(rawResult.category) ? rawResult.category : 'General';
      const confidenceScore = Math.min(1.0, Math.max(0.0, typeof rawResult.confidenceScore === 'number' ? rawResult.confidenceScore : 0.85));
      const referencedDocs = Array.isArray(rawResult.referencedDocs) ? rawResult.referencedDocs : [];
      const canAutoResolve = Boolean(rawResult.canAutoResolve) && confidenceScore >= 0.95;

      const result = {
        urgency,
        category,
        reasoning: rawResult.reasoning || 'Automated triage based on ticket description and runbook match.',
        suggestedReply: rawResult.suggestedReply || 'Thank you for reaching out. An agent is reviewing your request.',
        confidenceScore,
        referencedDocs,
        canAutoResolve,
        modelUsed: model,
        latencyMs,
      };

      logger.info('Gemini AI structured triage succeeded', {
        ticketId: ticketData.ticketId,
        tenantId: tenantContext.tenantId,
        urgency: result.urgency,
        category: result.category,
        confidenceScore: result.confidenceScore,
        canAutoResolve: result.canAutoResolve,
        modelUsed: model,
        latencyMs,
      });

      return result;
    } catch (err) {
      lastError = err;
      logger.warn(`Gemini invocation failed with model ${model}, trying next if available`, {
        model,
        error: err.message,
        status: err.status,
      });

      // If error is 503 or 429, wait a brief backoff before trying next model
      if (err.status === 503 || err.status === 429) {
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }

  throw lastError || new Error('All Gemini models exhausted');
}

module.exports = {
  triageTicket,
  GEMINI_TRIAGE_SCHEMA,
};
