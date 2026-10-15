const Tenant = require('../../../server/src/models/Tenant');
const logger = require('../config/logger');

/**
 * Retrieve tenant runbooks and match against ticket content
 */
async function getTenantContext(tenantId, title = '', description = '') {
  try {
    const tenant = await Tenant.findOne({ tenantId }).lean();
    if (!tenant) {
      logger.warn(`Tenant not found for context lookup: ${tenantId}`);
      return {
        tenantId,
        tenantName: tenantId,
        plan: 'FREE',
        matchedRunbooks: [],
        allRunbooks: [],
        formattedContext: 'No tenant-specific runbooks or documentation available.',
      };
    }

    const runbooks = tenant.knowledgeBase || [];
    if (runbooks.length === 0) {
      return {
        tenantId,
        tenantName: tenant.name,
        plan: tenant.plan,
        matchedRunbooks: [],
        allRunbooks: [],
        formattedContext: 'No tenant-specific runbooks or documentation available.',
      };
    }

    // Keyword matching between ticket text and runbooks
    const searchText = `${title} ${description}`.toLowerCase();
    const scoredRunbooks = runbooks.map((doc) => {
      let score = 0;
      const docTitle = (doc.title || '').toLowerCase();
      const docContent = (doc.content || '').toLowerCase();
      const tags = (doc.tags || []).map((t) => t.toLowerCase());

      // Check title keywords
      docTitle.split(/\s+/).forEach((w) => {
        if (w.length > 3 && searchText.includes(w)) score += 3;
      });

      // Check tags
      tags.forEach((tag) => {
        if (tag.length > 2 && searchText.includes(tag)) score += 5;
      });

      // Check content fragments
      docContent.split(/\s+/).forEach((w) => {
        if (w.length > 4 && searchText.includes(w)) score += 1;
      });

      return { doc, score };
    });

    scoredRunbooks.sort((a, b) => b.score - a.score);
    const matched = scoredRunbooks.filter((r) => r.score > 0).map((r) => r.doc);

    // If top matches exist, use them. If none matched specifically but list is small (<= 4), include all.
    const selected = matched.length > 0 ? matched : runbooks.slice(0, 4);

    const formattedContext = selected
      .map(
        (doc, idx) =>
          `[Runbook ${idx + 1}] "${doc.title}" (Tags: ${(doc.tags || []).join(', ') || 'none'})\nContent: ${doc.content}`
      )
      .join('\n\n');

    return {
      tenantId,
      tenantName: tenant.name,
      plan: tenant.plan,
      matchedRunbooks: selected,
      allRunbooks: runbooks,
      formattedContext,
    };
  } catch (err) {
    logger.error('Error fetching tenant knowledge base context', {
      tenantId,
      error: err.message,
    });
    return {
      tenantId,
      tenantName: tenantId,
      plan: 'UNKNOWN',
      matchedRunbooks: [],
      allRunbooks: [],
      formattedContext: 'Knowledge base retrieval unavailable due to error.',
    };
  }
}

module.exports = {
  getTenantContext,
};
