const mongoose = require('mongoose');

const CustomerSchema = new mongoose.Schema(
  {
    name: { type: String, trim: true, default: 'Anonymous' },
    email: { type: String, required: true, trim: true, lowercase: true },
  },
  { _id: false }
);

const AiDraftSchema = new mongoose.Schema(
  {
    suggestedReply: { type: String },
    reasoning: { type: String },
    confidenceScore: { type: Number, min: 0, max: 1 },
    referencedDocs: [{ type: String }],
    canAutoResolve: { type: Boolean, default: false },
  },
  { _id: false }
);

const HumanReviewSchema = new mongoose.Schema(
  {
    reviewedBy: { type: String, default: null },
    reviewedAt: { type: Date, default: null },
    finalReply: { type: String, default: null },
  },
  { _id: false }
);

const TicketSchema = new mongoose.Schema(
  {
    ticketId: {
      type: String,
      required: true,
      unique: true,
      index: true,
      trim: true,
    },
    tenantId: {
      type: String,
      required: true,
      index: true,
      trim: true,
    },
    customer: {
      type: CustomerSchema,
      required: true,
    },
    title: {
      type: String,
      required: true,
      trim: true,
    },
    description: {
      type: String,
      required: true,
    },
    channel: {
      type: String,
      default: 'API',
      enum: ['API', 'PORTAL', 'EMAIL', 'CHAT', 'WEBHOOK'],
    },
    status: {
      type: String,
      enum: ['QUEUED', 'PROCESSING', 'NEEDS_REVIEW', 'AUTO_RESOLVED', 'RESOLVED', 'CLOSED'],
      default: 'QUEUED',
      index: true,
    },
    urgency: {
      type: String,
      enum: ['P0_CRITICAL', 'P1_HIGH', 'P2_MEDIUM', 'P3_LOW', 'UNASSIGNED'],
      default: 'UNASSIGNED',
      index: true,
    },
    category: {
      type: String,
      default: 'General',
    },
    aiDraft: {
      type: AiDraftSchema,
      default: () => ({}),
    },
    humanReview: {
      type: HumanReviewSchema,
      default: () => ({}),
    },
    metadata: {
      ipAddress: { type: String },
      userAgent: { type: String },
    },
  },
  {
    timestamps: true,
  }
);

// Compound Indexes for fast tenant-scoped queries
TicketSchema.index({ tenantId: 1, status: 1, createdAt: -1 });
TicketSchema.index({ tenantId: 1, urgency: 1 });

const Ticket = mongoose.models.Ticket || mongoose.model('Ticket', TicketSchema);
module.exports = Ticket;
