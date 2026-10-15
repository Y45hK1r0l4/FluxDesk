const mongoose = require('mongoose');

const TenantSchema = new mongoose.Schema(
  {
    tenantId: {
      type: String,
      required: true,
      unique: true,
      index: true,
      trim: true,
    },
    name: {
      type: String,
      required: true,
      trim: true,
    },
    apiKey: {
      type: String,
      required: true,
      unique: true,
      index: true,
      trim: true,
    },
    plan: {
      type: String,
      enum: ['FREE', 'PRO', 'ENTERPRISE'],
      default: 'PRO',
    },
    rateLimitPerMinute: {
      type: Number,
      default: 60,
      min: 1,
    },
    knowledgeBase: [
      {
        title: { type: String, required: true },
        content: { type: String, required: true },
        tags: [{ type: String }],
      },
    ],
  },
  {
    timestamps: true,
  }
);

const Tenant = mongoose.models.Tenant || mongoose.model('Tenant', TenantSchema);
module.exports = Tenant;
