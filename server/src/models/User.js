const mongoose = require('mongoose');

const UserSchema = new mongoose.Schema(
  {
    tenantId: {
      type: String,
      required: true,
      index: true,
      trim: true,
    },
    name: {
      type: String,
      required: true,
      trim: true,
    },
    email: {
      type: String,
      required: true,
      unique: true,
      index: true,
      trim: true,
      lowercase: true,
    },
    passwordHash: {
      type: String,
      required: true,
    },
    role: {
      type: String,
      enum: ['ADMIN', 'AGENT'],
      default: 'AGENT',
    },
  },
  {
    timestamps: true,
  }
);

// Method to sanitize user object for responses
UserSchema.methods.toJSON = function () {
  const user = this.toObject();
  delete user.passwordHash;
  return user;
};

const User = mongoose.models.User || mongoose.model('User', UserSchema);
module.exports = User;
