const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const config = require('../config/env');
const User = require('../models/User');
const Tenant = require('../models/Tenant');

async function login(req, res, next) {
  try {
    const { email, password } = req.validatedBody;

    const user = await User.findOne({ email }).lean();
    if (!user) {
      return res.status(401).json({
        error: 'Unauthorized',
        message: 'Invalid email or password.',
      });
    }

    const isMatch = await bcrypt.compare(password, user.passwordHash);
    if (!isMatch) {
      return res.status(401).json({
        error: 'Unauthorized',
        message: 'Invalid email or password.',
      });
    }

    const tenant = await Tenant.findOne({ tenantId: user.tenantId }).lean();

    const token = jwt.sign(
      {
        userId: user._id.toString(),
        tenantId: user.tenantId,
        email: user.email,
        role: user.role,
      },
      config.jwt.secret,
      { expiresIn: config.jwt.expiresIn }
    );

    const { passwordHash, ...userProfile } = user;

    return res.json({
      success: true,
      token,
      user: userProfile,
      tenant: tenant ? {
        tenantId: tenant.tenantId,
        name: tenant.name,
        plan: tenant.plan,
        rateLimitPerMinute: tenant.rateLimitPerMinute,
      } : null,
    });
  } catch (err) {
    next(err);
  }
}

async function getMe(req, res, next) {
  try {
    const tenant = await Tenant.findOne({ tenantId: req.user.tenantId }).lean();
    return res.json({
      success: true,
      user: req.user,
      tenant: tenant ? {
        tenantId: tenant.tenantId,
        name: tenant.name,
        plan: tenant.plan,
        rateLimitPerMinute: tenant.rateLimitPerMinute,
      } : null,
    });
  } catch (err) {
    next(err);
  }
}

module.exports = {
  login,
  getMe,
};
