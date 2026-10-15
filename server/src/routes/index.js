const express = require('express');
const router = express.Router();

const ticketRoutes = require('./ticketRoutes');
const authRoutes = require('./authRoutes');
const tenantRoutes = require('./tenantRoutes');
const healthRoutes = require('./healthRoutes');
const realtimeRoutes = require('./realtimeRoutes');

router.use('/tickets', ticketRoutes);
router.use('/auth', authRoutes);
router.use('/tenants', tenantRoutes);
router.use('/health', healthRoutes);
router.use('/realtime', realtimeRoutes);

module.exports = router;
