const express = require('express');
const router = express.Router();

const { tenantAuth } = require('../middleware/tenantAuth');
const tenantController = require('../controllers/tenantController');

router.get('/', tenantController.listTenants);
router.get('/quota', tenantAuth, tenantController.getTenantQuota);

module.exports = router;
