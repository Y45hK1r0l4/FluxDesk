const express = require('express');
const router = express.Router();

const { validateBody, loginSchema } = require('../middleware/validate');
const { agentAuth } = require('../middleware/agentAuth');
const authController = require('../controllers/authController');

router.post('/login', validateBody(loginSchema), authController.login);
router.get('/me', agentAuth, authController.getMe);

module.exports = router;
