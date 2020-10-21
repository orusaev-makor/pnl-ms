const express = require('express');
const { getDaily } = require('./pnl.controller');
const router = express.Router();


router.get('/daily', getDaily);

module.exports = router;