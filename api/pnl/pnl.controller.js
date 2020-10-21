const pnlService = require('./pnl.service');

async function getDaily(req, res) {
    const rows = await pnlService.queryDailyView(req.query);
    res.send(rows);
}

module.exports = {
    getDaily
}