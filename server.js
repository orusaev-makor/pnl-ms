const app = require('express')();
const bodyParser = require('body-parser');

app.use(bodyParser.json());

const http = require('http').createServer(app);


const pnlRoutes = require('./api/pnl/pnl.routes');

app.get('/daily', (req, res) => {
    res.send({ message: 'Connected' });
});

// routes
app.use('/pnl-dashboard', pnlRoutes);

// (async () => {
//     const rows = await pnlService.query();
//     // rows.forEach(row => {
//     //     console.log(`${row.name} made net: ${row.pnl_net}`);
//     // });
//     console.log('Pnl:', rows);
// })();

const port = process.env.PORT || 4000;
http.listen(port, () => {
    console.log('Server is running on port: ' + port);
});