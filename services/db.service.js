var mysql = require('mysql');

var connection = mysql.createConnection({
    host: 'localhost',
    port: 3306,
    user: 'root',
    password: '123456',
    database: 'mydb',
    insecureAuth: true
});

connection.connect(error => {
    if (error) console.log('ERROR: ', error);

    console.log('connected to SQL server');
})

function runSQL(query) {
    return new Promise((resolve, reject) => {
        connection.query(query, function (error, results, fields) {
            if (error) reject(error);
            else resolve(results);
            // // not entirely clear on whether connection.end() should be called here or not.
            // // Leaning towards not.
            // connection.end();
        });
    })
}

module.exports = {
    runSQL
}