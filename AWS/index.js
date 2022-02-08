require('dotenv').config();
const mysql = require('mysql2');
const fetch = require('node-fetch');
const { database } = require('./keys');
const conexion = mysql.createConnection({
    host: database.host,
    user: database.user,
    password: database.password,
    port: database.port,
    database: database.database
});

exports.handler = async function (event) {
    const promise = new Promise(async function() {
        var sql = `SELECT name FROM ${process.env.TABLE_TICKERS_LIST}`;
        conexion.query(sql, function (err, resultado) {
            if (err) throw err;
            conexion.end();
            guardarIncomeStatement(resultado);
        });
        async function guardarIncomeStatement(resultado){
            for (let i = 0; i < resultado.length; i++) {
                var ticker = resultado[i].name;
                await fetch(`https://financialmodelingprep.com/api/v3/income-statement/${ticker}?period=quarter&apikey=${process.env.FMP_KEY}`)
                .then((res) => {
                    return res.json();
                }).then((json) => {
                    var incomeStatement = json
                    console.log(incomeStatement);
                });
            }
        };
    });
    return promise;
};