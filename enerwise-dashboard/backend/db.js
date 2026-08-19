const snowflake = require("snowflake-sdk");
require("dotenv").config();

let connection = null;

function getConnection() {
  if (connection) return connection;

  connection = snowflake.createConnection({
    account: process.env.SF_ACCOUNT,
    username: process.env.SF_USERNAME,
    password: process.env.SF_PASSWORD,
    database: process.env.SF_DATABASE,
    schema: process.env.SF_SCHEMA,
    warehouse: process.env.SF_WAREHOUSE,
    role: process.env.SF_ROLE,
    timezone: "UTC",
  });

  connection.connect((err) => {
    if (err) {
      console.error("Failed to connect to Snowflake:", err.message);
    } else {
      console.log("Connected to Snowflake.");
    }
  });

  return connection;
}

function runQuery(sqlText, binds = []) {
  return new Promise((resolve, reject) => {
    getConnection().execute({
      sqlText,
      binds,
      complete: (err, stmt, rows) => {
        if (err) return reject(err);
        resolve(rows);
      },
    });
  });
}

module.exports = { runQuery };
