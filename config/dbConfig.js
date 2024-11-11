const { Client, Pool } = require("pg");
const {
  NODE_POSTGRES_HOST,
  NODE_POSTGRES_PORT,
  NODE_POSTGRES_USER,
  NODE_POSTGRES_PASSWORD,
  NODE_POSTGRES_DATABASE,
} = require("./index");
const pool = new Pool({
  host: "localhost",
  port: "5432",
  user: "postgres",
  password: "postgres",
  database: "postgres",
});

pool.on("error", (error) => {
  console.log("error in pool: ", error);
});

module.exports = pool;


// # NODE_POSTGRES_HOST="rdsomsenguat.cc4wksboyk3y.ap-southeast-1.rds.amazonaws.com"
// # NODE_POSTGRES_PORT="5432"
// # NODE_POSTGRES_USER="omsengcfg"
// # NODE_POSTGRES_PASSWORD="UidTTj#09mcnZ"
// # NODE_POSTGRES_DATABASE="OMSENG"  