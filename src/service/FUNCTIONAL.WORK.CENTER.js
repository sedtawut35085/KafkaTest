const dbConfig = require("../../config/dbConfig");
const pool = dbConfig;

//SELERT
async function select(dataFromKafka) {
  const text = `SELECT COUNT(plant_code) FROM fd_oms_pm_work_center where plant_code=($1) AND  work_center=($2) `;
  const client = await pool.connect();
  let clientError = null;
  //client.on("error", clientErrorListener);
  try {
    for (var i = 0; i < dataFromKafka.length; i++) {
      const values = [dataFromKafka[i].plant, dataFromKafka[i].workCenter];
      const data = dataFromKafka[i];
      await client.query(text, values, async (err, result) => {
        console.log("DATA :", data);
        if (!err) {
          console.log(result.rows[0].count);
          if (result.rows[0].count != 0) {
            await update(data);
          } else {
            await insert(data);
          }
        } else {
          console.log(err);
        }
      });
    }
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
    // client.off("error", clientErrorListener);
  }
}
// INSERT
async function insert(dataFromKafka) {
  const client = await pool.connect();
  let clientError = null;
  var todayDate = new Date().toISOString();
  console.log("INSERT");
  const text =
    'INSERT INTO fd_oms_pm_work_center("plant_code", "work_center", desc_loc, "desc_eng", "effective_date", "cost_center", "cost_center_description", "person_responsible", "person_responsible_desc", "cancel_flag", "user_create","create_date",  "last_update_user","last_update_date",  "last_function"  ) VALUES($1, $2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) RETURNING *';

  try {
    const values = [
      dataFromKafka.plant,
      dataFromKafka.workCenter,
      dataFromKafka.workCenterDescription,
      dataFromKafka.workCenterDescription,
      dataFromKafka.start,
      dataFromKafka.costCenter,
      dataFromKafka.costCenterDescription,
      dataFromKafka.personResponsible,
      dataFromKafka.statusIndicator,
      "N",
      "Interface_SAPPM",
      todayDate.substring(0, todayDate.length - 5),
      "Interface_SAPPM",
      todayDate.substring(0, todayDate.length - 5),
      dataFromKafka.statusIndicator,
    ];
    await client.query(text, values, async (err, result) => {
      if (!err) {
        console.log(result.rows);
      } else {
        console.log("ERROR INSERT", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
    // client.off("error", clientErrorListener);
  }
}

//UPDATE
async function update(dataFromKafka) {
  const client = await pool.connect();
  let clientError = null;
  var todayDate = new Date().toISOString();
  console.log("UPDATE");
  const text = `UPDATE fd_oms_pm_work_center SET  desc_loc=($1),desc_eng=($2),effective_date=($3),cost_center=($4),cost_center_description=($5), person_responsible = ($6),person_responsible_desc=($7), cancel_flag=($8),last_update_user=($9),last_update_date=($10), last_function=($11) WHERE plant_code=($12) AND work_center =($13) RETURNING *`;

  const values = [
    dataFromKafka.workCenterDescription,
    dataFromKafka.workCenterDescription,
    dataFromKafka.start,
    dataFromKafka.costCenter,
    dataFromKafka.costCenterDescription,
    dataFromKafka.personResponsible,
    dataFromKafka.resDescription,
    "N",
    "Interface_SAPPM",
    todayDate.substring(0, todayDate.length - 5),
    dataFromKafka.statusIndicator,
    dataFromKafka.plant,
    dataFromKafka.workCenter,
  ];

  try {
    await client.query(text, values, async (err, result) => {
      if (!err) {
        console.log(result.rows);
      } else {
        console.log("ERROR UPDATE", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
    // client.off("error", clientErrorListener);
  }
}

module.exports = { select };