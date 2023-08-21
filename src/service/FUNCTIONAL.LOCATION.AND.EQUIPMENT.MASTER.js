const dbConfig = require("../../config/dbConfig");
const pool = dbConfig;

//SELECT
async function select(dataFromKafka) {
    const text = `SELECT COUNT(plant_code) FROM fd_oms_pm_equipment_location where company_code=($1) AND plant_code=($2) AND location_code=($3)`;
    const client = await pool.connect();
    let clientError = null;
    //client.on("error", clientErrorListener);
    try {
      for (var i = 0; i < dataFromKafka.length; i++) {
        const values = [dataFromKafka[i].companyCode, dataFromKafka[i].maintenancePlant, dataFromKafka[i].functionalLocation];
        const data = dataFromKafka[i];
        if(dataFromKafka[i].companyCode === "" || dataFromKafka[i].maintenancePlant === "" || dataFromKafka[i].functionalLocation === ""){
            continue;
        }else{
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
    console.log("INSERT");
    const text =
      'INSERT INTO fd_oms_pm_equipment_location( "company_code", "plant_code", "location_code", "desc_loc", "desc_eng", "location_create_date", "location_change_date", "location_cost_center", "location_standing_order", "planning_plant_code", "location_type", "section_code", "main_work_center", "plant_for_work_center", "superior_functional_location", "location_late", "location_long", "cancel_flag", "user_create", "create_date", "last_update_user", "last_update_date", "last_function" ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, now(), $20, now(), $21 ) RETURNING *';
    try { 
      let functionalLocationCreatedon = dataFromKafka.functionalLocationCreatedon
      let functionalLocationChangeon = dataFromKafka.functionalLocationChangeon
      if(functionalLocationChangeon === "00000000"){
        functionalLocationChangeon = null
      }else{
        functionalLocationChangeon = functionalLocationChangeon.substring(0,4) + "-" + functionalLocationChangeon.substring(4,6) + "-" + functionalLocationChangeon.substring(6,8)
      }
      functionalLocationCreatedon = functionalLocationCreatedon.substring(0,4) + "-" + functionalLocationCreatedon.substring(4,6) + "-" + functionalLocationCreatedon.substring(6,8)
      const values = [
        dataFromKafka.companyCode,
        dataFromKafka.maintenancePlant,
        dataFromKafka.functionalLocation,
        dataFromKafka.functionalLocationDescription,
        dataFromKafka.functionalLocationDescription,
        functionalLocationCreatedon,
        functionalLocationChangeon,
        dataFromKafka.functionalLocationCostCenter,
        dataFromKafka.functionalLocationStandingOrder,
        dataFromKafka.planningPlant,
        dataFromKafka.location,
        dataFromKafka.plantSection,
        dataFromKafka.mainWorkCenter,
        dataFromKafka.plantforWorkCenter,
        dataFromKafka.superiorFunctionalLocation,
        dataFromKafka.manufacturerPartNumber,
        dataFromKafka.manufacturerSerialNumber,
        "N",
        "Interface_SAPPM",
        "Interface_SAPPM",
        dataFromKafka.statusIndicator
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
    let values = null
    let text = ''
    let cancelFlag = null
    console.log("UPDATE");
    text = `UPDATE fd_oms_pm_equipment_location SET  desc_loc=($1),desc_eng=($2),location_create_date=($3),
    location_change_date=($4),location_cost_center=($5), location_standing_order = ($6),
    planning_plant_code=($7), location_type=($8),section_code=($9),main_work_center=($10), plant_for_work_center=($11),superior_functional_location=($12),location_late=($13),
    location_long=($14), cancel_flag=($15), last_update_user=($16), last_update_date=now(),
    last_function=($17)  WHERE 
    company_code=($18) AND 
    plant_code =($19) AND location_code=($20) RETURNING *`;
    let functionalLocationCreatedon = dataFromKafka.functionalLocationCreatedon
    let functionalLocationChangeon = dataFromKafka.functionalLocationChangeon
    if(functionalLocationChangeon === "00000000"){
        functionalLocationChangeon = null
    }else{
        functionalLocationChangeon = functionalLocationChangeon.substring(0,4) + "-" + functionalLocationChangeon.substring(4,6) + "-" + functionalLocationChangeon.substring(6,8)
    }
    functionalLocationCreatedon = functionalLocationCreatedon.substring(0,4) + "-" + functionalLocationCreatedon.substring(4,6) + "-" + functionalLocationCreatedon.substring(6,8)
    if(dataFromKafka.systemStatus.includes("INAC") || dataFromKafka.systemStatus.includes("DLFL")){
      console.log("UPDATE FLAG");
      cancelFlag = "Y"
    }else{
      cancelFlag = "N"
    }
    values = [
      dataFromKafka.functionalLocationDescription,
      dataFromKafka.functionalLocationDescription,
      functionalLocationCreatedon,
      functionalLocationChangeon,
      dataFromKafka.functionalLocationCostCenter,
      dataFromKafka.functionalLocationStandingOrder,
      dataFromKafka.planningPlant,
      dataFromKafka.location,
      dataFromKafka.plantSection,
      dataFromKafka.mainWorkCenter,
      dataFromKafka.plantforWorkCenter,
      dataFromKafka.superiorFunctionalLocation,
      dataFromKafka.manufacturerPartNumber,
      dataFromKafka.manufacturerSerialNumber,
      cancelFlag,
      "Interface_SAPPM",
      dataFromKafka.statusIndicator,
      dataFromKafka.companyCode,
      dataFromKafka.maintenancePlant,
      dataFromKafka.functionalLocation
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