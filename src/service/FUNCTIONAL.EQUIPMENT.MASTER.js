const dbConfig = require("../../config/dbConfig");
const pool = dbConfig;
const validCodes = ["153", "355"];

//SELECT
async function select(dataFromKafka) {
  const text = `SELECT qrcode_no FROM fd_oms_pm_equipment_info where company_code=($1) AND plant_code=($2) AND equipment_no=($3)`;
  const queryCheckCostCenter = `SELECT count(*) AS cost_center_count FROM fd_oms_pm_cost_center WHERE plant_code = ($1)  AND cost_center = ($2)`;
  const queryGetCostId = `SELECT min(cost_id) AS id FROM fd_oms_pm_equipment_cost`;
  const client = await pool.connect();
  let clientError = null;
  //client.on("error", clientErrorListener);
  try {
    for (var i = 0; i < dataFromKafka.length; i++) {
      const values = [
        dataFromKafka[i].companyCode,
        dataFromKafka[i].maintenancePlant,
        dataFromKafka[i].equipmentNumber,
      ];
      const valuesCheckCostCenter = [
        dataFromKafka[i].maintenancePlant,
        dataFromKafka[i].equipmentCostCenter,
      ];
      const data = dataFromKafka[i];
      if (
        dataFromKafka[i].companyCode === "" ||
        dataFromKafka[i].maintenancePlant === "" ||
        dataFromKafka[i].equipmentNumber === ""
      ) {
        continue;
      } else {
        // 1. check duplicate data equipment info
        console.log(
          `Received data topic CPF.TH.SAP.EQUIPMENT.MASTER: ${JSON.stringify(
            data
          )}`
        );
        await client.query(text, values, async (err, result) => {
          console.log(`result: ${JSON.stringify(result)}`);
          console.log("1. resultEquipmentInfo count :", result.rows.length);
          if (!err) {
            // 2. check cost center
            await client.query(
              queryCheckCostCenter,
              valuesCheckCostCenter,
              async (err, resultCostCenter) => {
                // 2.1 if don't have data, insert data
                console.log(
                  "2. resultCostCenter count :",
                  resultCostCenter.rows[0].cost_center_count
                );
                if (resultCostCenter.rows[0].cost_center_count == 0) {
                  await insertCostCenter(data);
                }
                // 3. get cost id
                var costId = 1;
                await client.query(
                  queryGetCostId,
                  async (err, resultCostId) => {
                    if (resultCostId.rows[0].id !== null) {
                      costId = resultCostId.rows[0].id;
                    }
                    console.log("3. costId :", costId);
                    // 4. insert or update
                    if (result.rows.length > 0) {
                      // 4.1 update
                      await update(data, costId, result.rows[0].qrcode_no);
                    } else {
                      // 4.2 insert
                      await insert(data, costId);
                    }
                  }
                );
              }
            );
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

// INSERT CostCenter
async function insertCostCenter(dataFromKafka) {
  const client = await pool.connect();
  let clientError = null;
  console.log("INSERT COST CENTER");
  const queryInsert =
    "INSERT INTO fd_oms_pm_cost_center (plant_code, cost_center, desc_loc, desc_eng, cancel_flag, user_create, create_date, last_update_user, last_update_date, last_function) VALUES ($1, $2, $3, $4, $5, $6, now(), null, null, $7) RETURNING *";
  try {
    const valuesInsert = [
      dataFromKafka.maintenancePlant,
      dataFromKafka.equipmentCostCenter,
      dataFromKafka.equipmentCostCenter,
      dataFromKafka.equipmentCostCenter,
      "N",
      "Interface_SAPPM",
      "A",
    ];
    await client.query(queryInsert, valuesInsert, async (err, result) => {
      if (!err) {
        console.log("SUCCESS INSERT COST CENTER");
      } else {
        console.log("ERROR INSERT COST CENTER", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
}

// INSERT
async function insert(dataFromKafka, costId) {
  const client = await pool.connect();
  let clientError = null;
  console.log("INSERT EQUIPMENT INFO");
  // continue cost_id
  const text =
    'INSERT INTO fd_oms_pm_equipment_info( "company_code", "plant_code", "equipment_no", "desc_loc", "desc_eng", "equipment_create_date", "equipment_change_date", "equipment_abc_indic", "equipment_cost_center", "equipment_standing_order", "main_work_center", "plant_for_work_center", "equipment_location", "superord_equip", "planning_plant_code", "inventory_number", "asset_number", "asset_sub_number", "material_number", "serial_no", "cancel_flag", "user_create", "create_date", "last_update_user", "last_update_date", "last_function", "cost_id", "equipment_type", "category", "production_work_center", "sort_field", "stock_type", "stock_plant", "stock_storage_location", "stock_batch", "special_stock", "qrcode_no" ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, now(), $23, now(), $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35) RETURNING *';
  try {
    let equipmentCreate = dataFromKafka.equipmentCreatedon;
    let equipmentChange = dataFromKafka.equipmentChangedon;
    if (equipmentChange === "00000000") {
      equipmentChange = null;
    } else {
      equipmentChange =
        equipmentChange.substring(0, 4) +
        "-" +
        equipmentChange.substring(4, 6) +
        "-" +
        equipmentChange.substring(6, 8);
    }
    equipmentCreate =
      equipmentCreate.substring(0, 4) +
      "-" +
      equipmentCreate.substring(4, 6) +
      "-" +
      equipmentCreate.substring(6, 8);
    // check company code if equal 355, 153
    qrCode = null;
    if (validCodes.includes(dataFromKafka.companyCode)) {
      if (dataFromKafka.serialnumber !== null && dataFromKafka.serialnumber !== undefined && dataFromKafka.serialnumber !== "") {
        qrCode = dataFromKafka.serialnumber + "-1-1";
      }
    }
    const values = [
      dataFromKafka.companyCode,
      dataFromKafka.maintenancePlant,
      dataFromKafka.equipmentNumber,
      dataFromKafka.equipmentDescription,
      dataFromKafka.equipmentDescription,
      equipmentCreate,
      equipmentChange,
      dataFromKafka.equipmentABCindic,
      dataFromKafka.equipmentCostCenter,
      dataFromKafka.equipmentStandingOrder,
      dataFromKafka.mainWorkCenter,
      dataFromKafka.plantforWorkCenter,
      dataFromKafka.equipmentFunctionalLocation,
      dataFromKafka.superordEquip,
      dataFromKafka.planningPlant,
      dataFromKafka.inventoryNumber,
      dataFromKafka.assetNumber,
      dataFromKafka.assetsubnumber,
      dataFromKafka.materialnumber,
      dataFromKafka.serialnumber,
      "N",
      "Interface_SAPPM",
      "Interface_SAPPM",
      dataFromKafka.statusIndicator,
      costId,
      dataFromKafka.objectType,
      dataFromKafka.equipmentCategory,
      dataFromKafka.workCenter,
      dataFromKafka.sortField,
      dataFromKafka.stockType,
      dataFromKafka.stockPlant,
      dataFromKafka.stockStorageLocation,
      dataFromKafka.stockBatch,
      dataFromKafka.specialStock,
      qrCode,
    ];
    await client.query(text, values, async (err, result) => {
      if (!err) {
        -console.log("SUCCESS INSERT EQUIPMENT INFO");
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
async function update(dataFromKafka, costId, qrCode) {
  const client = await pool.connect();
  let clientError = null;
  let values = null;
  let text = "";
  let cancelFlag = null;
  console.log("UPDATE EQUIPMENT INFO");
  text = `UPDATE fd_oms_pm_equipment_info SET  desc_loc=($1),desc_eng=($2),equipment_create_date=($3),
    equipment_change_date=($4),equipment_abc_indic=($5), equipment_cost_center = ($6),
    equipment_standing_order=($7), main_work_center=($8),plant_for_work_center=($9),equipment_location=($10), superord_equip=($11),planning_plant_code=($12),inventory_number=($13),
    asset_number=($14), asset_sub_number=($15), material_number=($16), serial_no=($17),
    cost_id=($24), equipment_type=($25), category=($26), production_work_center=($27), sort_field=($28), stock_type=($29), stock_plant=($30), stock_storage_location=($31), stock_batch=($32), special_stock=($33),
    cancel_flag=($18),last_update_user=($19),
    last_update_date=now(),
    last_function=($20),
    qrcode_no=($34)    WHERE 
    company_code=($21) AND 
    plant_code =($22) AND equipment_no=($23) RETURNING *`;
  let equipmentCreate = dataFromKafka.equipmentCreatedon;
  let equipmentChange = dataFromKafka.equipmentChangedon;
  if (equipmentChange === "00000000") {
    equipmentChange = null;
  } else {
    equipmentChange =
      equipmentChange.substring(0, 4) +
      "-" +
      equipmentChange.substring(4, 6) +
      "-" +
      equipmentChange.substring(6, 8);
  }
  equipmentCreate =
    equipmentCreate.substring(0, 4) +
    "-" +
    equipmentCreate.substring(4, 6) +
    "-" +
    equipmentCreate.substring(6, 8);
  if (
    dataFromKafka.systemStatus.includes("INAC") ||
    dataFromKafka.systemStatus.includes("DLFL")
  ) {
    console.log("UPDATE FLAG");
    cancelFlag = "Y";
  } else {
    cancelFlag = "N";
  }
  if (validCodes.includes(dataFromKafka.companyCode)) {
    if (qrCode === null && dataFromKafka.serialnumber !== null && dataFromKafka.serialnumber !== undefined && dataFromKafka.serialnumber !== "") {
      qrCode = dataFromKafka.serialnumber + "-1-1";
    }
  }
  values = [
    dataFromKafka.equipmentDescription,
    dataFromKafka.equipmentDescription,
    equipmentCreate,
    equipmentChange,
    dataFromKafka.equipmentABCindic,
    dataFromKafka.equipmentCostCenter,
    dataFromKafka.equipmentStandingOrder,
    dataFromKafka.mainWorkCenter,
    dataFromKafka.plantforWorkCenter,
    dataFromKafka.equipmentFunctionalLocation,
    dataFromKafka.superordEquip,
    dataFromKafka.planningPlant,
    dataFromKafka.inventoryNumber,
    dataFromKafka.assetNumber,
    dataFromKafka.assetsubnumber,
    dataFromKafka.materialnumber,
    dataFromKafka.serialnumber,
    cancelFlag,
    "Interface_SAPPM",
    dataFromKafka.statusIndicator,
    dataFromKafka.companyCode,
    dataFromKafka.maintenancePlant,
    dataFromKafka.equipmentNumber,
    costId,
    dataFromKafka.objectType,
    dataFromKafka.equipmentCategory,
    dataFromKafka.workCenter,
    dataFromKafka.sortField,
    dataFromKafka.stockType,
    dataFromKafka.stockPlant,
    dataFromKafka.stockStorageLocation,
    dataFromKafka.stockBatch,
    dataFromKafka.specialStock,
    qrCode,
  ];

  try {
    await client.query(text, values, async (err, result) => {
      if (!err) {
        console.log("SUCCESS UPDATE EQUIPMENT INFO");
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
