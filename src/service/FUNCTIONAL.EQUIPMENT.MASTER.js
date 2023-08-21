const dbConfig = require("../../config/dbConfig");
const pool = dbConfig;

//SELECT
async function select(dataFromKafka) {
    const text = `SELECT COUNT(plant_code) FROM fd_oms_pm_equipment_info where company_code=($1) AND plant_code=($2) AND equipment_no=($3)`;
    const client = await pool.connect();
    let clientError = null;
    //client.on("error", clientErrorListener);
    try {
      for (var i = 0; i < dataFromKafka.length; i++) {
        const values = [dataFromKafka[i].companyCode, dataFromKafka[i].maintenancePlant, dataFromKafka[i].equipmentNumber];
        const data = dataFromKafka[i];
        if(dataFromKafka[i].companyCode === "" || dataFromKafka[i].maintenancePlant === "" || dataFromKafka[i].equipmentNumber === ""){
            continue;
        }else{
            await client.query(text, values, async (err, result) => {
            console.log("DATA :", data);
            if (!err) {
                console.log(result);
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
      'INSERT INTO fd_oms_pm_equipment_info( "company_code", "plant_code", "equipment_no", "desc_loc", "desc_eng", "equipment_create_date", "equipment_change_date", "equipment_abc_indic", "equipment_cost_center", "equipment_standing_order", "main_work_center", "plant_for_work_center", "equipment_location", "superord_equip", "planning_plant_code", "inventory_number", "asset_number", "asset_sub_number", "material_number", "serial_no", "cancel_flag", "user_create", "create_date", "last_update_user", "last_update_date", "last_function" ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, now(), $23, now(), $24) RETURNING *';
    try {
      let equipmentCreatedon = dataFromKafka.equipmentCreatedon
      let equipmentChangedon = dataFromKafka.equipmentChangedon
      if(equipmentChangedon === "00000000"){
        equipmentChangedon = null
      }else{
        equipmentChangedon = equipmentChangedon.substring(0,4) + "-" + equipmentChangedon.substring(4,6) + "-" + equipmentChangedon.substring(6,8)
      }
      equipmentCreatedon = equipmentCreatedon.substring(0,4) + "-" + equipmentCreatedon.substring(4,6) + "-" + equipmentCreatedon.substring(6,8)
      const values = [
        dataFromKafka.companyCode,
        dataFromKafka.maintenancePlant,
        dataFromKafka.equipmentNumber,
        dataFromKafka.equipmentDescription,
        dataFromKafka.equipmentDescription,
        equipmentCreatedon,
        equipmentChangedon,
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
    text = `UPDATE fd_oms_pm_equipment_info SET  desc_loc=($1),desc_eng=($2),equipment_create_date=($3),
    equipment_change_date=($4),equipment_abc_indic=($5), equipment_cost_center = ($6),
    equipment_standing_order=($7), main_work_center=($8),plant_for_work_center=($9),equipment_location=($10), superord_equip=($11),planning_plant_code=($12),inventory_number=($13),
    asset_number=($14), asset_sub_number=($15), material_number=($16), serial_no=($17),
    cancel_flag=($18),last_update_user=($19),
    last_update_date=now(),
    last_function=($20)    WHERE 
    company_code=($21) AND 
    plant_code =($22) AND equipment_no=($23) RETURNING *`;
    let equipmentCreatedon = dataFromKafka.equipmentCreatedon
    let equipmentChangedon = dataFromKafka.equipmentChangedon
    if(equipmentChangedon === "00000000"){
        equipmentChangedon = null
    }else{
        equipmentChangedon = equipmentChangedon.substring(0,4) + "-" + equipmentChangedon.substring(4,6) + "-" + equipmentChangedon.substring(6,8)
    }
    equipmentCreatedon = equipmentCreatedon.substring(0,4) + "-" + equipmentCreatedon.substring(4,6) + "-" + equipmentCreatedon.substring(6,8)
    if(dataFromKafka.systemStatus.includes("INAC") || dataFromKafka.systemStatus.includes("DLFL")){
      console.log("UPDATE FLAG");
      cancelFlag = "Y"
    }else{
      cancelFlag = "N"
    }
    values = [
      dataFromKafka.equipmentDescription,
      dataFromKafka.equipmentDescription,
      equipmentCreatedon,
      equipmentChangedon,
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
      dataFromKafka.equipmentNumber
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