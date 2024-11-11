const dfConfig = require("../../config/dbConfig");
const pool = dfConfig;

const FormatDataNull = (data) => {
  if (data == "" || data == undefined) {
    return null;
  } else {
    return data;
  }
};

const ALL_SELECT = async (data, Toppic) => {
  console.log("DATA SENT : ", data);
  console.log("Data Count:", data.MaterialList.length);
  /* console.log("DATA SENT 2 : ", data.MaterialList[0]); */
  const client = await pool.connect();
  let clientError = null;

  /* console.log("MaterialList", MaterialList);
  console.log("AlternativeUnitList", AlternativeUnitList);
  console.log("MaterialPlantList", MaterialPlantList);
  console.log("MaterialNo", MaterialNo); */
  try {
    //* Material Array
    for (let mat = 0; mat < data.MaterialList.length; mat++) {
      let MaterialList = data.MaterialList[mat];
      // ! AlternativeUnitList เป็น [] อาจต้อง loop
      let AlternativeUnitList = data.MaterialList[mat].AlternativeUnitList[mat];
      let MaterialNo = MaterialList.MaterialNo;
      let PlantCode = await MaterialList.MaterialPlantList[mat].PlantCode;

      await SELECT_MAS_ORG(PlantCode, MaterialList, Toppic);

      if (AlternativeUnitList !== undefined) {
        await SELECT_MATERIAL_ALTERNATIVE_UNIT(MaterialNo, AlternativeUnitList);
      }
      // await SELECT_MATERIAL_GENERAL(MaterialList, Toppic);
      await SELECT_MATERIAL_TYPE(MaterialList);
      // await SELECT_MATERIAL_GROUP(MaterialList, Toppic);
      await SELECT_MATERIAL_MAIN_CATEGORY(MaterialList);
      await SELECT_MATERIAL_SUB_CATEGORY(MaterialList);
      await SELECT_MATERIAL_UNIT(MaterialList);
      if (MaterialList.ProductGroup1 !== null) {
        await SELECT_MATERIAL_PRODUCT_GROUP_1(MaterialList);
      }
      if (MaterialList.ProductGroup2 !== null) {
        await SELECT_MATERIAL_PRODUCT_GROUP_2(MaterialList);
      }
      if (MaterialList.ProductGroup3 !== null) {
        await SELECT_MATERIAL_PRODUCT_GROUP_3(MaterialList);
      }
      if (MaterialList.ProductGroup4 !== null) {
        await SELECT_MATERIAL_PRODUCT_GROUP_4(MaterialList);
      }
      if (MaterialList.ProductGroup5 !== null) {
        await SELECT_MATERIAL_PRODUCT_GROUP_5(MaterialList);
      }
      if (MaterialList.ProductGroup6 !== null) {
        await SELECT_MATERIAL_PRODUCT_GROUP_6(MaterialList);
      }
      //* Plant List

      for (
        let plant = 0;
        plant < MaterialList.MaterialPlantList.length;
        plant++
      ) {
        let MaterialPlantList = MaterialList.MaterialPlantList[mat];
        await SELECT_MATERIAL_PLANT(MaterialPlantList, MaterialNo);
      }
    }
  } catch (error) {
    console.log("ERR FUNCTION DATA IS EMPTY:", error);
  } finally {
    client.release(clientError);
  }
};

// ค้นหา company_code จาก mas_org เอาไปเก็บใน table fd_oms_pm_material_general, fd_oms_pm_material_group
const SELECT_MAS_ORG = async (org_code, MaterialList, Toppic) => {
  const client = await pool.connect();
  let clientError = null;

  //const text = `SELECT company_code FROM mas_org where org_code = ($1)`;
  const text = `SELECT DISTINCT o.company_code FROM mas_org o
  INNER JOIN fd_oms_pm_service_config sc ON o.org_code = sc.plant_code
  WHERE sc.business_rule = ($1) ;`;

  try {
    const values = [Toppic];
    await client.query(text, values, async (err, result) => {
      let company_code = "";
      if (!err) {
        // console.log("** Log Company_Code :", company_code);
        for (let i = 0; i < result.rows.length; i++) {
          console.log("** result.rows.companyCode :", result.rows[i].company_code);
          console.log("** result.rows :", result.rows[i]);
          if (result.rows[i] == undefined || result.rows[i] == null) {
            company_code = "NA";
          } else {
            company_code = result.rows[i].company_code;
          }
          await SELECT_MATERIAL_GENERAL(MaterialList, company_code);
          await SELECT_MATERIAL_GROUP(MaterialList, company_code);
        }
      } else {
        console.log(err);
      }
    });
  } catch (error) {
    clientError = error;
  } finally {
    client.release(clientError);
  }
};

// GENERAL
// ! แก้
const SELECT_MATERIAL_GENERAL = async (MaterialList, company_code) => {
  const text = `SELECT COUNT(material_code) FROM fd_oms_pm_material_general where material_code=($1)`;
  const client = await pool.connect();
  let clientError = null;
  // console.log("** Log Company_Code :", company_code);
  try {
    const values = [MaterialList.MaterialNo];
    // console.log(dataFormKafka[0].MaterialNo);
    await client.query(text, values, async (err, result) => {
      if (!err) {
        if (result.rows[0].count != 0) {
          //update
          await UPDATE_MATERIAL_GENERA(MaterialList, company_code);
        } else {
          //insert
          await INSERT_MATERIAL_GENERA(MaterialList, company_code);
        }
        console.log(result.rows);
      } else {
        console.log(err);
      }
    });
  } catch (error) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const INSERT_MATERIAL_GENERA = async (MaterialList, company_code) => {
  const client = await pool.connect();
  let clientError = null;

  console.log("INSERT fd_oms_pm_material_general");
  console.log("** Log Company_Code :", company_code);
  const text = `INSERT INTO fd_oms_pm_material_general
  ("material_code","desc_eng","common_name","short_name_loc",
  "short_name_eng","invoice_name_loc","invoice_name_eng",
  "commercial_name_loc","commercial_name_eng","model",
  "size_spec","brand","brand_description","main_category_code",
  "sub_category_code","material_group_code","material_type_code",
  "base_um_code","business_group_key","product_group_1",
  "product_group_4","product_group_5","product_group_6",
  "coutry_code","status","size_dimension","mat_pack_group_code",
  "gross_weight","net_weight_unit","aging_day","equi_stock",
  "unit_pack","equi_sale","equi_pur","equi_price","equi_analy",
  "equi_picking","equi_delivery","minimum_order","equi_pos",
  "equi_order","dim_stk_qty","dim_stk_width","dim_stk_length",
  "dim_stk_hight","dim_stk_weight","dim_inner_qty","dim_inner_width",
  "dim_inner_length","dim_inner_hight","dim_inner_weight",
  "dim_carton_qty","dim_carton_width","dim_carton_length",
  "dim_carton_hight","dim_carton_weight","cancel_flag","user_create",
  "create_date","last_update_user","last_update_date","last_function","company_code") VALUES($1,$2, $3,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14 ,$15 ,$16 ,$17 ,$18 ,$19 ,$20 ,$21 ,$22 ,$23 ,$24 ,$25 ,$26 ,$27 ,$28 ,$29 ,$30 ,$31 ,$32 ,$33 ,$34 ,$35 ,$36 ,$37 ,$38 ,$39 ,$40 ,$41 ,$42 ,$43 , $44 ,$45 ,$46 ,$47 ,$48 ,$49 ,$50 ,$51 ,$52 ,$53 ,$54 ,$55 ,$56 ,$57 ,$58 ,$59 ,$60 ,$61,$62,$63 ) RETURNING *`;

  const values = [
    MaterialList.MaterialNo,
    MaterialList.MaterialDescEng,
    MaterialList.CommonName,
    MaterialList.ShortNameLocal,
    MaterialList.ShortNameEng,
    MaterialList.InvoiceNameLocal,
    MaterialList.InvoiceNameEng,
    MaterialList.CommercialName,
    MaterialList.CommercialNameEng,
    MaterialList.Model,
    MaterialList.SizeSpec,
    MaterialList.Brand,
    MaterialList.BrandDescription,
    MaterialList.MainCategoryCode,
    MaterialList.SubCategoryCode,
    MaterialList.MaterialGroupCode,
    MaterialList.MaterialTypeCode,
    MaterialList.BaseUmCode,
    MaterialList.BusinessGroupKey,
    MaterialList.ProductGroup1,
    MaterialList.ProductGroup4,
    MaterialList.ProductGroup5,
    MaterialList.ProductGroup6,
    MaterialList.CoutryCode,
    MaterialList.Status,
    null, //MaterialList.SizeDimension,
    MaterialList.MatPackGroupCode,
    MaterialList.GrossWeight,
    MaterialList.NetWeightUnit,
    MaterialList.AgingDay,
    MaterialList.EquiStock,
    MaterialList.UnitPack,
    MaterialList.EquiSale,
    null, //MaterialList.EquiPur,
    MaterialList.EquiPrice,
    MaterialList.EquiAnaly,
    MaterialList.EquiPicking,
    null,  // MaterialList.EquiDelivery,
    MaterialList.MinimumOrder,
    null, // MaterialList.EquiPos,
    null, // MaterialList.EquiOrder,
    MaterialList.DimStkQty,
    MaterialList.DimStkWdth,
    MaterialList.DimStkLngth,
    MaterialList.DimStkHght,
    MaterialList.DimStkWgt,
    MaterialList.DimInnerQty,
    MaterialList.DimInnerWdth,
    MaterialList.DimInnerLngth,
    MaterialList.DimInnerHght,
    MaterialList.DimInnerWgt,
    MaterialList.DimCartonQty,
    MaterialList.DimCartonWdth,
    MaterialList.DimCartonLngth,
    MaterialList.DimCartonHght,
    MaterialList.DimCartonWgt,
    "N",
    "MDMs",
    new Date(),
    "MDMs",
    new Date(),
    "A",
    company_code,
  ];
  try {
    await client.query(text, values, (err, result) => {
      if (!err) {
        console.log(result.rows);
        console.log("INSERT SUSCESS fd_oms_pm_material_general");
      } else {
        console.log("ERROR INSERT fd_oms_pm_material_general", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const UPDATE_MATERIAL_GENERA = async (MaterialList, company_code) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("UPDATE fd_oms_pm_material_general");
  console.log("** Log Company_Code :", company_code);
  const text = `UPDATE fd_oms_pm_material_general SET  
  desc_loc=($1),
desc_eng=($2),
common_name=($3),
short_name_loc=($4),
short_name_eng=($5),
invoice_name_loc=($6),
invoice_name_eng=($7),
commercial_name_loc=($8),
commercial_name_eng=($9),
model=($10),
size_spec=($11),
brand=($12),
brand_description=($13),
main_category_code=($14),
sub_category_code=($15),
material_group_code=($16),
material_type_code=($17),
base_um_code=($18),
business_group_key=($19),
product_group_1=($20),
product_group_2=($21),
product_group_3=($22),
product_group_4=($23),
product_group_5=($24),
product_group_6=($25),
coutry_code=($26),
status=($27),
size_dimension=($28),
mat_pack_group_code=($29),
gross_weight=($30),
net_weight_unit=($31),
aging_day=($32),
equi_stock=($33),
unit_pack=($34),
equi_sale=($35),
equi_pur=($36),
equi_price=($37),
equi_analy=($38),
equi_cost=($39),
equi_picking=($40),
equi_delivery=($41),
minimum_order=($42),
equi_pos=($43),
equi_order=($44),
dim_stk_qty=($45),
dim_stk_width=($46),
dim_stk_length=($47),
dim_stk_hight=($48),
dim_stk_weight=($49),
dim_inner_qty=($50),
dim_inner_width=($51),
dim_inner_length=($52),
dim_inner_hight=($53),
dim_inner_weight=($54),
dim_carton_qty=($55),
dim_carton_width=($56),
dim_carton_length=($57),
dim_carton_hight=($58),
dim_carton_weight=($59),
cancel_flag=($60),
last_update_user=($61),
last_update_date=($62),
last_function=($63), company_code=($64)
WHERE material_code=($65) RETURNING *`;
  try {
    const values = [
      FormatDataNull(MaterialList.MaterialDescLocal),
      FormatDataNull(MaterialList.MaterialDescEng),
      FormatDataNull(MaterialList.CommonName),
      FormatDataNull(MaterialList.ShortNameLocal),
      FormatDataNull(MaterialList.ShortNameEng),
      FormatDataNull(MaterialList.InvoiceNameLocal),
      FormatDataNull(MaterialList.InvoiceNameEng),
      FormatDataNull(MaterialList.CommercialName),
      FormatDataNull(MaterialList.CommercialNameEng),
      FormatDataNull(MaterialList.Model),
      FormatDataNull(MaterialList.SizeSpec),
      FormatDataNull(MaterialList.Brand),
      FormatDataNull(MaterialList.BrandDescription),
      FormatDataNull(MaterialList.MainCategoryCode),
      FormatDataNull(MaterialList.SubCategoryCode),
      FormatDataNull(MaterialList.MaterialGroupCode),
      FormatDataNull(MaterialList.MaterialTypeCode),
      FormatDataNull(MaterialList.BaseUmCode),
      FormatDataNull(MaterialList.BusinessGroupKey),
      FormatDataNull(MaterialList.ProductGroup1),
      FormatDataNull(MaterialList.ProductGroup2),
      FormatDataNull(MaterialList.ProductGroup3),
      FormatDataNull(MaterialList.ProductGroup4),
      FormatDataNull(MaterialList.ProductGroup5),
      FormatDataNull(MaterialList.ProductGroup6),
      FormatDataNull(MaterialList.CoutryCode),
      FormatDataNull(MaterialList.Status),
      FormatDataNull(MaterialList.SizeDimension),
      FormatDataNull(MaterialList.MatPackGroupCode),
      FormatDataNull(MaterialList.GrossWeight),
      FormatDataNull(MaterialList.NetWeightUnit),
      FormatDataNull(MaterialList.AgingDay),
      FormatDataNull(MaterialList.EquiStock),
      FormatDataNull(MaterialList.UnitPack),
      FormatDataNull(MaterialList.EquiSale),
      FormatDataNull(MaterialList.EquiPur),
      FormatDataNull(MaterialList.EquiPrice),
      FormatDataNull(MaterialList.EquiAnaly),
      FormatDataNull(MaterialList.EquiCost),
      FormatDataNull(MaterialList.EquiPicking),
      FormatDataNull(MaterialList.EquiDelivery),
      FormatDataNull(MaterialList.MinimumOrder),
      FormatDataNull(MaterialList.EquiPos),
      FormatDataNull(MaterialList.EquiOrder),
      FormatDataNull(MaterialList.DimStkQty),
      FormatDataNull(MaterialList.DimStkWdth),
      FormatDataNull(MaterialList.DimStkLngth),
      FormatDataNull(MaterialList.DimStkHght),
      FormatDataNull(MaterialList.DimStkWgt),
      FormatDataNull(MaterialList.DimInnerQty),
      FormatDataNull(MaterialList.DimInnerWdth),
      FormatDataNull(MaterialList.DimInnerLngth),
      FormatDataNull(MaterialList.DimInnerHght),
      FormatDataNull(MaterialList.DimInnerWgt),
      FormatDataNull(MaterialList.DimCartonQty),
      FormatDataNull(MaterialList.DimCartonWdth),
      FormatDataNull(MaterialList.DimCartonLngth),
      FormatDataNull(MaterialList.DimCartonHght),
      FormatDataNull(MaterialList.DimCartonWgt),
      "N",
      "MDMs",
      new Date(),
      "C",
      company_code,
      FormatDataNull(MaterialList.MaterialNo),
    ];
    await client.query(text, values, async (err, result) => {
      if (!err) {
        console.log("UPDATE SUSCESS fd_oms_pm_material_general");
        console.log(result.rows);
      } else {
        console.log("ERROR UPDATE fd_oms_pm_material_general", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};
// PLANT

const SELECT_MATERIAL_PLANT = async (MaterialPlantList, MaterialNo) => {
  const text = `SELECT COUNT(material_code) FROM fd_oms_pm_material_plant Where plant_code = ($1)  AND material_code = ($2)`;
  const client = await pool.connect();
  let clientError = null;
  const PlantCode = MaterialPlantList.PlantCode;
  try {
    const values = [PlantCode, MaterialNo];
    await client.query(text, values, async (err, result) => {
      if (!err) {
        if (result.rows[0].count != 0) {
          //update
          console.log("UPDATE");
          UPDATE_MATERIAL_PLANT(MaterialPlantList, MaterialNo);
        } else {
          //insert
          console.log("INSERT");
          INSERT_MATERIAL_PLANT(MaterialPlantList, MaterialNo);
        }
        console.log(result.rows);
      } else {
        console.log(err);
      }
    });
  } catch (error) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const UPDATE_MATERIAL_PLANT = async (MaterialPlantList, MaterialNo) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("UPDATE");
  const text = `UPDATE fd_oms_pm_material_plant SET  
  purchase_group_code=($1),
storage_location_code=($2),
valuation_category_code=($3),
serialno_profile_code=($4),
do_not_cost=($5),
price_control_indicator=($6),
block_type=($7),
price_plan=($8),
valuation_class_code=($9),
origin_group_code=($10),
cancel_flag=($11),
last_update_user=($12),
last_update_date=($13),
last_function=($14)
  where 
  plant_code=($15) AND material_code=($16) 
  RETURNING *`;
  const values = [
    MaterialPlantList.PurchasingGroupCode,
    MaterialPlantList.StorageLocationCode,
    MaterialPlantList.ValuationCategoryCode,
    MaterialPlantList.SerialNoProfileCode,
    MaterialPlantList.DoNotCost,
    MaterialPlantList.PriceControlIndicator,
    MaterialPlantList.BlockType,
    MaterialPlantList.PricePlan,
    MaterialPlantList.ValuationClassCode,
    MaterialPlantList.OriginGroupCode,
    "N",
    "MDMs",
    new Date(),
    "C",
    MaterialPlantList.PlantCode,
    MaterialNo,
  ];
  try {
    await client.query(text, values, (err, result) => {
      if (!err) {
        console.log(result.rows);
        console.log("UPDATE SUSCESS");
      } else {
        console.log("ERROR UPDATE", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const INSERT_MATERIAL_PLANT = async (MaterialPlantList, MaterialNo) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("INSERT");
  const text = `INSERT INTO fd_oms_pm_material_plant(
    "plant_code",
"material_code",
"pca_code",
"purchase_group_code",
"storage_location_code",
"valuation_category_code",
"serialno_profile_code",
"do_not_cost",
"price_control_indicator",
"block_type",
"price_plan",
"valuation_class_code",
"origin_group_code",
"cancel_flag",
"user_create",
"create_date",
"last_update_user",
"last_update_date",
"last_function"
    ) 
      VALUES($1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14 ,$15 ,$16 ,$17 ,$18 ,$19 ) 
      RETURNING *`;
  try {
    const values = [
      MaterialPlantList.PlantCode,
      MaterialNo,
      MaterialPlantList.PCACode,
      MaterialPlantList.PurchasingGroupCode,
      MaterialPlantList.StorageLocationCode,
      MaterialPlantList.ValuationCategoryCode,
      MaterialPlantList.SerialNoProfileCode,
      MaterialPlantList.DoNotCost,
      MaterialPlantList.PriceControlIndicator,
      MaterialPlantList.BlockType,
      MaterialPlantList.PricePlan,
      MaterialPlantList.ValuationClassCode,
      MaterialPlantList.OriginGroupCode,
      "N",
      "MDMs",
      new Date(),
      "MDMs",
      new Date(),
      "A",
    ];
    await client.query(text, values, async (err, result) => {
      if (!err) {
        console.log(result.rows);
      } else {
        console.log("ERROR INSERT 7", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

// ALTERNATIVE_UNIT

const SELECT_MATERIAL_ALTERNATIVE_UNIT = async (
  MaterialNo,
  AlternativeUnitList
) => {
  console.log(
    "SELECT_MATERIAL_ALTERNATIVE_UNIT",
    MaterialNo,
    AlternativeUnitList
  );
  const text = `SELECT COUNT(material_code) FROM fd_oms_pm_material_alternative_unit Where material_code=($1) AND alternative_unit=($2) AND alternative_um_code =($3)`;
  const client = await pool.connect();
  let clientError = null;
  try {
    const values = [
      MaterialNo,
      AlternativeUnitList.AlternativeUnit,
      AlternativeUnitList.AlternativeUMCode,
    ];
    await client.query(text, values, async (err, result) => {
      console.log(result.rows[0].count);
      if (result.rows[0].count != 0) {
        //update
        console.log("UPDATE");
        UPDATE_MATERIAL_ALTERNATIVE_UNIT(MaterialNo, AlternativeUnitList);
      } else {
        //insert
        console.log("INSERT");
        INSERT_MATERIAL_ALTERNATIVE_UNIT(MaterialNo, AlternativeUnitList);
      }
      console.log(result.rows);
    });
  } catch (error) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const UPDATE_MATERIAL_ALTERNATIVE_UNIT = async (
  MaterialNo,
  AlternativeUnitList
) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("UPDATE");
  const text = `UPDATE fd_oms_pm_material_alternative_unit SET  
  base_unit=($1),
  base_um_code=($2),
  volumn_unit=($3),
  volumn_um_code=($4),
  gross_weight=($5),
  weight_um_code=($6),
  ean_upc_code=($7),
  ean_upc_name=($8),
  cancel_flag=($9),
  last_update_user=($10),
  last_update_date=($11),
  last_function=($12)
  where 
  material_code=($13) AND
  alternative_um_code=($14) AND
  alternative_unit =($15)
  RETURNING *`;
  const values = [
    AlternativeUnitList.BaseUnit,
    AlternativeUnitList.BaseUMCode,
    AlternativeUnitList.VolumnUnit,
    AlternativeUnitList.VolumnUMCode,
    AlternativeUnitList.GrossWeight,
    AlternativeUnitList.WeightUMCode,
    AlternativeUnitList.EanUPCCode,
    AlternativeUnitList.EanUPCName,
    "N",
    "MDMs",
    new Date(),
    "C",
    MaterialNo,
    AlternativeUnitList.AlternativeUMCode,
    AlternativeUnitList.AlternativeUnit,
  ];
  try {
    await client.query(text, values, (err, result) => {
      if (!err) {
        console.log(result.rows);
        console.log("UPDATE SUSCESS");
      } else {
        console.log("ERROR UPDATE", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const INSERT_MATERIAL_ALTERNATIVE_UNIT = async (
  MaterialNo,
  AlternativeUnitList
) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("INSERT");
  const text = `INSERT INTO fd_oms_pm_material_alternative_unit(
    "material_code",
"alternative_um_code",
"alternative_unit",
"base_unit",
"base_um_code",
"volumn_unit",
"volumn_um_code",
"gross_weight",
"weight_um_code",
"ean_upc_code",
"ean_upc_name",
"cancel_flag",
"user_create",
"create_date",
"last_update_user",
"last_update_date",
"last_function"
    ) VALUES($1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14 ,$15 ,$16 ,$17 ) 
    RETURNING *`;
  try {
    const values = [
      MaterialNo,
      AlternativeUnitList.AlternativeUMCode,
      AlternativeUnitList.AlternativeUnit,
      AlternativeUnitList.BaseUnit,
      AlternativeUnitList.BaseUMCode,
      AlternativeUnitList.VolumnUnit,
      AlternativeUnitList.VolumnUMCode,
      AlternativeUnitList.GrossWeight,
      AlternativeUnitList.WeightUMCode,
      AlternativeUnitList.EanUPCCode,
      AlternativeUnitList.EanUPCName,
      "N",
      "MDMs",
      new Date(),
      "MDMs",
      new Date(),
      "A",
    ];
    await client.query(text, values, async (err, result) => {
      if (!err) {
        console.log(result.rows);
      } else {
        console.log("ERROR INSERT 6", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

// TYPE

const SELECT_MATERIAL_TYPE = async (MaterialList) => {
  const text = `SELECT COUNT(material_type_code) FROM fd_oms_pm_material_type Where material_type_code=($1)`;
  const client = await pool.connect();
  let clientError = null;
  try {
    values = [MaterialList.MaterialTypeCode];
    await client.query(text, values, async (err, result) => {
      if (!err) {
        if (result.rows[0].count != 0) {
          //update
          console.log("UPDATE");
          UPDATE_MATERIAL_TYPE(MaterialList);
        } else {
          //insert
          console.log("INSERT");
          INSERT_MATERIAL_TYPE(MaterialList);
        }
        console.log(result.rows);
      } else {
        console.log(err);
      }
    });
  } catch (error) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const UPDATE_MATERIAL_TYPE = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("UPDATE");
  const text = `UPDATE fd_oms_pm_material_type SET  
  desc_loc=($1),
  desc_eng=($2),
  cancel_flag=($3),
  last_update_user=($4),
  last_update_date=($5),
  last_function=($6)
  where 
  material_type_code=($7)
  RETURNING *`;
  const values = [
    MaterialList.MaterialTypeName,
    MaterialList.MaterialTypeName,
    "N",
    "MDMs",
    new Date(),
    "A",
    MaterialList.MaterialTypeCode,
  ];
  try {
    await client.query(text, values, (err, result) => {
      if (!err) {
        console.log(result.rows);
        console.log("UPDATE SUSCESS");
      } else {
        console.log("ERROR UPDATE", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const INSERT_MATERIAL_TYPE = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("INSERT");
  const text = `INSERT INTO fd_oms_pm_material_type(
    "material_type_code",
    "desc_eng",
    "cancel_flag",
    "user_create",
    "create_date",
    "last_update_user",
    "last_update_date",
    "last_function"
    ) 
      VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`;
  try {
    const values = [
      MaterialList.MaterialTypeCode,
      MaterialList.MaterialTypeName,
      "N",
      "MDMs",
      new Date(),
      "MDMs",
      new Date(),
      "A",
    ];
    await client.query(text, values, async (err, result) => {
      if (!err) {
        console.log(result.rows);
      } else {
        console.log("ERROR INSERT 5", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

//GROUP

const SELECT_MATERIAL_GROUP = async (MaterialList, company_code) => {
  console.log("Aoy Test ");
  console.log("** Log Aoy Company_Code :", company_code);
  const text = `SELECT COUNT(material_group_code) FROM fd_oms_pm_material_group Where material_group_code=($1)`;
  const client = await pool.connect();
  let clientError = null;
  try {
    values = [MaterialList.MaterialGroupCode];
    await client.query(text, values, async (err, result) => {
      if (!err) {
        if (result.rows[0].count != 0) {
          //update
          console.log("** Log Aoy Company_Code 2 :", company_code);
          console.log("UPDATE fd_oms_pm_material_group");
          await UPDATE_MATERIAL_GROUP(MaterialList, company_code);
        } else {
          //insert
          console.log("INSERT fd_oms_pm_material_group");
          await INSERT_MATERIAL_GROUP(MaterialList, company_code);
        }
        console.log(result.rows);
      } else {
        console.log(err);
      }
    });
  } catch (error) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const UPDATE_MATERIAL_GROUP = async (MaterialList, company_code) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("** Log Company_Code :", company_code);
  const text = `UPDATE fd_oms_pm_material_group SET  
  desc_loc=($1),
  desc_eng=($2),
  cancel_flag=($3),
  last_update_user=($4),
  last_update_date=($5),
  last_function =($6),
  company_code=($7)
  where 
  material_group_code=($8) 
  RETURNING *`;
  const values = [
    MaterialList.MaterialGroupName,
    MaterialList.MaterialGroupName,
    "N",
    "MDMs",
    new Date(),
    "C",
    company_code,
    MaterialList.MaterialGroupCode,
  ];
  try {
    await client.query(text, values, (err, result) => {
      if (!err) {
        console.log(result.rows);
        console.log("UPDATE SUSCESS fd_oms_pm_material_group");
      } else {
        console.log("ERROR UPDATE fd_oms_pm_material_group", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const INSERT_MATERIAL_GROUP = async (MaterialList, company_code) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("INSERT fd_oms_pm_material_group");
  console.log("** Log Company_Code :", company_code);
  const text = `INSERT INTO fd_oms_pm_material_group(
    "material_group_code",
    "desc_eng",
    "cancel_flag",
    "user_create",
    "create_date",
    "last_update_user",
    "last_update_date",
    "last_function",
    "company_code"
     ) 
     VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) 
     RETURNING *`;
  try {
    const values = [
      MaterialList.MaterialGroupCode,
      MaterialList.MaterialGroupName,
      "N",
      "MDMs",
      new Date(),
      "MDMs",
      new Date(),
      "A",
      company_code,
    ];
    await client.query(text, values, async (err, result) => {
      if (!err) {
        console.log(result.rows);
      } else {
        console.log("ERROR INSERT 4", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

// MAIN_CATEGORY

const SELECT_MATERIAL_MAIN_CATEGORY = async (MaterialList) => {
  const text = `SELECT COUNT(main_category_code) FROM fd_oms_pm_material_main_category Where main_category_code=($1)`;
  const client = await pool.connect();
  let clientError = null;
  try {
    values = [MaterialList.MainCategoryCode];
    await client.query(text, values, async (err, result) => {
      if (result.rows[0].count != 0) {
        //update
        console.log("UPDATE");
        UPDATE_MATERIAL_MAIN_CATEGORY(MaterialList);
      } else {
        //insert
        console.log("INSERT");
        INSERT_MATERIAL_MAIN_CATEGORY(MaterialList);
      }
      console.log(result.rows);
    });
  } catch (error) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const UPDATE_MATERIAL_MAIN_CATEGORY = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("UPDATE");
  const text = `UPDATE fd_oms_pm_material_main_category SET  
  desc_loc=($1),
  desc_eng=($2),
  cancel_flag=($3),
  last_update_user=($4),
  last_update_date=($5),
  last_function =($6)
  where 
  main_category_code=($7) 
  RETURNING *`;
  const values = [
    MaterialList.MainCategoryName,
    MaterialList.MainCategoryName,
    "N",
    "MDMs",
    new Date(),
    "A",
    MaterialList.MainCategoryCode,
  ];
  try {
    await client.query(text, values, (err, result) => {
      if (!err) {
        console.log(result.rows);
        console.log("UPDATE SUSCESS");
      } else {
        console.log("ERROR UPDATE", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const INSERT_MATERIAL_MAIN_CATEGORY = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("INSERT");
  const text = `INSERT INTO fd_oms_pm_material_main_category(
    "main_category_code",
    "desc_eng",
    "cancel_flag",
    "user_create",
    "create_date",
    "last_update_user",
    "last_update_date",
    "last_function"
    )
    VALUES($1, $2,$3,$4,$5,$6,$7,$8)
    RETURNING *`;
  try {
    const values = [
      MaterialList.MainCategoryCode,
      MaterialList.MainCategoryName,
      "N",
      "MDMs",
      new Date(),
      "MDMs",
      new Date(),
      "A",
    ];
    await client.query(text, values, async (err, result) => {
      if (!err) {
        console.log(result.rows);
      } else {
        console.log("ERROR INSERT 3", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

//SUB_CATEGORY

const SELECT_MATERIAL_SUB_CATEGORY = async (MaterialList) => {
  const text = `SELECT COUNT(sub_category_code) FROM fd_oms_pm_material_sub_category Where  sub_category_code=($1)`;
  const client = await pool.connect();
  let clientError = null;
  console.log(MaterialList.SubCategoryCode);
  try {
    values = [MaterialList.SubCategoryCode];
    await client.query(text, values, async (err, result) => {
      if (result.rows[0].count != 0) {
        //update
        console.log("UPDATE");
        UPDATE_MATERIAL_SUB_CATEGORY(MaterialList);
      } else {
        //insert
        console.log("INSERT");
        INSERT_MATERIAL_SUB_CATEGORY(MaterialList);
      }
      console.log(result.rows);
    });
  } catch (error) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const UPDATE_MATERIAL_SUB_CATEGORY = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("UPDATE");
  const text = `UPDATE fd_oms_pm_material_sub_category SET  
  desc_loc=($1),
  desc_eng=($2),
  cancel_flag=($3),
  last_update_user=($4),
  last_update_date=($5),
  last_function =($6)
  where 
  sub_category_code=($7) 
  RETURNING *`;
  const values = [
    MaterialList.SubCategoryName,
    MaterialList.SubCategoryName,
    "N",
    "MDMs",
    new Date(),
    "A",
    MaterialList.SubCategoryCode,
  ];
  try {
    await client.query(text, values, (err, result) => {
      if (!err) {
        console.log(result.rows);
        console.log("UPDATE SUSCESS");
      } else {
        console.log("ERROR UPDATE", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const INSERT_MATERIAL_SUB_CATEGORY = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("INSERT");
  const text = `INSERT INTO fd_oms_pm_material_sub_category(
    "sub_category_code",
    "desc_eng",
    "cancel_flag",
    "user_create",
    "create_date",
    "last_update_user",
    "last_update_date",
    "last_function"
    )
    VALUES($1, $2,$3,$4,$5,$6,$7,$8)
    RETURNING *`;
  try {
    const values = [
      MaterialList.SubCategoryCode,
      MaterialList.SubCategoryName,
      "N",
      "MDMs",
      new Date(),
      "MDMs",
      new Date(),
      "A",
    ];
    await client.query(text, values, async (err, result) => {
      if (!err) {
        console.log(result.rows);
      } else {
        console.log("ERROR INSERT 2", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const SELECT_MATERIAL_UNIT = async (MaterialList) => {
  const text = `SELECT COUNT(unit_code) FROM fd_oms_pm_material_unit Where unit_code=($1) `;
  const client = await pool.connect();
  let clientError = null;
  try {
    values = [MaterialList.BaseUmCode];
    await client.query(text, values, async (err, result) => {
      if (result.rows[0].count != 0) {
        //update
        console.log("UPDATE");
        UPDATE_MATERIAL_UNIT(MaterialList);
      } else {
        //insert
        console.log("INSERT");
        INSERT_MATERIAL_UNIT(MaterialList);
      }
      console.log(result.rows);
    });
  } catch (error) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const UPDATE_MATERIAL_UNIT = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("UPDATE");
  const text = `UPDATE fd_oms_pm_material_unit SET  
  desc_loc=($1),
  desc_eng=($2),
  cancel_flag=($3),
  last_update_user=($4),
  last_update_date=($5),
  last_function =($6)
  where 
  unit_code=($7)  RETURNING *`;
  const values = [
    MaterialList.BaseUmName,
    MaterialList.BaseUmName,
    "N",
    "MDMs",
    new Date(),
    "A",
    MaterialList.BaseUmCode,
  ];
  try {
    await client.query(text, values, (err, result) => {
      if (!err) {
        console.log(result.rows);
        console.log("UPDATE SUSCESS");
      } else {
        console.log("ERROR UPDATE", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const INSERT_MATERIAL_UNIT = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("INSERT");
  // ! unit_code ใช้ key BaseUMCode ?
  const text = `INSERT INTO fd_oms_pm_material_unit(
    "unit_code",
    "desc_loc",
    "desc_eng",
    "cancel_flag",
    "user_create",
    "create_date",
    "last_update_user",
    "last_update_date",
    "last_function"
    )
    VALUES($1, $2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`;
  try {
    const values = [
      MaterialList.BaseUMCode,
      MaterialList.BaseUMNameLocal,
      MaterialList.BaseUMNameEng,
      "N",
      "MDMs",
      new Date(),
      "MDMs",
      new Date(),
      "A",
    ];
    await client.query(text, values, async (err, result) => {
      if (!err) {
        console.log(result.rows);
      } else {
        console.log("ERROR INSERT 1", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

//Product Group1

const SELECT_MATERIAL_PRODUCT_GROUP_1 = async (MaterialList) => {
  const text = `SELECT COUNT(product_group_1) FROM fd_oms_pm_material_product_group_1 Where product_group_1= ($1)`;
  const client = await pool.connect();
  let clientError = null;
  try {
    values = [MaterialList.ProductGroup1];
    await client.query(text, values, async (err, result) => {
      if (result.rows[0].count != 0) {
        //update
        console.log("UPDATE");
        UPDATE_MATERIAL_PRODUCT_GROUP_1(MaterialList);
      } else {
        //insert
        console.log("INSERT");
        INSERT_MATERIAL_PRODUCT_GROUP_1(MaterialList);
      }
      console.log(result.rows);
    });
  } catch (error) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const UPDATE_MATERIAL_PRODUCT_GROUP_1 = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("UPDATE");
  const text = `UPDATE fd_oms_pm_material_product_group_1 SET  
  desc_loc=($1),
  desc_eng=($2),
  cancel_flag=($3),
  last_update_user=($4),
  last_update_date=($5),
  last_function =($6)
  where 
  product_group_1=($7)
  RETURNING *`;
  const values = [
    MaterialList.ProductGroupName1,
    MaterialList.ProductGroupName1,
    "N",
    "MDMs",
    new Date(),
    "A",
    MaterialList.ProductGroup1,
  ];
  try {
    await client.query(text, values, (err, result) => {
      if (!err) {
        console.log(result.rows);
        console.log("UPDATE SUSCESS");
      } else {
        console.log("ERROR UPDATE", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const INSERT_MATERIAL_PRODUCT_GROUP_1 = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("INSERT");
  const text = `INSERT INTO fd_oms_pm_material_product_group_1(
    "product_group_1",
    "desc_eng",
    "cancel_flag",
    "user_create",
    "create_date",
    "last_update_user",
    "last_update_date",
    "last_function"
    )
    VALUES($1, $2,$3,$4,$5,$6,$7,$8)
    RETURNING *`;
  try {
    const values = [
      MaterialList.ProductGroup1,
      MaterialList.ProductGroupName1,
      "N",
      "MDMs",
      new Date(),
      "MDMs",
      new Date(),
      "A",
    ];
    await client.query(text, values, async (err, result) => {
      if (!err) {
        console.log(result.rows);
      } else {
        console.log("ERROR INSERT 8", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

//Product Group2

const SELECT_MATERIAL_PRODUCT_GROUP_2 = async (MaterialList) => {
  const text = `SELECT COUNT(product_group_2) FROM fd_oms_pm_material_product_group_2 Where product_group_2= ($1)`;
  const client = await pool.connect();
  let clientError = null;
  try {
    values = [MaterialList.ProductGroup2];
    await client.query(text, values, async (err, result) => {
      if (result.rows[0].count != 0) {
        //update
        console.log("UPDATE");
        UPDATE_MATERIAL_PRODUCT_GROUP_2(MaterialList);
      } else {
        //insert
        console.log("INSERT");
        INSERT_MATERIAL_PRODUCT_GROUP_2(MaterialList);
      }
      console.log(result.rows);
    });
  } catch (error) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const UPDATE_MATERIAL_PRODUCT_GROUP_2 = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("UPDATE");
  const text = `UPDATE fd_oms_pm_material_product_group_2 SET  
  desc_loc=($1),
  desc_eng=($2),
  cancel_flag=($3),
  last_update_user=($4),
  last_update_date=($5),
  last_function =($6)
  where 
  product_group_2=($7)
  RETURNING *`;
  const values = [
    MaterialList.ProductGroupName2,
    MaterialList.ProductGroupName2,
    "N",
    "MDMs",
    new Date(),
    "A",
    MaterialList.ProductGroup2,
  ];
  try {
    await client.query(text, values, (err, result) => {
      if (!err) {
        console.log(result.rows);
        console.log("UPDATE SUSCESS");
      } else {
        console.log("ERROR UPDATE", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const INSERT_MATERIAL_PRODUCT_GROUP_2 = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("INSERT");
  const text = `INSERT INTO fd_oms_pm_material_product_group_2(
    "product_group_2",
    "desc_eng",
    "cancel_flag",
    "user_create",
    "create_date",
    "last_update_user",
    "last_update_date",
    "last_function"
    )
    VALUES($1, $2,$3,$4,$5,$6,$7,$8)
    RETURNING *`;
  try {
    const values = [
      MaterialList.ProductGroup2,
      MaterialList.ProductGroupName2,
      "N",
      "MDMs",
      new Date(),
      "MDMs",
      new Date(),
      "A",
    ];
    await client.query(text, values, async (err, result) => {
      if (!err) {
        console.log(result.rows);
      } else {
        console.log("ERROR INSERT 9", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

//Product Group3
const SELECT_MATERIAL_PRODUCT_GROUP_3 = async (MaterialList) => {
  const text = `SELECT COUNT(product_group_3) FROM fd_oms_pm_material_product_group_3 Where product_group_3= ($1)`;
  const client = await pool.connect();
  let clientError = null;
  try {
    values = [MaterialList.ProductGroup3];
    await client.query(text, values, async (err, result) => {
      if (result.rows[0].count != 0) {
        //update
        console.log("UPDATE");
        UPDATE_MATERIAL_PRODUCT_GROUP_3(MaterialList);
      } else {
        //insert
        console.log("INSERT");
        INSERT_MATERIAL_PRODUCT_GROUP_3(MaterialList);
      }
      console.log(result.rows);
    });
  } catch (error) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const UPDATE_MATERIAL_PRODUCT_GROUP_3 = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("UPDATE");
  const text = `UPDATE fd_oms_pm_material_product_group_3 SET  
  desc_loc=($1),
  desc_eng=($2),
  cancel_flag=($3),
  last_update_user=($4),
  last_update_date=($5),
  last_function =($6)
  where 
  product_group_3=($7)
  RETURNING *`;
  const values = [
    MaterialList.ProductGroupName3,
    MaterialList.ProductGroupName3,
    "N",
    "MDMs",
    new Date(),
    "A",
    MaterialList.ProductGroup3,
  ];
  try {
    await client.query(text, values, (err, result) => {
      if (!err) {
        console.log(result.rows);
        console.log("UPDATE SUSCESS");
      } else {
        console.log("ERROR UPDATE", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const INSERT_MATERIAL_PRODUCT_GROUP_3 = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("INSERT");
  const text = `INSERT INTO fd_oms_pm_material_product_group_3(
    "product_group_3",
    "desc_eng",
    "cancel_flag",
    "user_create",
    "create_date",
    "last_update_user",
    "last_update_date",
    "last_function"
    )
    VALUES($1, $2,$3,$4,$5,$6,$7,$8)
    RETURNING *`;
  try {
    const values = [
      MaterialList.ProductGroup3,
      MaterialList.ProductGroupName3,
      "N",
      "MDMs",
      new Date(),
      "MDMs",
      new Date(),
      "A",
    ];
    await client.query(text, values, async (err, result) => {
      if (!err) {
        console.log(result.rows);
      } else {
        console.log("ERROR INSERT 10", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

//Product Group4
const SELECT_MATERIAL_PRODUCT_GROUP_4 = async (MaterialList) => {
  const text = `SELECT COUNT(product_group_4) FROM fd_oms_pm_material_product_group_4 Where product_group_4= ($1)`;
  const client = await pool.connect();
  let clientError = null;
  try {
    values = [MaterialList.ProductGroup4];
    await client.query(text, values, async (err, result) => {
      if (result.rows[0].count != 0) {
        //update
        console.log("UPDATE");
        UPDATE_MATERIAL_PRODUCT_GROUP_4(MaterialList);
      } else {
        //insert
        console.log("INSERT");
        INSERT_MATERIAL_PRODUCT_GROUP_4(MaterialList);
      }
      console.log(result.rows);
    });
  } catch (error) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const UPDATE_MATERIAL_PRODUCT_GROUP_4 = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("UPDATE");
  const text = `UPDATE fd_oms_pm_material_product_group_4 SET  
  desc_loc=($1),
  desc_eng=($2),
  cancel_flag=($3),
  last_update_user=($4),
  last_update_date=($5),
  last_function =($6)
  where 
  product_group_4=($7)
  RETURNING *`;
  const values = [
    MaterialList.ProductGroupName4,
    MaterialList.ProductGroupName4,
    "N",
    "MDMs",
    new Date(),
    "A",
    MaterialList.ProductGroup4,
  ];
  try {
    await client.query(text, values, (err, result) => {
      if (!err) {
        console.log(result.rows);
        console.log("UPDATE SUSCESS");
      } else {
        console.log("ERROR UPDATE", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const INSERT_MATERIAL_PRODUCT_GROUP_4 = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("INSERT");
  const text = `INSERT INTO fd_oms_pm_material_product_group_4(
    "product_group_4",
    "desc_eng",
    "cancel_flag",
    "user_create",
    "create_date",
    "last_update_user",
    "last_update_date",
    "last_function"
    )
    VALUES($1, $2,$3,$4,$5,$6,$7,$8)
    RETURNING *`;
  try {
    const values = [
      MaterialList.ProductGroup4,
      MaterialList.ProductGroupName4,
      "N",
      "MDMs",
      new Date(),
      "MDMs",
      new Date(),
      "A",
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
  }
};

//Product Group5

const SELECT_MATERIAL_PRODUCT_GROUP_5 = async (MaterialList) => {
  const text = `SELECT COUNT(product_group_5) FROM fd_oms_pm_material_product_group_5 Where product_group_5= ($1)`;
  const client = await pool.connect();
  let clientError = null;
  try {
    values = [MaterialList.ProductGroup5];
    await client.query(text, values, async (err, result) => {
      if (result.rows[0].count != 0) {
        //update
        console.log("UPDATE");
        UPDATE_MATERIAL_PRODUCT_GROUP_5(MaterialList);
      } else {
        //insert
        console.log("INSERT");
        INSERT_MATERIAL_PRODUCT_GROUP_5(MaterialList);
      }
      console.log(result.rows);
    });
  } catch (error) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const UPDATE_MATERIAL_PRODUCT_GROUP_5 = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("UPDATE");
  const text = `UPDATE fd_oms_pm_material_product_group_5 SET  
  desc_loc=($1),
  desc_eng=($2),
  cancel_flag=($3),
  last_update_user=($4),
  last_update_date=($5),
  last_function =($6)
  where 
  product_group_5=($7)
  RETURNING *`;
  const values = [
    MaterialList.ProductGroupName5,
    MaterialList.ProductGroupName5,
    "N",
    "MDMs",
    new Date(),
    "A",
    MaterialList.ProductGroup5,
  ];
  try {
    await client.query(text, values, (err, result) => {
      if (!err) {
        console.log(result.rows);
        console.log("UPDATE SUSCESS");
      } else {
        console.log("ERROR UPDATE", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const INSERT_MATERIAL_PRODUCT_GROUP_5 = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("INSERT");
  const text = `INSERT INTO fd_oms_pm_material_product_group_5(
    "product_group_5",
    "desc_eng",
    "cancel_flag",
    "user_create",
    "create_date",
    "last_update_user",
    "last_update_date",
    "last_function"
    )
    VALUES($1, $2,$3,$4,$5,$6,$7,$8)
    RETURNING *`;
  try {
    const values = [
      MaterialList.ProductGroup5,
      MaterialList.ProductGroupName5,
      "N",
      "MDMs",
      new Date(),
      "MDMs",
      new Date(),
      "A",
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
  }
};

//Product Group6

const SELECT_MATERIAL_PRODUCT_GROUP_6 = async (MaterialList) => {
  const text = `SELECT COUNT(product_group_6) FROM fd_oms_pm_material_product_group_6 Where product_group_6= ($1)`;
  const client = await pool.connect();
  let clientError = null;
  try {
    values = [MaterialList.ProductGroup6];
    await client.query(text, values, async (err, result) => {
      if (result.rows[0].count != 0) {
        //update
        console.log("UPDATE");
        UPDATE_MATERIAL_PRODUCT_GROUP_6(MaterialList);
      } else {
        //insert
        console.log("INSERT");
        INSERT_MATERIAL_PRODUCT_GROUP_6(MaterialList);
      }
      console.log(result.rows);
    });
  } catch (error) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const UPDATE_MATERIAL_PRODUCT_GROUP_6 = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("UPDATE");
  const text = `UPDATE fd_oms_pm_material_product_group_6 SET  
  desc_loc=($1),
  desc_eng=($2),
  cancel_flag=($3),
  last_update_user=($4),
  last_update_date=($5),
  last_function =($6)
  where 
  product_group_6=($7)
  RETURNING *`;
  const values = [
    MaterialList.ProductGroupName6,
    MaterialList.ProductGroupName6,
    "N",
    "MDMs",
    new Date(),
    "A",
    MaterialList.ProductGroup6,
  ];
  try {
    await client.query(text, values, (err, result) => {
      if (!err) {
        console.log(result.rows);
        console.log("UPDATE SUSCESS");
      } else {
        console.log("ERROR UPDATE", err);
      }
    });
  } catch (err) {
    clientError = err;
  } finally {
    client.release(clientError);
  }
};

const INSERT_MATERIAL_PRODUCT_GROUP_6 = async (MaterialList) => {
  const client = await pool.connect();
  let clientError = null;
  console.log("INSERT");
  const text = `INSERT INTO fd_oms_pm_material_product_group_6(
    "product_group_6",
    "desc_eng",
    "cancel_flag",
    "user_create",
    "create_date",
    "last_update_user",
    "last_update_date",
    "last_function"
    )
    VALUES($1, $2,$3,$4,$5,$6,$7,$8)
    RETURNING *`;
  try {
    const values = [
      MaterialList.ProductGroup6,
      MaterialList.ProductGroupName6,
      "N",
      "MDMs",
      new Date(),
      "MDMs",
      new Date(),
      "A",
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
  }
};

module.exports = { ALL_SELECT };
