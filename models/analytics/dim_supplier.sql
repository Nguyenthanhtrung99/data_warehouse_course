WITH Purchasing_Supplier_Source AS (

  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`

),

Purchasing_Supplier_Rename AS (

  SELECT
    supplier_id   AS supplier_key,
    supplier_name AS supplier_name
  FROM Purchasing_Supplier_Source

),

Purchasing_Supplier_ConvertType AS (

  SELECT
    CAST(supplier_key AS INTEGER) AS supplier_key,
    CAST(supplier_name AS STRING) AS supplier_name
  FROM Purchasing_Supplier_Rename

)

SELECT
  supplier_key,
  supplier_name
FROM Purchasing_Supplier_ConvertType