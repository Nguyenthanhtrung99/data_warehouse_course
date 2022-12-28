WITH undersupply_backordered AS (

  SELECT DISTINCT is_undersupply_backordered
  FROM {{ ref('stg_fact_sales_order') }}

),

Packed_Type AS (

  SELECT DISTINCT package_type_id, package_type_name
  FROM `vit-lam-data.wide_world_importers.warehouse__package_types`

)

SELECT *, FARM_FINGERPRINT( CONCAT(package_type_id, is_undersupply_backordered)) AS Indicator_Key
FROM undersupply_backordered, Packed_Type