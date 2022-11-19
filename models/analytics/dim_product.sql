WITH dim_product_source AS (

  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`

),

dim_product_renamecolumn AS (

  SELECT
    stock_item_id     AS product_key,
    stock_item_name   AS product_name,
    brand             AS brand_name,
    customer_id       AS customer_key,
    customer_name     AS customer_name
  FROM dim_product_source

),

dim_product_convertdatatype AS(

  SELECT
    CAST(product_key     AS INTEGER)   AS product_key,
    CAST(product_name    AS STRING)    AS product_name,
    CAST(brand_name      AS STRING)    AS brand_name,
    CAST(customer_key    AS INTEGER)   AS customer_key,
    CAST(customer_name   AS STRING)    AS customer_name
  FROM dim_product_renamecolumn
)

SELECT
  product_key,
  product_name,
  brand_name,
  customer_key,
  customer_name
FROM dim_product_convertdatatype


