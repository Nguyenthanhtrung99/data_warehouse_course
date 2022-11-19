WITH dim_sale_customer_source AS (

  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.sales__customers`

), sale_customer_rename AS (

SELECT
  customer_id AS customer_key,
  customer_name AS customer_name
FROM dim_sale_customer_source

), sale_customer_casttype AS (

SELECT
    CAST(customer_key     AS INTEGER)   AS product_key,
    CAST(customer_name    AS STRING)    AS product_name,
FROM sale_customer_rename

)

SELECT
  product_key,
  product_name
FROM sale_customer_casttype