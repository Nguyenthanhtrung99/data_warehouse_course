WITH stg_fact_sales_order_source AS (

  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__orders`

),

fact_sales_order_rename AS (

  SELECT
    order_id      AS sales_order_key,
    customer_id   AS customer_key,
    picked_by_person_id AS picked_by_person_key

  FROM stg_fact_sales_order_source

),

fact_sales_order_convertdatatype AS (

  SELECT
    CAST (sales_order_key AS INTEGER ) AS sales_order_key,
    CAST (customer_key AS INTEGER )    AS customer_key,
    CAST ( picked_by_person_key AS INTEGER) AS picked_by_person_key

  FROM fact_sales_order_rename

)

SELECT
  sales_order_key,
  customer_key,
  picked_by_person_key
FROM fact_sales_order_convertdatatype
