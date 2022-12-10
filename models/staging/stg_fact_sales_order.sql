WITH stg_fact_sales_order_source AS (

  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__orders`

),

fact_sales_order_rename AS (

  SELECT
    order_id      AS sales_order_key,
    customer_id   AS customer_key,
    picked_by_person_id AS picked_by_person_key,
    salesperson_person_id	 AS salesperson_person_key,
    order_date    AS order_date,
    backorder_order_id AS backorder_order_key,
    expected_delivery_date AS expected_delivery_date,
    customer_purchase_order_number AS customer_purchase_order_number,
    is_undersupply_backordered AS is_undersupply_backordered

  FROM stg_fact_sales_order_source

),

fact_sales_order_convertdatatype AS (

  SELECT
    CAST (sales_order_key AS INTEGER ) AS sales_order_key,
    CAST (customer_key AS INTEGER )    AS customer_key,
    CAST ( picked_by_person_key AS INTEGER) AS picked_by_person_key,
    CAST ( salesperson_person_key AS INTEGER) AS salesperson_person_key,
    CAST ( order_date AS DATE) AS order_date,
    CAST ( backorder_order_key AS INTEGER) AS backorder_order_key,
    CAST ( expected_delivery_date AS DATE) AS expected_delivery_date,
    CAST ( customer_purchase_order_number AS STRING) AS customer_purchase_order_number,
    CAST ( is_undersupply_backordered AS BOOLEAN) AS is_undersupply_backordered

  FROM fact_sales_order_rename

)

SELECT
  sales_order_key,
  customer_key,
  COALESCE(salesperson_person_key, 0) AS salesperson_person_key,
  is_undersupply_backordered,
  order_date,
  COALESCE(picked_by_person_key, 0 ) AS picked_by_person_key,
  COALESCE(backorder_order_key, 0 ) AS backorder_order_key,
  expected_delivery_date,
  customer_purchase_order_number
FROM fact_sales_order_convertdatatype
