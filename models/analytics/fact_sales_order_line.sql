WITH fact_sales_order_line_source AS (

  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`

),

fact_sales_order_line_rename AS (

  SELECT
    order_line_id   AS sales_order_line_key,
    stock_item_id   AS product_key,
    quantity        AS quantity,
    unit_price      AS unit_price
  FROM fact_sales_order_line_source

),

fact_sales_order_line_convertdatatype AS (

  SELECT
    CAST (sales_order_line_key AS INTEGER )   AS sales_order_line_key,
    CAST (product_key AS INTEGER )            AS product_key,
    CAST (quantity AS INTEGER )               AS quantity,
    CAST (unit_price AS NUMERIC )             AS unit_price
  FROM fact_sales_order_line_rename

),

fact_sales_order_line_calculate_fact AS (

SELECT
  sales_order_line_key,
  product_key,
  quantity,
  unit_price,
  quantity*unit_price AS gross_amount
FROM fact_sales_order_line_convertdatatype

)

SELECT
  sales_order_line_key,
  product_key,
  quantity,
  unit_price,
  gross_amount
FROM fact_sales_order_line_calculate_fact
