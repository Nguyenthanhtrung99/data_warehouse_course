WITH fact_sales_order_line_source AS (

  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`

),

fact_sales_order_source AS (

  SELECT
    sales_order_key,
    customer_key,
    picked_by_person_key AS   picked_by_person_key
  FROM {{ ref('stg_fact_sales_order') }}

),

fact_sales_order_line_rename AS (

  SELECT
    order_line_id   AS sales_order_line_key,
    stock_item_id   AS product_key,
    quantity        AS quantity,
    unit_price      AS unit_price,
  FROM fact_sales_order_line_source

),

fact_sales_order_line_convertdatatype AS (

  SELECT
    CAST (sales_order_line_key AS INTEGER )   AS sales_order_line_key,
    CAST (product_key AS INTEGER )            AS product_key,
    CAST (quantity AS INTEGER )               AS quantity,
    CAST (unit_price AS NUMERIC )             AS unit_price,

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
  Sales_Order_Line.sales_order_line_key,
  Sales_Order_Line.product_key,
  Sales_Order_Line.quantity,
  Sales_Order_Line.unit_price,
  Sales_Order_Line.gross_amount,
  Sales_Order.sales_order_key,
  Sales_Order.customer_key,
  Sales_Order.picked_by_person_key
FROM fact_sales_order_line_calculate_fact AS Sales_Order_Line
LEFT JOIN fact_sales_order_source AS Sales_Order
ON Sales_Order_Line.sales_order_line_key = Sales_Order.sales_order_key
