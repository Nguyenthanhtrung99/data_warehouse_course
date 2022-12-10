WITH fact_sales_order_line_source AS (

  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`

),

fact_sales_order_source AS (

  SELECT
    sales_order_key,
    customer_key,
    order_date,
    picked_by_person_key,
    salesperson_person_key,
    is_undersupply_backordered,
    backorder_order_key,
    expected_delivery_date,
    customer_purchase_order_number
  FROM {{ ref('stg_fact_sales_order') }}

),

fact_sales_order_line_rename AS (

  SELECT
    order_line_id   AS sales_order_line_key,
    stock_item_id   AS product_key,
    quantity        AS quantity,
    unit_price      AS unit_price,
    package_type_id AS package_type_key,
    tax_rate        AS tax_rate,
    picked_quantity AS picked_quantity,
    picking_completed_when AS picking_completed_when
  FROM fact_sales_order_line_source

),

fact_sales_order_line_convertdatatype AS (

  SELECT
    CAST(sales_order_line_key AS INTEGER )            AS sales_order_line_key,
    CAST(product_key AS INTEGER )                     AS product_key,
    CAST(quantity AS INTEGER )                        AS quantity,
    CAST(unit_price AS NUMERIC )                      AS unit_price,
    CAST(package_type_key AS INTEGER )                AS package_type_key,
    CAST(tax_rate AS NUMERIC )                        AS tax_rate,
    CAST(picked_quantity AS INTEGER )                 AS picked_quantity,
    CAST(picking_completed_when AS TIMESTAMP )        AS picking_completed_when

  FROM fact_sales_order_line_rename

),

fact_sales_order_line_calculate_fact AS (

SELECT
  sales_order_line_key,
  product_key,
  quantity,
  unit_price,
  package_type_key,
  tax_rate,
  picked_quantity,
  picking_completed_when,
  quantity*unit_price AS gross_amount
FROM fact_sales_order_line_convertdatatype

)

SELECT
  Sales_Order.sales_order_key,
  Sales_Order.customer_key,
  Sales_Order.order_date,
  Sales_Order.picked_by_person_key,
  Sales_Order.salesperson_person_key,
  Sales_Order.is_undersupply_backordered,
  Sales_Order.backorder_order_key,
  Sales_Order.expected_delivery_date,
  Sales_Order.customer_purchase_order_number,
  Sales_Order_Line.sales_order_line_key,
  Sales_Order_Line.product_key,
  Sales_Order_Line.package_type_key,
  Sales_Order_Line.tax_rate,
  Sales_Order_Line.picked_quantity,
  Sales_Order_Line.picking_completed_when,
  Sales_Order_Line.quantity,
  Sales_Order_Line.unit_price,
  Sales_Order_Line.gross_amount
FROM fact_sales_order_line_calculate_fact AS Sales_Order_Line
LEFT JOIN fact_sales_order_source AS Sales_Order
ON Sales_Order_Line.sales_order_line_key = Sales_Order.sales_order_key
