WITH stg_fact_purchase_order_Source AS (

  SELECT *
  FROM `vit-lam-data.wide_world_importers.purchasing__purchase_order_lines`

),

fact_purchase_order_rename AS (

  SELECT
    purchase_order_line_id         AS purchase_order_line_key,
    purchase_order_id              AS purchase_order_key,
    stock_item_id                  AS stock_item_key,
    ordered_outers	               AS ordered_outers,
    `description`                  AS `description`,
    received_outers                AS received_outers,
    package_type_id                AS package_type_id,
    expected_unit_price_per_outer	 AS expected_unit_price_per_outer,
    last_receipt_date              AS last_receipt_date,
    is_order_line_finalized        AS is_order_line_finalized

  FROM stg_fact_purchase_order_source

),

fact_purchase_order_convertdatatype AS (

  SELECT
    CAST ( purchase_order_line_key       AS INTEGER )   AS purchase_order_line_key,
    CAST ( purchase_order_key            AS INTEGER )   AS purchase_order_key,
    CAST ( stock_item_key                AS INTEGER )   AS stock_item_key,
    CAST ( ordered_outers                AS INTEGER )   AS ordered_outers,
    CAST ( `description`                 AS STRING )    AS `description`,
    CAST ( received_outers               AS INTEGER )   AS received_outers,
    CAST ( package_type_id               AS INTEGER )   AS package_type_key,
    CAST ( expected_unit_price_per_outer AS NUMERIC )   AS expected_unit_price_per_outer,
    CAST ( last_receipt_date             AS TIMESTAMP ) AS last_receipt_date,
    CAST ( is_order_line_finalized       AS BOOLEAN )   AS is_order_line_finalized


  FROM fact_purchase_order_rename

)

SELECT
  purchase_order_line_key,
  purchase_order_key,
  stock_item_key,
  ordered_outers,
  `description`,
  received_outers,
  package_type_key,
  expected_unit_price_per_outer,
  COALESCE(last_receipt_date, TIMESTAMP('1970-01-01')) AS last_receipt_date,
  is_order_line_finalized
FROM fact_purchase_order_convertdatatype
