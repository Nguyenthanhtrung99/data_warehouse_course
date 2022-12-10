WITH dim_product_source AS (

  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`

),

dim_supplier_source AS (

  SELECT *
  FROM {{ ref('dim_supplier')}}

),

dim_product_renamecolumn AS (

  SELECT
    stock_item_id            AS product_key,
    stock_item_name          AS product_name,
    brand                    AS brand_name,
    supplier_id              AS supplier_key,
    is_chiller_stock         AS is_chiller_stock,
    color_id                 AS color_key,
    unit_package_id          AS unit_package_key,
    outer_package_id         AS outer_package_key,
    size                     AS size,
    lead_time_days           AS lead_time_days,
    quantity_per_outer       AS quantity_per_outer,
    tax_rate                 AS tax_rate,
    unit_price               AS unit_price,
    recommended_retail_price AS recommended_retail_price,
    typical_weight_per_unit  AS typical_weight_per_unit,
    marketing_comments       AS marketing_comments,
    internal_comments        AS internal_comments,
    search_details           AS search_details
  FROM dim_product_source

),

dim_product_convertdatatype AS(

  SELECT
    CAST(product_key     AS INTEGER)           AS product_key,
    CAST(product_name    AS STRING)            AS product_name,
    CAST(brand_name      AS STRING)            AS brand_name,
    CAST(supplier_key    AS INTEGER)           AS supplier_key,
    CAST(is_chiller_stock AS BOOLEAN)          AS is_chiller_stock_boolean,
    CAST(color_key AS INTEGER )                AS color_key,
    CAST(unit_package_key AS INTEGER)          AS unit_package_key,
    CAST(outer_package_key AS INTEGER)         AS outer_package_key,
    CAST(size AS STRING)                       AS size,
    CAST(lead_time_days AS INTEGER)            AS lead_time_days,
    CAST(quantity_per_outer AS INTEGER)        AS quantity_per_outer,
    CAST(tax_rate AS NUMERIC)                  AS tax_rate,
    CAST(unit_price AS NUMERIC)                AS unit_price,
    CAST(recommended_retail_price AS NUMERIC)  AS recommended_retail_price,
    CAST(typical_weight_per_unit AS NUMERIC)   AS typical_weight_per_unit,
    CAST(marketing_comments AS STRING)         AS marketing_comments,
    CAST(internal_comments AS STRING)          AS internal_comments,
    CAST(search_details AS STRING)             AS search_details,
  FROM dim_product_renamecolumn
)

SELECT
  Product.product_key,
  Product.product_name,
  COALESCE(Product.color_key, -1) AS color_key,
  COALESCE(Product.unit_package_key, -1) AS unit_package_key,
  COALESCE(Product.outer_package_key, -1) AS outer_package_key,
  COALESCE(Product.size, 'Undefined') AS size,
  COALESCE(Product.lead_time_days, -1) AS lead_time_days,
  COALESCE(Product.quantity_per_outer, -1) AS quantity_per_outer,
  COALESCE(Product.tax_rate, -1) AS tax_rate,
  COALESCE(Product.unit_price, -1) AS unit_price,
  COALESCE(Product.recommended_retail_price, -1) AS recommended_retail_price,
  COALESCE(Product.typical_weight_per_unit, -1) AS typical_weight_per_unit,
  COALESCE(Product.marketing_comments, 'Undefined') AS marketing_comments,
  COALESCE(Product.internal_comments, 'Undefined') AS internal_comments,
  COALESCE(Product.search_details, 'Undefined') AS search_details,
  COALESCE(Product.brand_name, 'Undefined') AS brand_name,
  Product.supplier_key,
  COALESCE(Supplier.supplier_name, 'Error') AS supplier_name,
  CASE Product.is_chiller_stock_boolean
    WHEN TRUE  THEN 'Chiller Stock'
    WHEN FALSE THEN 'Not Chiller Stock'
    ELSE 'Error'
  END AS Type_Stock,
FROM dim_product_convertdatatype AS Product
LEFT JOIN dim_supplier_source AS Supplier
ON Product.supplier_key = Supplier.supplier_key