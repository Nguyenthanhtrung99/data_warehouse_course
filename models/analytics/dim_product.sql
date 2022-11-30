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
    stock_item_id     AS product_key,
    stock_item_name   AS product_name,
    brand             AS brand_name,
    supplier_id       AS supplier_key,
    is_chiller_stock  AS is_chiller_stock
  FROM dim_product_source

),

dim_product_convertdatatype AS(

  SELECT
    CAST(product_key     AS INTEGER)   AS product_key,
    CAST(product_name    AS STRING)    AS product_name,
    CAST(brand_name      AS STRING)    AS brand_name,
    CAST(supplier_key    AS INTEGER)   AS supplier_key,
    CAST(is_chiller_stock AS BOOLEAN)  AS is_chiller_stock_boolean
  FROM dim_product_renamecolumn
)

SELECT
  Product.product_key,
  Product.product_name,
  COALESCE(Product.brand_name, 'Undefined') AS brand_name,
  Product.supplier_key,
  CASE Product.is_chiller_stock_boolean
    WHEN TRUE  THEN 'Chiller Stock'
    WHEN FALSE THEN 'Not Chiller Stock'
    ELSE 'Error'
  END AS Type_Stock,
  COALESCE(Supplier.supplier_name, 'Error') AS supplier_name
FROM dim_product_convertdatatype AS Product
LEFT JOIN dim_supplier_source AS Supplier
ON Product.supplier_key = Supplier.supplier_key