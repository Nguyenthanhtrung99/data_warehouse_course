WITH dim_sale_customer_source AS (

  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.sales__customers`

),

dim_sale_customer_category_source AS (

  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.sales__customer_categories`

),

dim_buying_group_source AS (

  SELECT *
  FROM {{ ref('dim_buying_group')}}
),

sale_customer_category_rename AS (

  SELECT
    customer_category_id     AS customer_category_key,
    customer_category_name	 AS customer_category_name
  FROM dim_sale_customer_category_source

),

sale_customer_category_casttype AS (

SELECT
  CAST(customer_category_key     AS INTEGER)       AS customer_category_key,
  CAST(customer_category_name    AS STRING)        AS customer_category_name
FROM sale_customer_category_rename

),

sale_customer_rename AS (

SELECT
  customer_id AS customer_key,
  customer_name AS customer_name,
  customer_category_id AS customer_category_key,
  buying_group_id AS buying_group_key,
  is_on_credit_hold AS is_on_credit_hold_boolean
FROM dim_sale_customer_source

), sale_customer_casttype AS (

SELECT
  CAST(customer_key     AS INTEGER)                AS customer_key,
  CAST(customer_name    AS STRING)                 AS customer_name,
  CAST(customer_category_key     AS INTEGER)       AS customer_category_key,
  CAST(buying_group_key     AS INTEGER)            AS buying_group_key,
  CAST(is_on_credit_hold_boolean     AS BOOLEAN)   AS is_on_credit_hold_boolean
FROM sale_customer_rename

)

SELECT
  Customer.customer_key,
  Customer.customer_name,
  COALESCE(Customer.buying_group_key, -1) AS buying_group_key,
  COALESCE(BuyingGroup.buying_group_name, 'error') AS buying_group_name,
  COALESCE(CustomerCategory.customer_category_key, -1) AS customer_category_key,
  COALESCE(CustomerCategory.customer_category_name, 'error' ) AS customer_category_name,
  CASE Customer.is_on_credit_hold_boolean
    WHEN TRUE THEN 'on credit hold'
    WHEN FALSE THEN 'not on credit hold'
    ELSE 'error'
  END AS on_credit_hold_type
FROM sale_customer_casttype AS Customer
LEFT JOIN dim_buying_group_source AS BuyingGroup
USING (buying_group_key)
LEFT JOIN sale_customer_category_casttype AS CustomerCategory
USING (customer_category_key)