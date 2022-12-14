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
  is_on_credit_hold AS is_on_credit_hold_boolean,
  primary_contact_person_id AS primary_contact_person_key,
  alternate_contact_person_id AS alternate_contact_person_key,
  delivery_method_id AS delivery_method_key,
  delivery_city_id AS delivery_city_key,
  postal_city_id AS postal_city_key,
  credit_limit AS credit_limit,
  account_opened_date AS account_opened_date,
  standard_discount_percentage AS standard_discount_percentage,
  is_statement_sent AS is_statement_sent,
  is_on_credit_hold AS is_on_credit_hold,
  payment_days AS payment_days,
  delivery_run AS delivery_run,
  run_position AS run_position,
  delivery_postal_code AS delivery_postal_code,
  postal_postal_code AS postal_postal_code,
FROM dim_sale_customer_source

), sale_customer_casttype AS (

SELECT
  CAST(customer_key                 AS INTEGER)   AS customer_key,
  CAST(customer_name                AS STRING)    AS customer_name,
  CAST(customer_category_key        AS INTEGER)   AS customer_category_key,
  CAST(buying_group_key             AS INTEGER)   AS buying_group_key,
  CAST(is_on_credit_hold_boolean    AS BOOLEAN)   AS is_on_credit_hold_boolean,
  CAST(primary_contact_person_key   AS INTEGER)   AS primary_contact_person_key,
  CAST(alternate_contact_person_key AS INTEGER)   AS alternate_contact_person_key,
  CAST(delivery_method_key          AS INTEGER)   AS delivery_method_key,
  CAST(delivery_city_key            AS INTEGER)   AS delivery_city_key,
  CAST(postal_city_key              AS INTEGER)   AS postal_city_key,
  CAST(credit_limit                 AS NUMERIC)   AS credit_limit,
  CAST(account_opened_date          AS TIMESTAMP) AS account_opened_date,
  CAST(standard_discount_percentage AS NUMERIC)   AS standard_discount_percentage,
  CAST(is_statement_sent            AS BOOLEAN)   AS is_statement_sent,
  CAST(is_on_credit_hold            AS BOOLEAN)   AS is_on_credit_hold,
  CAST(payment_days                 AS INTEGER)   AS payment_days,
  CAST(delivery_run                 AS STRING)    AS delivery_run,
  CAST(run_position                 AS STRING)    AS run_position,
  CAST(delivery_postal_code         AS STRING)    AS delivery_postal_code,
  CAST(postal_postal_code           AS STRING)    AS postal_postal_code,
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
  END AS on_credit_hold_type,
  Customer.primary_contact_person_key,
  Customer.alternate_contact_person_key,
  Customer.delivery_method_key,
  Customer.delivery_city_key,
  Customer.postal_city_key,
  Customer.credit_limit,
  Customer.account_opened_date,
  Customer.standard_discount_percentage,
  Customer.is_statement_sent,
  Customer.is_on_credit_hold,
  Customer.payment_days,
  Customer.delivery_run,
  Customer.run_position,
  Customer.delivery_postal_code,
  Customer.postal_postal_code

FROM sale_customer_casttype AS Customer
LEFT JOIN dim_buying_group_source AS BuyingGroup
USING (buying_group_key)
LEFT JOIN sale_customer_category_casttype AS CustomerCategory
USING (customer_category_key)