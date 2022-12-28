WITH stg_fact_purchase_order_Source AS (

  SELECT *
  FROM `vit-lam-data.wide_world_importers.external__salesperson_target`

),

fact_sales_order_line AS (

  SELECT
    CAST(DATE_TRUNC( order_date, Month) AS DATE ) AS year_month
    salesperson_person_key,
    SUM(gross_amount) AS gross_amount
  FROM {{ ref('fact_sales_order_line')}}
  GROUP BY 1, 2
),

fact_purchase_order_rename AS (

  SELECT
    year_month                     AS year_month,
    salesperson_person_id	         AS salesperson_person_key,
    target_revenue	               AS target_revenue,

  FROM stg_fact_purchase_order_source

),

fact_purchase_order_convertdatatype AS (

  SELECT
    CAST ( year_month                    AS DATE )      AS year_month,
    CAST ( salesperson_person_key        AS INTEGER )   AS salesperson_person_key,
    CAST ( target_revenue                AS NUMERIC )   AS target_revenue,
  FROM fact_purchase_order_rename

),

fact_salesperson_target AS (

  SELECT
    year_month,
    salesperson_person_key,
    target_revenue
  FROM fact_purchase_order_convertdatatype

)

