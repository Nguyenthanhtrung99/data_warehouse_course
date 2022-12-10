WITH Purchasing_Supplier_Source AS (

  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`

),

Purchasing_Supplier_Rename AS (

  SELECT
    supplier_id                  AS supplier_key,
    supplier_name                AS supplier_name,
    supplier_category_id         AS supplier_category_key,
    primary_contact_person_id    AS primary_contact_person_key,
    alternate_contact_person_id  AS alternate_contact_person_key,
    delivery_method_id           AS delivery_method_key,
    delivery_city_id             AS delivery_city_key,
    postal_city_id               AS postal_city_key,
    supplier_reference           AS supplier_reference,
    bank_account_name            AS bank_account_name,
    bank_account_branch          AS bank_account_branch,
  FROM Purchasing_Supplier_Source

),

Purchasing_Supplier_ConvertType AS (

  SELECT
    CAST(supplier_key                  AS INTEGER)  AS supplier_key,
    CAST(supplier_name                 AS STRING)   AS supplier_name,
    CAST(supplier_category_key         AS INTEGER)  AS supplier_category_key,
    CAST(primary_contact_person_key    AS INTEGER)  AS primary_contact_person_key,
    CAST(alternate_contact_person_key  AS INTEGER)  AS alternate_contact_person_key,
    CAST(delivery_method_key           AS INTEGER)  AS delivery_method_key,
    CAST(delivery_city_key             AS INTEGER)  AS delivery_city_key,
    CAST(postal_city_key               AS INTEGER)  AS postal_city_key,
    CAST(supplier_reference            AS STRING)   AS supplier_reference,
    CAST(bank_account_name             AS STRING)   AS bank_account_name,
    CAST(bank_account_branch           AS STRING)   AS bank_account_branch,
  FROM Purchasing_Supplier_Rename

)

SELECT
  supplier_key,
  supplier_name,
  supplier_category_key,
  primary_contact_person_key,
  alternate_contact_person_key,
  delivery_method_key,
  delivery_city_key,
  postal_city_key,
  supplier_reference,
  bank_account_name,
  bank_account_branch,
FROM Purchasing_Supplier_ConvertType