WITH Application_People_Source AS (

  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.application__people`

),

Application_People_Rename AS (

  SELECT
    person_id   AS person_key,
    full_name AS full_name
  FROM Application_People_Source

),

Application_People_ConvertType AS (

  SELECT
    CAST(person_key AS INTEGER) AS person_key,
    CAST(full_name AS STRING) AS full_name
  FROM Application_People_Rename

), Final AS (

SELECT
  person_key,
  full_name
FROM Application_People_ConvertType

UNION ALL

SELECT
  0, 'Undefined'

)

SELECT DISTINCT *
FROM Final