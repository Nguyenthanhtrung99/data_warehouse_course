WITH Application_People_Source AS (

  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.application__people`

),

Application_People_Rename AS (

  SELECT
    person_id                      AS person_key,
    full_name                      AS full_name,
    preferred_name                 AS preferred_name,
    search_name                    AS search_name,
    is_permitted_to_logon          AS is_permitted_to_logon,
    logon_name                     AS logon_name,
    is_external_logon_provider     AS is_external_logon_provider,
    is_system_user                 AS is_system_user,
    is_employee                    AS is_employee,
    is_salesperson                 AS is_salesperson,
  FROM Application_People_Source

),

Application_People_ConvertType AS (

  SELECT
    CAST(person_key                  AS INTEGER) AS person_key,
    CAST(full_name                   AS STRING)  AS full_name,
    CAST(preferred_name              AS STRING)  AS preferred_name,
    CAST(search_name                 AS STRING)  AS search_name,
    CAST(is_permitted_to_logon       AS BOOLEAN) AS is_permitted_to_logon,
    CAST(logon_name                  AS STRING)  AS logon_name,
    CAST(is_external_logon_provider  AS BOOLEAN) AS is_external_logon_provider,
    CAST(is_system_user              AS BOOLEAN) AS is_system_user,
    CAST(is_employee                 AS BOOLEAN) AS is_employee,
    CAST(is_salesperson              AS BOOLEAN) AS is_salesperson,
  FROM Application_People_Rename

), Final AS (

SELECT
  person_key,
  full_name,
  preferred_name,
  search_name,
  is_permitted_to_logon,
  logon_name,
  is_external_logon_provider,
  is_system_user,
  is_employee,
  is_salesperson,
FROM Application_People_ConvertType

UNION ALL

SELECT
  0,
  'Undefined',
  'Undefined',
  'Undefined',
  CAST(1 AS BOOLEAN ),
  'Undefined',
  CAST(1 AS BOOLEAN ),
  CAST(1 AS BOOLEAN ),
  CAST(1 AS BOOLEAN ),
  CAST(1 AS BOOLEAN ),

)

SELECT DISTINCT *
FROM Final