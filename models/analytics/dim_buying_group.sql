WITH dim_buying_group_source AS (

  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.sales__buying_groups`

), buying_group_rename AS (

SELECT
  buying_group_id AS buying_group_key,
  buying_group_name AS buying_group_name
FROM dim_buying_group_source

), buying_group_casttype AS (

SELECT
    CAST(buying_group_key     AS INTEGER)   AS buying_group_key,
    CAST(buying_group_name    AS STRING)    AS buying_group_name,
FROM buying_group_rename

)

SELECT
  buying_group_key,
  buying_group_name
FROM buying_group_casttype