SELECT
  person_key AS picked_person_key,
  full_name  AS picked_full_name,
FROM {{ ref('dim_person')}}