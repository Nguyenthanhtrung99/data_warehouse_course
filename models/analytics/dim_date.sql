WITH Dim_Date_Source AS (

  SELECT `Date`
  FROM UNNEST(GENERATE_DATE_ARRAY('2010-01-01', '2030-12-31', INTERVAL 1 DAY)) AS `Date`

),

Dim_Date_Cast_Type AS (

  SELECT CAST( `Date` AS DATE) AS `Date`
  FROM Dim_Date_Source
)

SELECT
  FORMAT_DATE('%F', `Date`) as `Date`,
  FORMAT_DATE('%A', `Date`) as Day_Of_Week,
  FORMAT_DATE('%a', `Date`) as Day_Of_Week_Short,
  CASE
    WHEN FORMAT_DATE('%A', `Date`) IN ('Sunday', 'Saturday') THEN 'Weekend'
    ELSE 'Weekday'
  END AS Is_Weekday_Or_Weekend,
  DATE_TRUNC(`Date` , YEAR) AS Year,
  FORMAT_DATE('%Y', `Date`) as Year_Number
  DATE_TRUNC(`Date` , MONTH) AS Year_Month,
  FORMAT_DATE('%B', `Date`) as Month
FROM Dim_Date_Cast_Type