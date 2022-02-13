SELECT
    COUNT(DISTINCT dispatching_base_num)
  FROM `coral-muse-339314.tripdata.fhv_tripdata`
-- 791

SELECT
    COUNT(*)
  FROM `coral-muse-339314.tripdata.fhv_tripdata`
 WHERE pickup_datetime BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02060', 'B02279');
-- 26560