-- Q3
SELECT COUNT(*)
  FROM ny_taxi.public.yellow_taxi_data
 WHERE tpep_pickup_datetime::DATE = '2021-01-15';

-- 53024
 
-- Q4
SELECT tpep_pickup_datetime::DATE, MAX(tip_amount) AS tip_max
  FROM ny_taxi.public.yellow_taxi_data
 WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = '2021'
   AND EXTRACT(MONTH FROM tpep_pickup_datetime) = '01'
 GROUP BY 1
 ORDER BY 2 DESC;
 
-- 2021-01-20: 1140.44

-- Q5
-- a. Define an ID of a Central Park zone
SELECT "LocationID" 
  FROM ny_taxi.public.zones
 WHERE LOWER("Zone") LIKE '%central park%';
-- 43

SELECT
    COALESCE(t2."Zone", 'Unknown'), COUNT(*)
  FROM ny_taxi.public.yellow_taxi_data t1
  LEFT JOIN ny_taxi.public.zones t2
    ON t2."LocationID" = t1."DOLocationID"
 WHERE t1."PULocationID" = 43
   AND t1.tpep_pickup_datetime::DATE = '2021-01-14'
 GROUP BY 1
 ORDER BY 2 DESC;
-- Upper East Side South

-- Q6
SELECT
    CONCAT(COALESCE(t2."Zone", 'Unknown'), ' / ', COALESCE(t3."Zone", 'Unknown')), AVG(t1.fare_amount)
  FROM ny_taxi.public.yellow_taxi_data t1
  LEFT JOIN ny_taxi.public.zones t2
    ON t2."LocationID" = t1."PULocationID"
  LEFT JOIN ny_taxi.public.zones t3
    ON t3."LocationID" = t1."DOLocationID"
 GROUP BY 1
 ORDER BY 2 DESC
-- Alphabet City / Unknown