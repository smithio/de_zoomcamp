-- Q1
SELECT COUNT(*)
FROM `coral-muse-339314.production.fact_trips`
WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020)
-- 61635418

-- Q3
SELECT COUNT(*)
FROM `coral-muse-339314.production.stg_fhv_tripdata`
WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019)
-- 42084899

-- Q4
SELECT COUNT(*)
FROM `coral-muse-339314.production.fact_fhv_trips`
WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019)
-- 22676253