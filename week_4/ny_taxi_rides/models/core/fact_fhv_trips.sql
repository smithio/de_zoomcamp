{{ config(materialized='table') }}

with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    t_trips.dispatching_base_num,
    t_trips.pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    t_trips.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    t_trips.pickup_datetime,
    t_trips.dropoff_datetime,
    t_trips.SR_Flag
from {{ ref('stg_fhv_tripdata') }} t_trips
inner join dim_zones as pickup_zone
on t_trips.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on t_trips.dropoff_locationid = dropoff_zone.locationid