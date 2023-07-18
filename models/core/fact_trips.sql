{{ config(materialized='table') }}

WITH  green_data as (SELECT *, 'Green' as service_type  FROM {{ref('stg_green_tripdata')}})
    , yellow_data as (SELECT *, 'Yellow' as service_type FROM {{ref('stg_yellow_tripdata')}})
    , trip_unioned as ( 
        SELECT * FROM  green_data
            union all
        SELECT * FROM  yellow_data
    ),
    dim_zone as ( SELECT * FROM  {{ref('dim_zones')}} WHERE Borough!= 'Unknown' )

SELECT 
    trip_unioned.tripid
    , trip_unioned.vendorid
    , trip_unioned.service_type
    , trip_unioned.store_and_fwd_flag
    , trip_unioned.passenger_count
    , trip_unioned.trip_distance
    , trip_unioned.trip_type
    , trip_unioned.fare_amount
    , trip_unioned.extra
    , trip_unioned.mta_tax
    , trip_unioned.tip_amount
    , trip_unioned.tolls_amount
    , trip_unioned.ehail_fee
    , trip_unioned.improvement_surcharge
    , trip_unioned.total_amount
    , trip_unioned.payment_type
    , trip_unioned.payment_type_desc
    , trip_unioned.congestion_surcharge

FROM trip_unioned
    inner join dim_zone as pickup_zone
ON trip_unioned.pickup_locationID = pickup_zone.LocationID
    inner join dim_zone as dropoff_zone
ON trip_unioned.dropoff_locationID = dropoff_zone.LocationID
