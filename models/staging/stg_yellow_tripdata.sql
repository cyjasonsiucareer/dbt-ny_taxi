{{ config(materialized='view') }}


SELECT 
    {{ dbt_utils.surrogate_key(['VendorID', 'tpep_pickup_datetime']) }} as tripid,
	-- identifer
	cast (  VendorID  as integer) as VendorID,
	cast (  PULocationID  as integer) as pickup_locationID,
	cast (  DOLocationID  as integer) as dropoff_locationID,
	cast (  RatecodeID  as integer) as ratecodeID,


	-- timestamps
	cast (  tpep_pickup_datetime  as timestamp) as pickup_datetime,
	cast (  tpep_dropoff_datetime  as timestamp) as dropoff_datetime,

	-- trip information 
	store_and_fwd_flag , 
	cast (  passenger_count  as integer) as passenger_count,
	cast (  trip_distance  as numeric) as trip_distance,
	1 as trip_type,

	-- payment information 
	cast (  fare_amount  as numeric) as fare_amount,
	cast (  extra  as numeric) as extra,
	cast (  mta_tax  as numeric) as mta_tax,
	cast (  tip_amount  as numeric) as tip_amount,
	cast (  tolls_amount  as numeric) as tolls_amount,
	0 as ehail_fee,
	cast (  improvement_surcharge  as numeric) as improvement_surcharge,
	cast (  total_amount  as numeric) as total_amount,
	cast (  payment_type  as integer) as payment_type,
    {{get_payment_type_desc('payment_type')}} as payment_type_desc,
	cast (  congestion_surcharge  as numeric) as congestion_surcharge

FROM {{ source('staging', 'yellow_trips_all') }}
WHERE VendorID is not null


--dbt build --m <model.sql> --variable is_test_run: 'false'
{%if var('is_test_run', default = true) %}

    LIMIT 100

{% endif %}