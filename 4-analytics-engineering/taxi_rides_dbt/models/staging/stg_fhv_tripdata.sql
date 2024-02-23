{{
    config(
        materialized='view'
    )
}}

select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as trip_id,
    dispatching_base_num,
    {{ dbt.safe_cast("pu_location_id", api.Column.translate_type("integer")) }} as pickup_location_id,
    {{ dbt.safe_cast("do_location_id", api.Column.translate_type("integer")) }} as dropoff_location_id,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    sr_flag,
    affiliated_base_number
from {{ source('staging','fhv_tripdata') }}
where extract(year from pickup_datetime) = 2019

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}