with raw_data as (
    select
        stateprovinceid
        , name as state_name
    from {{ source('sap_adw', 'stateprovince') }}
)
select *
from raw_data
