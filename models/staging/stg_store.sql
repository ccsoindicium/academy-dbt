with raw_data as (
    select
        businessentityid
        , name as storename
    from {{ source('sap_adw', 'store') }}
)
select *
from raw_data
