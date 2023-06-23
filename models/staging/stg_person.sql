with raw_data as (
    select
        businessentityid
        , firstname
        , middlename
        , lastname
    from {{ source('sap_adw', 'person') }}
)
select *
from raw_data
