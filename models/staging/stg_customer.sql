with raw_data as (
    select
        customerid
        , personid
        , storeid
    from {{ source('sap_adw', 'customer') }}
)
select *
from raw_data
