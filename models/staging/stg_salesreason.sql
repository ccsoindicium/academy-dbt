with raw_data as (
    select
        salesreasonid
        , name as reason_name
    from {{ source('sap_adw', 'salesreason') }}
)
select *
from raw_data
