with raw_data as (
    select
        salesorderid
        , salesreasonid
    from {{ source('sap_adw', 'salesorderheadersalesreason') }}
)
select *
from raw_data
