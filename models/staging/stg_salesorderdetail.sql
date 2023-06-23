with raw_data as (
    select
        salesorderid
        , orderqty
        , unitprice
        , productid
    from {{ source('sap_adw', 'salesorderdetail') }}
)
select *
from raw_data
