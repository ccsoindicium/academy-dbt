with raw_data as (
    select
        salesorderid
        , shiptoaddressid
        , status as order_status
        , orderdate
        , creditcardid
        , customerid
        , case 
            when status = 1 then 'In_process'
            when status = 2 then 'Approved'
            when status = 3 then 'Backordered' 
            when status = 4 then 'Rejected' 
            when status = 5 then 'Shipped'
            when status = 6 then 'Cancelled' 
            else 'Without status'
        end as order_status_name  
    from {{ source('sap_adw', 'salesorderheader') }}
)
select *
from raw_data
