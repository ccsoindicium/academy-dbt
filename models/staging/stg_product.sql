with raw_data as (
    select
        productid
        , name as product_name
    from {{ source('sap_adw', 'product') }}
)
select *
from raw_data
