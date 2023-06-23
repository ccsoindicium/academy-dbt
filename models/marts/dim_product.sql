with product as (
    select *
    from {{ref('stg_product')}}
)

, salesorderdetail as (
    select 
        distinct(productid)
    from {{ref('stg_salesorderdetail')}}
)

, joined as (
    select 
        row_number() over (order by salesorderdetail.productid) as product_sk
        , salesorderdetail.productid
        , product.product_name 
    from salesorderdetail
    left join product on salesorderdetail.productid = product.productid
)

select *
from joined