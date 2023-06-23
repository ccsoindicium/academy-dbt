with customers as (
    select
        customer_sk
        , customerid
    from {{ref('dim_customer')}} 
)

, products as (
    select
        product_sk
        , productid
    from {{ref('dim_product')}}
)

, locations as (
    select
        shiptoaddress_sk
        , shiptoaddressid
    from {{ref('dim_location')}}
)

, creditcards as (
    select
        creditcard_sk
        , creditcardid
    from {{ref('dim_creditcard')}}
)

, reasons as (
    select *
    from {{ref('dim_reason')}}
)

, first_joined as (
    select
        first_joined.salesorderid
        , products.product_sk as product_fk
        , first_joined.productid
        , first_joined.orderqty
        , first_joined.unitprice               
        , reasons.reason_name  
    from {{ref('stg_salesorderdetail')}} first_joined
        left join products on first_joined.productid = products.productid
        left join reasons on first_joined.salesorderid = reasons.salesorderid
)

, second_joined as (
    select
        salesorderid
        , customers.customer_sk as customer_fk
        , creditcards.creditcard_sk as creditcard_fk
        , locations.shiptoaddress_sk as shiptoadress_fk
        , orderdate     
        , order_status_name
    from {{ref('stg_salesorderheader')}} 
    left join customers on stg_salesorderheader.customerid = customers.customerid
    left join creditcards on stg_salesorderheader.creditcardid = creditcards.creditcardid
    left join locations on stg_salesorderheader.shiptoaddressid = locations.shiptoaddressid
)

, final_table_joined as (
    select
        first_joined.salesorderid
        , first_joined.product_fk
        , second_joined.customer_fk
        , second_joined.shiptoadress_fk
        , second_joined.creditcard_fk
        , first_joined.unitprice
        , first_joined.orderqty
        , first_joined.reason_name
        , second_joined.orderdate
        , second_joined.order_status_name
    from first_joined
    left join second_joined on first_joined.salesorderid = second_joined.salesorderid
)
select *
from final_table_joined
