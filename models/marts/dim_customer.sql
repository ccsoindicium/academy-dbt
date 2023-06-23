with customer as (
    select 
        customerid
        , personid
        , storeid
    from {{ref('stg_customer')}}
)

, person as (
    select
        businessentityid        
        , concat(ifnull(firstname,' '),' ',ifnull(middlename,' '),' ',ifnull(lastname,' ')) as fullname
    from {{ref('stg_person')}}
)

, store as (
    select
        businessentityid as storebusinessentityid
        , storename
    from {{ref('stg_store')}}
)

, joined as (
    select
    row_number() over (order by customer.customerid) as customer_sk   
    , customer.customerid
    , case        
        when person.fullname is null
            then store.storename
            else person.fullname 
        end as customer_fullname
    from customer
    left join person on customer.personid = person.businessentityid
    left join store on customer.storeid = store.storebusinessentityid
)
select *
from joined
