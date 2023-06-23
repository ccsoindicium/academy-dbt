with creditcard as (
    select *
    from {{ref('stg_creditcard')}}
)

, salesorderheader as (
    select 
        distinct(creditcardid)
    from {{ref('stg_salesorderheader')}}
    where creditcardid is not null
)

, joined as (
    select 
        row_number() over (order by salesorderheader.creditcardid) as creditcard_sk	
        , salesorderheader.creditcardid
        , creditcard.cardtype
    from salesorderheader 
    left join creditcard on salesorderheader.creditcardid = creditcard.creditcardid
)
select *
from joined
