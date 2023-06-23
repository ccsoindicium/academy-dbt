with salesorderheadersalesreason as (
    select *
    from {{ref('stg_salesorderheadersalesreason')}}
)

, salesreason as (
    select *
    from {{ref('stg_salesreason')}}
)

, joined as (
    select 
        salesorderheadersalesreason.salesorderid
        , salesreason.reason_name as reason_name
    from salesorderheadersalesreason
    left join salesreason on salesorderheadersalesreason.salesreasonid = salesreason.salesreasonid 
)

, int_tranformation as (
    select
        salesorderid        
        , string_agg(reason_name, ', ') as reason_name_agg
    from joined
    group by salesorderid
)

, final_tranformation as (
    select
        salesorderid        
        , ifnull(reason_name_agg,'Not indicated') as reason_name
    from int_tranformation
)
select *
from final_tranformation
