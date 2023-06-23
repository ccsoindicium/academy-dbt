with address as (
    select *
    from {{ref('stg_address')}}
)

, stateprovince as (
    select *
    from {{ref('stg_stateprovince')}}
)

, countryregion as (
    select *
    from {{ref('stg_countryregion')}}
)

, salesorderheader as (
    select 
        distinct(shiptoaddressid)
    from {{ref('stg_salesorderheader')}}
)

, joined as (
    select
        row_number() over (order by salesorderheader.shiptoaddressid) as shiptoaddress_sk
        , salesorderheader.shiptoaddressid 
        , address.city as city_name
        , stateprovince.state_name
        , countryregion.country_name
    from salesorderheader
    left join address on salesorderheader.shiptoaddressid = address.addressid
    left join stateprovince on address.stateprovinceid = stateprovince.stateprovinceid
    left join countryregion on stateprovince.countryregioncode = countryregion.countryregioncode 
)
select *
from joined