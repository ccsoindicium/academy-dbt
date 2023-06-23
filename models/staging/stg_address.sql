with raw_data as (
    select
        addressid
        , city
	
    from {{ source('sap_adw', 'address') }}
)
select *
from raw_data
