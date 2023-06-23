with raw_data as (
    select
        creditcardid
        , cardtype
    from {{ source('sap_adw', 'creditcard') }}
)
select *
from raw_data
