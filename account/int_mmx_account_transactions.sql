WITH account_transactions as (
    select * from {{ source('fhir4silver','account_transactions')}}
)
select * from account_transactions;