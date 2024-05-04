WITH account_guarantor as (
    SELECT * FROM {{source('fhir4silver','accountguarantor')}}
)
select * from account_guarantor;