WITH account_coverage as (
    select * from {{ source('fhir4silver','accountcoverage')}}
)
select * from account_coverage;