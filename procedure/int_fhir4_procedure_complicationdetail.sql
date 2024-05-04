WITH procedureCompilationDetail AS (
    SELECT * from {{source('fhir4silver','procedurecomplicationdetail') }}
)
SELECT *
FROM procedureCompilationDetail