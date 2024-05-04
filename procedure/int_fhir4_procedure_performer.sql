WITH procedurePerformer AS (
    SELECT * from {{source('fhir4silver','procedureperformer') }}
)
SELECT *
FROM procedurePerformer