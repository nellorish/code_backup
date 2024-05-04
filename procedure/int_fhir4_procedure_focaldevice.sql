WITH procedureFocalDevice AS (
    SELECT * from {{source('fhir4silver','procedurefocaldevice') }}
)
SELECT *
FROM procedureFocalDevice