WITH chargeItemPerformer AS (
    SELECT * from {{source('fhir4silver','chargeitemperformer') }}
)
SELECT *
FROM chargeItemPerformer