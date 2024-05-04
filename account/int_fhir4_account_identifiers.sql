WITH procedureIdentifiers AS (
    SELECT account.accountid,
            identifier.identifierid,
            identifier.use,
            identifier.identifier_value,
            identifier.namespace_system,
            identifier.identifier_system,
            identifier.extension,
            identifier.period_start_at,
            identifier.period_end_at,
            identifier.assigner,
            identifier.version,
            identifier.code,
            identifier.display,
            identifier.userselected,
            identifier.id,
            {{ window_row_number('rn', 'accountid', 'identifier.identifier_value')}}
    FROM {{ source('fhir4silver','account') }} account
    CROSS JOIN unnest(identifier_identifierid) as i(id)
    JOIN {{ ref('stg_fhir4_identifiers') }} identifier on identifier.identifierid = i.id

)
SELECT *
FROM procedureIdentifiers