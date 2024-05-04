WITH chargeitemAccounts AS (
    SELECT chargeitem.chargeitemid,
            accountRef.reference_resource as account_reference_resource,
            accountRef.reference_resource_id as account_reference_id,
            accountRef.display,
            {{ window_row_number('rn', 'chargeitemid', 'account_referenceid')}}

    FROM {{ source('fhir4silver','chargeitem') }}
    CROSS JOIN unnest(account_referenceid) as i(id)
    JOIN {{ ref('stg_fhir4_references')}} accountRef on accountRef.referenceid =  i.id
)
SELECT *
FROM chargeitemAccounts