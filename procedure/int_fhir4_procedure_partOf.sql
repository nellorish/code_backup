WITH procedurePartOf AS (
    SELECT procedure.procedureid,
            partOfRef.reference_resource as partOfRef_reference_resource,
            partOfRef.reference_resource_id as partOfRef_reference_id,
            partOfRef.display,
            {{ window_row_number('rn', 'procedureid', 'partof_referenceid')}}

    FROM {{ source('fhir4silver','procedure') }}
    CROSS JOIN unnest(partof_referenceid) as i(id)
    JOIN {{ ref('stg_fhir4_references')}} partOfRef on partOfRef.referenceid =  i.id
)
SELECT *
FROM procedurePartOf