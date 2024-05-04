WITH procedureReport AS (
    SELECT procedure.procedureid,
            report.reference_resource as report_reference_resource,
            report.reference_resource_id as report_reference_id,
            report.display,
            {{ window_row_number('rn', 'procedureid', 'report_referenceid')}}

    FROM {{ source('fhir4silver','procedure') }}
    CROSS JOIN unnest(report_referenceid) as i(id)
    JOIN {{ ref('stg_fhir4_references')}} report on report.referenceid =  i.id
)
SELECT *
FROM procedureReport