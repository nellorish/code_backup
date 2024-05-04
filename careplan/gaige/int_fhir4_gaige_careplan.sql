WITH
        {{ codeable_concept_by_system('careplan', 'careplanid', 'category','http://hl7.org/fhir/ValueSet/subscriber-relationship',true) }},

    final as
(
    SELECT
            careplan.careplanid as SourceIdentifier,
            category_cte.struct_array[1] as CategoryConcept,
            title as Title,
            description as Description,
            period.start_at as StartDate,
            period.end_at as EndDate,
            created as Created,
            ARRAY[
                 CAST(ROW( 'Organization', organizationAuthored.organization_id, 'Authored') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar))
                ] as ForeignKeys
    FROM {{ source('fhir4silver', 'careplan')}} careplan
    LEFT JOIN category_cte ON category_cte.careplanid = careplan.careplanid
    LEFT JOIN {{ ref('stg_fhir4_references')}} organizationAuthorRef on organizationAuthorRef.referenceid=  careplan.author_referenceid
    LEFT JOIN {{ source('fhir4gold','organizations')}} organizationAuthored on organizationAuthored.fhir_id = organizationAuthorRef.reference_resource_id
    LEFT JOIN {{ ref('stg_fhir4_periods')}} period on period.period_id = careplan.period_periodid
    )
Select *
from final