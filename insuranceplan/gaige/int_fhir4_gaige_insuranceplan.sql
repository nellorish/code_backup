/*WITH
        {{ codeable_concept_by_system('insuranceplan', 'insuranceplanid', 'type','http://hl7.org/fhir/ValueSet/subscriber-relationship',true) }},

    final as
(
    SELECT
            type_cte.struct_array[1] as TypeConcept,
            name as Name,

            ARRAY[
                 CAST(ROW( 'Organization', payorOrg.organization_id, 'ownedById') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
                 CAST(ROW( 'Organization', payorOrg.organization_id, 'administeredById') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
                 CAST(ROW( 'Location', payorOrg.organization_id, 'LocationId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
                ] as ForeignKeys
    FROM {{ source('fhir4silver', 'insuranceplan')}} insuranceplan
    LEFT JOIN
        type_cte ON type_cte.id = insuranceplan.insuranceplanid
    LEFT JOIN {{ ref('stg_fhir4_references')}} organizationOwnedRef on organizationOwnedRef.referenceid=  insuranceplan.ownedby_referenceid
    LEFT JOIN {{ source('fhir4gold','organizations')}} organizationOwned on organization.fhir_id = organizationOwnedRef.reference_resource_id
    LEFT JOIN {{ ref('stg_fhir4_references')}} organizationAdminRef on organizationAdminRef.referenceid=  insuranceplan.administeredby_referenceid
    LEFT JOIN {{ source('fhir4gold','organizations')}} organizationAdministered on organizationAdministered.fhir_id = organizationAdminRef.reference_resource_id
    CROSS JOIN unnest(coveragearea_referenceid) as e(id)
    LEFT JOIN {{ ref('stg_fhir4_references')}} LocationRef on LocationRef.referenceid= e.id
    LEFT JOIN {{ source('fhir4gold','Location')}} location on location.fhir_id = locationRef.reference_resource_id

    )
Select *
from final*/