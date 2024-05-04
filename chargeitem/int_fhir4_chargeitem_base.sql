--int_fhir4_chargeitem_base

WITH
     {{ codeable_concept_by_system('chargeitem', 'chargeitemid', ' bodySite','http://hl7.org', True) }},
     {{ codeable_concept_by_system('chargeitem', 'chargeitemid', ' reason','http://hl7.org', True) }},
final as (
    SELECT
       dr.fhir_id,
       dr.version_id,
       dr.last_updated_at,
       dr.resource_summary,
       bodySite_cte.code as bodySite_type_code,
       bodySite_cte.code_system as bodySite_type_code_system,
       bodySite_cte.code_mappings as bodySite_type_code_mappings,
       reason_cte.code as reason_type_code,
       reason_cte.code_system as reason_type_code_system,
       reason_cte.code_mappings as reason_type_code_mappings,
       subjectRef.reference_resource as subject_reference_resource,
       subjectRef.reference_resource_id as subject_reference_id,
       contextRef.reference_resource as context_reference_resource,
       contextRef.reference_resource_id as context_reference_id,
       performer.chargeitemperformerid as chargeitem_performer_id,
       performer.practitioneractorid as chargeitem_practitioner_actor_id,
       performer.practitionerroleactorid as practitioner_role_actor_id,
       performer.organizationactorid AS organization_actor_id,
       performer.patientactorid AS patient_actor_id,
       performer.careteamactorid AS care_team_actor_id,
       performer.deviceactorid AS device_actor_id,
       performer.function_codeableconceptid AS function_codeableconcept_id,
       performer.actor_referenceid AS actor_reference_id,
       performer.relatedpersonactorid AS related_person_actor_id,
       performingOrganizationRef.reference_resource as performing_organization_reference_resource,
       performingOrganizationRef.reference_resource_id as performing_organization_reference_id,
       requestingOrganizationRef.reference_resource as requesting_organization_reference_resource,
       requestingOrganizationRef.reference_resource_id as requesting_organization_reference_id,
       costCenterRef.reference_resource as cost_center_reference_resource,
       costCenterRef.reference_resource_id as cost_center_reference_id,
       entererRef.reference_resource as enterer_reference_resource,
       entererRef.reference_resource_id as enterer_reference_id
     FROM
      {{ source ('fhir4silver', 'chargeitem')}} chargeitem
    JOIN
      {{ ref('stg_fhir4_domainresource')}} dr ON dr.domainresourceid = chargeitem.domainresourceid
     LEFT JOIN
        bodySite_cte ON bodySite_cte.chargeitemid = chargeitem.chargeitemid
     LEFT JOIN
      reason_cte ON reason_cte.chargeitemid = chargeitem.chargeitemid
    LEFT JOIN
      {{ ref ('int_fhir4_chargeitem_identifiers') }} identifiers ON identifiers.chargeitemid = chargeitem.chargeitemid
    LEFT JOIN
        {{ ref ('stg_fhir4_references')}} subjectRef on subjectRef.referenceid= chargeitem.subject_referenceid
    LEFT JOIN
        {{ ref ('stg_fhir4_references')}} contextRef on contextRef.referenceid= chargeitem.context_referenceid
    LEFT JOIN
        {{ ref ('stg_fhir4_references')}} performingOrganizationRef on performingOrganizationRef.referenceid= chargeitem.performingorganization_referenceid
    LEFT JOIN
        {{ ref ('stg_fhir4_references')}} requestingOrganizationRef on requestingOrganizationRef.referenceid= chargeitem.requestingorganization_referenceid

    LEFT JOIN
        {{ ref ('stg_fhir4_references')}} costCenterRef on costCenterRef.referenceid= chargeitem.costcenter_referenceid
    LEFT JOIN
        {{ ref ('stg_fhir4_references')}} entererRef on entererRef.referenceid= chargeitem.enterer_referenceid
    LEFT JOIN
        {{ ref ('stg_fhir4_references')}} productRef on productRef.referenceid= chargeitem.productreference_referenceid
    LEFT JOIN
        {{ref('int_fhir4_chargeitem_performer')}} performer on performer.chargeitemid = chargeitem.chargeitemid
    LEFT JOIN
        {{ ref ('int_fhir4_chargeitem_notes') }} note on note.chargeitemid = chargeitem.chargeitemid and note.rn = 1

)
SELECT * FROM final