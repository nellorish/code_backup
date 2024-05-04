WITH
     {{ codeable_concept_by_system('chargeitem', 'chargeitemid', ' code','http://hl7.org') }},
     {{ codeable_concept_by_system('chargeitem', 'chargeitemid', ' bodySite','http://hl7.org', True) }},
     {{ codeable_concept_by_system('chargeitem', 'chargeitemid', ' reason','http://hl7.org', True) }},
final as (
    SELECT
       chargeitem.chargeitemid as SourceIdentifier,
       code_cte.struct_array as CodeConcept,
       bodySite_cte.struct_array[1] as BodySiteConcept,
       reason_cte.struct_array[1] as ReasonConcept,
       chargeitem.factoroverride as FactorOverride,
       chargeitem.priceoverride as PriceOverride,
       chargeitem.overridereason as OverrideReason,
       chargeitem.entereddate as EnteredDate,

     ARRAY[
                     CAST(ROW( 'Encounter', encounter.encounter_id, 'EncounterContextId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
                     CAST(ROW( 'Patient', patientSubject.patient_id, 'PatientSubjectId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
                     CAST(ROW( 'Patient', patientEnterer.patient_id, 'PatientEnterertId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
                     CAST(ROW( ' Organization', organizationCostCenter.organization_id, 'CostCenterId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
                     CAST(ROW( 'Organization', organizationRequesting.organization_id, 'RequestingOrganizationId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
                     CAST(ROW( 'Organization', organizationPerforming.organization_id, 'PerformingOrganizationId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar))
--                     CAST(ROW( 'Observation', observationService.observation_id, 'ServiceId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar))
                   -- CAST(ROW( 'Account', observationService.observation_id, 'ServiceId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar))
                 ] as ForeignKeys
     FROM
      {{ source ('fhir4silver', 'chargeitem')}} chargeitem
     JOIN
      {{ ref('stg_fhir4_domainresource')}} dr ON dr.domainresourceid = chargeitem.domainresourceid
     LEFT JOIN code_cte on code_cte.chargeitemid = chargeitem.chargeitemid
     LEFT JOIN bodySite_cte on bodySite_cte.chargeitemid = chargeitem.chargeitemid
     LEFT JOIN reason_cte on reason_cte.chargeitemid = chargeitem.chargeitemid
     LEFT JOIN {{ ref('stg_fhir4_references') }} subRef on subRef.referenceid = chargeitem.subject_referenceid and subRef.reference_resource='Patient'
     LEFT JOIN {{ source('fhir4gold', 'patients')}} patientSubject on patientSubject.fhir_id = subRef.reference_resource_id
     LEFT JOIN {{ ref('stg_fhir4_references') }} contextRef on contextRef.referenceid = chargeitem.context_referenceid
     LEFT JOIN {{ source('fhir4gold', 'encounters')}} encounter on encounter.fhir_id = contextRef.reference_resource_id
    LEFT JOIN {{ ref('stg_fhir4_references') }} performingOrgRef on performingOrgRef.referenceid = chargeitem.performingorganization_referenceid
    LEFT JOIN {{source('fhir4gold','organizations')}}  organizationPerforming on organizationPerforming.fhir_id = performingOrgRef.reference_resource_id
    LEFT JOIN {{ ref('stg_fhir4_references') }} requestingOrgRef on requestingOrgRef.referenceid = chargeitem.requestingorganization_referenceid
    LEFT JOIN {{source('fhir4gold','organizations')}}  organizationRequesting on organizationRequesting.fhir_id = requestingOrgRef.reference_resource_id
    LEFT JOIN {{ ref('stg_fhir4_references') }} costCenterRef on costCenterRef.referenceid = chargeitem.costcenter_referenceid
    LEFT JOIN {{source('fhir4gold','organizations')}}  organizationCostCenter on organizationCostCenter.fhir_id = costCenterRef.reference_resource_id
    LEFT JOIN {{ ref('stg_fhir4_references') }} entererRef on entererRef.referenceid = chargeitem.enterer_referenceid
    LEFT JOIN {{source('fhir4gold','patients')}}  patientEnterer on patientEnterer.fhir_id = entererRef.reference_resource_id
/*    CROSS JOIN unnest(service_referenceid) as e(id)
    LEFT JOIN {{ ref('stg_fhir4_references') }} serviceRef on serviceRef.referenceid = e.id*/
--     LEFT JOIN {{source('fhir4gold','observations')}}  observationService on observationService.fhir_id = serviceRef.reference_resource_id
--     JOIN {{ ref('stg_fhir4_references') }} accountRef on accountRef.referenceid = chargeitem.account_referenceid
--     JOIN {{source('fhir4gold','accounts')}}  accounts on accounts.fhir_id = serviceRef.reference_resource_id
--     LEFT JOIN
--         {{ ref ('int_fhir4_chargeitem_notes') }} note on note.chargeitemid = chargeitem.chargeitemid and note.rn = 1

)
SELECT * FROM final