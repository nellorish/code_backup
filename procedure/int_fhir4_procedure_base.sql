--int_fihr_procedure_base.sql
    WITH
        {{ codeable_concept_by_system('procedure', 'procedureid', 'statusReason','http://hl7.org') }},
        {{ codeable_concept_by_system('procedure', 'procedureid', 'category','http://hl7.org') }},
        {{ codeable_concept_by_system('procedure', 'procedureid', 'code','http://hl7.org') }},
        {{ codeable_concept_by_system('procedure', 'procedureid', 'reasonCode','http://hl7.org', True)}},
        {{ codeable_concept_by_system('procedure', 'procedureid', 'followUp','http://hl7.org', True)}},
        {{ codeable_concept_by_system('procedure', 'procedureid', ' usedCode','http://hl7.org', True)}},
        {{ codeable_concept_by_system('procedure', 'procedureid', ' outcome','http://hl7.org') }},
        {{ codeable_concept_by_system('procedure', 'procedureid', ' bodySite','http://hl7.org', True) }},
        {{ codeable_concept_by_system('procedure', 'procedureid', ' complication','http://hl7.org', True) }},

    final as (
    SELECT
       dr.fhir_id,
       dr.version_id,
       dr.last_updated_at,
       dr.resource_summary,
       procedure.procedureid as procedure_id,
       procedure.instantiatescanonical as procedure_instantiate_canonical,
       procedure.instantiatesuri as procedure_instantiates_uri,
       procedure.performeddatetime as procedure_performed_datatime,
       procedure.performedstring as procedure_performed_string,
       procedure.patientsubjectid as procedure_patient_subject_id,
       procedure.groupsubjectid as procedure_group_subject_id,
       procedure.encounterencounterid as procedure_encounterid,
       procedure.practitionerrecorderid as procedure_practitioner_recorderid,
       procedure.practitionerrolerecorderid as procedure_practitioner_role_recorderid,
       procedure.patientasserterid as procedure_patient_asserter_id,
       procedure.relatedpersonasserterid as related_person_asserter_id,
       procedure.practitionerasserterid as procedure_practitioner_asserterid,
       procedure.locationlocationid as procedure_location_id,
       code_cte.code as procedure_code,
       code_cte.code_mappings[code_cte.code]['description'] as procedure_code_description,
       code_cte.code_system as procedure_type_code_system,
       code_cte.code_mappings as procedure_type_code_mappings,
       category_cte.code as category_type_code,
       category_cte.code_system as category_type_code_system,
       category_cte.code_mappings as category_type_code_mappings,
       bodySite_cte.code as bodySite_type_code,
       bodySite_cte.code_system as bodySite_type_code_system,
       bodySite_cte.code_mappings as bodySite_type_code_mappings,
       statusReason_cte.code as status_reason_code,
       reasonCode_cte.code as procedure_reason_code,
       followup_cte.code as procedure_followup_code,
      -- useCode_cte.code as procedure_use_code,
       outCome_cte.code as procedure_outcome_code,
       subjectRef.reference_resource as subject_reference_resource,
       subjectRef.reference_resource_id as subject_reference_id,
       encounterRef.reference_resource as encounter_reference_resource,
       encounterRef.reference_resource_id as encounter_reference_id,
       recorderRef.reference_resource as recorder_reference_resource,
       recorderRef.referenceid as recorder_reference_id,
       asserterRef.reference_resource as asserter_reference_resource,
       asserterRef.reference_resource_id as asserter_reference_id,
       locationRef.reference_resource as location_reference_resource,
       locationRef.reference_resource_id as location_reference_id,
       performer.procedureperformerid as procedure_performer_id,
       performer.practitioneractorid as procedure_practitioner_actor_id,
       performer.practitionerroleactorid as practitioner_role_actor_id,
       performer.organizationactorid AS organization_actor_id,
       performer.patientactorid AS patient_actor_id,
       performer.relatedpersonactorid AS related_person_actor_id,
       performer.deviceactorid AS device_actor_id,
       performer.organizationonbehalfofid AS organization_on_behalf_of_id,
       performer.function_codeableconceptid AS function_codeableconcept_id,
       performer.actor_referenceid AS actor_reference_id,
       performer.onbehalfof_referenceid AS onbehalf_of_referenceid,
       complicationDetails.procedurecomplicationdetailid AS procedure_complication_details_id,
       complicationDetails.condition_complicationdetailid AS procedure_condition_complication_detail_id,
       focalDevice.procedurefocaldeviceid AS procedure_focal_device_id,
       focalDevice.action AS procedure_focal_device_action,
       focalDevice.devicemanipulatedid AS procedure_focal_device_manipulated_id,
       focalDevice.manipulated AS procedure_focal_device_manipulated
    FROM
      {{source ('fhir4silver', 'procedure')}} procedure
    JOIN
      {{ ref('stg_fhir4_domainresource')}} dr ON dr.domainresourceid = procedure.domainresourceid
    LEFT JOIN
      code_cte ON code_cte.procedureid = procedure.procedureid
    LEFT JOIN
       statusReason_cte ON statusReason_cte.procedureid = procedure.procedureid
    LEFT JOIN
       reasonCode_cte ON reasonCode_cte.procedureid = procedure.procedureid
    LEFT JOIN
        followUp_cte ON followUp_cte.procedureid = procedure.procedureid
    LEFT JOIN
        usedCode_cte ON usedCode_cte.procedureid = procedure.procedureid
    LEFT JOIN
        outcome_cte ON outcome_cte.procedureid = procedure.procedureid
    LEFT JOIN
        bodySite_cte ON bodySite_cte.procedureid = procedure.procedureid
    LEFT JOIN
      category_cte ON category_cte.procedureid = procedure.procedureid
    LEFT JOIN
      {{ ref ('int_fhir4_procedure_identifiers') }} identifiers ON procedure.procedureid = identifiers.procedureid
    LEFT JOIN
        {{ ref ('int_fhir4_procedure_notes') }} note on note.procedureid = procedure.procedureid and note.rn = 1
    LEFT JOIN
        {{ ref ('stg_fhir4_references')}} subjectRef on subjectRef.referenceid= procedure.subject_referenceid
    LEFT JOIN
        {{ ref ('stg_fhir4_references')}} encounterRef on encounterRef.referenceId = procedure.encounter_referenceid
    LEFT JOIN
        {{ ref ('stg_fhir4_references')}} recorderRef on recorderRef.referenceId = procedure.recorder_referenceid
    LEFT JOIN
        {{ ref ('stg_fhir4_references')}} asserterRef on asserterRef.referenceId = procedure.asserter_referenceid
     LEFT JOIN
        {{ ref ('stg_fhir4_references')}} locationRef on locationRef.referenceId = procedure.location_referenceid
    LEFT JOIN
        {{ ref ('int_fhir4_procedure_partOf')}} partOfRef on partOfRef.procedureid = procedure.procedureid
    LEFT JOIN
        {{ ref ('int_fhir4_procedure_report')}} report on report.procedureid = procedure.procedureid
    LEFT JOIN
        {{ref('int_fhir4_procedure_performer')}} performer on performer.procedureid = procedure.procedureid
    LEFT JOIN
        {{ref('int_fhir4_procedure_complicationdetail')}} complicationDetails on complicationDetails.procedureid = procedure.procedureid
    LEFT JOIN
        {{ref('int_fhir4_procedure_focaldevice')}} focalDevice on focalDevice.procedureid = procedure.procedureid
)
SELECT * from final