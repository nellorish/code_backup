 WITH
        {{ codeable_concept_by_system('procedure', 'procedureid', 'statusReason','http://hl7.org')}},
        {{ codeable_concept_by_system('procedure', 'procedureid', 'category','http://hl7.org')}},
        {{ codeable_concept_by_system('procedure', 'procedureid', 'code','http://hl7.org')}},
        {{ codeable_concept_by_system('procedure', 'procedureid', 'reasonCode','http://hl7.org', True)}},
        {{ codeable_concept_by_system('procedure', 'procedureid', 'followUp','http://hl7.org', True)}},
        {{ codeable_concept_by_system('procedure', 'procedureid', ' usedCode','http://hl7.org', True)}},
        {{ codeable_concept_by_system('procedure', 'procedureid', ' outcome','http://hl7.org')}},
        {{ codeable_concept_by_system('procedure', 'procedureid', ' bodySite','http://hl7.org', True) }},
        {{ codeable_concept_by_system('procedure', 'procedureid', ' complication','http://hl7.org', True) }},


    final as (
    SELECT
       procedure.procedureid as SourceIdentifier,
       code_cte.struct_array as codeConcept,
      -- function_cte.struct_array as functionConcept,
       category_cte.struct_array as categoryConcept,
       statusReason_cte.struct_array as statusReasonConcept,
       bodySite_cte.struct_array[1] as bodyTypeReasonConcept,
       reasonCode_cte.struct_array[1] as reasonCodeConcept,
       followup_cte.struct_array[1] as followupCodeConcept,
       outCome_cte.struct_array as outcomeCodeConcept,
       ageRef.value as performedAge,
       performedString,
       performedDateTime,

       ARRAY[
                    CAST(ROW( 'Encounter', encounter.encounter_id, 'EncounterId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
                    CAST(ROW( 'Patient', patientSubject.patient_id, 'PatientSubjectId') as ROW("Type" varchar, "SourceIdentifier" varchar, "TargetProperty" varchar)),
                    CAST(ROW( 'Patient', recorder.patient_id, 'RecorderId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
                    CAST(ROW( 'Patient', asserter.patient_id, 'AsserterId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar))
                 ] as ForeignKeys
    FROM
      {{source ('fhir4silver', 'procedure')}} procedure
    JOIN
      {{ ref('stg_fhir4_domainresource')}} dr ON dr.domainresourceid = procedure.domainresourceid
    LEFT JOIN code_cte on code_cte.procedureid = procedure.procedureid
   -- LEFT JOIN function_cte on function_cte.procedureid = procedure.procedureid
    LEFT JOIN category_cte on category_cte.procedureid = procedure.procedureid
    LEFT JOIN statusReason_cte on statusReason_cte.procedureid = procedure.procedureid
    LEFT JOIN bodySite_cte on bodySite_cte.procedureid = procedure.procedureid
    LEFT JOIN reasonCode_cte on reasonCode_cte.procedureid = procedure.procedureid
    LEFT JOIN followup_cte on followup_cte.procedureid = procedure.procedureid
    LEFT JOIN outCome_cte on outCome_cte.procedureid = procedure.procedureid
    LEFT JOIN {{ ref('stg_fhir4_references') }} subRef on subRef.referenceid = procedure.subject_referenceid and subRef.reference_resource='Patient'
    LEFT JOIN {{ source('fhir4gold', 'patients')}} patientSubject on substring(patientSubject.patient_id,38) = subRef.reference_resource_id
    LEFT JOIN {{ ref ('stg_fhir4_references')}} encounterRef on encounterRef.referenceId = procedure.encounter_referenceid
    LEFT JOIN {{ source('fhir4gold','encounters')}} encounter on encounter.fhir_id = encounterRef.reference_resource_id
    LEFT JOIN {{ ref ('stg_fhir4_references')}} recorderRef on recorderRef.referenceid = procedure.recorder_referenceid and recorderRef.reference_resource='Patient'
    LEFT JOIN {{ source('fhir4gold', 'patients')}} recorder on substring(recorder.patient_id,38) = recorderRef.reference_resource_id
    LEFT JOIN {{ ref ('stg_fhir4_references')}} asserterRef on asserterRef.referenceid = procedure.asserter_referenceid and asserterRef.reference_resource='Patient'
    LEFT JOIN {{ source('fhir4gold', 'patients')}} asserter on substring(asserter.patient_id,38) = asserterRef.reference_resource_id
    LEFT JOIN
      {{ ref ('int_fhir4_procedure_identifiers') }} identifiers ON procedure.procedureid = identifiers.procedureid
    LEFT JOIN {{ref('stg_fhir4_ages')}} ageref on ageref.age_id = procedure.performedage_ageid

)
SELECT * from final
