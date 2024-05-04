 WITH
     {{ codeable_concept_by_system('condition', 'conditionid', ' code','http://hl7.org') }},
     {{ codeable_concept_by_system('condition', 'conditionid', ' clinicalStatus','http://hl7.org') }},
     {{ codeable_concept_by_system('condition', 'conditionid', ' verificationStatus','http://hl7.org') }},
     {{ codeable_concept_by_system('condition', 'conditionid', ' category','http://hl7.org',True) }},
     {{ codeable_concept_by_system('condition', 'conditionid', ' severity','http://hl7.org') }},
     {{ codeable_concept_by_system('condition', 'conditionid', ' bodySite','http://hl7.org',True) }},

final as (
    SELECT
       condition.conditionid as SourceIdentifier,
       code_cte.struct_array as CodeConcept,
       bodySite_cte.struct_array[1] as BodySiteConcept,
       category_cte.struct_array[1] as CategoryConcept,
       clinicalStatus_cte.struct_array as ClinicalStatusConcept,
       severity_cte.struct_array as SeverityConcept,
       verificationStatus_cte.struct_array as VerficationStatusConcept,
       abatementdatetime as AbatementDate,
       abatementstring as AbatementDescription,
       abatementAgeRef.value as AbatementAge,
       abatementPeriod.start_at as AbatementStartDate,
       abatementPeriod.end_at as AbatementEndDate,
       onSetAgeref.value as OnSetAge,
       onSetPeriod.start_at as OnsetStartDate,
       onSetPeriod.end_at as OnsetEndDate,
       recordeddate as RecordDate,
       ARRAY[
           CAST(ROW( 'Encounter', encounter.encounter_id, 'EncounterId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
           CAST(ROW( 'Patient', patientSubject.patient_id, 'PatientSubjectId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
           CAST(ROW( 'Patient', recorder.patient_id, 'RecorderId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
           CAST(ROW( 'Patient', asserter.patient_id, 'AsserterId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar))
           ] as ForeignKeys


    FROM
     {{ source ('fhir4silver', 'condition')}} condition
     LEFT JOIN code_cte on code_cte.conditionid = condition.conditionid
     LEFT JOIN bodySite_cte on bodySite_cte.conditionid = condition.conditionid
     LEFT JOIN clinicalStatus_cte on clinicalStatus_cte.conditionid = condition.conditionid
     LEFT JOIN category_cte on category_cte.conditionid = condition.conditionid
     LEFT JOIN severity_cte on severity_cte.conditionid = condition.conditionid
     LEFT JOIN verificationStatus_cte on verificationStatus_cte.conditionid = condition.conditionid
    LEFT JOIN {{ ref('stg_fhir4_references') }} subRef on subRef.referenceid = condition.subject_referenceid and subRef.reference_resource='Patient'
    LEFT JOIN {{ source('fhir4gold', 'patients')}} patientSubject on substring(patientSubject.patient_id,38) = subRef.reference_resource_id
    LEFT JOIN {{ ref ('stg_fhir4_references')}} encounterRef on encounterRef.referenceId = condition.encounter_referenceid
    LEFT JOIN {{ source('fhir4gold','encounters')}} encounter on encounter.fhir_id = encounterRef.reference_resource_id
    LEFT JOIN {{ ref ('stg_fhir4_references')}} recorderRef on recorderRef.referenceid = condition.recorder_referenceid and recorderRef.reference_resource='Patient'
    LEFT JOIN {{ source('fhir4gold', 'patients')}} recorder on substring(recorder.patient_id,38) = recorderRef.reference_resource_id
    LEFT JOIN {{ ref ('stg_fhir4_references')}} asserterRef on asserterRef.referenceid = condition.asserter_referenceid and asserterRef.reference_resource='Patient'
    LEFT JOIN {{ source('fhir4gold', 'patients')}} asserter on substring(asserter.patient_id,38) = asserterRef.reference_resource_id
    LEFT JOIN {{ref('stg_fhir4_ages')}} onSetAgeref on onSetAgeref.age_id = condition.onsetage_ageid
    LEFT JOIN {{ref('stg_fhir4_ages')}} abatementAgeRef on abatementAgeRef.age_id = condition.abatementage_ageid
    LEFT JOIN {{ ref('stg_fhir4_periods')}} onSetPeriod on onSetPeriod.period_id = condition.onsetperiod_periodid
    LEFT JOIN {{ ref('stg_fhir4_periods')}} abatementPeriod on abatementPeriod.period_id = condition.abatementperiod_periodid
)
 SELECT * FROM final
