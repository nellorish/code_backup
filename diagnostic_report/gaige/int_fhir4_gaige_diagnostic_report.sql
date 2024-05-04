WITH
    {{ codeable_concept_by_system('diagnosticreport', 'diagnosticreportid', 'code','http://hl7.org/fhir/ValueSet/subscriber-relationship') }},
    {{ codeable_concept_by_system('diagnosticreport', 'diagnosticreportid', 'category','http://hl7.org/fhir/ValueSet/subscriber-relationship',true) }},
    {{ codeable_concept_by_system('diagnosticreport', 'diagnosticreportid', 'conclusionCode','http://hl7.org/fhir/ValueSet/subscriber-relationship',true) }},
    final as
(
    SELECT
            diagnosticreport.diagnosticreportid as SourceIdentifier,
            code_cte.struct_array as CodeConcept,
            category_cte.struct_array[1] as CategoryConcept,
            conclusionCode_cte.struct_array as ConclusionCodeConcept,
            issued as Issued,
            period.start_at as EffectiveStartDate,
            period.end_at as EfffectiveEndDate,
            conclusion as Conclusion,
            ARRAY[
                 CAST(ROW( 'Organization', organizationSubject.organization_id, 'OrganizationSubjectId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
                 CAST(ROW( 'Organization', organizationPerformer.organization_id, 'OrganizationPerformerId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
                  CAST(ROW( 'Organization', organizationResultInterpter.organization_id, 'ResultInterpreterOrganizationId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar))
                ] as ForeignKeys
    FROM {{ source('fhir4silver', 'diagnosticreport')}} diagnosticreport
    LEFT JOIN code_cte ON code_cte.diagnosticreportid = diagnosticreport.diagnosticreportid
    LEFT JOIN category_cte ON category_cte.diagnosticreportid = diagnosticreport.diagnosticreportid
    LEFT JOIN conclusionCode_cte ON conclusionCode_cte.diagnosticreportid = diagnosticreport.diagnosticreportid
    LEFT JOIN {{ ref('stg_fhir4_references')}} subjectRef on subjectRef.referenceid=  diagnosticreport.subject_referenceid
    LEFT JOIN {{ source('fhir4gold','organizations')}} organizationSubject on organizationSubject.fhir_id = subjectRef.reference_resource_id
    CROSS JOIN unnest(performer_referenceid) as e(id)
    LEFT JOIN {{ ref('stg_fhir4_references')}} organizationPerformerRef on organizationPerformerRef.referenceid= e.id
    LEFT JOIN {{ source('fhir4gold','organizations')}} organizationPerformer on organizationPerformer.fhir_id = organizationPerformerRef.reference_resource_id
     CROSS JOIN unnest(resultsinterpreter_referenceid) as i(id)
    LEFT JOIN {{ ref('stg_fhir4_references')}} organizationResultInterpeterRef on organizationResultInterpeterRef.referenceid= i.id
    LEFT JOIN {{ source('fhir4gold','organizations')}} organizationResultInterpter on organizationResultInterpter.fhir_id = organizationResultInterpeterRef.reference_resource_id
      LEFT JOIN {{ ref('stg_fhir4_periods')}} period on period.period_id = diagnosticreport.effectiveperiod_periodid
    )
Select *
from final