WITH
    {{ codeable_concept_by_system('medicationrequest', 'medicationrequestid', 'statusReason','http://hl7.org/fhir/ValueSet/subscriber-relationship') }},
    {{ codeable_concept_by_system('medicationrequest', 'medicationrequestid', 'category','http://hl7.org/fhir/ValueSet/subscriber-relationship',true) }},
    {{ codeable_concept_by_system('medicationrequest', 'medicationrequestid', 'performerType','http://hl7.org/fhir/ValueSet/subscriber-relationship') }},
    {{ codeable_concept_by_system('medicationrequest', 'medicationrequestid', 'reasonCode','http://hl7.org/fhir/ValueSet/subscriber-relationship',true) }},
    {{ codeable_concept_by_system('medicationrequest', 'medicationrequestid', 'courseOfTherapyType','http://hl7.org/fhir/ValueSet/subscriber-relationship') }},

    final as
(
    SELECT
            medicationrequest.medicationrequestid as SourceIdentifier,
            statusReason_cte.struct_array as StatusReasonConcept,
            category_cte.struct_array[1] as CategoryConcept,
            performerType_cte.struct_array as PerformerTypeConcept,
            reasonCode_cte.struct_array[1] as ReasonCodeConcept,
            courseOfTherapyType_cte.struct_array as CourseTypeConcept,
            doNotPerform as DoNotPerform,
            authoredOn as AuthoredOn,
            reportedBoolean as Reported,
            ARRAY[
                 CAST(ROW( 'Organization', organizationReported.organization_id, 'ReportedOrganizationId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
                 CAST(ROW( 'Organization', organizationRequester.organization_id, 'RequesterOrganizationId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
                  CAST(ROW( 'Organization', organizationPerformer.organization_id, 'PerformerOrganizationId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar))
                ] as ForeignKeys
    FROM {{ source('fhir4silver', 'medicationrequest')}} medicationrequest
    LEFT JOIN statusReason_cte ON statusReason_cte.medicationrequestid = medicationrequest.medicationrequestid
    LEFT JOIN category_cte ON category_cte.medicationrequestid = medicationrequest.medicationrequestid
    LEFT JOIN performerType_cte ON performerType_cte.medicationrequestid = medicationrequest.medicationrequestid
    LEFT JOIN reasonCode_cte ON reasonCode_cte.medicationrequestid = medicationrequest.medicationrequestid
    LEFT JOIN courseOfTherapyType_cte ON courseOfTherapyType_cte.medicationrequestid = medicationrequest.medicationrequestid
    LEFT JOIN {{ ref('stg_fhir4_references')}} organizationReportedRef on organizationReportedRef.referenceid=  medicationrequest.reportedreference_referenceid
    LEFT JOIN {{ source('fhir4gold','organizations')}} organizationReported on organizationReported.fhir_id = organizationReportedRef.reference_resource_id
    LEFT JOIN {{ ref('stg_fhir4_references')}} organizationRequesterRef on organizationRequesterRef.referenceid=  medicationrequest.requester_referenceid
    LEFT JOIN {{ source('fhir4gold','organizations')}} organizationRequester on organizationRequester.fhir_id = organizationRequesterRef.reference_resource_id
    LEFT JOIN {{ ref('stg_fhir4_references')}} organizationPerformerRef on organizationPerformerRef.referenceid=  medicationrequest.performer_referenceid
    LEFT JOIN {{ source('fhir4gold','organizations')}} organizationPerformer on organizationPerformer.fhir_id = organizationPerformerRef.reference_resource_id
    )
Select *
from final