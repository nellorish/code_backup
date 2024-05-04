WITH
    {{ codeable_concept_by_system('goal', 'goalid', 'achievementStatus','http://hl7.org/fhir/ValueSet/subscriber-relationship') }},
    {{ codeable_concept_by_system('goal', 'goalid', 'category','http://hl7.org/fhir/ValueSet/subscriber-relationship',true) }},
    {{ codeable_concept_by_system('goal', 'goalid', 'priority','http://hl7.org/fhir/ValueSet/subscriber-relationship') }},
    {{ codeable_concept_by_system('goal', 'goalid', 'description','http://hl7.org/fhir/ValueSet/subscriber-relationship') }},
    {{ codeable_concept_by_system('goal', 'goalid', 'outcomeCode','http://hl7.org/fhir/ValueSet/subscriber-relationship',true) }},
    final as
(
    SELECT
            goal.goalid as SourceIdentifier,
            achievementStatus_cte.struct_array as AchievementStatusConcept,
            category_cte.struct_array[1] as CategoryConcept,
            priority_cte.struct_array as PriorityCodeConcept,
            description_cte.struct_array as DescriptionCodeConcept,
            outcomeCode_cte.struct_array[1] as OutComeCodeConcept,
            startdate as StartDate,
            statusDate as StatusDate,
            statusReason as StatusReason,
            note.note_text,
            note.note_written_at,
            note.note_author,
            ARRAY[
                 CAST(ROW( 'Organization', organizationSubject.organization_id, 'OrganizationSubjectId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar))
                ] as ForeignKeys
    FROM {{ source('fhir4silver', 'goal')}} goal
    LEFT JOIN achievementStatus_cte ON achievementStatus_cte.goalid = goal.goalid
    LEFT JOIN category_cte ON category_cte.goalid = goal.goalid
    LEFT JOIN priority_cte ON priority_cte.goalid = goal.goalid
    LEFT JOIN description_cte ON description_cte.goalid = goal.goalid
    LEFT JOIN outcomeCode_cte ON outcomeCode_cte.goalid = goal.goalid
    LEFT JOIN {{ ref('stg_fhir4_references')}} subjectRef on subjectRef.referenceid=  goal.subject_referenceid
    LEFT JOIN {{ source('fhir4gold','organizations')}} organizationSubject on organizationSubject.fhir_id = subjectRef.reference_resource_id
     LEFT JOIN
    {{ ref('int_fhir4_goal_notes') }} note on note.goalid = goal.goalid and note.rn = 1
    )
Select *
from final