--int_fhir4_family_member_history_conditions
WITH familyMemberHistoryConditions as (
    select
        familymemberhistorycondition.familymemberhistoryconditionid as family_member_history_condition_id,
        familymemberhistorycondition.familymemberhistoryid as family_member_history_id,
        familymemberhistorycondition.contributedtodeath as contributed_to_death,
        snomedcode.code as condition_code,
        snomedcode.display as condition_code_display,
        snomedcode.userselected as condition_code_user_selected,
        snomedcode.system as condition_system,
        familymemberhistorycondition.outcome_codeableconceptid as outcome_code,
        notes.note as extra_info_note
    from {{ source('fhir4silver', 'familymemberhistorycondition') }} familymemberhistorycondition
    LEFT JOIN {{ref('stg_fhir4_codeableconcept')}} snomedcode on familymemberhistorycondition.code_codeableconceptid = snomedcode.codeableconceptid and snomedcode.system='http://snomed.info/sct'
    LEFT JOIN {{ref('int_fhir4_family_member_history_condition_notes') }} notes on notes.familymemberhistoryconditionid = familymemberhistorycondition.familymemberhistoryconditionid

    where code_codeableconceptid is not null

)
select * from familyMemberHistoryConditions