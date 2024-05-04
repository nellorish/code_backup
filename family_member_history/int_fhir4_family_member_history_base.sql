WITH
    final as
    (
SELECT fmh.familymemberhistoryid as family_member_history_id,
       dr.fhir_id,
       dr.version_id,
       dr.last_updated_at,
       dr.resource_summary,
       fmh."date" as history_recorded_on,
       absentreason.code as data_absent_reason_code,
       absentreason.coding_mappings as data_absent_reason_code_mappings,
       fmh.name as family_member_description,
       relation.code as relation_to_subject_code,
       relation.coding_mappings as relation_to_subject_code_mappings,
       sex.code as family_member_gender,
       born.family_member_date_of_birth
FROM {{ source('fhir4silver', 'familymemberhistory')}} fmh
JOIN {{ ref('stg_fhir4_domainresource') }} dr on fmh.domainresourceid = dr.domainresourceid
LEFT JOIN {{ ref('stg_fhir4_codeableconcept') }} absentreason on absentreason.codeableconceptid=fmh.dataabsentreason_codeableconceptid
LEFT JOIN {{ ref('stg_fhir4_codeableconcept') }} relation on relation.codeableconceptid = fmh.relationship_codeableconceptid
LEFT JOIN {{ ref('stg_fhir4_codeableconcept') }} sex on sex.codeableconceptid = fmh.sex_codeableconceptid
LEFT JOIN {{ ref('int_fhir4_family_member_history_born_value') }} born on born.familymemberhistoryid = fmh.familymemberhistoryid



    )
Select * from final