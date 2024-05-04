--int_fhir4_family_member_history_condition_notes
with notes as
(
    select cond.familymemberhistoryconditionid,
           concat_ws('|||', ann.author, cast(ann.time as varchar), ann.text) as note
     from {{ source('fhir4silver', 'familymemberhistorycondition') }} cond
    LEFT JOIN unnest(cond.note_annotationid) as note(annotation_id) ON TRUE
    JOIN {{ ref('stg_fhir4_annotations') }} ann on ann.annotation_id = note.annotation_id
)
select familymemberhistoryconditionid, LISTAGG(note, ',') WITHIN GROUP (ORDER BY note) as note
from notes
group by familymemberhistoryconditionid