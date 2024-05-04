WITH chargeItemNotes AS (
    SELECT chargeitem.chargeitemid,
            note.text as note_text,
            note.time as note_written_at,
            note.author as note_author,
            {{ window_row_number('rn', 'chargeitemid', 'note.time')}}
    FROM {{ source('fhir4silver', 'chargeitem') }} chargeitem
    CROSS JOIN unnest(note_annotationid) as n(id)
    JOIN {{ ref('stg_fhir4_annotations') }} note on note.annotation_id = n.id
)
SELECT *
FROM chargeItemNotes