WITH source as (SELECT *
                FROM {{ source('fhir4silver', 'familymemberhistory') }}),
     final as (SELECT
                   source.familymemberhistoryid,
                   coalesce(cast(source.borndate as timestamp), cast(source.bornstring as timestamp), period.start_at) as family_member_date_of_birth
               FROM source
               LEFT JOIN {{ ref('stg_fhir4_periods')}} period on source.bornperiod_periodid = period.period_id

               )

select *
from final