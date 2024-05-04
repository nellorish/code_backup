WITH deviceTelecoms as (
        SELECT device.deviceid,
                contactpoint.contactpoint_id,
                contactpoint.system,
                contactpoint.value,
                contactpoint.use,
                contactpoint.rank,
                contactpoint.id,
                contactpoint.period_start_at,
                contactpoint.period_end_at,
                contactpoint.period_id

        FROM {{ source('fhir4silver','device') }} device
        LEFT JOIN unnest(contact_contactpointid) as tcp(contactpoint_id) on true
        JOIN {{ ref('stg_fhir4_contactpoints')}} contactpoint on tcp.contactpoint_id = contactpoint.contactpoint_id
        where contactpoint.value is not null
)
SELECT deviceid, value,
       arbitrary(use) as user,
       arbitrary(system) as system,
       arbitrary(rank) as rank,
       arbitrary(id) as id,
       arbitrary(period_start_at) as period_start_at,
       arbitrary(period_end_at) as period_end_at
FROM deviceTelecoms
group by deviceid, value