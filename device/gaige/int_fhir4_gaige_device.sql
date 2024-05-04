WITH
    {{ codeable_concept_by_system('device', 'deviceid', 'type','http://hl7.org/fhir/ValueSet/subscriber-relationship') }},
    {{ codeable_concept_by_system('device', 'deviceid', 'statusReason','http://hl7.org/fhir/ValueSet/subscriber-relationship',true) }},
    {{ codeable_concept_by_system('device', 'deviceid', 'safety','http://hl7.org/fhir/ValueSet/subscriber-relationship',true) }},
    final as
(
    SELECT
            device.deviceid as SourceIdentifier,
            type_cte.struct_array as TypeConcept,
            statusReason_cte.struct_array[1] as StatusReasonConcept,
            safety_cte.struct_array[1] as SafetyConcept,
            distinctidentifier as DistinctIdentifier,
            manufacturer as DeviceManufacturer,
            manufacturedate as ManufactureDate,
            expirationdate as ExpirationDate,
            lotnumber as LotNumber,
            serialnumber as SerialNumber,
            modelnumber as ModelNumber,
            partnumber as PartNumber,
            url as URL,
            issuer as UdiCarrierIssuer,
            jurisdiction as DeviceJurisdiction,
            carrierhrf as DeviceCarrierHrf,
            deviceidentifier as DeviceIdentifier,

            ARRAY[
                 CAST(ROW( 'Organizations', organizationOwner.organization_id, 'OrganizationDeviceOwnerId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar)),
                 CAST(ROW( 'gaige_location', deviceLocation.sourceidentifier, 'DeviceLocationId') as ROW("Type" varchar, "SourceIdentifier" varchar, "ForeignKey" varchar))
                ] as ForeignKeys
    FROM {{ source('fhir4silver', 'device')}} device
    LEFT JOIN type_cte ON type_cte.deviceid = device.deviceid
    LEFT JOIN statusReason_cte ON statusReason_cte.deviceid = device.deviceid
    LEFT JOIN safety_cte ON safety_cte.deviceid = device.deviceid
    LEFT JOIN {{ ref('stg_fhir4_references')}} ownerRef on ownerRef.referenceid=  device.owner_referenceid
    LEFT JOIN {{ source('fhir4gold','organizations')}} organizationOwner on organizationOwner.fhir_id = ownerRef.reference_resource_id
    LEFT JOIN {{ ref('stg_fhir4_references')}} locationRef on locationRef.referenceid= location_referenceid
    LEFT JOIN {{ source('fhir4gold','gaige_location')}} deviceLocation on deviceLocation.sourceidentifier = locationRef.reference_resource_id
    LEFT JOIN {{ source('fhir4silver','deviceudicarrier')}} udiCarrier on udiCarrier.deviceid = device.deviceid
    LEFT JOIN {{ ref('int_fhir4_device_telecoms') }} telecomFax on telecomFax.deviceid = device.deviceid and telecomFax.system = 'fax'
    LEFT JOIN {{ ref('int_fhir4_device_telecoms') }} telecomEmail on telecomEmail.deviceid = device.deviceid and telecomEmail.system = 'email'
    )
Select *
from final