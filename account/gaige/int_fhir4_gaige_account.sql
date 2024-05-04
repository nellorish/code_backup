
WITH
       {{ codeable_concept_by_system('account', 'accountid', 'type','http://hl7.org/fhir/ValueSet/')}},
final as (
     SELECT mmxpatientnumber as PatientNumber,
            cyclenumber as CycleNumber,
            transactiondate
            ARRAY[
                    CAST(ROW( 'Patient', patientSubject.patient_id, 'PatientSubjectId') as ROW("Type" varchar, "SourceIdentifier" varchar, "TargetProperty" varchar)),
                    CAST(ROW( 'Organization', Organization.organization_id, 'OrganizationOwnerId') as ROW("Type" varchar, "SourceIdentifier" varchar, "TargetProperty" varchar)),
                    CAST(ROW( 'Account', accountRef.reference_resource_id, 'AccountReferenceID') as ROW("Type" varchar, "SourceIdentifier" varchar, "TargetProperty" varchar)),
                 ] as ForeignKeys
     FROM {{source ('fhir4silver', 'account')}} account

     CROSS JOIN unnest(subject_referenceid) as e(id)
     LEFT JOIN {{ ref('stg_fhir4_references') }} subRef on subRef.referenceid = e.id and subRef.reference_resource='Patient'
     LEFT JOIN {{ source('fhir4gold', 'patients')}} patientSubject on substring(patientSubject.patient_id,38) = subRef.reference_resource_id
     LEFT JOIN {{ ref('stg_fhir4_references')}} organizationRef on organizationRef.referenceid=  account.owner_referenceid
     LEFT JOIN {{ source('fhir4gold','organizations')}} organization on organization.fhir_id = organizationRef.reference_resource_id
     LEFT JOIN {{ ref('stg_fhir4_references')}} accountRef on accountRef.referenceid=  account.partof_referenceid and accountRef.reference_resource= 'Account'
     LEFT JOIN
           type_cte on type_cte.accountid = account.accountid
--      LEFT JOIN
--       {{ ref ('int_fhir4_account_identifiers') }} identifiers ON account.accountid = identifiers.accountid
    LEFT JOIN
      {{ ref ('stg_fhir4_periods') }} period ON account.serviceperiod_periodid = period.period_id
    LEFT JOIN
      {{ ref ('int_fhir4_account_coverage') }} coverage ON account.accountid  = coverage.accountid
--       LEFT JOIN
--           {{ ref('stg_fhir4_references')}} beneficiaryRef on beneficiaryRef.referenceid=  account.owner_referenceid

)
select * from final