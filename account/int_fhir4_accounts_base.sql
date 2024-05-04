
WITH final as (
     SELECT account.accountid as account_id,
            dr.fhir_id,
            dr.version_id,
            dr.last_updated_at,
            dr.resource_summary,
            period.start_at as StartAt,
            period.end_at as EndAt,
            coverage.coverage as account_coverage,
            coverage.priority as account_priority,
            account.owner_referenceid as account_owner_reference_id,
            account.description as account_description,
            guarantor.party as account_guarantor_party,
            guarantor.patientpartyid as guarantor_patient_party_id,
            guarantor.relatedpersonpartyid as account_related_person_id,
            guarantor.organizationpartyid as account_organization_party_id,
            guarantor.period as account_guarantor_period,
            account.partof_referenceid as account_partOf_reference_id

     FROM
          {{source ('fhir4silver', 'account')}} account
     JOIN
      {{ ref('stg_fhir4_domainresource')}} dr ON dr.domainresourceid = account.domainresourceid
      LEFT JOIN type_cte on typr_cte.accountid = account.accountid
      LEFT JOIN
      {{ ref ('int_fhir4_account_identifiers') }} identifiers ON account.accountid = identifiers.accountid
      LEFT JOIN
      {{ ref ('stg_fhir4_periods') }} period ON account.serviceperiod_periodid = period.period_id
      LEFT JOIN
      {{ ref ('int_fhir4_account_coverage') }} coverage ON account.accountid  = coverage.accountid
      LEFT JOIN
      {{ ref ('int_fhir4_account_guarantor') }} guarantor ON account.accountid  = guarantor.accountid

)
select * from final